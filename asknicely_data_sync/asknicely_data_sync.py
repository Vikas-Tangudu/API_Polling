import os
from datetime import timedelta 
import airflow
import json
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import Variable
from asknicely_data_sync.dependencies.asknicely_data_load import run

REGION_ID           = os.environ.get('GCP_REGION_ID','')
PROJECT_ID          = os.environ.get('GCP_PROJECT_ID', 'data-warehouse-242619') 
DATASET             = Variable.get('asknicely_dataset','asknicely_test')  
ENV                 = os.environ.get('ENV', 'DEV1')
DAG_NAME            =  'asknicely_data_sync'
GCP_BUCKET_NAME     = os.environ.get('GCP_BUCKET_NAME', 'us-central1-snt-composer-po-91fdee65-bucket')
GCP_CONNECTION_ID   = 'google_cloud_default'
SLACK_CONNECTION    = os.environ.get('SLACK_CONNECTION', 'slack_connection')
temp_folder_path    = f'{GCP_BUCKET_NAME}/asknicely_data_sync'
reports             = ['response_snapshot']

def task_failure_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONNECTION).password
    
    task=context.get('task_instance').task_id
    dag=context.get('task_instance').dag_id
    exec_date=context.get('execution_date')
    log_url=context.get('task_instance').log_url

    failed_alert = SlackWebhookOperator(
        task_id='slack-connection',
        http_conn_id=SLACK_CONNECTION,
        webhook_token=slack_webhook_token,
        message=f"""
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """,
        channel='#cron-dev1')
    return failed_alert.execute(context=context)


def task_failure_slack_alert_v2(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONNECTION).password
    slack_webhook_token = Variable.get("ASKNICELY_SLACK_WEBHOOK", "{Paste your webhook here}")
    
    task=context.get('task_instance').task_id
    dag=context.get('task_instance').dag_id
    exec_date=context.get('execution_date')
    log_url=context.get('task_instance').log_url

    failed_alert = SlackWebhookOperator(
        task_id='slack-connection',
        http_conn_id=SLACK_CONNECTION,
        webhook_token=slack_webhook_token,
        message=f"""
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """,
        channel='#cron-dev1')
    return failed_alert.execute(context=context)

with DAG(
    DAG_NAME,
    default_args={
        'owner': 'Cloudwerx',
        'depends_on_past': False,
        'email': [''],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
    description='',
    schedule_interval= None,
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['asknicely-data-sync'],
    template_searchpath=['/home/airflow/gcs/dags/'],
    on_failure_callback=task_failure_slack_alert,
    dagrun_timeout=timedelta(minutes=20),
) as dag:

    start_task = DummyOperator(task_id='start_task', dag=dag)
    end_task = DummyOperator(task_id="end_task", dag=dag)

    delete_csv_files = GoogleCloudStorageDeleteOperator(
        task_id="delete_csv_files",
        bucket_name = GCP_BUCKET_NAME,
        prefix = temp_folder_path,
        gcp_conn_id=GCP_CONNECTION_ID,
    )

with open(f'/home/airflow/gcs/dags/{DAG_NAME}/dependencies/url-mapping.json') as f:
    data = json.load(f)
    for record in data:
        api = record["ApiName"] 
        table_name = record["TableName"]
        asknicely_data_sync = PythonOperator(
            task_id=f'{api}_data_sync_in_gcs',
            python_callable=run,
            op_kwargs = {"api" : api, "temp_folder_path" : temp_folder_path, "details": record, "project_id":PROJECT_ID, "dataset":DATASET },
            provide_context=True,
            dag=dag,
        )
        load_task = GoogleCloudStorageToBigQueryOperator(
            task_id=f'{api}_data_bq_load',
            bucket=GCP_BUCKET_NAME,
            source_objects=[f'asknicely_data_sync/asknicely_{api}_data_*.csv'],
            destination_project_dataset_table=f'{PROJECT_ID}.{DATASET}.{table_name}',
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            schema_object= f'dags/asknicely_data_sync/dependencies/{table_name}.json',
            source_format='CSV',
            skip_leading_rows = 1,
            autodetect = False,
            allow_quoted_newlines = True,
            gcp_conn_id=GCP_CONNECTION_ID,
            dag=dag
        )
        view_creation=BigQueryExecuteQueryOperator(
            task_id = f"{table_name}_views",
            sql = f"/{DAG_NAME}/sql_queries/{api}_view.sql",
            gcp_conn_id=GCP_CONNECTION_ID,
            use_legacy_sql=False,
            params={
                "project_id":f"{PROJECT_ID}", "dataset":f"{DATASET}"
            }
        )
        start_task >>  asknicely_data_sync >> load_task >> view_creation >> delete_csv_files


for report in reports:
        transform_task=BigQueryExecuteQueryOperator(
            task_id = f"{report}_sql_transform",
            sql = f"/{DAG_NAME}/sql_queries/{report}.sql",
            gcp_conn_id=GCP_CONNECTION_ID,
            use_legacy_sql=False,
            params={
                "project_id":f"{PROJECT_ID}", "dataset":f"{DATASET}"
            }
        )
        delete_csv_files >> transform_task >> end_task
