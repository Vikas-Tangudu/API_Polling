import os
import sys
import requests
import pandas as pd
from time import time
from google.cloud import storage
from airflow.models import Variable
from google.cloud import bigquery

cwd = os.getcwd()
sys.path.insert(0,cwd)
client = storage.Client()
bq_client = bigquery.Client()

def fetch_results_from_bq(query):
    print('Fetching results from BigQuery')
    if query == None or query == '' :
        raise Exception('Query cannot be empty') 
    if bq_client :
        try :
            query_job = bq_client.query(query)
            if query_job.result().total_rows > 0 :
                return True, query_job    
        except Exception as e :
            print('Error while fetching data from bigquery', str(e))
    else :
        print('Not connected to bigquery', str(e))
    return False, None

def get_start_time(project_id, dataset):
    query = f'Select IFNULL(max(cast(lastemailed as INTEGER)),0) from {project_id}.{dataset}._raw_snapshot_responses'
    status, results = fetch_results_from_bq(query)
    if status:
            for row in results:
                print(f"The start time is --> {row[0]}.")
                return row[0]

def create_scv_file(df, file_name, temp_folder_path):
    folder_path = temp_folder_path.split('/')[1]
    gcs_bucket_name = temp_folder_path.split('/')[0]
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    gcs_folder_path = f'{folder_path}/{file_name}'
    csv_data = df.to_csv(encoding='utf-8', index = False)
    bucket = client.get_bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_folder_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f" ########    {blob.path} is created   #########")

def process_encodings(value):
    if isinstance(value, str):
        return str(value.encode('unicode_escape'))[2:-1]

def get_reponses(asknicely_url,asknicely_key,details,temp_folder_path, since_time):
    api = details['ApiName']
    addition = details['Addition']
    schema = details['Schema']
    params = details["Params"]
    params['X-apikey'] =  asknicely_key
    filters = details["Filters"]
    if filters['sort_direction'] is not None: sort_direction = filters['sort_direction']
    if filters['pagesize'] is not None: pagesize = filters['pagesize']
    if filters['pagenumber'] is not None: pagenumber = filters['pagenumber']
    if filters['response_format'] is not None: response_format = filters['response_format']
    if filters['filter_type'] is not None: filter_type = filters['filter_type']
    if filters['sort_by'] is not None: sort_by = filters['sort_by']
    if filters['end_time'] is not None: end_time = int(time())

    while (True):
        url = asknicely_url + f'{addition}/{sort_direction}/{pagesize}/{pagenumber}/{since_time}/{response_format}/{filter_type}/{sort_by}/{end_time}'
        print(f'API CALL --> {url}')
        response = requests.get(url,params)
        response_json = response.json()
        response_data = response_json['data']
        response_df = pd.json_normalize(response_data)
        response_df = response_df.drop_duplicates('response_id',keep = 'last')
        response_df = response_df.applymap(process_encodings) # Handling newline and other unexpected encodings in the comments section
        response_df['_sync_unix_timestamp'] = int(time())
        if set(schema.keys()).issubset(response_df.columns):
            response_df = response_df[schema.keys()].astype(schema) # Selecting Req columns and casting
        else :
            raise ValueError(f'{api} --> Some columns in schema are not present in the API.')
        create_scv_file(response_df, f'asknicely_{api}_data_{pagenumber}.csv', temp_folder_path)
        if response_json['pagenumber'] == response_json['totalpages']:
            break
        pagenumber+=1
    return True

def get_historical_stats(asknicely_url, asknicely_key,details,temp_folder_path ): 
    api = details['ApiName']
    addition = details['Addition']
    schema = details['Schema']
    params = details["Params"]
    params['X-apikey'] =  asknicely_key
    filters = details["Filters"]

    url = asknicely_url + addition
    response = requests.get(url,params)
    response_json = response.json()
    response_data = response_json['data']
    response_df = pd.json_normalize(response_data)
    response_df['_sync_unix_timestamp'] = int(time())
    if set(schema.keys()).issubset(response_df.columns):
            response_df = response_df[schema.keys()].astype(schema) # Selecting Req columns and casting
    else :
        raise ValueError(f'{api} --> Some columns in schema are not present in the API.')
    create_scv_file(response_df, f'asknicely_{api}_data_0.csv', temp_folder_path)
    return True

def run(**context):
    api = context['api']
    temp_folder_path = context['temp_folder_path']
    details = context["details"]
    project_id = context["project_id"]
    dataset = context["dataset"]
    asknicely_url = Variable.get('asknicely_url', 'Default_Url')
    asknicely_key = Variable.get('asknicely_key', 'Default_Key')
    
    since_time = get_start_time(project_id, dataset)
    if api == 'responses':
        get_reponses(asknicely_url, asknicely_key, details, temp_folder_path, since_time)
    elif api == 'stats':
        get_historical_stats(asknicely_url, asknicely_key,details,temp_folder_path )