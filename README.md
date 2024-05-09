# API_Polling

Here we poll an API(arbitrary name #asknicely) to incrementally load data to warehouse system like Bigquery to generate reports.  

We use DAG construct in Airflow to orchestrate the stages and the launguage used is python.

To prepare meaningful reports we use SQL queries to be ran upon Bigquery and the results are saved as respective tables to power dashboards.