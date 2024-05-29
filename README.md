# API_Polling

Here we poll an API(arbitrary name #asknicely) to incrementally load data to a warehouse system like Bigquery to generate reports.  

We use DAG construct in Airflow to orchestrate the stages and the language used is Python.

To prepare meaningful reports we use SQL queries to be run upon Bigquery and the results are saved as respective tables to power dashboards.
