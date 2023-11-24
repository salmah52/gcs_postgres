import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from web.operators.WebToGCSHK import WebToGCSHKOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
ENDPOINT = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
SERVICE = "green"
OBJECT = SERVICE + '_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
FILE_NAME = SERVICE + '_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
PATH_TO_SAVED_FILE = f"{AIRFLOW_HOME}/{FILE_NAME}"
TABLE_NAME_TEMPLATE = SERVICE + '_taxi_data'

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "email": [os.getenv("ALERT_EMAIL", "")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# Define a Python function for creating the PostgreSQL table
def create_green_taxi_table():
    from airflow.hooks.postgres_hook import PostgresHook

    postgres_conn_id = 'pg_conn'  # Replace with your PostgreSQL connection ID
    table_name = 'nyctabl009'  # Replace with your PostgreSQL table name

    # Assuming that you have a CSV file, read its first row to get column names and types
    df = pd.read_csv(PATH_TO_SAVED_FILE, nrows=1)
    column_definitions = ", ".join([f"{col} {df[col].dtype}" for col in df.columns])

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {column_definitions}
        )
    """
    pg_hook.run(create_table_sql)

with DAG(
    dag_id="LoadGreenTaxiWeb-To-GCS-To-Postgres4",
    description="Job to move data from website to Google Cloud Storage and then from GCS to Postgres Database",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 2 * *",
    max_active_runs=1,
    catchup=True,
    tags=["Website-to-GCS-Bucket-to-PG"]
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    download_to_gcs = WebToGCSHKOperator(
        task_id="extract_from_web_to_gcs",
        endpoint=ENDPOINT,
        destination_path=OBJECT,
        destination_bucket=BUCKET,
        service=SERVICE
    )
    
    download_file = GCSToLocalFilesystemOperator(
        task_id="download_file_from_gcs",
        object_name=SERVICE+"/"+FILE_NAME,
        bucket=BUCKET,
        filename=PATH_TO_SAVED_FILE
    )
    
    create_table = PythonOperator(
        task_id="create_green_taxi_table",
        python_callable=create_green_taxi_table,
        provide_context=True,
    )
   
    load_data = PostgresOperator(
        task_id="load_data_to_PD_DB",
        postgres_conn_id='pg_conn',
        sql=f"COPY {TABLE_NAME_TEMPLATE} FROM '{PATH_TO_SAVED_FILE}' WITH CSV HEADER;",
    )
    
    end = EmptyOperator(task_id="end")
   
    start >> download_to_gcs >> download_file >> create_table >> load_data >> end
