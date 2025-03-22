from airflow import DAG
from airflow.operators.python import PythonOperator  # Updated import path
from datetime import datetime
import requests
import pandas as pd
from google.cloud import storage
import json

# --------------------------
# Data Ingestion Logic (previously in ingest_carpark_info.py)
# --------------------------
def fetch_and_upload_carpark_info():
    # Fetch data from API
    dataset_id = "d_23f946fa557947f93a8043bbef41dd09"
    url = f"https://data.gov.sg/api/action/datastore_search?resource_id={dataset_id}"
    response = requests.get(url)
    data = response.json()['result']['records']
    df = pd.DataFrame(data)

    # Upload to GCS
    BUCKET_NAME = "de-zoomcamp-project-453801-terra-bucket"
    KEY_PATH = "config/my-creds.json"  # Ensure this path is correct

    csv_data = df.to_csv(index=False)
    destination_blob_name = "carpark_info/CarparkInformation.csv"

    # GCS Upload Function
    storage_client = storage.Client.from_service_account_json(KEY_PATH)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(csv_data, content_type="text/csv")
    print(f"Data uploaded to GCS: {destination_blob_name}")

# --------------------------
# Airflow DAG Definition
# --------------------------
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'ingest_carpark_info_dag',
    default_args=default_args,
    description='One-time DAG to ingest static carpark info',
    schedule_interval='@monthly',  # Run once manually
    catchup=False,
)

ingest_task = PythonOperator(
    task_id='ingest_carpark_info',
    python_callable=fetch_and_upload_carpark_info,  # Directly call the function
    dag=dag,
)

ingest_task


