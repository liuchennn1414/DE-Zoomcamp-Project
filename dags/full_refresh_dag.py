import aiohttp
import pandas as pd
import json
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import storage
from datetime import datetime, timedelta
import asyncio
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os 

# Constants
API_URL = "https://api.data.gov.sg/v1/transport/carpark-availability"
BUCKET_NAME = os.getenv("GOOGLE_BUCKET_NAME") # must be your own bucket name 
KEY_PATH = "config/google-credential.json"  

# Function to fetch carpark availability data
async def fetch_carpark_availability(session, date_time):
    async with session.get(API_URL, params={"date_time": date_time}) as response:
        response.raise_for_status()
        return await response.json()

# Function to generate date range
def generate_date_range(start_date, end_date, time_of_day="09:00:00"):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_list = []
    while start <= end:
        date_time = start.strftime(f"%Y-%m-%dT{time_of_day}")
        date_list.append(date_time)
        start += timedelta(days=1)
    return date_list

def upload_to_gcs(bucket_name, destination_blob_name, data):
    """Upload JSON data to Google Cloud Storage."""
    storage_client = storage.Client.from_service_account_json(KEY_PATH)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    # Upload JSON string
    blob.upload_from_string(data, content_type="text/csv")
    print(f"Data successfully uploaded to GCS: {destination_blob_name}")

# Asynchronous main function
async def full_refresh(start_date, end_date, time_of_day="09:00:00"):
    # Generate date range
    date_range = generate_date_range(start_date, end_date, time_of_day)
    
    # Initialize an empty DataFrame to aggregate all data
    all_data = pd.DataFrame()

    # Create an aiohttp session
    async with aiohttp.ClientSession() as session:
        # Create a list of tasks to fetch data concurrently
        tasks = [fetch_carpark_availability(session, date_time) for date_time in date_range]
        
        # Wait for all tasks to complete and collect the results
        results = await asyncio.gather(*tasks)

        for result, date_time in zip(results, date_range):
            # Process only successful responses
            if result:
                try:
                    # Normalize JSON data
                    normalized_data = pd.json_normalize(
                        result['items'][0]['carpark_data'],  # Focus on carpark_data
                        'carpark_info',  # Flatten the carpark_info list
                        ['carpark_number', 'update_datetime'],  # Include these attributes from the parent
                        record_prefix='info_'  # Add a prefix to carpark_info fields
                    )

                    # Add the timestamp from the parent level
                    normalized_data['timestamp'] = result['items'][0]['timestamp']

                    # Append normalized data to the overall DataFrame
                    all_data = pd.concat([all_data, normalized_data], ignore_index=True)

                    print(f"Fetched data for {date_time}")

                except Exception as e:
                    print(f"Error processing data for {date_time}: {e}")

    # Convert DataFrame to CSV string and upload to GCS
    try: 
        csv_data = all_data.to_csv(index=False)
        destination_blob_name = f"carpark_data/{start_date}-{end_date}.csv"
        upload_to_gcs(BUCKET_NAME, destination_blob_name, csv_data)
    except Exception as e:
        print(f"Error uploading data to GCS: {e}")

# --------------------------
# Airflow DAG Definition
# --------------------------
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_ingest_full_refresh(**kwargs):
    """Wrapper function to run the async ingestion logic."""
    start_date = Variable.get("backfill_start_date", default_var='2024-01-01')
    end_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')  # Yesterday

    print(f"Running full refresh from {start_date} to {end_date}")

    # Run the async ingestion logic
    asyncio.run(full_refresh(start_date=start_date, end_date=end_date))

dag = DAG(
    'full_refresh_dag',
    default_args=default_args,
    description='A DAG to ingest past carpark availability data',
    schedule_interval=None,  # Manual trigger only
    catchup=False,  # No catchup needed for manual DAGs
    tags=['manual', 'backfill'],
)

ingest_task = PythonOperator(
    task_id='full_refresh',
    python_callable=run_ingest_full_refresh,
    provide_context=True,
    dag=dag,
)

ingest_task