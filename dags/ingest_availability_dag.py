from airflow import DAG
from airflow.operators.python import PythonOperator  # Updated import path
from airflow.models import Variable
from datetime import datetime, timedelta
import asyncio
import aiohttp
import pandas as pd
from google.cloud import storage
import json

# --------------------------
# Data Ingestion Logic (previously in ingest_availability.py)
# --------------------------
API_URL = "https://api.data.gov.sg/v1/transport/carpark-availability"
BUCKET_NAME = "de-zoomcamp-project-453801-terra-bucket"
KEY_PATH = "config/my-creds.json"  # Ensure this path is correct

async def fetch_carpark_availability(session, date_time):
    """Fetch carpark availability data for a specific date and time."""
    async with session.get(API_URL, params={"date_time": date_time}) as response:
        response.raise_for_status()
        return await response.json()

def generate_date_range(start_date, end_date, time_of_day="09:00:00"):
    """Generate a list of date-time strings between start_date and end_date."""
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
    blob.upload_from_string(data, content_type="text/csv")
    print(f"Data uploaded to GCS: {destination_blob_name}")

async def ingest_availability(start_date, end_date, time_of_day="09:00:00"):
    """Main function to fetch and process carpark availability data."""
    date_range = generate_date_range(start_date, end_date, time_of_day)
    all_data = pd.DataFrame()

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_carpark_availability(session, date_time) for date_time in date_range]
        results = await asyncio.gather(*tasks)

        for result, date_time in zip(results, date_range):
            if result:
                try:
                    normalized_data = pd.json_normalize(
                        result['items'][0]['carpark_data'],
                        'carpark_info',
                        ['carpark_number', 'update_datetime'],
                        record_prefix='info_'
                    )
                    normalized_data['timestamp'] = result['items'][0]['timestamp']
                    all_data = pd.concat([all_data, normalized_data], ignore_index=True)
                    print(f"Fetched data for {date_time}")
                except Exception as e:
                    print(f"Error processing data for {date_time}: {e}")

    # Upload to GCS
    try:
        csv_data = all_data.to_csv(index=False)
        destination_blob_name = f"carpark_data/{end_date}.csv"
        upload_to_gcs(BUCKET_NAME, destination_blob_name, csv_data)
    except Exception as e:
        print(f"Error uploading data to GCS: {e}")

# --------------------------
# Airflow DAG Definition
# --------------------------
default_args = {
    'owner': 'airflow',
    'start_date': datetime.today(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_ingest_availability(**kwargs):
    """Wrapper function to run the async ingestion logic."""
    execution_date = kwargs['execution_date']
    start_date = execution_date.strftime('%Y-%m-%d')
    end_date = start_date  # Same as start_date for daily refresh

    # Check if backfill dates are provided via Airflow Variables
    backfill_start_date = Variable.get("backfill_start_date", default_var=None)
    backfill_end_date = Variable.get("backfill_end_date", default_var=None)

    # Override dates if backfill is requested
    if backfill_start_date and backfill_end_date:
        start_date = backfill_start_date
        end_date = backfill_end_date
        print(f"Running backfill from {start_date} to {end_date}")
    else:
        print(f"Running daily refresh for {start_date}")

    # Run the async ingestion logic
    asyncio.run(ingest_availability(start_date=start_date, end_date=end_date))

dag = DAG(
    'ingest_availability_dag',
    default_args=default_args,
    description='A DAG to ingest carpark availability data',
    schedule_interval='0 3 * * *',  # Run daily
    catchup=True,  # Enable backfill
)

ingest_task = PythonOperator(
    task_id='ingest_availability',
    python_callable=run_ingest_availability,
    provide_context=True,
    dag=dag,
)

ingest_task