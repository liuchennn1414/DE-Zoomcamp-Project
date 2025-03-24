import aiohttp
import pandas as pd
import json
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import storage
from datetime import datetime, timedelta
import asyncio
import argparse

# Constants
API_URL = "https://api.data.gov.sg/v1/transport/carpark-availability"
BUCKET_NAME = "de-zoomcamp-project-453801-terra-bucket"
KEY_PATH = "/home/chenchen/.gc/my-creds.json"

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
async def main(start_date, end_date, time_of_day="09:00:00"):
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
                    # Convert DataFrame to CSV string
    try: 
        csv_data = all_data.to_csv(index=False)
        GCS_FILE_PATH = f"carpark_data/{start_date}-{end_date}.csv"  # File named as the execution date

        # Upload CSV to GCS
        upload_to_gcs(BUCKET_NAME, GCS_FILE_PATH, csv_data)

    except Exception as e:
        print(f"Error processing data: {e}")

# Allow manual execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and save carpark availability data to CSV.")
    parser.add_argument("--start_date", type=str, required=True, help="Start date in YYYY-MM-DD format (e.g., 2024-01-01).")
    parser.add_argument("--end_date", type=str, required=True, help="End date in YYYY-MM-DD format (e.g., 2025-03-21).")
    parser.add_argument("--time_of_day", type=str, default="09:00:00", help="Time of day in HH:MM:SS format (default: 09:00:00).")
    
    args = parser.parse_args()

    # Run the asynchronous main function
    asyncio.run(main(start_date=args.start_date, end_date=args.end_date, time_of_day=args.time_of_day))
