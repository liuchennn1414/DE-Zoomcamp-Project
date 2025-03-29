import dlt
import argparse
import os
from datetime import datetime, timedelta
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
import requests
import pandas as pd
import json
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.destinations import filesystem

# Define the API endpoint
API_URL = 'https://api.marketstack.com/v1/eod'
GCP_URL = "gs://de-zoomcamp-project-453801-terra-bucket"

# Retrieve the API access key from environment variables
ACCESS_KEY = os.environ.get("API_ACCESS_KEY")
if not ACCESS_KEY:
    raise ValueError("API_ACCESS_KEY environment variable is not set.")

def main(initial_value):
    @dlt.resource(name="stock", write_disposition="append")
    def extract_raw_incremental(
        cursor_date=dlt.sources.incremental(
            "date",  # Field to track (timestamp in your data)
            initial_value=initial_value,  # Use the user-provided initial value
        )
    ):
        client = RESTClient(
            base_url=API_URL,
            paginator=OffsetPaginator(limit=100, offset=0, total_path=None)
        )

        # Set date_to to one day before today
        date_to = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        params = {
            'access_key': ACCESS_KEY,
            'symbols': 'AAPL,MSFT',  # Example symbols
            'date_from': cursor_date.last_value,  # Use the last fetched timestamp
            'date_to': date_to  # Fetch up to one day before today
        }

        for page in client.paginate(params=params):
            yield page

    # Load credentials from JSON file
    with open("/home/chenchen/.gc/my-creds.json") as f:
        credentials = json.load(f)

    # Initialize filesystem client
    # file_date = datetime.now().strftime("%Y-%m-%d")
    # custom_filename = f"stock_data_{file_date}.csv.gz"  

    gcp_bucket = filesystem(GCP_URL, credentials=credentials)

    # Define the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="marketstack_pipeline",
        destination=gcp_bucket,  # Set the destination to GCS
        dataset_name='stock_dataset_prod'
    )

    # Run the pipeline with the new resource
    load_info = pipeline.run(
            extract_raw_incremental, 
            loader_file_format="csv" # data is compressed? 
    )
    print(load_info)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run the Marketstack pipeline.')
    parser.add_argument('--initial_value', required=True, help='Initial date for incremental load (YYYY-MM-DD)')
    args = parser.parse_args()

    main(args.initial_value)