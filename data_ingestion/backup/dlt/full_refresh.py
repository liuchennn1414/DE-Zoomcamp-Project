import dlt
import requests
import pandas as pd
import json
import argparse
import os
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.destinations import filesystem

# Define the API endpoint and your access key
API_URL = 'https://api.marketstack.com/v1/eod'
GCP_URL = "gs://de-zoomcamp-project-453801-terra-bucket"

ACCESS_KEY = os.environ.get("API_ACCESS_KEY")
if not ACCESS_KEY:
    raise ValueError("API_ACCESS_KEY environment variable is not set.")


# Extract raw data, auto normalization and Load data into GCP Bucket
@dlt.resource(name="stock", write_disposition="replace")
def extract_raw_full_refresh(date_from, date_to):
    client = RESTClient(
        base_url=API_URL,
        paginator=OffsetPaginator(  # Handles pagination using offset
            limit=100, offset=0, total_path=None
        )
    )

    for page in client.paginate(
        params={
            'access_key': ACCESS_KEY,
            'symbols': 'AAPL,MSFT',  # Example symbols
            'date_from': date_from,
            'date_to': date_to
        }
    ):
        yield page

def main(date_from, date_to):
    # Load credentials from JSON file
    with open("/home/chenchen/.gc/my-creds.json") as f:
        credentials = json.load(f)

    # Initialize filesystem client
    gcp_bucket = filesystem(GCP_URL, credentials=credentials)

    # Define the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="marketstack_pipeline",
        destination=gcp_bucket,  # Set the destination to GCS
        dataset_name='stock_dataset'
    )

    # Run the pipeline with the new resource
    load_info = pipeline.run(
        extract_raw_full_refresh(date_from, date_to),  # Pass date_from and date_to to the resource
        write_disposition="replace",
        loader_file_format="csv"
    )
    print(load_info)

        
if __name__ == '__main__':
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description='Ingest CSV compressed data to GCP')
    parser.add_argument('--date_from', required=True, help='Start date for the query (YYYY-MM-DD)')
    parser.add_argument('--date_to', required=True, help='End date for the query (YYYY-MM-DD)')
    args = parser.parse_args()

    # Call the main function with the parsed arguments
    main(args.date_from, args.date_to)