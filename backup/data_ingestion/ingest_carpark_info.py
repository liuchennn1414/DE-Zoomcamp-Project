import requests
import json
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import storage
import pandas as pd 
          
dataset_id = "d_23f946fa557947f93a8043bbef41dd09"
url = "https://data.gov.sg/api/action/datastore_search?resource_id="  + dataset_id
        
response = requests.get(url)
data = response.json()['result']['records']
df = pd.DataFrame(data)


BUCKET_NAME = "de-zoomcamp-project-453801-terra-bucket"
KEY_PATH = "/home/chenchen/.gc/my-creds.json"

def upload_to_gcs(bucket_name, destination_blob_name, data):
    """Upload JSON data to Google Cloud Storage."""
    storage_client = storage.Client.from_service_account_json(KEY_PATH)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    # Upload JSON string
    blob.upload_from_string(data, content_type="text/csv")
    print(f"Data successfully uploaded to GCS: {destination_blob_name}")

def main(): 
    try: 
        csv_data = df.to_csv(index=False)
        GCS_FILE_PATH = f"carpark_info/CarparkInformation.csv"  # File named as the execution date

        # Upload CSV to GCS
        upload_to_gcs(BUCKET_NAME, GCS_FILE_PATH, csv_data)

    except Exception as e:
        print(f"Error processing data: {e}")

if __name__ == "__main__":
    main()


