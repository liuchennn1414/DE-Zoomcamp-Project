# DE-Zoomcamp-Project

## Problem Description: Singapore HDB Carpark Utilization Analysis 


## Data Architecture 
-- Data Ingestion: Orchestrated using airflow 
-- Cloud and IaC: Used GCP and Terraform 
-- Data Warehouse with partition / clustering by date 
-- Transformation: used spark for batch processing, and it is also orchestrated in airflow 

## Dashboard 
-- You can access the dashbaord with this link: 
-- or view the dashboard here: 

## Reproducability 
-- Pre-requisit: You need to have a GCP account and a GCP Project, and save your service account credentials in ~/.gc/my-creds.json 
-- git clone this repo 
-- run the makefile with this commans: 
    -- The makefile will automatically help you to create VM, download the necessary package in VM, create bucket and bigquery dataset, set up airflow, perform data ingestion (full refresh + activate incremental load everyday), perform transformation and partition and load your transformed data into bigquery.

## Next Step of improvement 
The pipeline is still very naive. Here are some next step: 
1. Use dataproc on GCP for spark job other than local spark job 
2. Re-examine the efficiency of using spark for data transformation since the data size could be big and also could be small. I am using spark because... I want to be familiarize with how to use it ;) 
3. Optimize the airflow pipeline, from setting up & downstream to incremental load, to more efficienct and cleaner repo structure. 
4. Potential expansion in 2 direction: 
    a. Look into HDB rent / selling price
    b. Look into expanding this carpark app into a real time api project since the api is available for real time access 
5. Use other API, which could potentially require authentication and use of tools like dlt

## Implementation Journal 
1. Set up gitrepo and Project on GCP 
2. Set up VM on GCP with docker, anaconda, terraform downloaded. Cloned gitrepo into VM  
3. Set up Terraform which can be used to create google cloud bucket and bigquery dataset 
4. dlt data pipeline (full refresh & delta load both implemented)
5. 