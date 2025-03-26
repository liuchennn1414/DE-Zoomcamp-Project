# DE-Zoomcamp-Project

## Problem Description: Singapore HDB Carpark Utilization Analysis 
The effective utilization of Housing & Developemtn Board (HDB) car parks is crucial for efficient urban mobility and resource management in Singapore. However, varying utilization rates across different car park types, fluctuations in weekly usage trends, and inconsistent slot availability present challenges in optimizing space allocation in this concise island. This project aims to analyze daily, weekly and monthly HDB car park utilization patterns, identify peak and underutilized periods, potentially provide data-driven recommendations to improve parking efficiency, reduce congestion, and enhance user experience.

## Data Source
The project use open data source provided by Singapore government in data.gov.sg. For simplicity, the project use 2 main data source: 
1. **Carpark Information**. This is a static csv updated monthly containing master information about carpark data, such as carpark location, type and other properties. 
 ```
Housing & Development Board. (2015). HDB Carpark Information (2025) [Dataset]. data.gov.sg. Retrieved March 25, 2025 from https://data.gov.sg/datasets/d_23f946fa557947f93a8043bbef41dd09/view
```
2. **Carpark Availability**. This is an open RestAPI which provide real time carpark availability information for every minutes. The API only takes in 1 parameter, which is the exact timestamp in YYYY-MM-DD[T]HH:mm:ss (SGT). For this project, we are doing batch processing, retriving data at 9am SGT per day from 2023 onwards. 
```
Housing & Development Board. (2018). Carpark Availability (2023) [Dataset]. data.gov.sg. Retrieved March 25, 2025 from https://data.gov.sg/datasets/d_ca933a644e55d34fe21f28b8052fac63/view
```

## Data Architecture
![Alt text](assets/Architecture.jpg)
- **Data Ingestion**: 
    - **Carpark Information**: Updated **monthly**, orchestrated using **Airflow**.  
    - **Carpark Availability**: Updated **daily**, also managed by **Airflow**.  
    - **Full Refresh Job**: A backup strategy, you can trigger this manually in Airflow UI for a **bulk backfill** of historical data recovery in case of failures.  
    
- **Cloud and IaC**:
    - **Google Cloud Storage (GCS)**: Used as the primary data storage.  
    - **Terraform**: Manages GCP resources efficiently.  

- **Data Warehouse**: 
    - **Google BigQuery**: Stored processed data.  
    - **Partitioning & Clustering**: Optimized using **date-based partitions** for better query performance.  This step is done as part of spark transformation. 
    -- need to justify why used partition, else change accordingly 

- **Spark Data Transformation**: 
    - **Apache Spark**: Used for **batch processing** and data transformation.  
    - need to describe the transformation 
    - **Airflow Orchestration**: Ensures transformations run **daily** after data ingestion.  

- **Airflow**: 
    - to give an description for how I use airflow 

## Dashboard 
- You can access the dashbaord with this link: [Carpark Availability Dashboard](https://lookerstudio.google.com/s/tTT2DmQAzzU)
- or view the dashboard here: 
![Alt text](assets/dashboard.jpg)


## Reproducability 
- **Pre-requisit**: 
    1. You need to have a GCP account and a GCP Project, and save your service account credentials in ~/.gc/my-creds.json 
    2. git clone this repo 
- run the makefile with this commans: 
    - The makefile will automatically help you to create VM, download the necessary package in VM, create bucket and bigquery dataset, set up airflow, perform data ingestion (full refresh + activate incremental load everyday), perform transformation and partition and load your transformed data into bigquery.

## Next Step of improvement 
The pipeline is still very naive. Here are some next step: 
1. Use dataproc on GCP for spark job other than local spark job 
2. Re-examine the efficiency of using spark for data transformation since the data size could be big and also could be small. I am using spark because... I want to be familiarize with how to use it ;) 
3. Optimize the airflow pipeline, from setting up & downstream to incremental load, to more efficienct and cleaner repo structure. 
4. Potential expansion in 2 direction: 
    a. Look into HDB rent / selling price
    b. Look into expanding this carpark app into a real time api project since the api is available for real time access 
5. Use other API, which could potentially require authentication and use of tools like dlt