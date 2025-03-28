from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.python import PythonOperator  # Updated import path
from airflow.models import Variable
from datetime import datetime, timedelta
import asyncio
import aiohttp
import pandas as pd
from google.cloud import storage
import json

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime.today(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'carpark_transformation_dag',
    default_args=default_args,
    description='DAG to process carpark data and load to BigQuery',
    schedule_interval='10 3 * * *',  # Runs daily
    catchup=False,
    max_active_runs=1,
    tags=['spark', 'bigquery']
)

# BashOperator to run Spark job
spark_job = BashOperator(
    task_id='run_spark_job',
    bash_command='spark-submit \
        --jars $SPARK_HOME/jars/gcs-connector-hadoop3-latest.jar,$SPARK_HOME/jars/spark-bigquery-latest_2.12.jar \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
        --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/airflow/config/google-credential.json \
        /opt/airflow/spark_transformation/transformation.py',
    dag=dag
)

spark_job