import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import types
from pyspark.sql.functions import col, expr, to_timestamp, date_format, trunc, lit, avg 
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.functions import to_date
import os 

# start a spark session 
# spark = SparkSession.builder \
#     .appName("Data_Transformation") \
#     .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
#     .config("spark.jars", "gcs-connector-hadoop3-latest.jar,spark-bigquery-latest_2.12.jar") \
#     .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
#     .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/chenchen/.gc/google-credential.json") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("Data_Transformation") \
    .getOrCreate()

gcs_bucket = os.getenv("GOOGLE_BUCKET_NAME")
bq_project = os.getenv("GOOGLE_PROJECT_ID")
bq_dataset = os.getenv("GOOGLE_BQ_DATASET")

# carpark master information 
carpark_info = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"gs://{gcs_bucket}/carpark_info/CarparkInformation.csv") 


# daily detail 
carpark_details = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"gs://{gcs_bucket}/carpark_data/*.csv") 


# convert time format 
carpark_details = carpark_details.withColumn("update_datetime", to_timestamp(col("update_datetime")))\
                                 .withColumn("timestamp", to_timestamp(col("timestamp")))


# Join carpark_info with carpark_details to form complete data 
carpark_df = carpark_details.join(carpark_info, carpark_details.carpark_number == carpark_info.car_park_no, "left")

# Compute utilization rate
carpark_df = carpark_df.withColumn(
    "utilization_rate",
    expr("(info_total_lots - info_lots_available) / info_total_lots * 100")
)

carpark_df = carpark_df.withColumn("year_month", date_format(trunc(col("timestamp"), "MM"), "yyyy-MM-01"))\
                               .withColumn("day_of_week", date_format(col("timestamp"), "EEEE"))


# Compute average utilization rate per region and time period
utilization_by_region = carpark_df.groupBy("car_park_type","carpark_number", "year_month", "day_of_week")\
                                      .agg(avg("utilization_rate").alias("avg_utilization"))


# Convert year_month to DATE type
utilization_by_region = utilization_by_region.withColumn("year_month", to_date(col("year_month"), "yyyy-MM-dd"))


# Upload transformed files into bigquery 
utilization_by_region.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .option("table", f"{bq_project}.{bq_dataset}.utilization_by_region") \
    .option("partitionField", "year_month") \
    .mode("overwrite") \
    .save()

carpark_details.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .option("table", f"{bq_project}.{bq_dataset}.carpark_details") \
    .option("partitionField", "timestamp") \
    .mode("overwrite") \
    .save()

carpark_df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .option("table", f"{bq_project}.{bq_dataset}.carpark_data") \
    .option("partitionField", "timestamp") \
    .mode("overwrite") \
    .save()





