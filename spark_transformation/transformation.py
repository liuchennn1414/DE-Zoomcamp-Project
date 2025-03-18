import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import types
from pyspark.sql import functions as F
from pyspark.sql.types import DateType


# Start a spark session 
spark = SparkSession.builder \
    .appName("Data_Transformation") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.jars", "gcs-connector-hadoop3-latest.jar,spark-bigquery-latest_2.12.jar") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/chenchen/.gc/my-creds.json") \
    .getOrCreate()


# Read the raw data 
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("compression", "gzip") \
    .option("encoding", "us-ascii") \
    .csv("gs://de-zoomcamp-project-453801-terra-bucket/stock_dataset/stock/1742135256.3842237.3b60b69d81.csv.gz") 


# Monthly Transformation 
df = df.withColumn("date", F.to_date(df["date"], "yyyy-MM-dd"))
df = df.withColumn("year_month", F.date_format("date", "yyyy-MM"))
monthly_df = df.groupBy("symbol", "year_month") \
    .agg(
        F.avg("open").alias("avg_open"),
        F.max("high").alias("max_high"),
        F.min("low").alias("min_low"),
        F.avg("close").alias("avg_close"),
        F.max("volume").alias("max_volume"),
        F.avg("adj_high").alias("avg_adj_high"),
        F.min("adj_low").alias("min_adj_low"),
        F.avg("adj_close").alias("avg_adj_close"),
        F.avg("adj_open").alias("avg_adj_open"),
        F.avg("adj_volume").alias("avg_adj_volume"),
        F.avg("split_factor").alias("avg_split_factor"),
        F.avg("dividend").alias("avg_dividend")
    )

#Weekly Transformation: 
df = df.withColumn("year", F.year(df["date"]))
df = df.withColumn("week", F.weekofyear(df["date"]))
df = df.withColumn("year_week", F.concat_ws("-", df["year"], df["week"]))
weekly_df = df.groupBy("symbol", "year_week") \
    .agg(
        F.avg("open").alias("avg_open"),
        F.max("high").alias("max_high"),
        F.min("low").alias("min_low"),
        F.avg("close").alias("avg_close"),
        F.max("volume").alias("max_volume"),
        F.avg("adj_high").alias("avg_adj_high"),
        F.min("adj_low").alias("min_adj_low"),
        F.avg("adj_close").alias("avg_adj_close"),
        F.avg("adj_open").alias("avg_adj_open"),
        F.avg("adj_volume").alias("avg_adj_volume"),
        F.avg("split_factor").alias("avg_split_factor"),
        F.avg("dividend").alias("avg_dividend")
    )


# Save the outpt to bigQuery 
#  #.option("clusteredFields", "symbol, year_month") \
monthly_df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", "de-zoomcamp-project-453801-terra-bucket") \
    .option("table", "de-zoomcamp-project-453801.demo_dataset.monthly_stock_data") \
    .option("partitionField", "symbol") \
    .mode("overwrite") \
    .save()


weekly_df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", "de-zoomcamp-project-453801-terra-bucket") \
    .option("table", "de-zoomcamp-project-453801.demo_dataset.weekly_stock_data") \
    .option("partitionField", "symbol") \
    .mode("overwrite") \
    .save()


df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", "de-zoomcamp-project-453801-terra-bucket") \
    .option("table", "de-zoomcamp-project-453801.demo_dataset.daily_stock_data") \
    .option("partitionField", "symbol") \
    .mode("overwrite") \
    .save()





