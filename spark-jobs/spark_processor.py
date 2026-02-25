import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType
import os

bucket, key = sys.argv[1], sys.argv[2]

spark = SparkSession.builder \
    .appName("SparkProcessor") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.hive_local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hive_local.type", "hive") \
    .config("spark.sql.catalog.hive_local.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.hive_local.warehouse", "s3a://lakehouse-warehouse/warehouse") \
    .config("spark.sql.catalog.hive_local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.hive_local.s3.endpoint", os.environ.get("LOCALSTACK_HOST")) \
    .config("spark.sql.catalog.hive_local.s3.region", os.environ.get("AWS_REGION")) \
    .config("spark.sql.catalog.hive_local.s3.path-style-access", "true") \
    .config("spark.sql.catalog.hive_local.client.region", os.environ.get("AWS_REGION")) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("LOCALSTACK_HOST")) \
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.region", os.environ.get("AWS_REGION")) \
    .config("spark.hadoop.fs.s3a.change.detection.mode", "none") \
    .config("spark.hadoop.hive.metastore.warehouse.dir", "s3a://lakehouse-warehouse/warehouse") \
    .config("spark.hadoop.hive.metastore.pre.event.listeners", "") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. Create the database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS hive_local.lakehouse_events")


print(f"DEBUG: LOCALSTACK_HOST is {os.environ.get('LOCALSTACK_HOST')}")
print(f"DEBUG: AWS_ACCESS_KEY_ID is {os.environ.get('AWS_ACCESS_KEY_ID')}")
# 2. Create the table with Partitioning
# We'll partition by 'days' on the event_time column for better query speed
spark.sql("""
    CREATE TABLE IF NOT EXISTS hive_local.lakehouse_events.enriched_events (
        event_id STRING,
        user_id STRING,
        event_time TIMESTAMP,
        topic STRING,
        action STRING,
        ip_address STRING,
        device_os STRING,
        device_browser STRING,
        page_url STRING,
        page_referrer STRING,
        country STRING,
        city STRING,
        lat DOUBLE,
        lon DOUBLE
    )
    USING iceberg
    PARTITIONED BY (days(event_time))
    TBLPROPERTIES ('format-version'='2')
""")

path = f"s3a://{bucket}/{key}"

print(f"Reading data from: {path}")

event_df = spark.read.parquet(path)

geo_path = f"s3a://{bucket}/data/ip_geolocation.parquet"

geo_df = spark.read.parquet(geo_path)

df = event_df.join(geo_df, on="ip_address", how="left") \
    .select(    
        event_df.event_id,
        event_df.user_id,
        event_df.event_time,
        event_df.topic,
        event_df.action,
        event_df.ip_address,
        event_df.device.os.alias("device_os"),
        event_df.device.browser.alias("device_browser"),
        event_df.page.url.alias("page_url"),
        event_df.page.referrer.alias("page_referrer"),
        geo_df.country,
        geo_df.city,
        geo_df.lat,
        geo_df.lon
    )


df.writeTo("hive_local.lakehouse_events.enriched_events").append()

print("✅ Data processing complete and written to Iceberg table. Total records appended: " + str(df.count()))

