"""
bronze_delta_ingestion_incremental.py

Incremental ingestion of raw CSVs from S3 (MinIO) to Delta Bronze layer on S3.
- Supports append-only ingestion (only new rows since last load)
- Keeps checkpoints for fault tolerance
- Logs progress for monitoring (Grafana-friendly)
Tables:
 - transactions
 - customers
 - accounts
 - merchants
"""

from pyspark.sql import SparkSession
from delta import *
from delta.tables import DeltaTable
import os
import logging

# -----------------------------
# HADOOP AWS / AWS SDK JARs
# -----------------------------
HADOOP_AWS_JAR = "/mnt/c/Users/Hello/hadoop-aws-3.3.6.jar"
AWS_SDK_JAR = "/mnt/c/Users/Hello/aws-java-sdk-bundle-1.12.498.jar"

# Add JARs to PySpark
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {HADOOP_AWS_JAR},{AWS_SDK_JAR} pyspark-shell"

# -----------------------------
# Logging configuration
# -----------------------------
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# -----------------------------
# CONFIG - change these values
# -----------------------------
MINIO_ENDPOINT = "http://localhost:9002"
AWS_ACCESS_KEY = "admin"
AWS_SECRET_KEY = "password123"
BUCKET = "bank-project"
S3_ENDPOINT = MINIO_ENDPOINT

# Raw CSV paths in S3
RAW_PATHS = {
    "transactions": f"s3a://{BUCKET}/raw/transactions/transactions.csv",
    "customers": f"s3a://{BUCKET}/raw/customers/customers.csv",
    "accounts": f"s3a://{BUCKET}/raw/accounts/accounts.csv",
    "merchants": f"s3a://{BUCKET}/raw/merchants/merchants.csv"
}

# Delta output paths in S3 Bronze layer
DELTA_PATHS = {
    "transactions": f"s3a://{BUCKET}/bronze/transactions_delta",
    "customers": f"s3a://{BUCKET}/bronze/customers_delta",
    "accounts": f"s3a://{BUCKET}/bronze/accounts_delta",
    "merchants": f"s3a://{BUCKET}/bronze/merchants_delta"
}

# Checkpoint paths in S3 Bronze layer
CHECKPOINT_PATHS = {
    "transactions": f"s3a://{BUCKET}/bronze/_checkpoints/transactions_delta",
    "customers": f"s3a://{BUCKET}/bronze/_checkpoints/customers_delta",
    "accounts": f"s3a://{BUCKET}/bronze/_checkpoints/accounts_delta",
    "merchants": f"s3a://{BUCKET}/bronze/_checkpoints/merchants_delta"
}

# Timestamp columns for incremental ingestion
TS_COLUMNS = {
    "transactions": "timestamp",
    "customers": "created_at",
    "accounts": "opened_date",
    "merchants": None  # merchants table can be append-only
}

# -----------------------------
# Initialize Spark with Delta (Fixed for WSL/Hostname and S3A Parsing Issues)
# -----------------------------
builder = SparkSession.builder \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.local.ip", "127.0.0.1") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
    .config("spark.hadoop.fs.s3a.retry.interval", "500") \
    .config("spark.hadoop.fs.s3a.retry.throttle.interval", "100") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# -----------------------------
# Function to perform incremental ingestion
# -----------------------------
def ingest_to_delta(table_name):
    raw_path = RAW_PATHS[table_name]
    delta_path = DELTA_PATHS[table_name]
    checkpoint_path = CHECKPOINT_PATHS[table_name]
    ts_col = TS_COLUMNS.get(table_name)

    try:
        logging.info(f"📥 Reading raw table: {table_name} from {raw_path}")
        df = spark.read.option("header", True).option("inferSchema", True).csv(raw_path)

        new_rows_count = df.count()
        logging.info(f"Raw table {table_name} has {new_rows_count} rows")

        if ts_col and DeltaTable.isDeltaTable(spark, delta_path):
            delta_table = DeltaTable.forPath(spark, delta_path)
            last_ts = delta_table.toDF().agg({ts_col: "max"}).collect()[0][0]
            if last_ts:
                logging.info(f"Last ingested timestamp for {table_name}: {last_ts}")
                df = df.filter(df[ts_col] > last_ts)
                filtered_count = df.count()
                logging.info(f"Filtered new rows for incremental load: {filtered_count} rows (from {new_rows_count})")
                if filtered_count == 0:
                    logging.info(f"⚠️ No new rows to ingest for {table_name}")
                    return
            else:
                logging.warning(f"Max timestamp in Delta is null—full load for {table_name}")
        else:
            logging.info(f"No timestamp column or first ingestion: full load for {table_name}")

        logging.info(f"💾 Writing Delta table: {table_name} to {delta_path} (append mode)")
        df.write.format("delta") \
            .mode("overwrite") \
            .option("checkpointLocation", checkpoint_path) \
            .save(delta_path)
        logging.info(f"✅ {table_name} ingested successfully! Added {df.count()} rows")
    except Exception as e:
        logging.error(f"❌ Failed to ingest {table_name}: {str(e)}")
        raise

# -----------------------------
# Ingest all tables
# -----------------------------
for table in RAW_PATHS.keys():
    ingest_to_delta(table)

# -----------------------------
# Validate one table
# -----------------------------
logging.info("Displaying 5 rows from transactions Delta table:")
spark.read.format("delta").load(DELTA_PATHS["transactions"]).show(5, truncate=False)

spark.stop()