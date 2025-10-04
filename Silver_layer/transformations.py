"""
silver_delta_ingestion_incremental.py

Transforms Bronze Delta tables to Silver layer in S3 (MinIO) incrementally.
- Supports append-only incremental ingestion
- Adds watermarking for late-arriving data (transactions)
- Maintains checkpoints
- Logs progress for monitoring (Grafana-friendly)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import *
from delta.tables import DeltaTable
import os
import logging

# -----------------------------
# HADOOP AWS / AWS SDK JARs
# -----------------------------
HADOOP_AWS_JAR = "/mnt/c/Users/Hello/hadoop-aws-3.3.6.jar"
AWS_SDK_JAR = "/mnt/c/Users/Hello/aws-java-sdk-bundle-1.12.498.jar"
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {HADOOP_AWS_JAR},{AWS_SDK_JAR} pyspark-shell"

# -----------------------------
# Logging configuration
# -----------------------------
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# ============================
# CONFIG
# ============================
S3_ENDPOINT = "http://localhost:9002"
AWS_ACCESS_KEY = "admin"
AWS_SECRET_KEY = "password123"
BUCKET = "bank-project"

# Bronze input paths
BRONZE_PATHS = {
    "customers": f"s3a://{BUCKET}/bronze/customers_delta",
    "merchants": f"s3a://{BUCKET}/bronze/merchants_delta",
    "accounts": f"s3a://{BUCKET}/bronze/accounts_delta",
    "transactions": f"s3a://{BUCKET}/bronze/transactions_delta"
}

# Silver output paths
SILVER_PATHS = {
    "customers": f"s3a://{BUCKET}/silver/customers_silver",
    "merchants": f"s3a://{BUCKET}/silver/merchants_silver",
    "accounts": f"s3a://{BUCKET}/silver/accounts_silver",
    "transactions": f"s3a://{BUCKET}/silver/transactions_silver"
}

# Checkpoint paths
CHECKPOINT_PATHS = {
    "customers": f"s3a://{BUCKET}/silver/_checkpoints/customers_silver",
    "merchants": f"s3a://{BUCKET}/silver/_checkpoints/merchants_silver",
    "accounts": f"s3a://{BUCKET}/silver/_checkpoints/accounts_silver",
    "transactions": f"s3a://{BUCKET}/silver/_checkpoints/transactions_silver"
}

# Timestamp columns for incremental ingestion
TS_COLUMNS = {
    "customers": "created_at",
    "merchants": None,
    "accounts": "opened_date",
    "transactions": "timestamp"
}

# -----------------------------
# Initialize Spark with Delta
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
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# -----------------------------
# Transformation Functions
# -----------------------------
def transform_customers(df):
    return df \
        .withColumn("dob", to_date(col("dob"))) \
        .withColumn("created_at", to_timestamp(col("created_at"))) \
        .withColumn("age", round(datediff(current_date(), col("dob")) / 365.25, 0)) \
        .filter(col("age") >= 18) \
        .filter(col("email").rlike("@")) \
        .withColumn("name_clean", regexp_replace(initcap(col("name")), "[^a-zA-Z\\s]", "")) \
        .withColumn("gender", when(col("gender") == "O", "Other").otherwise(col("gender"))) \
        .dropDuplicates(["customer_id"]) \
        .select("customer_id", "name_clean", "dob", "gender", "address", "phone", "email", "created_at", "age")

def transform_merchants(df):
    return df \
        .withColumn("category", initcap(col("category"))) \
        .withColumn("city", initcap(col("city"))) \
        .withColumn("country", upper(col("country"))) \
        .filter(col("merchant_name").isNotNull()) \
        .dropDuplicates(["merchant_id"]) \
        .select("merchant_id", "merchant_name", "category", "city", "country")

def transform_accounts(df):
    return df \
        .withColumn("opened_date", to_date(col("opened_date"))) \
        .withColumn("balance", col("balance").cast("decimal(12,2)")) \
        .withColumn("status", when(col("status") == "closed", "inactive").otherwise(col("status"))) \
        .filter(col("opened_date") < current_date()) \
        .filter(col("balance").isNotNull()) \
        .dropDuplicates(["account_id"]) \
        .select("account_id", "customer_id", "account_type", "balance", "opened_date", "status")

def transform_transactions(df):
    return df \
        .withWatermark("timestamp", "10 minutes") \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("amount", abs(col("amount")).cast("decimal(10,2)")) \
        .withColumn("txn_date", to_date(col("timestamp"))) \
        .withColumn("txn_year", year(col("timestamp"))) \
        .withColumn("txn_month", month(col("timestamp"))) \
        .withColumn("is_debit", col("txn_type") == "debit") \
        .filter(col("status") == "success") \
        .filter(col("timestamp") < current_timestamp()) \
        .dropDuplicates(["txn_id"]) \
        .select("txn_id", "account_id", "merchant_id", "timestamp", "amount", "currency", "txn_type", "status",
                "channel", "city", "country", "description", "txn_date", "txn_year", "txn_month", "is_debit")

TRANSFORMERS = {
    "customers": transform_customers,
    "merchants": transform_merchants,
    "accounts": transform_accounts,
    "transactions": transform_transactions
}

# -----------------------------
# Incremental ingestion to Silver
# -----------------------------
def ingest_to_silver(table_name):
    bronze_path = BRONZE_PATHS[table_name]
    silver_path = SILVER_PATHS[table_name]
    checkpoint_path = CHECKPOINT_PATHS[table_name]
    ts_col = TS_COLUMNS.get(table_name)

    try:
        logging.info(f"📥 Reading Bronze table: {table_name} from {bronze_path}")
        df = spark.read.format("delta").load(bronze_path)

        # Incremental filter if timestamp column exists
        if ts_col and DeltaTable.isDeltaTable(spark, silver_path):
            delta_table = DeltaTable.forPath(spark, silver_path)
            last_ts = delta_table.toDF().agg({ts_col: "max"}).collect()[0][0]
            if last_ts:
                logging.info(f"Last ingested timestamp for {table_name}: {last_ts}")
                df = df.filter(df[ts_col] > last_ts)
        else:
            logging.info(f"No timestamp column or first ingestion: full load")

        if not df.rdd.isEmpty():
            logging.info(f"🔄 Transforming {table_name}...")
            transformed_df = TRANSFORMERS[table_name](df)

            logging.info(f"💾 Writing Silver table: {table_name} to {silver_path}")
            if table_name == "transactions":
                transformed_df.write.format("delta") \
                    .partitionBy("txn_year", "txn_month") \
                    .mode("overwrite") \
                    .option("checkpointLocation", checkpoint_path) \
                    .option("mergeSchema", "true") \
                    .save(silver_path)
            else:
                transformed_df.write.format("delta") \
                    .mode("overwrite") \
                    .option("checkpointLocation", checkpoint_path) \
                    .option("mergeSchema", "true") \
                    .save(silver_path)

            logging.info(f"✅ {table_name} Silver saved! Row count: {transformed_df.count()}")
        else:
            logging.info(f"⚠️ No new rows to ingest for {table_name}")
    except Exception as e:
        logging.error(f"❌ Failed to ingest {table_name}: {str(e)}")
        raise

# -----------------------------
# Ingest all tables
# -----------------------------
for table in BRONZE_PATHS.keys():
    ingest_to_silver(table)

# -----------------------------
# Validate Silver in S3
# -----------------------------
logging.info("Displaying 5 rows from transactions Silver table:")
spark.read.format("delta").load(SILVER_PATHS["transactions"]).show(5, truncate=False)

spark.stop()
