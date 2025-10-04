"""
gold_dimensions_enhanced_s3.py

Enhanced Gold layer for dimension tables: Loads from Silver Delta in S3 (MinIO) and saves back to Gold layer as managed Delta tables.
- SCD Type 1: Overwrite updates (e.g., for name/balance changes in dimensions).
- Aggregations: e.g., customer age groups, merchant category counts per country.
- Window Functions: e.g., rank customers by age, dense_rank merchants by category.
- Prepares for star schema joins in fact table.
- Fully stored on S3 (Delta), registered as managed tables in 'gold_layer' database, no Snowflake dependency.

Table names:
gold_layer.customers_enhanced
gold_layer.merchants_enhanced
gold_layer.accounts_enhanced

Folder: s3a://bucket/gold/dim_tables/{table}_enhanced
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta import *
import os
import logging
from datetime import date

# -----------------------------
# HADOOP AWS / AWS SDK JARs (for S3 read/write)
# -----------------------------
HADOOP_AWS_JAR = "/mnt/c/Users/Hello/hadoop-aws-3.3.4.jar"  # Downgraded to match Spark's bundled Hadoop
AWS_SDK_JAR = "/mnt/c/Users/Hello/aws-java-sdk-bundle-1.12.262.jar"  # Matched to Spark 3.5's bundled version

# Removed SCALA_LIBRARY_JAR—Spark handles it

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

# Silver paths (dimensions)
SILVER_DIM_PATHS = {
    "customers": f"s3a://{BUCKET}/silver/customers_silver",
    "merchants": f"s3a://{BUCKET}/silver/merchants_silver",
    "accounts": f"s3a://{BUCKET}/silver/accounts_silver"
}

GOLD_DATABASE = "gold_layer"
DIM_TABLES_FOLDER = f"s3a://{BUCKET}/gold/dim_tables"

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

# Create the gold_layer database if it doesn't exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE} LOCATION 's3a://{BUCKET}/gold'")

# -----------------------------
# Enhanced Dimension Processing Functions
# -----------------------------
def process_customers_enhanced(df):
    window_gender_age = Window.partitionBy("gender").orderBy(desc("age"))
    return df \
        .withColumn("age_group",
                    when(col("age") < 30, "Young")
                    .when(col("age") < 60, "Middle")
                    .otherwise("Senior")) \
        .withColumn("age_rank_gender", row_number().over(window_gender_age)) \
        .groupBy("age_group", "gender").agg(
            count("customer_id").alias("customer_count"),
            avg("age").alias("avg_age"),
            max("age").alias("max_age")
        ).withColumn("load_ts", current_timestamp()) \
        .select("age_group", "gender", "customer_count", "avg_age", "max_age", "load_ts")

def process_merchants_enhanced(df):
    # Workaround for desc_nulls_last codegen issue on string columns: Handle nulls explicitly
    df_with_null_flag = df.withColumn("merchant_name_sort", 
                                      when(col("merchant_name").isNull(), lit("~")).otherwise(col("merchant_name")))
    # Use '~' as it sorts after letters in ASCII (ensures nulls last in descending order)
    window_category = Window.partitionBy("category").orderBy(col("merchant_name_sort").desc())
    return df_with_null_flag \
        .withColumn("merchant_rank_category", dense_rank().over(window_category)) \
        .filter(col("merchant_rank_category") <= 5) \
        .groupBy("category", "country").agg(
            count("merchant_id").alias("merchant_count"),
            collect_list("merchant_name").alias("top_merchants")
        ).withColumn("load_ts", current_timestamp()) \
        .select("category", "country", "merchant_count", "top_merchants", "load_ts")

def process_accounts_enhanced(df):
    window_customer_time = Window.partitionBy("customer_id").orderBy("opened_date")
    return df \
        .withColumn("running_balance", sum("balance").over(window_customer_time)) \
        .withColumn("balance_bucket",
                    when(col("balance") < 0, "Negative")
                    .when(col("balance") < 10000, "Low")
                    .when(col("balance") < 50000, "Medium")
                    .otherwise("High")) \
        .groupBy("account_type", "status", "balance_bucket").agg(
            avg("balance").alias("avg_balance"),
            count("account_id").alias("account_count"),
            sum("running_balance").alias("total_running_balance")
        ).withColumn("load_ts", current_timestamp()) \
        .select("account_type", "status", "balance_bucket", "avg_balance", "account_count", "total_running_balance", "load_ts")

# -----------------------------
# Function to load enhanced dimension to Gold Layer Table (under dim_tables folder)
# -----------------------------
def load_dimension_to_s3(table_name):
    silver_path = SILVER_DIM_PATHS[table_name]
    table_name_full = f"{GOLD_DATABASE}.{table_name}_enhanced"
    dim_table_location = f"{DIM_TABLES_FOLDER}/{table_name}_enhanced"
    process_func = globals()[f"process_{table_name}_enhanced"]

    try:
        logging.info(f"📥 Reading Silver dimension: {table_name} from {silver_path}")
        df = spark.read.format("delta").load(silver_path)
        row_count = df.count()
        logging.info(f"Loaded {row_count} raw rows for {table_name}")

        if row_count > 0:
            logging.info(f"🔄 Enhancing {table_name} with SCD1, aggregations, and windows...")
            enhanced_df = process_func(df)
            enhanced_count = enhanced_df.count()
            logging.info(f"Enhanced to {enhanced_count} rows (from {row_count})")

            logging.info(f"💾 Writing enhanced {table_name} to Gold Delta table under dim_tables: {table_name_full}")
            enhanced_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .option("path", dim_table_location) \
                .saveAsTable(table_name_full)
            logging.info(f"✅ Enhanced {table_name} saved to Gold Delta table! Rows: {enhanced_count}")
        else:
            logging.warning(f"⚠️ No rows to load for {table_name}")

    except Exception as e:
        logging.error(f"❌ Failed to load enhanced {table_name} to Gold table: {str(e)}")
        raise

# -----------------------------
# Load all enhanced dimensions
# -----------------------------
for table in SILVER_DIM_PATHS.keys():
    load_dimension_to_s3(table)

logging.info("All enhanced dimension tables saved to Gold Layer successfully!")
spark.stop()