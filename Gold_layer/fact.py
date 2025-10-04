"""
gold_facts_enhanced_s3.py

Enhanced Gold layer for fact tables: Loads from Silver Delta in S3 (MinIO), joins with Silver dimensions, 
applies advanced aggregations (e.g., window functions for running totals, percentiles, correlations), 
and saves as managed Delta tables in gold_layer database under 'fact_tables' subfolder.
- Joins: Full outer joins with silver dimensions for denormalization (e.g., add customer age_group, merchant category).
- Advanced Aggregations: Group by time windows, customer segments; use corr, percentile_approx, lag/lead for trends.
- Window Functions: Running spend per customer, rank transactions by amount.
- Prepares denormalized facts for BI queries (e.g., monthly spend by age_group and category).
- Fully stored on S3 (Delta), registered in 'gold_layer' database.
- Folder: s3a://bucket/gold/fact_tables/transactions_enhanced (and others as added).

Table: gold_layer.transactions_enhanced
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

# Silver paths
SILVER_PATHS = {
    "transactions": f"s3a://{BUCKET}/silver/transactions_silver",
    "customers": f"s3a://{BUCKET}/silver/customers_silver",
    "merchants": f"s3a://{BUCKET}/silver/merchants_silver",
    "accounts": f"s3a://{BUCKET}/silver/accounts_silver"
}

GOLD_DATABASE = "gold_layer"
FACT_TABLES_FOLDER = f"s3a://{BUCKET}/gold/fact_tables"

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
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create the gold_layer database if it doesn't exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE} LOCATION 's3a://{BUCKET}/gold'")

# -----------------------------
# Load Silver Tables (Full Dimensions for Joins)
# -----------------------------
logging.info("📥 Loading Silver tables for joins...")
customers_df = spark.read.format("delta").load(SILVER_PATHS["customers"])
merchants_df = spark.read.format("delta").load(SILVER_PATHS["merchants"])
accounts_df = spark.read.format("delta").load(SILVER_PATHS["accounts"])
logging.info(f"Loaded customers: {customers_df.count()} rows")
logging.info(f"Loaded merchants: {merchants_df.count()} rows")
logging.info(f"Loaded accounts: {accounts_df.count()} rows")

# -----------------------------
# Enhanced Fact Processing Function
# -----------------------------
def process_transactions_enhanced(transactions_df):
    # Derive age_group from customers (assuming age column exists)
    customers_with_group = customers_df.withColumn("age_group",
                                                  when(col("age") < 30, "Young")
                                                  .when(col("age") < 60, "Middle")
                                                  .otherwise("Senior"))

    # Join transactions with accounts first (to get customer_id), then customers, then merchants
    # Use txn_date for date operations, txn_type for debit/credit
    joined_df = transactions_df \
        .join(accounts_df, "account_id", "left") \
        .join(customers_with_group, "customer_id", "left") \
        .join(merchants_df, "merchant_id", "left") \
        .withColumn("transaction_month", date_trunc("month", col("txn_date"))) \
        .withColumn("spend_amount", when(col("is_debit") == True, col("amount")).otherwise(0)) \
        .withColumn("income_amount", when(col("is_debit") == False, col("amount")).otherwise(0))

    # Window for advanced: Running total spend per customer, rank by amount
    window_customer = Window.partitionBy("customer_id").orderBy("txn_date")
    window_global = Window.orderBy(desc("amount"))

    enriched_df = joined_df \
        .withColumn("running_spend", sum("spend_amount").over(window_customer)) \
        .withColumn("amount_rank", row_number().over(window_global)) \
        .withColumn("prev_txn_spend", lag("spend_amount", 1).over(window_customer)) \
        .withColumn("spend_growth", 
                    when(col("prev_txn_spend").isNotNull() & (col("prev_txn_spend") != 0), 
                         (col("spend_amount") - col("prev_txn_spend")) / col("prev_txn_spend"))
                    .otherwise(lit(0)))

    # Advanced Aggregations: Group by month, age_group, category; include corr, percentile
    agg_df = enriched_df \
        .groupBy("transaction_month", "age_group", "category") \
        .agg(
            sum("spend_amount").alias("total_spend"),
            count("txn_id").alias("transaction_count"),
            avg("amount").alias("avg_transaction_value"),
            countDistinct("customer_id").alias("unique_customers"),
            percentile_approx("amount", 0.95).alias("p95_amount"),
            corr("amount", "balance").alias("amount_balance_corr"),  # Advanced: Correlation
            max("running_spend").alias("max_running_spend")
        ) \
        .withColumn("load_ts", current_timestamp()) \
        .orderBy(desc("transaction_month"), "age_group", "category")

    return agg_df

# -----------------------------
# Function to load enhanced fact to Gold Layer Table (under fact_tables folder)
# -----------------------------
def load_fact_to_gold(table_name="transactions"):
    silver_path = SILVER_PATHS[table_name]
    table_name_full = f"{GOLD_DATABASE}.{table_name}_enhanced"
    fact_table_location = f"{FACT_TABLES_FOLDER}/{table_name}_enhanced"
    process_func = process_transactions_enhanced

    try:
        logging.info(f"📥 Reading Silver fact: {table_name} from {silver_path}")
        df = spark.read.format("delta").load(silver_path)
        row_count = df.count()
        logging.info(f"Loaded {row_count} raw rows for {table_name}")

        if row_count > 0:
            logging.info(f"🔄 Enhancing {table_name} with joins, advanced aggs, and windows...")
            enhanced_df = process_func(df)
            enhanced_count = enhanced_df.count()
            logging.info(f"Enhanced to {enhanced_count} rows (from {row_count})")

            logging.info(f"💾 Writing enhanced {table_name} to Gold Delta table under fact_tables: {table_name_full}")
            enhanced_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .option("path", fact_table_location) \
                .saveAsTable(table_name_full)
            logging.info(f"✅ Enhanced {table_name} saved to Gold Delta table! Rows: {enhanced_count}")
        else:
            logging.warning(f"⚠️ No rows to load for {table_name}")

    except Exception as e:
        logging.error(f"❌ Failed to load enhanced {table_name} to Gold table: {str(e)}")
        raise

# -----------------------------
# Load enhanced fact
# -----------------------------
load_fact_to_gold("transactions")

logging.info("Enhanced fact table saved to Gold Layer successfully!")
spark.stop()