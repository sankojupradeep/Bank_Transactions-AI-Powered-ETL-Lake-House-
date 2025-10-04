"""
generate_and_upload_bank_data_incremental.py

Generates synthetic banking tables:
 - customers
 - merchants
 - accounts
 - transactions

Appends new rows to existing CSVs in MinIO / S3 under:
 s3://<bucket>/raw/customers/customers.csv
 s3://<bucket>/raw/merchants/merchants.csv
 s3://<bucket>/raw/accounts/accounts.csv
 s3://<bucket>/raw/transactions/transactions.csv

This is Airflow-friendly: you can run multiple times without creating new CSVs.
"""

import random
import uuid
from faker import Faker
import pandas as pd
import numpy as np
import boto3
from io import StringIO
from datetime import datetime, timedelta, timezone
import os

# ============================
# CONFIG - change these values
# ============================
# Use env vars for Docker compatibility (S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
MINIO_ENDPOINT = os.environ.get('S3_ENDPOINT_URL', 'http://localhost:9000')  # e.g., http://localhost:9002 for local
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID', 'admin')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', 'password123')
REGION = "us-east-1"
BUCKET = "bank-project"

# counts
NUM_CUSTOMERS = 1000         # e.g., 100 new customers per run
NUM_MERCHANTS = 50          # 50 new merchants
NUM_ACCOUNTS = 200          # 200 new accounts
NUM_TRANSACTIONS = 500      # 500 new transactions

# file names / prefixes
RAW_PREFIX = "raw/"
CUSTOMERS_KEY = f"{RAW_PREFIX}customers/customers.csv"
MERCHANTS_KEY = f"{RAW_PREFIX}merchants/merchants.csv"
ACCOUNTS_KEY = f"{RAW_PREFIX}accounts/accounts.csv"
TRANSACTIONS_KEY = f"{RAW_PREFIX}transactions/transactions.csv"

# seed for reproducibility
RANDOM_SEED = 42
# ============================
# End CONFIG
# ============================

random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)
fake = Faker()
Faker.seed(RANDOM_SEED)

# create s3 client for MinIO / S3
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION,
)

# ---------------------------
# Helper functions
# ---------------------------
def ensure_bucket(bucket_name):
    """Create bucket if it doesn't exist"""
    try:
        existing = s3.list_buckets()
        bucket_names = [b['Name'] for b in existing.get('Buckets', [])]
    except Exception:
        bucket_names = []
    if bucket_name not in bucket_names:
        print(f"Creating bucket: {bucket_name}")
        try:
            s3.create_bucket(Bucket=bucket_name)
        except Exception as exc:
            print("Warning: create_bucket raised:", exc)
    else:
        print(f"Bucket exists: {bucket_name}")

def append_df_to_s3(df, bucket, key, unique_col=None):
    """
    Append DataFrame to existing CSV in S3/MinIO.
    If file doesn't exist, create it.
    Optionally deduplicate based on unique_col.
    """
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        existing_df = pd.read_csv(obj['Body'])
        df_to_upload = pd.concat([existing_df, df], ignore_index=True)
        if unique_col:
            df_to_upload = df_to_upload.drop_duplicates(subset=[unique_col])
        print(f"Appending {len(df)} rows to existing CSV with {len(existing_df)} rows.")
    except s3.exceptions.NoSuchKey:
        df_to_upload = df
        print(f"CSV does not exist. Creating new file with {len(df)} rows.")

    csv_buffer = StringIO()
    df_to_upload.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    print(f"Uploaded {len(df_to_upload):,} rows to s3://{bucket}/{key}")

# ---------------------------
# 1) Generate customers table
# ---------------------------
def gen_customers(n_customers):
    customers = []
    now_utc = datetime.now(timezone.utc)
    for i in range(n_customers):
        customer_id = f"CUST{int(now_utc.timestamp()*1000) + i}"
        name = fake.name()
        dob = fake.date_of_birth(minimum_age=18, maximum_age=85).isoformat()
        gender = random.choice(["M", "F", "O"])
        address = fake.address().replace("\n", ", ")
        phone = fake.phone_number()
        email = fake.safe_email()
        created_at = now_utc.isoformat()
        customers.append([customer_id, name, dob, gender, address, phone, email, created_at])
    df = pd.DataFrame(customers, columns=[
        "customer_id","name","dob","gender","address","phone","email","created_at"
    ])
    return df

# ---------------------------
# 2) Generate merchants table
# ---------------------------
def gen_merchants(n_merchants):
    merchant_categories = ["Groceries","Shopping","Travel","Entertainment","Food","Bills","Transport","Health","Education","Electronics"]
    merchants = []
    now_utc = datetime.now(timezone.utc)
    for i in range(n_merchants):
        merchant_id = f"M{int(now_utc.timestamp()*1000) + i}"
        merchant_name = fake.company()
        category = random.choice(merchant_categories)
        city = fake.city()
        country = fake.country()
        merchants.append([merchant_id, merchant_name, category, city, country])
    df = pd.DataFrame(merchants, columns=["merchant_id","merchant_name","category","city","country"])
    return df

# ---------------------------
# 3) Generate accounts table
# ---------------------------
def gen_accounts(n_accounts, customer_ids):
    account_types = ["savings","checking","credit_card","loan"]
    statuses = ["active","inactive","closed"]
    accounts = []
    now_utc = datetime.now(timezone.utc)
    for i in range(n_accounts):
        account_id = f"ACCT{int(now_utc.timestamp()*1000) + i}"
        customer_id = random.choice(customer_ids)
        account_type = random.choices(account_types, weights=[0.5,0.35,0.1,0.05])[0]
        opened_date = fake.date_between(start_date='-5y', end_date='today').isoformat()
        if account_type == "credit_card":
            balance = round(random.uniform(-5000, 5000), 2)
        elif account_type == "loan":
            balance = round(random.uniform(-50000, 0), 2)
        else:
            balance = round(random.uniform(0, 200000), 2)
        status = random.choices(statuses, weights=[0.8,0.15,0.05])[0]
        accounts.append([account_id, customer_id, account_type, balance, opened_date, status])
    df = pd.DataFrame(accounts, columns=["account_id","customer_id","account_type","balance","opened_date","status"])
    return df

# ---------------------------
# 4) Generate transactions table
# ---------------------------
def gen_transactions(n_txns, account_ids, merchant_ids):
    txn_types = ["debit","credit","transfer","payment"]
    statuses = ["success","failed","pending"]
    channels = ["online","pos","atm","mobile"]
    currencies = ["USD","EUR","INR","GBP","JPY"]
    txns = []
    now_utc = datetime.now(timezone.utc)
    for i in range(n_txns):
        txn_id = f"TXN{int(now_utc.timestamp()*1000) + i}"
        account_id = random.choice(account_ids)
        merchant_id = random.choice(merchant_ids + [None]*5)
        txn_dt = now_utc - timedelta(seconds=random.randint(0, 365*24*60*60))
        amount = float(np.round(np.random.normal(loc=0, scale=200), 2))
        if random.random() < 0.02:
            amount = float(np.round(np.random.lognormal(mean=6, sigma=1), 2)) * (1 if random.random()<0.5 else -1)
        if abs(amount) < 1:
            amount += np.sign(amount or 1) * round(random.uniform(1, 50), 2)
        currency = random.choice(currencies)
        txn_type = random.choices(txn_types, weights=[0.7,0.15,0.1,0.05])[0]
        status = random.choices(statuses, weights=[0.95,0.03,0.02])[0]
        channel = random.choice(channels)
        city = fake.city()
        country = fake.country()
        description = fake.sentence(nb_words=6)
        txns.append([
            txn_id, account_id, merchant_id, txn_dt.isoformat(), round(amount,2),
            currency, txn_type, status, channel, city, country, description
        ])
    df = pd.DataFrame(txns, columns=[
        "txn_id","account_id","merchant_id","timestamp","amount",
        "currency","txn_type","status","channel","city","country","description"
    ])
    return df

# ---------------------------
# MAIN
# ---------------------------
def main():
    print("Ensuring bucket exists...")
    ensure_bucket(BUCKET)

    print("Generating customers...")
    df_customers = gen_customers(NUM_CUSTOMERS)
    append_df_to_s3(df_customers, BUCKET, CUSTOMERS_KEY, unique_col="customer_id")

    print("Generating merchants...")
    df_merchants = gen_merchants(NUM_MERCHANTS)
    append_df_to_s3(df_merchants, BUCKET, MERCHANTS_KEY, unique_col="merchant_id")

    print("Generating accounts...")
    # get all existing customer_ids from S3 to link new accounts
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=CUSTOMERS_KEY)
        existing_customers = pd.read_csv(obj['Body'])
        customer_ids = existing_customers["customer_id"].tolist()
    except s3.exceptions.NoSuchKey:
        customer_ids = df_customers["customer_id"].tolist()
    df_accounts = gen_accounts(NUM_ACCOUNTS, customer_ids)
    append_df_to_s3(df_accounts, BUCKET, ACCOUNTS_KEY, unique_col="account_id")

    print("Generating transactions...")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=ACCOUNTS_KEY)
        existing_accounts = pd.read_csv(obj['Body'])
        account_ids = existing_accounts["account_id"].tolist()
    except s3.exceptions.NoSuchKey:
        account_ids = df_accounts["account_id"].tolist()
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=MERCHANTS_KEY)
        existing_merchants = pd.read_csv(obj['Body'])
        merchant_ids = existing_merchants["merchant_id"].tolist()
    except s3.exceptions.NoSuchKey:
        merchant_ids = df_merchants["merchant_id"].tolist()
    df_transactions = gen_transactions(NUM_TRANSACTIONS, account_ids, merchant_ids)
    append_df_to_s3(df_transactions, BUCKET, TRANSACTIONS_KEY, unique_col="txn_id")

    print("All tables generated and uploaded incrementally!")

if __name__ == "__main__":
    main()