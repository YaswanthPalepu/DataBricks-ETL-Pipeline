# config.py

import os

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = "databricks-one" # Your S3 bucket name

# Base paths for different layers in S3
BASE_S3_PATH = f"s3a://{S3_BUCKET_NAME}/telco/"
RAW_DATA_S3_PATH = f"s3a://{S3_BUCKET_NAME}/raw/"
BRONZE_S3_PATH = f"{BASE_S3_PATH}bronze/"
SILVER_S3_PATH = f"{BASE_S3_PATH}silver/"
GOLD_S3_PATH = f"{BASE_S3_PATH}gold/"

# Local paths within the Docker container
LOCAL_DATA_DIR = "/home/Personal_Projects/Databricks/work/datasets"
KAGGLE_ZIP_PATH = os.path.join(LOCAL_DATA_DIR, "telco-customer-churn.zip")
KAGGLE_CSV_PATH = os.path.join(LOCAL_DATA_DIR, "WA_Fn-UseC_-Telco-Customer-Churn.csv")

# Database Names
RAW_DB_NAME = "raw_data"
BRONZE_DB_NAME = "bronze"
SILVER_DB_NAME = "silver"
GOLD_DB_NAME = "gold"

# Table Names
RAW_CSV_S3_KEY = "raw/telco.csv"
BRONZE_TABLE_NAME = "telco_churn_parquet"
SILVER_TABLE_NAME = "telco_churn"
GOLD_DIM_CUSTOMER_TABLE = "dim_customer"
GOLD_DIM_CONTRACT_TABLE = "dim_contract"
GOLD_DIM_SERVICES_TABLE = "dim_services"
GOLD_DIM_PAYMENT_TABLE = "dim_payment"
GOLD_FACT_CHURN_TABLE = "fact_churn"

# Spark Configuration (if needed, though often passed directly to SparkSession)
SPARK_APP_NAME = "TelcoChurnLakehouse"
SPARK_MASTER = "local[*]"
SPARK_HIVE_WAREHOUSE_DIR = f"s3a://{S3_BUCKET_NAME}/telco/catalog/"