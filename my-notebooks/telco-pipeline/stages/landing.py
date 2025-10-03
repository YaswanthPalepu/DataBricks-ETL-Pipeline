# stages/landing.py

import os
import kaggle
import boto3
from zipfile import ZipFile

from config import (
    LOCAL_DATA_DIR, KAGGLE_ZIP_PATH, KAGGLE_CSV_PATH, S3_BUCKET_NAME, RAW_CSV_S3_KEY,
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, RAW_DB_NAME, RAW_DATA_S3_PATH
)
from utils.spark_utils import get_spark_session, create_database_if_not_exists

def run_landing_stage(spark):
    """
    Executes the landing stage: downloads data, uploads to S3, and creates raw database.
    """
    print("\n--- Starting Landing Stage ---")

    # 1. Ensure local data directory exists
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)

    # 2. Download dataset using Kaggle CLI (assuming kaggle.json is set up)
    print(f"Downloading Kaggle dataset to {LOCAL_DATA_DIR}...")
    try:
        # kaggle.api.authenticate() # Not strictly needed if `kaggle.json` is correctly placed and chmodded
        kaggle.api.dataset_download_files(
            dataset='blastchar/telco-customer-churn',
            path=LOCAL_DATA_DIR,
            unzip=False # We'll unzip manually
        )
        print("Kaggle dataset downloaded.")
    except Exception as e:
        print(f"Error downloading Kaggle dataset: {e}")
        print("Please ensure your Kaggle API key (kaggle.json) is correctly set up.")
        return False # Indicate failure

    # 3. Unzip the downloaded file
    print(f"Unzipping {KAGGLE_ZIP_PATH}...")
    with ZipFile(KAGGLE_ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(LOCAL_DATA_DIR)
    print("Dataset unzipped.")

    # 4. Upload CSV to S3
    print(f"Uploading {KAGGLE_CSV_PATH} to s3://{S3_BUCKET_NAME}/{RAW_CSV_S3_KEY}...")
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    try:
        s3_client.upload_file(KAGGLE_CSV_PATH, S3_BUCKET_NAME, RAW_CSV_S3_KEY)
        print("CSV file uploaded to S3.")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        return False # Indicate failure

    # 5. Create raw database in Hive
    create_database_if_not_exists(spark, RAW_DB_NAME, RAW_DATA_S3_PATH)

    # 6. Read raw data from S3 to show a sample (optional, for verification)
    csv_s3_path = f"s3a://{S3_BUCKET_NAME}/{RAW_CSV_S3_KEY}"
    print(f"Reading raw CSV from S3 for verification: {csv_s3_path}")
    raw_df = spark.read.csv(csv_s3_path, header=True, inferSchema=True)
    print("First 10 rows of raw data:")
    raw_df.show(10)
    print(f"Raw data schema: ")
    raw_df.printSchema()

    print("--- Landing Stage Finished ---")
    return True