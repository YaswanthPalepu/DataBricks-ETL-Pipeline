# stages/bronze.py

from config import (
    S3_BUCKET_NAME, RAW_CSV_S3_KEY, BRONZE_S3_PATH, BRONZE_DB_NAME, BRONZE_TABLE_NAME
)
from utils.spark_utils import get_spark_session, create_database_if_not_exists, register_external_table
from utils.validation_utils import perform_validation

def run_bronze_stage(spark):
    """
    Executes the bronze stage: reads raw CSV, converts to Parquet, and registers bronze table.
    """
    print("\n--- Starting Bronze Stage ---")

    # 1. Create Bronze database
    create_database_if_not_exists(spark, BRONZE_DB_NAME, BRONZE_S3_PATH)

    # 2. Read raw CSV from S3
    csv_s3_path = f"s3a://{S3_BUCKET_NAME}/{RAW_CSV_S3_KEY}"
    print(f"Reading raw CSV from {csv_s3_path}...")
    raw_df = spark.read.csv(csv_s3_path, header=True, inferSchema=True)
    print(f"Raw DataFrame loaded with {raw_df.count()} rows.")

    # 3. Perform initial validation on raw data (optional but good practice)
    if not perform_validation(raw_df, "Raw Data"):
        print("Raw data validation failed. Aborting Bronze stage.")
        return False

    # 4. Save raw data as Parquet in the Bronze layer
    bronze_table_location = f"{BRONZE_S3_PATH}{BRONZE_TABLE_NAME}"
    register_external_table(spark, raw_df, BRONZE_DB_NAME, BRONZE_TABLE_NAME, bronze_table_location, format_type="PARQUET")

    # 5. Query and verify the Bronze table
    print(f"Querying {BRONZE_DB_NAME}.{BRONZE_TABLE_NAME} for verification...")
    bronze_df = spark.table(f"{BRONZE_DB_NAME}.{BRONZE_TABLE_NAME}")
    print("First 10 rows of Bronze data:")
    bronze_df.show(10)
    print("Bronze data schema:")
    bronze_df.printSchema()

    # 6. Perform validation on Bronze data
    if not perform_validation(bronze_df, "Bronze"):
        print("Bronze data validation failed. Aborting Bronze stage.")
        return False

    print("--- Bronze Stage Finished ---")
    return True