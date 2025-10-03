# stages/silver.py

from pyspark.sql.functions import col, trim, when
from config import (
    BRONZE_DB_NAME, BRONZE_TABLE_NAME, SILVER_S3_PATH, SILVER_DB_NAME, SILVER_TABLE_NAME
)
from utils.spark_utils import get_spark_session, create_database_if_not_exists, register_external_table
from utils.validation_utils import perform_validation

def run_silver_stage(spark):
    """
    Executes the silver stage: reads bronze data, applies transformations,
    saves as Parquet, and registers the silver table.
    """
    print("\n--- Starting Silver Stage ---")

    # 1. Create Silver database
    create_database_if_not_exists(spark, SILVER_DB_NAME, SILVER_S3_PATH)

    # 2. Read from Bronze Hive Table
    bronze_df = spark.table(f"{BRONZE_DB_NAME}.{BRONZE_TABLE_NAME}")
    print(f"Read {bronze_df.count()} rows from {BRONZE_DB_NAME}.{BRONZE_TABLE_NAME}.")

    # 3. Silver transformations
    print("Applying Silver transformations...")
    silver_df = (
        bronze_df
        # Trim string columns
        .withColumn("customerID", trim(col("customerID")))
        .withColumn("gender", trim(col("gender")))
        .withColumn("Partner", trim(col("Partner")))
        .withColumn("Dependents", trim(col("Dependents")))
        .withColumn("PhoneService", trim(col("PhoneService")))
        .withColumn("MultipleLines", trim(col("MultipleLines")))
        .withColumn("InternetService", trim(col("InternetService")))
        .withColumn("OnlineSecurity", trim(col("OnlineSecurity")))
        .withColumn("OnlineBackup", trim(col("OnlineBackup")))
        .withColumn("DeviceProtection", trim(col("DeviceProtection")))
        .withColumn("TechSupport", trim(col("TechSupport")))
        .withColumn("StreamingTV", trim(col("StreamingTV")))
        .withColumn("StreamingMovies", trim(col("StreamingMovies")))
        .withColumn("Contract", trim(col("Contract")))
        .withColumn("PaperlessBilling", trim(col("PaperlessBilling")))
        .withColumn("PaymentMethod", trim(col("PaymentMethod")))
        
        # Convert data types
        .withColumn("SeniorCitizen", col("SeniorCitizen").cast("int"))
        .withColumn("tenure", col("tenure").cast("int"))
        .withColumn("MonthlyCharges", col("MonthlyCharges").cast("double"))
        .withColumn("TotalCharges", when(trim(col("TotalCharges")) == "", None)
                    .otherwise(col("TotalCharges").cast("double")))

        # Handle missing values
        .dropna(subset=["customerID", "tenure", "MonthlyCharges", "TotalCharges"])

        # Derived column
        .withColumn("IsSenior", when(col("SeniorCitizen") == 1, True).otherwise(False))

        # Data quality checks
        .filter(col("tenure") >= 0)

        # Deduplicate
        .dropDuplicates(["customerID"])
    )
    print(f"Silver DataFrame created with {silver_df.count()} rows after transformations.")


    # 4. Save Silver layer in S3 as Parquet + Register in Hive
    silver_table_location = f"{SILVER_S3_PATH}{SILVER_TABLE_NAME}"
    register_external_table(spark, silver_df, SILVER_DB_NAME, SILVER_TABLE_NAME, silver_table_location, format_type="PARQUET")

    # 5. Query & Verify
    print(f"Querying {SILVER_DB_NAME}.{SILVER_TABLE_NAME} for verification...")
    silver_verified_df = spark.table(f"{SILVER_DB_NAME}.{SILVER_TABLE_NAME}")
    print("First 10 rows of Silver data:")
    silver_verified_df.show(10, truncate=False)
    print("Silver data schema:")
    silver_verified_df.printSchema()

    # 6. Perform validation on Silver data
    if not perform_validation(silver_verified_df, "Silver"):
        print("Silver data validation failed. Aborting Silver stage.")
        return False

    print("--- Silver Stage Finished ---")
    return True