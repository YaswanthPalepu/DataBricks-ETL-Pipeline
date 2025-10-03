# stages/gold.py

from pyspark.sql.functions import col
from config import (
    SILVER_DB_NAME, SILVER_TABLE_NAME, GOLD_S3_PATH, GOLD_DB_NAME,
    GOLD_DIM_CUSTOMER_TABLE, GOLD_DIM_CONTRACT_TABLE, GOLD_DIM_SERVICES_TABLE,
    GOLD_DIM_PAYMENT_TABLE, GOLD_FACT_CHURN_TABLE
)
from utils.spark_utils import get_spark_session, create_database_if_not_exists, register_external_table
from utils.validation_utils import perform_validation

def run_gold_stage(spark):
    """
    Executes the gold stage: reads silver data, creates dimension and fact tables,
    saves as Parquet, and registers gold tables.
    """
    print("\n--- Starting Gold Stage ---")

    # 1. Create Gold database
    create_database_if_not_exists(spark, GOLD_DB_NAME, GOLD_S3_PATH)
    spark.sql(f"USE {GOLD_DB_NAME}")

    # 2. Read Silver Hive Table
    silver_df = spark.table(f"{SILVER_DB_NAME}.{SILVER_TABLE_NAME}")
    print(f"Read {silver_df.count()} rows from {SILVER_DB_NAME}.{SILVER_TABLE_NAME}.")
    silver_df.show(5, truncate=False)

    # 3. Create Dimension Tables
    print("Creating Dimension Tables...")

    # Customer Dimension
    dim_customer = silver_df.select(
        "customerID", "gender", "SeniorCitizen", "IsSenior", "Partner", "Dependents",
        "PhoneService", "MultipleLines"
    ).dropDuplicates(["customerID"])
    print(f"dim_customer has {dim_customer.count()} rows.")
    if not perform_validation(dim_customer, "Gold - Dim Customer"): return False

    # Contract Dimension
    dim_contract = silver_df.select(
        "Contract", "PaperlessBilling"
    ).dropDuplicates(["Contract"]) # Ensure PK uniqueness for Contract
    print(f"dim_contract has {dim_contract.count()} rows.")
    if not perform_validation(dim_contract, "Gold - Dim Contract"): return False

    # Internet & Services Dimension
    dim_services = silver_df.select(
        "InternetService", "OnlineSecurity", "OnlineBackup", "DeviceProtection",
        "TechSupport", "StreamingTV", "StreamingMovies"
    ).dropDuplicates(["InternetService"]) # Ensure PK uniqueness for InternetService
    print(f"dim_services has {dim_services.count()} rows.")
    if not perform_validation(dim_services, "Gold - Dim Services"): return False

    # Payment Dimension
    dim_payment = silver_df.select(
        "PaymentMethod"
    ).dropDuplicates(["PaymentMethod"]) # Ensure PK uniqueness for PaymentMethod
    print(f"dim_payment has {dim_payment.count()} rows.")
    if not perform_validation(dim_payment, "Gold - Dim Payment"): return False

    # 4. Create Fact Table
    print("Creating Fact Table...")
    fact_churn = silver_df.select(
        "customerID",       # FK to Customer
        "Contract",         # FK to Contract
        "InternetService",  # FK to Services
        "PaymentMethod",    # FK to Payment
        "tenure",
        "MonthlyCharges",
        "TotalCharges"
    )
    print(f"fact_churn initially has {fact_churn.count()} rows.")
    if not perform_validation(fact_churn, "Gold - Fact Churn (Pre-join)"): return False


    # 5. Save Gold Layer to S3 & Register in Hive
    print("Saving Gold Layer to S3 and registering tables...")

    # Customer Dimension
    customer_schema_str = """
    customerID STRING COMMENT 'PK', gender STRING, SeniorCitizen INT, IsSenior BOOLEAN,
    Partner STRING, Dependents STRING, PhoneService STRING, MultipleLines STRING
    """
    register_external_table(spark, dim_customer, GOLD_DB_NAME, GOLD_DIM_CUSTOMER_TABLE,
                            f"{GOLD_S3_PATH}{GOLD_DIM_CUSTOMER_TABLE}",
                            schema_str=customer_schema_str.strip(), pk_column="customerID")

    # Contract Dimension
    contract_schema_str = """
    Contract STRING COMMENT 'PK', PaperlessBilling STRING
    """
    register_external_table(spark, dim_contract, GOLD_DB_NAME, GOLD_DIM_CONTRACT_TABLE,
                            f"{GOLD_S3_PATH}{GOLD_DIM_CONTRACT_TABLE}",
                            schema_str=contract_schema_str.strip(), pk_column="Contract")

    # Services Dimension
    services_schema_str = """
    InternetService STRING COMMENT 'PK', OnlineSecurity STRING, OnlineBackup STRING,
    DeviceProtection STRING, TechSupport STRING, StreamingTV STRING, StreamingMovies STRING
    """
    register_external_table(spark, dim_services, GOLD_DB_NAME, GOLD_DIM_SERVICES_TABLE,
                            f"{GOLD_S3_PATH}{GOLD_DIM_SERVICES_TABLE}",
                            schema_str=services_schema_str.strip(), pk_column="InternetService")

    # Payment Dimension
    payment_schema_str = """
    PaymentMethod STRING COMMENT 'PK'
    """
    register_external_table(spark, dim_payment, GOLD_DB_NAME, GOLD_DIM_PAYMENT_TABLE,
                            f"{GOLD_S3_PATH}{GOLD_DIM_PAYMENT_TABLE}",
                            schema_str=payment_schema_str.strip(), pk_column="PaymentMethod")

    # Fact Table (Churn) - Join to validate foreign keys
    # Re-read dimensions to ensure consistency, though generally not strictly necessary if previous steps ran successfully
    dim_customer_verified = spark.table(f"{GOLD_DB_NAME}.{GOLD_DIM_CUSTOMER_TABLE}")
    dim_contract_verified = spark.table(f"{GOLD_DB_NAME}.{GOLD_DIM_CONTRACT_TABLE}")
    dim_services_verified = spark.table(f"{GOLD_DB_NAME}.{GOLD_DIM_SERVICES_TABLE}")
    dim_payment_verified = spark.table(f"{GOLD_DB_NAME}.{GOLD_DIM_PAYMENT_TABLE}")

    fact_churn_clean = (
        fact_churn
        .join(dim_customer_verified.select("customerID"), on="customerID", how="inner")
        .join(dim_contract_verified.select("Contract"), on="Contract", how="inner")
        .join(dim_services_verified.select("InternetService"), on="InternetService", how="inner")
        .join(dim_payment_verified.select("PaymentMethod"), on="PaymentMethod", how="inner")
    )
    print(f"fact_churn after FK validation has {fact_churn_clean.count()} rows.")
    if not perform_validation(fact_churn_clean, "Gold - Fact Churn"): return False

    fact_churn_schema_str = """
    customerID STRING COMMENT 'FK -> dim_customer.customerID',
    Contract STRING COMMENT 'FK -> dim_contract.Contract',
    InternetService STRING COMMENT 'FK -> dim_services.InternetService',
    PaymentMethod STRING COMMENT 'FK -> dim_payment.PaymentMethod',
    tenure INT, MonthlyCharges DOUBLE, TotalCharges DOUBLE
    """
    fact_churn_fk_details = {
        'fk_customer': 'dim_customer.customerID',
        'fk_contract': 'dim_contract.Contract',
        'fk_services': 'dim_services.InternetService',
        'fk_payment': 'dim_payment.PaymentMethod'
    }
    register_external_table(spark, fact_churn_clean, GOLD_DB_NAME, GOLD_FACT_CHURN_TABLE,
                            f"{GOLD_S3_PATH}{GOLD_FACT_CHURN_TABLE}",
                            schema_str=fact_churn_schema_str.strip(), fk_details=fact_churn_fk_details)


    # 6. Query & Verify Gold tables
    print("\nVerifying Gold tables:")
    spark.sql(f"SELECT * FROM {GOLD_DB_NAME}.{GOLD_DIM_CUSTOMER_TABLE} LIMIT 5").show(truncate=False)
    spark.sql(f"SELECT * FROM {GOLD_DB_NAME}.{GOLD_FACT_CHURN_TABLE} LIMIT 5").show(truncate=False)

    print("--- Gold Stage Finished ---")
    return True