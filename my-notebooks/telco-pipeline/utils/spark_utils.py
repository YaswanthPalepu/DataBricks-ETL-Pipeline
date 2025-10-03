# utils/spark_utils.py

from pyspark.sql import SparkSession
import os
from config import S3_BUCKET_NAME, SPARK_APP_NAME, SPARK_MASTER, SPARK_HIVE_WAREHOUSE_DIR, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

def get_spark_session(app_name=SPARK_APP_NAME, master=SPARK_MASTER, hive_warehouse_dir=SPARK_HIVE_WAREHOUSE_DIR):
    """
    Initializes and returns a SparkSession with S3 and Hive support.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.jars", "/opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.340.jar") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.warehouse.dir", hive_warehouse_dir) \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def create_database_if_not_exists(spark, db_name, location):
    """
    Creates a Hive database if it doesn't already exist.
    """
    print(f"Creating database {db_name} at {location} if it doesn't exist...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{location}'")
    print(f"Database {db_name} ensured.")

def register_external_table(spark, df, db_name, table_name, location, format_type="PARQUET", schema_str=None, pk_column=None, fk_details=None):
    """
    Registers a Spark DataFrame as an external Hive table.
    Args:
        spark: SparkSession object.
        df: DataFrame to save and register.
        db_name: Name of the database.
        table_name: Name of the table.
        location: S3 path where the data is stored.
        format_type: Format of the data (e.g., 'PARQUET', 'DELTA').
        schema_str: Optional, a string defining the table schema for CREATE EXTERNAL TABLE.
                    If None, it's assumed the table is created without explicit schema for simpler cases.
        pk_column: Optional, primary key column name for TBLPROPERTIES.
        fk_details: Optional, a dictionary for foreign key TBLPROPERTIES.
                    e.g., {'fk_customer': 'dim_customer.customerID'}
    """
    print(f"Saving DataFrame to {location} and registering external table {db_name}.{table_name}...")
    df.write \
        .mode("overwrite") \
        .format(format_type.lower()) \
        .save(location)

    spark.sql(f"USE {db_name}")

    if schema_str:
        create_table_sql = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{table_name} (
            {schema_str}
        )
        USING {format_type.upper()}
        LOCATION '{location}'
        """
        if pk_column or fk_details:
            props = []
            if pk_column:
                props.append(f"'primary_key'='{pk_column}'")
            if fk_details:
                for fk_name, fk_ref in fk_details.items():
                    props.append(f"'{fk_name}'='{fk_ref}'")
            create_table_sql += f"TBLPROPERTIES ({', '.join(props)})"
        
        spark.sql(create_table_sql)
    else:
         # Simpler registration if schema_str is not provided (e.g., for initial stages)
        spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{table_name}
        USING {format_type.upper()}
        LOCATION '{location}'
        """)

    print(f"External table {db_name}.{table_name} registered.")
    spark.sql(f"REFRESH TABLE {db_name}.{table_name}") # Ensure metadata is refreshed