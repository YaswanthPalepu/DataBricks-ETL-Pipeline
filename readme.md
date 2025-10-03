# Databricks Telco Churn Prediction Project

This repository contains a data pipeline and analysis project focused on predicting customer churn in a telecommunications company. The project utilizes Apache Spark for data processing, transforms raw data into a medallion architecture (Bronze, Silver, Gold layers), and provides tools for loading processed data into a PostgreSQL database.

## Project Structure

- `docker-compose.yml`: Docker Compose configuration for setting up the Spark environment, PostgreSQL, and other services.
- `jars/`: Contains necessary JAR files for Spark (e.g., AWS SDK, Hadoop AWS).
- `loader/`: Python script to load data from the Gold layer into PostgreSQL.
- `my-notebooks/`: Jupyter notebooks for exploratory data analysis, pipeline development, and model building.
- `notebooks/`: Contains raw datasets and processed data from the Bronze, Silver, and Gold layers.
- `spark-data/`: Directory for Spark's persistent data (e.g., Hive Metastore, warehouse directory).
- `temp/`: Temporary files generated during development or execution.

## Data Pipeline

The data pipeline follows a medallion architecture:
1.  **Landing (Raw Data Acquisition):** The initial raw CSV data is considered the landing zone.
2.  **Bronze Layer:** Raw data is ingested, cleansed, and stored in a consistent format (e.g., Parquet).
3.  **Silver Layer:** Business rules are applied, and data is transformed into a more structured and refined format, often involving feature engineering.
4.  **Gold Layer:** Aggregate and highly curated data ready for consumption by business intelligence tools, dashboards, or machine learning models.

## Getting Started

Refer to `process.md` for detailed instructions on setting up the environment and running the project.

## Contributions

Feel free to open issues or submit pull requests.

# Project File Structure

```bash
Databricks/
├── docker-compose2.yml
├── docker-compose.yml
├── jars/
│ ├── aws-java-sdk-bundle-1.12.340.jar
│ └── hadoop-aws-3.3.4.jar
├── loader/
│ └── load_gold_to_postgres.py
├── my-notebooks/
│ ├── real_one.ipynb
│ ├── spark-warehouse/
│ │ └── telco_catalog.db/
│ ├── telco-pipeline/
│ │ ├── config.py
│ │ ├── data_pipeline.py
│ │ ├── pycache/ # Compiled Python files (auto-generated)
│ │ │ └── config.cpython-311.pyc
│ │ ├── running.ipynb
│ │ ├── stages/
│ │ │ ├── bronze.py
│ │ │ ├── gold.py
│ │ │ ├── landing.py
│ │ │ ├── pycache/ # Compiled Python files (auto-generated)
│ │ │ │ ├── bronze.cpython-311.pyc
│ │ │ │ ├── gold.cpython-311.pyc
│ │ │ │ ├── landing.cpython-311.pyc
│ │ │ │ └── silver.cpython-311.pyc
│ │ │ └── silver.py
│ │ └── utils/
│ │ ├── pycache/ # Compiled Python files (auto-generated)
│ │ │ ├── spark_utils.cpython-311.pyc
│ │ │ └── validation_utils.cpython-311.pyc
│ │ ├── spark_utils.py
│ │ └── validation_utils.py
│ └── testing.ipynb
├── notebooks/
│ ├── data/
│ │ ├── gold/ # Gold layer processed data (Parquet)
│ │ │ ├── dim_contract/
│ │ │ │ ├── part-00000-...parquet
│ │ │ │ └── _SUCCESS
│ │ │ ├── dim_customer/
│ │ │ │ ├── part-00000-...parquet
│ │ │ │ └── _SUCCESS
│ │ │ ├── dim_payment/
│ │ │ │ ├── part-00000-...parquet
│ │ │ │ └── _SUCCESS
│ │ │ ├── dim_services/
│ │ │ │ ├── part-00000-...parquet
│ │ │ │ └── _SUCCESS
│ │ │ └── fact_churn/
│ │ │ ├── part-00000-...parquet
│ │ │ └── _SUCCESS
│ │ └── silver/ # Silver layer processed data (Parquet)
│ │ └── telco_churn/
│ │ ├── part-00000-...parquet
│ │ └── _SUCCESS
│ └── datasets/ # Raw input datasets
│ ├── telco-customer-churn.zip
│ └── WA_Fn-UseC-Telco-Customer-Churn.csv
├── process.md
├── readme.md
└── spark-data/ # Directory for Spark-related temporary/external data
