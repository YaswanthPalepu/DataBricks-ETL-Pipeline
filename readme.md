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
