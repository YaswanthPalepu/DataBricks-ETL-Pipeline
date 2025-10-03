# Databricks Telco Churn Prediction Project

A comprehensive guide to setting up and running the data pipeline for the Databricks Telco Churn Prediction project. This document covers environment setup using Docker, execution of Spark-based data processing stages, and loading final data to PostgreSQL, all designed to facilitate churn prediction analysis.

## ✨ Features

*   **Dockerized Environment**: Manages Spark, Hadoop, PostgreSQL, and Jupyter services for consistent execution.
*   **Multi-Stage Data Pipeline**: Processes raw Telco churn data through Bronze, Silver, and Gold layers using Spark.
*   **AWS S3 Connectivity**: Configured to interact with S3-compatible storage (e.g., MinIO) for data ingress/egress.
*   **PostgreSQL Integration**: Loads processed Gold layer data into a PostgreSQL database for analytical querying.
*   **Jupyter Notebook Access**: Provides a Jupyter Lab environment for interactive data analysis and pipeline development.
*   **Reproducible Setup**: Uses `docker-compose` for easy spin-up and tear-down of the entire development stack.

## 🚀 Getting Started

Follow these steps to set up and run the Databricks Telco Churn Prediction Project locally.

### Prerequisites

Before you begin, ensure you have the following installed on your system:

*   **Git:** For cloning the repository.
*   **Docker Desktop:** Includes Docker Engine and Docker Compose. This is essential for running the project's services.

### 1. Installation Steps

1.  **Clone the Repository:**
    First, clone the project repository to your local machine:
    ```bash
    git clone <your-repository-url>
    cd Databricks
    ```

2.  **Docker Environment Setup:**
    The project uses Docker Compose to manage Spark, Hadoop, PostgreSQL, and Jupyter services.
    *   **Start the Docker services:**
        Navigate to the root directory of the cloned project (where `docker-compose.yml` is located) and run:
        ```bash
        docker-compose up -d
        ```
        This command will build and start all defined services in detached mode. It might take some time on the first run as it downloads images and builds containers.
    *   **Verify services are running:**
        You can check the status of your running containers:
        ```bash
        docker-compose ps
        ```
        You should see `spark`, `jupyter`, `postgres`, and `spark-hadoop-s3` (or similar names) containers listed as `Up`.

3.  **Copy Jars to Spark Container:**
    The Spark container requires specific JARs for AWS S3 connectivity.
    *   **Copy the JAR files:**
        Execute the following commands from the root `Databricks` directory to copy the necessary JARs into the Spark container's `/opt/spark/jars` directory:
        ```bash
        docker cp ./jars/aws-java-sdk-bundle-1.12.340.jar spark:/opt/spark/jars/
        docker cp ./jars/hadoop-aws-3.3.4.jar spark:/opt/spark/jars/
        ```
        *Note: The `spark` in the command refers to the name of your Spark service in `docker-compose.yml`. Adjust if your service name is different.*

4.  **Install Python Packages in Spark Container:**
    The data pipeline and notebooks depend on specific Python libraries. These need to be installed inside the Spark container.
    *   **Access the Spark container's shell:**
        ```bash
        docker exec -it spark bash
        ```
    *   **Install Python dependencies:**
        Once inside the Spark container, install `pyspark`, `pandas`, `psycopg2-binary`, and `minio`:
        ```bash
        pip install pyspark pandas psycopg2-binary minio
        ```
        *Note: While `pyspark` is typically available in a Spark environment, it's good practice to ensure it's installed if you're running scripts directly. `psycopg2-binary` is for PostgreSQL connectivity from Python, and `minio` is if you're interacting with an S3-compatible storage like MinIO.*
    *   **Exit the container shell:**
        ```bash
        exit
        ```

### 2. Running the Data Pipeline

The data pipeline processes the raw data through Bronze, Silver, and Gold layers.

1.  **Preparing the Raw Data:**
    Ensure the raw `WA_Fn-UseC_-Telco-Customer-Churn.csv` file is located in `notebooks/datasets/`. If you downloaded the `telco-customer-churn.zip`, extract it to place the CSV file correctly.

2.  **Executing the Pipeline Stages:**
    The pipeline scripts are located in `my-notebooks/telco-pipeline/stages/`. You can run these using `spark-submit` from your local machine (which will execute them in the Dockerized Spark environment).

    *   **Run the Bronze stage:**
        ```bash
        docker exec spark spark-submit \
            --master spark://spark:7077 \
            --jars /opt/spark/jars/aws-java-sdk-bundle-1.12.340.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar \
            /home/project/my-notebooks/telco-pipeline/stages/bronze.py
        ```
    *   **Run the Silver stage:**
        ```bash
        docker exec spark spark-submit \
            --master spark://spark:7077 \
            --jars /opt/spark/jars/aws-java-sdk-bundle-1.12.340.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar \
            /home/project/my-notebooks/telco-pipeline/stages/silver.py
        ```
    *   **Run the Gold stage:**
        ```bash
        docker exec spark spark-submit \
            --master spark://spark:7077 \
            --jars /opt/spark/jars/aws-java-sdk-bundle-1.12.340.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar \
            /home/project/my-notebooks/telco-pipeline/stages/gold.py
        ```
    *   **Explanation of `spark-submit` flags:**
        *   `--master spark://spark:7077`: Connects to the Spark master running in the `spark` container on port 7077.
        *   `--jars ...`: Specifies the extra JAR files needed for S3/MinIO connectivity. These must be accessible within the Spark container.
        *   `/home/project/...`: The path to the Python script inside the Spark container. The `docker-compose.yml` mounts the host's project directory to `/home/project` in the containers.

### 3. Loading Gold Data to PostgreSQL

After the Gold layer has been generated (located in `notebooks/data/gold/`), you can load these tables into the PostgreSQL database.

1.  **Run the PostgreSQL loader script:**
    ```bash
    docker exec spark spark-submit \
        --master spark://spark:7077 \
        --jars /opt/spark/jars/aws-java-sdk-bundle-1.12.340.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar \
        /home/project/loader/load_gold_to_postgres.py
    ```
    This script will read the Gold layer Parquet files and insert them into corresponding tables in the `postgres` database.

2.  **Verify data in PostgreSQL (Optional):**
    You can connect to the PostgreSQL container and check the tables:
    ```bash
    docker exec -it postgres psql -U user -d mydatabase
    ```
    Once in the `psql` prompt, you can list tables (`\dt`) and select data (`SELECT * FROM dim_customer LIMIT 5;`).

### 4. Accessing Jupyter Notebooks

Jupyter Lab is set up to run in a separate Docker container, accessible from your web browser.

1.  **Open Jupyter Lab:**
    Open your web browser and navigate to: `http://localhost:8888`
    You will likely be prompted for a token. You can find the token in your Docker logs or by executing:
    ```bash
    docker logs jupyter
    ```
    Look for a line similar to `http://127.0.0.1:8888/lab?token=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`.

2.  **Explore Notebooks:**
    Inside Jupyter Lab, you can navigate to `my-notebooks/` to open and run the provided notebooks (`real_one.ipynb`, `testing.ipynb`, etc.) to interact with Spark and analyze data.

## 🖥️ Usage

1.  Follow the "Getting Started" steps to set up and run all Docker services, install dependencies, and execute the data pipeline.
2.  Once the Gold data is loaded into PostgreSQL, you can access Jupyter Lab via `http://localhost:8888` to perform interactive analysis on the processed data.
3.  The pipeline ensures that the `my-notebooks/data/gold/` directory contains the final processed Parquet files, which are then loaded into PostgreSQL.

## 6. Stopping the Environment

When you are finished working, stop and remove the Docker containers:
```bash
docker-compose down
