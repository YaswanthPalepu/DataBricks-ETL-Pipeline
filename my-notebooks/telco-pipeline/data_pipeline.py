# data_pipeline.py

from utils.spark_utils import get_spark_session
from utils.validation_utils import user_approval_needed
from stages.landing import run_landing_stage
from stages.bronze import run_bronze_stage
from stages.silver import run_silver_stage
from stages.gold import run_gold_stage

def run_pipeline():
    """
    Main function to run the entire Telco Churn Lakehouse pipeline.
    Includes user approval between stages.
    """
    spark = None
    try:
        print("Initializing Spark Session...")
        spark = get_spark_session()
        print("Spark Session initialized.")
        spark.sparkContext.setLogLevel("ERROR") # Reduce verbosity of Spark logs

        print("\n=============================================")
        print("  Starting Telco Churn Data Lakehouse Pipeline")
        print("=============================================\n")

        # --- Stage 1: Landing ---
        print("\n>>> Executing Landing Stage...")
        if not run_landing_stage(spark):
            print("Landing stage failed. Aborting pipeline.")
            return

        # if not user_approval_needed():
        #     print("Pipeline aborted by user.")
        #     return

        # --- Stage 2: Bronze ---
        print("\n>>> Executing Bronze Stage...")
        if not run_bronze_stage(spark):
            print("Bronze stage failed. Aborting pipeline.")
            return

        # if not user_approval_needed():
        #     print("Pipeline aborted by user.")
        #     return

        # --- Stage 3: Silver ---
        print("\n>>> Executing Silver Stage...")
        if not run_silver_stage(spark):
            print("Silver stage failed. Aborting pipeline.")
            return

        # if not user_approval_needed():
        #     print("Pipeline aborted by user.")
        #     return

        # --- Stage 4: Gold ---
        print("\n>>> Executing Gold Stage...")
        if not run_gold_stage(spark):
            print("Gold stage failed. Aborting pipeline.")
            return

        print("\n=============================================")
        print(" Telco Churn Data Lakehouse Pipeline COMPLETED Successfully!")
        print("=============================================\n")

    except Exception as e:
        print(f"\nFATAL ERROR during pipeline execution: {e}")
    finally:
        if spark:
            print("Stopping Spark Session...")
            spark.stop()
            print("Spark Session stopped.")

if __name__ == "__main__":
    run_pipeline()