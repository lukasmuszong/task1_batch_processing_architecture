import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date
from pyspark.sql.types import TimestampType, StructType, LongType, StructField, StringType, DoubleType
import shutil
from misc.parameters import JARS_PATH, EXECUTOR_MEMORY, EXECUTOR_CORES, EXECUTOR_INSTANCES, DRIVER_MEMORY, JDBC_URL


def preprocess_data():
    # Database connection details
    db_url = JDBC_URL  # PostgreSQL connection string
    table_name = 'airflow'  # Target table in PostgreSQL
    csv_file_path = '/opt/airflow/data/new_month/2019-Oct.csv'  # Path to the CSV file
    processed_folder = '/opt/airflow/data/processed'  # Folder to move processed files

    # Check if the file exists
    if not os.path.exists(csv_file_path):
        print(f"ERROR: CSV file not found: {csv_file_path}")
        return

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Preprocess and Load CSV to PostgreSQL") \
        .config("spark.jars", JARS_PATH) \
        .config("spark.executor.memory", EXECUTOR_MEMORY) \
        .config("spark.executor.cores", EXECUTOR_CORES) \
        .config("spark.executor.instances", EXECUTOR_INSTANCES) \
        .config("spark.driver.memory", DRIVER_MEMORY) \
        .getOrCreate()

    try:
        # Define schema for validation
        schema = StructType([
            StructField("event_time", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("category_id", LongType(), True),
            StructField("category_code", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("user_id", LongType(), True),
            StructField("user_session", StringType(), True),
            StructField("processed_date", TimestampType(), True)
        ])

        # Read CSV file into a Spark DataFrame with schema enforcement
        print(f"Reading CSV file: {csv_file_path}")
        try:
            df = spark.read.format("csv") \
                .option("header", "true") \
                .schema(schema) \
                .load(csv_file_path)
        except Exception as e:
            raise RuntimeError(f"Failed to read the CSV file: {csv_file_path}. Error: {e}")

        # Preprocessing steps
        print("Preprocessing data...")

        # Step 1: Deduplication
        df = df.dropDuplicates(["event_time", "user_session", "product_id"])

        # Step 2: Null checks for critical columns
        df = df.dropna(subset=["event_time", "event_type", "product_id", "user_id", "user_session"])

        # Step 3: Add processed date if not already in the file
        df = df.withColumn("processed_date", lit(current_date()))

        # Step 4: Write the DataFrame to PostgreSQL
        print("Writing data to PostgreSQL...")
        try:
            df.write \
                .format("jdbc") \
                .option("url", db_url) \
                .option("dbtable", table_name) \
                .option("user", "airflow") \
                .option("password", "airflow") \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()
        except Exception as e:
            raise RuntimeError(f"Failed to write data to PostgreSQL table {table_name}. Error: {e}")

        print(f"Data from {csv_file_path} has been successfully loaded into the {table_name} table.")

        # Move the processed file to the "processed" folder
        try:
            if not os.path.exists(processed_folder):
                os.makedirs(processed_folder)
            shutil.move(csv_file_path, os.path.join(processed_folder, os.path.basename(csv_file_path)))
            print(f"File moved to processed folder: {processed_folder}")
        except Exception as e:
            raise RuntimeError(f"Failed to move file to processed folder. Error: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Stop the Spark session
        spark.stop()


if __name__ == "__main__":
    preprocess_data()