import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date
from misc.parameters import JARS_PATH, EXECUTOR_MEMORY, EXECUTOR_CORES, EXECUTOR_INSTANCES, DRIVER_MEMORY, JDBC_URL


def load_csv_to_postgres():
    # Database connection details
    db_url = JDBC_URL  # PostgreSQL connection string
    table_name = 'airflow'  # Target table in PostgreSQL
    csv_file_path = '/opt/airflow/data/new_month/2019-Oct.csv'  # Path to the CSV file
    processed_folder = '/opt/airflow/data/processed'  # Folder to move processed files

    # Check if the file exists
    if not os.path.exists(csv_file_path):
        print("ERROR: CSV file not found.")
        print(f"CSV file not found: {csv_file_path}")
        return

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Load CSV to PostgreSQL") \
        .config("spark.jars", JARS_PATH) \
        .config("spark.executor.memory", EXECUTOR_MEMORY) \
        .config("spark.executor.cores", EXECUTOR_CORES) \
        .config("spark.executor.instances", EXECUTOR_INSTANCES) \
        .config("spark.driver.memory", DRIVER_MEMORY) \
        .getOrCreate()

    try:
        # Read CSV file into a Spark DataFrame
        print(f"Reading CSV file: {csv_file_path}")
        try:
            df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(csv_file_path)
        except Exception as e:
            raise RuntimeError(f"Failed to read the CSV file: {csv_file_path}. Error: {e}")

        # Add a column to track when the data was processed (optional)
        df = df.withColumn("processed_date", lit(current_date()))

        # Write the DataFrame to PostgreSQL
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

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Stop the Spark session
        spark.stop()


if __name__ == "__main__":
    load_csv_to_postgres()