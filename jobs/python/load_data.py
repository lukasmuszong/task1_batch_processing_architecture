import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import shutil


def load_csv_to_postgres():
    # Database connection details
    db_url = os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')  # PostgreSQL connection string
    table_name = 'airflow'  # Target table in PostgreSQL
    csv_file_path = '/opt/airflow/data/new_month/2019-Oct.csv'  # Path to the CSV file
    processed_folder = '/opt/airflow/data/processed'  # Folder to move processed files

    # Check if the file exists
    if not os.path.exists(csv_file_path):
        print(f"CSV file not found: {csv_file_path}")
        return

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Load CSV to PostgreSQL") \
        .config("spark.jars", "/path/to/postgresql-connector.jar") \
        .getOrCreate()

    try:
        # Read CSV file into a Spark DataFrame
        print(f"Reading CSV file: {csv_file_path}")
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(csv_file_path)

        # Add a column to track when the data was processed (optional)
        df = df.withColumn("processed_date", lit("2025-01-18"))

        # Write the DataFrame to PostgreSQL
        print("Writing data to PostgreSQL...")
        df.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", table_name) \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

        print(f"Data from {csv_file_path} has been successfully loaded into the {table_name} table.")

        # Move the processed file to the "processed" folder
        if not os.path.exists(processed_folder):
            os.makedirs(processed_folder)
        shutil.move(csv_file_path, os.path.join(processed_folder, os.path.basename(csv_file_path)))
        print(f"File moved to processed folder: {processed_folder}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Stop the Spark session
        spark.stop()


if __name__ == "__main__":
    load_csv_to_postgres()