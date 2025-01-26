import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date
from misc.parameters import JARS_PATH, NEW_DATA_PATH, EXECUTOR_MEMORY, EXECUTOR_CORES, EXECUTOR_INSTANCES, DRIVER_MEMORY, JDBC_URL


def load_csvs_to_postgres():
    """
        Loads CSV files from a specified folder into PostgreSQL.
        - Reads all `.csv` files from the specified base folder.
        - Creates a backup table in PostgreSQL for each file, named `backup_FILENAME`.
        - Ensures no file overwrites another by dynamically naming PostgreSQL tables.

        Steps:
            1. Iterate over all `.csv` files in the folder.
            2. Load each file into a Spark DataFrame.
            3. Add a column for the `processed_date` to track when the data was processed.
            4. Write the DataFrame to a PostgreSQL table named `backup_FILENAME`.

        Exception Handling:
            - If a file cannot be processed, an error message is printed, and the script continues with the next file.

        Folder Structure:
            - Input Folder: base folder specified in `misc.parameters`.

        Dependencies:
            - Spark JARs and configuration settings from `misc.parameters`.

        Raises:
            - RuntimeError for any issues with reading or writing files.

        Returns:
            None
        """
    # Database connection details
    db_url = JDBC_URL  # PostgreSQL connection string
    base_folder = NEW_DATA_PATH  # Folder containing CSV files

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Load CSVs to PostgreSQL") \
        .config("spark.jars", JARS_PATH) \
        .config("spark.executor.memory", EXECUTOR_MEMORY) \
        .config("spark.executor.cores", EXECUTOR_CORES) \
        .config("spark.executor.instances", EXECUTOR_INSTANCES) \
        .config("spark.driver.memory", DRIVER_MEMORY) \
        .getOrCreate()

    try:
        # Iterate over all files in the base folder
        for filename in os.listdir(base_folder):
            if filename.endswith(".csv"):
                csv_file_path = os.path.join(base_folder, filename)
                # Replace problematic characters (like '-') in the table name
                backup_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', f"backup_{os.path.splitext(filename)[0]}")

                print(f"Processing file: {csv_file_path} -> Backup table: {backup_table_name}")

                try:
                    # Read CSV file into a Spark DataFrame
                    df = spark.read.format("csv") \
                        .option("header", "true") \
                        .option("inferSchema", "true") \
                        .load(csv_file_path)

                    # Add a column to track when the data was processed
                    df = df.withColumn("processed_date", lit(current_date()))

                    # Write the DataFrame to PostgreSQL (as a backup table)
                    print(f"Writing data to PostgreSQL table: {backup_table_name}...")
                    df.write \
                        .format("jdbc") \
                        .option("url", db_url) \
                        .option("dbtable", backup_table_name) \
                        .option("user", "airflow") \
                        .option("password", "airflow") \
                        .option("driver", "org.postgresql.Driver") \
                        .mode("overwrite") \
                        .save()

                    print(
                        f"Data from {csv_file_path} has been successfully loaded into the {backup_table_name} table.")

                except Exception as e:
                    print(f"Error processing file {csv_file_path}: {e}")

    except Exception as e:
        print(f"An error occurred during processing: {e}")

    finally:
        # Stop the Spark session
        spark.stop()


if __name__ == "__main__":
    load_csvs_to_postgres()