import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date
from pyspark.sql.types import TimestampType, StructType, LongType, StructField, StringType, DoubleType
import shutil
from misc.parameters import JARS_PATH, NEW_DATA_PATH, PROCESSED_DATA_PATH, WEBSHOP_ACTIVITIES_TABLE, EXECUTOR_MEMORY, EXECUTOR_CORES, EXECUTOR_INSTANCES, DRIVER_MEMORY, JDBC_URL
from misc.secrets import POSTGRES_USER, POSTGRES_PASSWORD

def preprocess_data():
    """
        Processes CSV files from a specified input folder and loads the data into PostgreSQL.
        - Reads all `.csv` files from the specified base folder.
        - Preprocesses data by deduplicating, checking for null values, and adding a `processed_date` column.
        - Loads the processed data into a specified PostgreSQL table.

        Steps:
            1. Iterate over all `.csv` files in the folder.
            2. For each file, read the CSV into a Spark DataFrame with a predefined schema.
            3. Deduplicate the data based on `event_time`, `user_session`, and `product_id`.
            4. Drop rows with null values in critical columns (`event_time`, `event_type`, `product_id`, `user_id`, `user_session`).
            5. Add a `processed_date` column to track when the data was processed.
            6. Write the processed DataFrame to the specified PostgreSQL table.

        Exception Handling:
            - If a file cannot be read, processed, or written to PostgreSQL, an error message is printed for that file, and the script continues with the next file.
            - Files that are successfully processed are moved to the `processed` folder to prevent reprocessing.

        Folder Structure:
            - Input Folder: base folder specified in `misc.parameters`.
            - Processed Folder: processed folder specified in `misc.parameters`.

        Dependencies:
            - Spark JARs and configuration settings from `misc.parameters`.
            - PostgreSQL connection details from `misc.secrets`.

        Raises:
            - RuntimeError for any issues with reading, processing, or writing to PostgreSQL.

        Returns:
            None
    """
    # Database connection details
    db_url = JDBC_URL  # PostgreSQL connection string
    table_name = WEBSHOP_ACTIVITIES_TABLE  # Target table in PostgreSQL
    input_folder = NEW_DATA_PATH  # Folder containing the CSV files
    processed_folder = PROCESSED_DATA_PATH  # Folder to move processed files

    # Get the list of CSV files in the input folder
    csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]

    if not csv_files:
        print("ERROR: No CSV files found in the input folder.")
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

    for csv_file in csv_files:
        csv_file_path = os.path.join(input_folder, csv_file)  # Full path to the CSV file
        print(f"Processing file: {csv_file_path}")

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

        try:
            # Read CSV file into a Spark DataFrame with schema enforcement
            df = spark.read.format("csv") \
                .option("header", "true") \
                .schema(schema) \
                .load(csv_file_path)
        except Exception as e:
            print(f"Failed to read the CSV file: {csv_file_path}. Error: {e}")
            continue  # Move on to the next file if one fails

        # Record the initial count of rows
        initial_count = df.count()
        print(f"Initial row count: {initial_count}")

        # Preprocessing steps
        print("Preprocessing data...")

        # Step 1: Deduplication
        df_deduplicated = df.dropDuplicates(["event_time", "user_session", "product_id"])
        dedup_count = initial_count - df_deduplicated.count()
        print(f"Rows dropped in deduplication: {dedup_count}")

        # Step 2: Null checks for critical columns
        df_cleaned = df_deduplicated.dropna(
            subset=["event_time", "event_type", "product_id", "user_id", "user_session"])
        null_check_count = df_deduplicated.count() - df_cleaned.count()
        print(f"Rows dropped in null checks: {null_check_count}")

        # Step 3: Add processed date if not already in the file
        df_final = df_cleaned.withColumn("processed_date", lit(current_date()))

        # Final row count after all preprocessing
        final_count = df_final.count()
        print(f"Final row count after preprocessing: {final_count}")

        # Step 4: Write the DataFrame to PostgreSQL
        print(f"Writing data to PostgreSQL table: {table_name}...")
        try:
            df_final.write \
                .format("jdbc") \
                .option("url", db_url) \
                .option("dbtable", table_name) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()
        except Exception as e:
            print(f"Failed to write data to PostgreSQL table {table_name}. Error: {e}")
            continue  # Continue processing other files even if one fails

        print(f"Data from {csv_file_path} has been successfully loaded into the {table_name} table.")

        # Move the processed file to the "processed" folder
        try:
            if not os.path.exists(processed_folder):
                os.makedirs(processed_folder)
            shutil.move(csv_file_path, os.path.join(processed_folder, os.path.basename(csv_file_path)))
            print(f"File moved to processed folder: {processed_folder}")
        except Exception as e:
            print(f"Failed to move file to processed folder. Error: {e}")


    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    preprocess_data()