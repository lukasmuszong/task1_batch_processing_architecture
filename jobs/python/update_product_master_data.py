from sqlalchemy import (create_engine, MetaData, Table, Column,
                        BigInteger, String)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from misc.parameters import (JDBC_URL, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
                             INTERMEDIATE_PROCESSING_TABLE, PRODUCT_MASTER_TABLE,
                             JARS_PATH, EXECUTOR_MEMORY, EXECUTOR_CORES,
                             EXECUTOR_INSTANCES, DRIVER_MEMORY)
from misc.secrets import POSTGRES_USER, POSTGRES_PASSWORD


def ensure_table_exists(table_name):
    """
    Ensures a specified table exists in PostgreSQL using SQLAlchemy.
    If the table does not exist, it is created.

    :param table_name: Name of the table to ensure exists.
    :raises: RuntimeError if the table cannot be created or if there is an error with the connection.
    """
    try:
        # Extract database name and construct the SQLAlchemy connection string
        db_name = POSTGRES_DB
        host = POSTGRES_HOST
        port = POSTGRES_PORT

        # Construct the SQLAlchemy connection string
        connection_string = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{host}:{port}/{db_name}'

        # Create SQLAlchemy engine
        engine = create_engine(connection_string)
        metadata = MetaData()

        # Define the table schema
        product_master_table = Table(
            table_name,
            metadata,
            Column("product_id", BigInteger, nullable=False),
            Column("category_id", BigInteger, nullable=True),
            Column("category_code", String, nullable=True),
            Column("category_hierarchy_1", String, nullable=True),
            Column("category_hierarchy_2", String, nullable=True),
            Column("category_hierarchy_3", String, nullable=True),
            Column("category_hierarchy_4", String, nullable=True),
            Column("brand", String, nullable=True)
        )

        # Create a connection and ensure the table exists
        with engine.connect() as conn:
            # Use SQLAlchemy's metadata to check and create the table if necessary
            print(f"Ensuring table {table_name} exists...")
            metadata.create_all(bind=engine)
            print(f"Table {table_name} has been successfully created or already exists.")

    except Exception as e:
        print(f"An error occurred while ensuring the table {table_name} exists: {e}")
        raise RuntimeError(f"Failed to ensure table {table_name}. Error: {e}")


def split_category_code(product_df):
    """
    Splits the category_code column into hierarchical categories.

    :param product_df: Spark DataFrame containing the product data.
    :return: Spark DataFrame with additional category hierarchy columns.
    """
    return product_df \
        .withColumn("category_hierarchy_1", split(col("category_code"), "\\.").getItem(0)) \
        .withColumn("category_hierarchy_2", split(col("category_code"), "\\.").getItem(1)) \
        .withColumn("category_hierarchy_3", split(col("category_code"), "\\.").getItem(2)) \
        .withColumn("category_hierarchy_4", split(col("category_code"), "\\.").getItem(3))



def update_product_master_data():
    """
    Updates the product_master_data table with unique records from the product table.
    Ensures no duplicates in the product_master_data table.

    :raises RuntimeError: If there is an error reading from or writing to PostgreSQL.
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Update Product Master Data in PostgreSQL") \
        .config("spark.jars", JARS_PATH) \
        .config("spark.executor.memory", EXECUTOR_MEMORY) \
        .config("spark.executor.cores", EXECUTOR_CORES) \
        .config("spark.executor.instances", EXECUTOR_INSTANCES) \
        .config("spark.driver.memory", DRIVER_MEMORY) \
        .getOrCreate()

    # Define PostgreSQL connection properties
    db_url = JDBC_URL
    user = POSTGRES_USER
    password = POSTGRES_PASSWORD

    try:
        # Step 1: Ensure the product_master_data table exists
        ensure_table_exists(PRODUCT_MASTER_TABLE)

        # Step 2: Read new product data from the processing table
        product_df = spark.read.format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", INTERMEDIATE_PROCESSING_TABLE) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        print(f"Loaded data from {INTERMEDIATE_PROCESSING_TABLE} table successfully.")

        # Step 2.1: Check if the intermediate_processing_table schema matches the product_master_table schema
        try:
            master_df = spark.read.format("jdbc") \
                .option("url", db_url) \
                .option("dbtable", PRODUCT_MASTER_TABLE) \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .load()

            print(f"Loaded data from {PRODUCT_MASTER_TABLE} table successfully.")

            # Compare schemas
            required_columns = {"product_id", "category_id", "category_code",
                                "category_hierarchy_1", "category_hierarchy_2",
                                "category_hierarchy_3", "category_hierarchy_4", "brand"}

            product_df_columns = set(product_df.columns)

            if not required_columns.issubset(product_df_columns):
                print("Schema mismatch detected. Applying split_category_code function...")
                product_df = split_category_code(product_df)

        except Exception as e:
            # If the master table does not exist, assume it requires splitting category_code
            print(f"{PRODUCT_MASTER_TABLE} table could not be loaded, initializing schema transformation: {e}")
            product_df = split_category_code(product_df)

        # Step 3: Deduplicate new product data
        unique_product_df = product_df.select(
            "product_id",
            "category_id",
            "category_code",
            "category_hierarchy_1",
            "category_hierarchy_2",
            "category_hierarchy_3",
            "category_hierarchy_4",
            "brand"
        ).dropDuplicates()

        # Step 4: Combine with existing master data and remove duplicates
        try:
            master_df = master_df.select(*unique_product_df.columns)  # Ensure compatibility
        except NameError:
            # master_df was not loaded successfully, initialize as empty
            master_df = spark.createDataFrame([], unique_product_df.schema)

        combined_df = master_df.unionByName(unique_product_df, allowMissingColumns=True).dropDuplicates()

        # Step 5: Write the updated product master data back to PostgreSQL
        combined_df.write.format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", PRODUCT_MASTER_TABLE) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

        print(f"Updated {PRODUCT_MASTER_TABLE} table with new unique records successfully.")

    except Exception as e:
        raise RuntimeError(f"Error updating {PRODUCT_MASTER_TABLE} table: {e}")

    finally:
        # Stop the Spark session
        spark.stop()

if __name__ == "__main__":
    update_product_master_data()