from pyspark.sql import SparkSession
from misc.parameters import JDBC_URL, INTERMEDIATE_PROCESSING_TABLE, PRODUCT_MASTER_TABLE
from misc.secrets import POSTGRES_USER, POSTGRES_PASSWORD


def update_product_master_data():
    """
    Updates the product_master_data table with unique records from the product table.
    Ensures no duplicates in the product_master_data table.

    :raises RuntimeError: If there is an error reading from or writing to PostgreSQL.
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Update Product Master Data in PostgreSQL") \
        .getOrCreate()

    # Define PostgreSQL connection properties
    db_url = JDBC_URL
    user = POSTGRES_USER
    password = POSTGRES_PASSWORD

    try:
        # Step 1: Read new product data from the processing table
        product_df = spark.read.format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", INTERMEDIATE_PROCESSING_TABLE) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        print(f"Loaded data from {INTERMEDIATE_PROCESSING_TABLE} table successfully.")

        # Step 2: Read existing product master data
        try:
            master_df = spark.read.format("jdbc") \
                .option("url", db_url) \
                .option("dbtable", PRODUCT_MASTER_TABLE) \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .load()

            print(f"Loaded data from {PRODUCT_MASTER_TABLE} table successfully.")
        except Exception as e:
            # If the master table does not exist, initialize it as empty
            print(f"{PRODUCT_MASTER_TABLE} table does not exist or could not be loaded: {e}")
            master_df = spark.createDataFrame([], product_df.schema)

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
        combined_df = master_df.union(unique_product_df).dropDuplicates()

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