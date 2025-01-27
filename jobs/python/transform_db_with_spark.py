from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import countDistinct, col, split, row_number
from misc.parameters import (JARS_PATH, INTERMEDIATE_PROCESSING_TABLE,
                             WEBSHOP_ACTIVITIES_TABLE, EXECUTOR_MEMORY,
                             EXECUTOR_CORES, EXECUTOR_INSTANCES, DRIVER_MEMORY,
                             JDBC_URL, DB_PROPERTIES)

class PostgresIntegration:
    def __init__(self, app_name, jars_path, executor_memory, executor_cores,
                 executor_instances, driver_memory):
        """
        Initializes the Spark session with the given configurations.

        :param app_name: Name of the Spark application.
        :param jars_path: Path to the necessary JAR files (e.g., JDBC drivers).
        :param executor_memory: Memory allocated per executor (e.g., '2g').
        :param executor_cores: Number of cores allocated per executor.
        :param executor_instances: Number of executor instances.
        :param driver_memory: Memory allocated to the driver (e.g., '1g').
        """
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars", jars_path) \
            .config("spark.executor.memory", executor_memory) \
            .config("spark.executor.cores", executor_cores) \
            .config("spark.executor.instances", executor_instances) \
            .config("spark.driver.memory", driver_memory) \
            .getOrCreate()

        self.spark_context = self.spark.sparkContext
        self.df = None  # Initialize as None, will be assigned dynamically

    def print_configurations(self):
        """
        Prints the Spark configurations to verify settings.
        """
        print("Spark Configuration:")
        for key, value in self.spark_context.getConf().getAll():
            print(f"{key} = {value}")

    def read_table(self, jdbc_url, table_name, db_properties):
        """
        Reads a table from PostgreSQL into a DataFrame and stores it in self.df.

        :param jdbc_url: JDBC URL for the PostgreSQL database.
        :param table_name: Name of the table to read from the database.
        :param db_properties: Dictionary containing database properties (e.g., user, password).
        """
        try:
            print(f"Reading data from table: {table_name}")
            self.df = self.spark.read.jdbc(
                url=jdbc_url,
                table=table_name,
                properties=db_properties
            )
            print(f"Table '{table_name}' successfully loaded into DataFrame.")
        except Exception as e:
            print(f"Error reading table {table_name}: {e}")
            raise

    def show_dataframe(self, rows=10):
        """
        Displays the specified number of rows from the class's DataFrame.

        :param rows: Number of rows to display (default is 10).
        """
        if self.df is None:
            print("No DataFrame is currently loaded. Please load data first.")
            return

        print(f"Displaying the first {rows} rows of the DataFrame:")
        self.df.show(rows)

    def display_statistics(self):
        """
        Display statistics of the class's DataFrame, including unique values per column.
        """
        if self.df is None:
            print("No DataFrame is currently loaded. Please load the data first.")
            return

        print("Statistics of the DataFrame:")
        print("-" * 50)

        # Get column-wise statistics
        for column in self.df.columns:
            try:
                # Count unique values
                unique_count = self.df.select(countDistinct(col(column)).alias("unique_count")).collect()[0][
                    "unique_count"]
                print(f"Column: {column}, Unique Values Count: {unique_count}")

                # If unique values are fewer than 15, list them
                if unique_count < 15:
                    unique_values = self.df.select(col(column)).distinct().limit(15).rdd.flatMap(lambda x: x).collect()
                    print(f"--Unique '{column}' Values: {unique_values}")
                else:
                    # If unique values are 15 or more, display 5 examples of unique values
                    sample_values = self.df.select(col(column)).distinct().limit(5).rdd.flatMap(lambda x: x).collect()
                    print(f"--Sample of Unique '{column}' Values (5 examples): {sample_values}")

            except Exception as e:
                print(f"Error computing statistics for column {column}: {e}")


    def split_category_code(self):
        """
        Splits the 'category_code' column by '.' and adds new columns to the DataFrame.
        The new columns represent parts of the split category code and are named as:
        category_hierarchy_1, category_hierarchy_2, etc.

        :return: Modified DataFrame with additional columns for category hierarchy.
        """
        if self.df is None:
            print("DataFrame is not loaded. Please load the data first.")
            return None

        # Ensure the category_code column exists
        if 'category_code' not in self.df.columns:
            print("Error: 'category_code' column not found in the DataFrame.")
            return self.df

        # Split the 'category_code' column by '.' separator
        # This will create an array column with the parts of the category_code
        split_col = split(col('category_code'), '\.')

        # Add new columns to the DataFrame based on the split parts
        # Naming the columns as category_hierarchy_1, category_hierarchy_2, etc.
        self.df = self.df.withColumn("category_hierarchy_1", split_col.getItem(0)) \
            .withColumn("category_hierarchy_2", split_col.getItem(1)) \
            .withColumn("category_hierarchy_3", split_col.getItem(2)) \
            .withColumn("category_hierarchy_4", split_col.getItem(3))  # Optional, depends on your needs

        # Handle None values by filling them with empty strings or another placeholder if needed
        self.df = self.df.fillna(
            {"category_hierarchy_1": "", "category_hierarchy_2": "", "category_hierarchy_3": "",
             "category_hierarchy_4": ""})
        print("Category codes split successfully.")

        return self.df

    def add_purchase_movement(self):
        """
        Adds a 'purchase_movement' column to the DataFrame.
        The value increments based on the 'product_id' and 'user_session' columns, ordered by 'event_time'.

        :return: Modified DataFrame with the new 'purchase_movement' column.
        """
        if self.df is None:
            print("DataFrame is not loaded. Please load the data first.")
            return None

        # Ensure required columns exist
        if 'product_id' not in self.df.columns or 'user_session' not in self.df.columns or 'event_time' not in self.df.columns:
            print("Error: Required columns ('product_id', 'user_session', 'event_time') not found in the DataFrame.")
            return self.df

        # Ensure event_time is in the correct format (timestamp)
        self.df = self.df.withColumn("event_time", col("event_time").cast("timestamp"))

        # Define a window specification to partition by product_id and user_session, and order by event_time
        window_spec = Window.partitionBy("product_id", "user_session").orderBy("event_time")

        # Add a new column 'purchase_movement' based on the row number for each partition
        self.df = self.df.withColumn("purchase_movement", row_number().over(window_spec))
        print("Purchase movement columns added successfully.")

        return self.df

    def stop(self):
        """
        Stops the Spark session, releasing all resources.
        """
        self.spark.stop()
        print("Spark session stopped successfully.")

    def write_to_postgres(self, jdbc_url, table_name, db_properties, mode="append"):
        """
        Writes the current DataFrame to a PostgreSQL database table.

        :param jdbc_url: JDBC URL for the PostgreSQL database.
        :param table_name: Target table name in the PostgreSQL database.
        :param db_properties: Dictionary containing database properties (e.g., user, password).
        :param mode: Save mode for the DataFrame (e.g., 'overwrite', 'append').
        """
        if self.df is None:
            print("No DataFrame is currently loaded. Please load data first.")
            return

        try:
            print(f"Writing DataFrame to PostgreSQL table: {table_name}")
            self.df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode=mode,
                properties=db_properties
            )
            print(f"DataFrame successfully written to table: {table_name}")
        except Exception as e:
            print(f"Error writing DataFrame to table {table_name}: {e}")
            raise


if __name__ == "__main__":
    # Initialize PostgresIntegration class
    postgres_integration = PostgresIntegration(
        app_name="Data Transformation",
        jars_path=JARS_PATH,
        executor_memory=EXECUTOR_MEMORY,
        executor_cores=EXECUTOR_CORES,
        executor_instances=EXECUTOR_INSTANCES,
        driver_memory=DRIVER_MEMORY
    )

    # Print Spark configurations
    postgres_integration.print_configurations()

    # Read data from PostgreSQL
    try:
        # Load table into df (stored as a class property)
        postgres_integration.read_table(
            jdbc_url=JDBC_URL,
            table_name=INTERMEDIATE_PROCESSING_TABLE,
            db_properties=DB_PROPERTIES
        )

        # Access the DataFrame from the class instance's property
        dataframe = postgres_integration.df

        # Display contents of the DataFrame
        postgres_integration.show_dataframe(rows=10)

        # Apply transformations
        postgres_integration.split_category_code()
        postgres_integration.add_purchase_movement()
        # dataframe = postgres_integration.df

        # Write the DataFrame back to PostgreSQL
        postgres_integration.write_to_postgres(
            jdbc_url=JDBC_URL,
            table_name=WEBSHOP_ACTIVITIES_TABLE,
            db_properties=DB_PROPERTIES
        )

        # Display contents of the DataFrame
        postgres_integration.show_dataframe(rows=10)
        postgres_integration.display_statistics()


        #####

        # Add functions to this class
        # create a item database
        # create category database
        # create user database

        # Maybe set it up so that the dag only calls individual functions of this class??
        # so that the microservice architecture is complete

        #####


    except Exception as e:
        print(f"An error occurred: {e}")

    # Stop the Spark session
    postgres_integration.stop()