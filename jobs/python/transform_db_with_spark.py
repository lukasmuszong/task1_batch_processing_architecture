from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import count, sum as _sum, countDistinct, col, split, row_number

class PostgresIntegration:
    def __init__(self, app_name, jars_path, executor_memory, executor_cores, executor_instances, driver_memory):
        """
        Initializes the Spark session with the given configurations.
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
        """
        try:
            print(f"Reading data from table: {table_name}")
            self.df = self.spark.read.jdbc(url=jdbc_url, table=table_name, properties=db_properties)
            print(f"Table '{table_name}' successfully loaded into DataFrame.")
        except Exception as e:
            print(f"Error reading table {table_name}: {e}")
            raise

    def show_dataframe(self, rows=10):
        """
        Displays the specified number of rows from the class's DataFrame.
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

    def count_unique_events(self, output_path=None):
        """
        Counts unique events per event_type in the class's DataFrame.
        """
        if self.df is None:
            print("No DataFrame is currently loaded. Please load the data first.")
            return

        print("Counting unique events per event_type...")
        try:
            # Group by 'event_type' and count the events
            event_counts = self.df.groupBy("event_type").agg(count("*").alias("event_count"))

            # Optionally save the output to the specified path
            if output_path:
                event_counts.write.csv(output_path, header=True, mode="overwrite")
                print(f"Event counts written to: {output_path}")

            return event_counts
        except Exception as e:
            print(f"Error while counting unique events: {e}")
            raise

    def count_unique_products(self, output_path=None):
        """
        Counts unique product_id occurrences per event_type in the class's DataFrame.
        """
        if self.df is None:
            print("No DataFrame is currently loaded. Please load the data first.")
            return

        print("Counting unique product_id occurrences per event_type...")
        try:
            # Group by 'event_type' and count unique product_id occurrences
            product_event_counts = self.df.groupBy("event_type").agg(count("product_id").alias("product_count"))

            # Optionally save the output to the specified path
            if output_path:
                product_event_counts.write.csv(output_path, header=True, mode="overwrite")
                print(f"Product event counts written to: {output_path}")

            return product_event_counts
        except Exception as e:
            print(f"Error while counting unique product_id occurrences: {e}")
            raise

    def aggregate_total_prices(self, output_path=None):
        """
        Aggregates the total prices per product_id in the class's DataFrame.
        """
        if self.df is None:
            print("No DataFrame is currently loaded. Please load the data first.")
            return

        print("Aggregating total prices per product_id...")
        try:
            # Group by 'product_id' and aggregate the total prices
            total_prices = self.df.groupBy("product_id").agg(_sum("price").alias("total_price"))

            # Optionally save the output to the specified path
            if output_path:
                total_prices.write.csv(output_path, header=True, mode="overwrite")
                print(f"Total prices written to: {output_path}")

            return total_prices
        except Exception as e:
            print(f"Error while aggregating total prices: {e}")
            raise


    def split_category_code(self):
        """
        Split the 'category_code' column by '.' and add new columns to the DataFrame.
        Each new column represents a part of the split category code.
        The columns are named category_hierarchy_1, category_hierarchy_2, etc.
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
        Add a column 'purchase_movement' to the DataFrame that increases based on
        the keys product_id and user_session, as the event_time transitions into the future.
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
        Stops the Spark session.
        """
        self.spark.stop()
        print("Spark session stopped successfully.")

# Configuration parameters
APP_NAME = "Postgres Integration"
JARS_PATH = "/opt/airflow/spark/jars/postgresql-42.7.4.jar"
EXECUTOR_MEMORY = "2g"
EXECUTOR_CORES = "2"
EXECUTOR_INSTANCES = "3"
DRIVER_MEMORY = "4g"

JDBC_URL = "jdbc:postgresql://postgres:5432/airflow"
TABLE_NAME = "airflow"
DB_PROPERTIES = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

if __name__ == "__main__":
    # Initialize PostgresIntegration class
    postgres_integration = PostgresIntegration(
        app_name=APP_NAME,
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
            table_name=TABLE_NAME,
            db_properties=DB_PROPERTIES
        )

        # Access the DataFrame from the class instance's property
        dataframe = postgres_integration.df

        # Display contents of the DataFrame
        postgres_integration.show_dataframe(rows=10)

        # Apply transformations
        dataframe = postgres_integration.split_category_code()
        dataframe = postgres_integration.add_purchase_movement()


        # Perform transformations and save results
        # event_counts = postgres_integration.count_unique_events(output_path="output/event_counts")
        # product_event_counts = postgres_integration.count_unique_products(output_path="output/product_event_counts")
        # total_prices = postgres_integration.aggregate_total_prices(output_path="output/total_prices")



    except Exception as e:
        print(f"An error occurred: {e}")

    # Stop the Spark session
    postgres_integration.stop()