from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as _sum
import argparse
import logging
import sys
import traceback

# Set up basic logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_data_with_spark(input_path, output_dir):
    """
    Load data using Spark, perform transformations, and save the results.
    """
    try:
        # Create a Spark session
        spark = SparkSession.builder \
            .appName("Webshop Transformations") \
            .getOrCreate()
        logger.info("Spark session initialized successfully.")

        # Read data from the input path
        try:
            logger.info(f"The given input path is: {input_path}")
            df = spark.read.csv(input_path, header=True, inferSchema=True)
            logger.info(f"Data loaded successfully from input path: {input_path}")
        except Exception as e:
            logger.error(f"Error reading data from input path {input_path}: {e}")
            logger.error(traceback.format_exc())
            raise

        # Step 1: Count unique events per event_type
        try:
            logger.info("Transformation is moving to step 1: Counting unique events per event_type.")
            event_counts = df.groupBy("event_type").agg(count("*").alias("event_count"))
            event_counts.write.csv(f"{output_dir}/event_counts", header=True, mode="overwrite")
            logger.info("Step 1 completed successfully: Event counts written to output.")
        except Exception as e:
            logger.error(f"Error in step 1 (counting events): {e}")
            logger.error(traceback.format_exc())
            raise

        # Step 2: Count unique product_id occurrences per event_type
        try:
            logger.info("Transformation is moving to step 2: Counting unique product_id occurrences.")
            product_event_counts = df.groupBy("event_type").agg(count("product_id").alias("product_count"))
            product_event_counts.write.csv(f"{output_dir}/product_event_counts", header=True, mode="overwrite")
            logger.info("Step 2 completed successfully: Product event counts written to output.")
        except Exception as e:
            logger.error(f"Error in step 2 (counting product occurrences): {e}")
            logger.error(traceback.format_exc())
            raise

        # Step 3: Aggregate the total prices per product_id
        try:
            logger.info("Transformation is moving to step 3: Aggregating total prices per product_id.")
            total_prices = df.groupBy("product_id").agg(_sum("price").alias("total_price"))
            total_prices.write.csv(f"{output_dir}/total_prices", header=True, mode="overwrite")
            logger.info("Step 3 completed successfully: Total prices written to output.")
        except Exception as e:
            logger.error(f"Error in step 3 (calculating total prices): {e}")
            logger.error(traceback.format_exc())
            raise

        # Stop the Spark session
        try:
            spark.stop()
            logger.info("Spark session stopped successfully.")
        except Exception as e:
            logger.error(f"Error stopping Spark session: {e}")
            logger.error(traceback.format_exc())
            raise

    except Exception as e:
        logger.error(f"An unexpected error occurred during the transformation process: {e}")
        logger.error(traceback.format_exc())
        raise

if __name__ == "__main__":
    # Parse arguments from the command line
    parser = argparse.ArgumentParser(description="Spark Data Transformation Script")
    parser.add_argument("--input_path", required=True, help="Path to the input CSV file")
    parser.add_argument("--output_dir", required=True, help="Directory to save the transformed data")
    args = parser.parse_args()

    logger.info("Starting the transformation process...")
    logger.info(f"Input Path: {args.input_path}")
    logger.info(f"Output Directory: {args.output_dir}")

    try:
        transform_data_with_spark(args.input_path, args.output_dir)
        logger.info("Transformation process completed successfully.")
    except Exception as e:
        logger.error(f"Transformation process failed: {e}")
        sys.exit(1)