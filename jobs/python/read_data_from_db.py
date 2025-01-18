import pandas as pd
from sqlalchemy import create_engine
import os

def fetch_table_to_dataframe(table_name, output_path="/opt/airflow/data/"):
    """
    Fetches data from a PostgreSQL table and saves it as a CSV file using SQLAlchemy.

    :param table_name: Name of the PostgreSQL table to fetch data from.
    :param output_path: Directory to save the CSV file.
    """
    # PostgreSQL connection parameters
    postgres_config = {
        'host': 'postgres',  # Update with your PostgreSQL host in the Docker Compose setup
        'database': 'airflow',
        'user': 'airflow',
        'password': 'airflow',
        'port': 5432
    }

    # Construct the SQLAlchemy database URL
    db_url = f"postgresql://{postgres_config['user']}:{postgres_config['password']}@" \
             f"{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"

    try:
        # Create the SQLAlchemy engine
        engine = create_engine(db_url)

        # Use Pandas to execute SQL and fetch data
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)

        # Log DataFrame details
        print(f"Fetched {df.shape[0]} rows and {df.shape[1]} columns from {table_name}.")

        # Save the DataFrame to a CSV file
        # csv_file = f"{output_path}{table_name}_data.csv"
        csv_file = f"{output_path}raw_data.csv"
        os.makedirs(output_path, exist_ok=True)  # Ensure the directory exists
        df.to_csv(csv_file, index=False)
        print(f"Data saved to {csv_file}.")

    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        if 'engine' in locals():
            engine.dispose()  # Clean up the connection pool