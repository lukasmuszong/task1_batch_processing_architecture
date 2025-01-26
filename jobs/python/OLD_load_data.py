import os
import pandas as pd
from sqlalchemy import create_engine

print('Current working directory: {}'.format(os.getcwd()))

def load_csv_to_postgres():
    db_username = 'airflow' # os.getenv('DB_USERNAME')
    db_password = 'airflow' # os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT', '5432')  # Default port is 5432
    db_name = 'postgresQL' # os.getenv('DB_NAME')
    table_name = 'airflow' # os.getenv('TABLE_NAME')
    csv_file_path = '/opt/airflow/data/new_month/2019-Oct.csv' # os.getenv('CSV_FILE_PATH')

    # Create the SQLAlchemy engine for PostgreSQL
    engine = create_engine(os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'))

    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file_path)

    # Load the DataFrame into the PostgreSQL table
    try:
        print('Connecting to: {}'.format(os.getenv('DB_HOST')))
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data from {csv_file_path} has been successfully loaded into {table_name} in the {db_name} database.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    load_csv_to_postgres()


    # add functionality to remove the file and copy it into a "processes" folder
    # so that the file disappears from "new raw data" folder
    # Add this step into the orchestration flow