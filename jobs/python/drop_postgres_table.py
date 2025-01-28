from sqlalchemy import create_engine, text
from misc.parameters import (POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
                             INTERMEDIATE_PROCESSING_TABLE)
from misc.secrets import POSTGRES_USER, POSTGRES_PASSWORD


def drop_postgres_table(table_name):
    """
    Drops a specified table from PostgreSQL using SQLAlchemy.

    :param table_name: Name of the table to be dropped.
    :raises: RuntimeError if the table cannot be dropped or if there is an error with the connection.
    """
    try:
        # DB access variables
        db_name = POSTGRES_DB
        host = POSTGRES_HOST
        port = POSTGRES_PORT

        # SQLAlchemy connection string
        connection_string = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{host}:{port}/{db_name}'

        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        # Create a connection
        with engine.connect() as conn:
            # DROP TABLE query
            drop_table_query = f"DROP TABLE IF EXISTS {table_name};"

            # Execute the query
            print(f"Dropping table {table_name}...")
            conn.execute(text(drop_table_query))

        print(f"Table {table_name} has been successfully dropped.")

    except Exception as e:
        print(f"An error occurred while dropping the table {table_name}: {e}")
        raise RuntimeError(f"Failed to drop table {table_name}. Error: {e}")


if __name__ == "__main__":
    # Dropping the intermediate processing table
    table_name = INTERMEDIATE_PROCESSING_TABLE
    drop_postgres_table(table_name)