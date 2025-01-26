import psycopg2
from misc.parameters import JDBC_URL, INTERMEDIATE_PROCESSING_TABLE
from misc.secrets import POSTGRES_USER, POSTGRES_PASSWORD


def drop_postgres_table(table_name):
    """
    Drops a specified table from PostgreSQL.

    :param table_name: Name of the table to be dropped.
    :raises: RuntimeError if the table cannot be dropped or if there is an error with the connection.
    """
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=JDBC_URL.split('/')[-1],  # Extract the database name from JDBC_URL
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )

        # Create a cursor object
        cur = conn.cursor()

        # Construct the DROP TABLE query
        drop_table_query = f"DROP TABLE IF EXISTS {table_name};"

        # Execute the query
        print(f"Dropping table {table_name}...")
        cur.execute(drop_table_query)

        # Commit the transaction
        conn.commit()

        print(f"Table {table_name} has been successfully dropped.")

    except Exception as e:
        print(f"An error occurred while dropping the table {table_name}: {e}")
        raise RuntimeError(f"Failed to drop table {table_name}. Error: {e}")

    finally:
        # Close the cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    # Dropping the intermediate processing table
    table_name = INTERMEDIATE_PROCESSING_TABLE
    drop_postgres_table(table_name)