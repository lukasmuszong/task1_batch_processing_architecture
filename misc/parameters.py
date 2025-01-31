# Path to the JDBC driver
JARS_PATH = "/opt/airflow/spark/jars/postgresql-42.7.4.jar"
EXECUTOR_MEMORY = "2g"
EXECUTOR_CORES = "2"
EXECUTOR_INSTANCES = "3"
DRIVER_MEMORY = "4g"
JDBC_URL = "jdbc:postgresql://postgres:5432/airflow"
DB_PROPERTIES = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}
POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "airflow"
# Folder containing CSV files
NEW_DATA_PATH = "/opt/airflow/data/new_month/"
PROCESSED_DATA_PATH = "/opt/airflow/data/processed/"
INTERMEDIATE_PROCESSING_TABLE = "airflow"
WEBSHOP_ACTIVITIES_TABLE = "webshop_activity_data"
PRODUCT_MASTER_TABLE = "product_master_data"