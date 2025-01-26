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
# New webshop data folder
DATA_PATH = "/opt/airflow/data/new_month/"