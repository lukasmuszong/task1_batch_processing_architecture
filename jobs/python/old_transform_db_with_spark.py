from pyspark.sql import SparkSession

# Initialize Spark Session with executor settings
spark = SparkSession.builder \
    .appName("Postgres Integration") \
    .config("spark.jars", "/opt/airflow/spark/jars/postgresql-42.7.4.jar") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "3") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Verify Spark Configuration
print("Spark Configuration:")
for key, value in spark.sparkContext.getConf().getAll():
    print(f"{key} = {value}")

# JDBC connection properties
jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
table_name = "airflow"
db_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# Read the table into a DataFrame
df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=db_properties)

# Perform operations on the DataFrame
df.show(10)  # Display contents of the DataFrame
