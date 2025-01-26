import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta
import sys
import os
from misc.parameters import JARS_PATH

# Add the jobs/python directory to the system path for importing scripts
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../jobs/python"))

# Import the function from read_data_from_db.py
from read_data_from_db import fetch_table_to_dataframe

# Default arguments
default_args = {
    'owner': 'Lukas Muszong',
    'start_date': airflow.utils.dates.days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# Define the DAG
dag = DAG(
    dag_id='sparking_flow',
    default_args=default_args,
    schedule_interval = '@daily',
    catchup=False,
)

# Task 1: Start job
start = PythonOperator(
    task_id='start',
    python_callable= lambda: print('Jobs started'),
    dag=dag,
)
# Task 2: Load data
load_data_backup_task = SparkSubmitOperator(
    task_id='load_data_backup_from_csv_task',
    application='/opt/airflow/jobs/python/load_data_as_backup.py',
    conn_id='spark-conn',
    name='load_data_backup_from_csv_task',
    conf={
        'spark.jars': JARS_PATH,  # Path to the JDBC driver
    },
    verbose=True,
)

# Task 3: Preprocess data
preprocess_data_task = SparkSubmitOperator(
    task_id='preprocess_data_from_csv_task',
    application='/opt/airflow/jobs/python/preprocess_data.py',
    conn_id='spark-conn',
    name='preprocess_data_from_csv_task',
    conf={
        'spark.jars': JARS_PATH,  # Path to the JDBC driver
    },
    verbose=True,
)

# Task 2: Read data from PostgreSQL
read_data_task = PythonOperator(
    task_id='read_data_from_postgres',
    python_callable=fetch_table_to_dataframe,
    op_kwargs={
        'table_name': 'airflow',  # Replace with your table name
        'output_path': '/opt/airflow/data/'  # Path to save the data as CSV
    },
    dag=dag,
)

# Task 3: Transform data with Spark
transform_with_spark = SparkSubmitOperator(
    task_id='transform_data_with_spark',
    conn_id='spark-conn',
    application='/opt/airflow/jobs/python/transform_db_with_spark.py',  # Path to the Spark script
    name='transform_data_with_spark',
    conf={
        'spark.jars': JARS_PATH,  # Path to the JDBC driver
    },
    dag=dag,
)

# Task 4: Submit Spark job
python_job = SparkSubmitOperator(
    task_id='python_job',
    conn_id='spark-conn',
    application='jobs/python/wordcountjob.py',
    dag=dag,
)

# Task 5: End job
end = PythonOperator(
    task_id='end',
    python_callable= lambda: print('Jobs completed successfully'),
    dag=dag,
)

# Define task dependencies
start >> load_data_backup_task >> preprocess_data_task >> read_data_task >> transform_with_spark >> python_job >> end