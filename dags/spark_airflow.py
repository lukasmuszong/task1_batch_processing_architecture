import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta
import sys
import os
from misc.parameters import JARS_PATH, INTERMEDIATE_PROCESSING_TABLE

# Add the jobs/python directory to the system path for importing scripts
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../jobs/python"))
from drop_postgres_table import drop_postgres_table

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
    schedule_interval = '@monthly',
    catchup=False,
)

# Task 1: Start job
start = PythonOperator(
    task_id='start',
    python_callable= lambda: print('Batch processing jobs are started..'),
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

# Task 4: Transform data with Spark
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

# Task 5: Drop the processing table after ETL processes
drop_table_task = PythonOperator(
    task_id='drop_table_task',
    python_callable=drop_postgres_table,
    op_args=[INTERMEDIATE_PROCESSING_TABLE],  # Replace with the actual table name
    dag=dag,
)

# Task 6: End job
end = PythonOperator(
    task_id='end',
    python_callable= lambda: print('Jobs completed successfully'),
    dag=dag,
)

# Define task dependencies
start >> load_data_backup_task >> preprocess_data_task >> transform_with_spark >> drop_table_task >> end