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
from update_product_master_data import update_product_master_data


# Default arguments
default_args = {
    'owner': 'Lukas Muszong',
    'start_date': airflow.utils.dates.days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# Specifying the DAG
with DAG(
    dag_id='sparking_flow',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
    description="ETL workflow for product master data updates using Spark and Python."
) as dag:
    # Task 1: Start job
    start = PythonOperator(
        task_id='start',
        python_callable=lambda: print('Batch processing jobs are started..'),
    )

    # Task 2: Load data
    load_data_backup_task = SparkSubmitOperator(
        task_id='load_data_backup_from_csv_task',
        application='/opt/airflow/jobs/python/load_data_as_backup.py',
        conn_id='spark-conn',
        name='load_data_backup_from_csv_task',
        conf={'spark.jars': JARS_PATH},
        verbose=True,
    )

    # Task 3: Preprocess data
    preprocess_data_task = SparkSubmitOperator(
        task_id='preprocess_data_from_csv_task',
        application='/opt/airflow/jobs/python/preprocess_data.py',
        conn_id='spark-conn',
        name='preprocess_data_from_csv_task',
        conf={'spark.jars': JARS_PATH},
        verbose=True,
    )

    # Task 4: Transform data with Spark
    transform_with_spark = SparkSubmitOperator(
        task_id='transform_data_with_spark',
        conn_id='spark-conn',
        application='/opt/airflow/jobs/python/transform_db_with_spark.py',
        name='transform_data_with_spark',
        conf={'spark.jars': JARS_PATH},
    )

    # Task 5: Update the product master data table
    update_product_master_data_task = PythonOperator(
        task_id="update_product_master_task",
        python_callable=update_product_master_data,
    )

    # Task 6: Drop the processing table after ETL processes
    drop_table_task = PythonOperator(
        task_id='drop_table_task',
        python_callable=drop_postgres_table,
        op_args=[INTERMEDIATE_PROCESSING_TABLE],
    )

    # Task 7: End job
    end = PythonOperator(
        task_id='end',
        python_callable=lambda: print('Jobs completed successfully'),
    )

    # Define task dependencies
    start >> load_data_backup_task >> preprocess_data_task >> transform_with_spark >> update_product_master_data_task >> drop_table_task >> end