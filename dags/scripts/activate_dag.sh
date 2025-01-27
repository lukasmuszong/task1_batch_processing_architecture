#!/bin/bash

# Wait for the Airflow webserver to be up
sleep 20

# Trigger the DAG activation via CLI
airflow dags unpause sparking_flow