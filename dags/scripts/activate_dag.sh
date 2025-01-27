#!/bin/bash

# Wait for 30 seconds
echo "Waiting for 30 seconds..."
sleep 30

# Unpause the DAG
echo "Unpausing the DAG sparking_flow..."
airflow dags unpause sparking_flow