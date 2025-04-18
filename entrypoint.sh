#!/bin/bash
echo "Setting up db..."
airflow db init
airflow db migrate

echo "Starting ETL Scheduler in a background terminal..."
airflow scheduler &

echo "Starting Web Server..."
uvicorn main:app --host 0.0.0.0 --port 8000
