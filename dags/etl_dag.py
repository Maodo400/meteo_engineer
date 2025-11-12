from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add your app to PYTHONPATH

from scripts.data_extractor import fetch_weather_data, transform_weather_data

default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "api_to_postgres_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=fetch_weather_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_weather_data,
    )

    extract_task >> transform_task
