from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from pandas import json_normalize
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String
import sys
sys.path.append("/opt/airflow/app")

#from etl_meteo_utils.py import *
from etl_meteo_utils import *

# ----------------------------
# CONFIG
# ----------------------------

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ------------------------------------
# DAG AIRFLOW
# ------------------------------------
with DAG(
    "meteo_pipeline_etl",
    default_args=default_args,
    description="ETL météo avec Airflow (extract, transform, load)",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    extract_weather_task = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
        provide_context=True,
    )

    transform_weather_task = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
        provide_context=True,
    )

    load_weather_task = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather,
        provide_context=True,
    )

    extract_weather_task >> transform_weather_task >> load_weather_task
