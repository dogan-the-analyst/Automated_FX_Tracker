from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

## Import functions from etl_fx.py

from etl_fx import extract, transform, load

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fx_etl_dag',
    default_args=default_args,
    description='ETL for FX rates',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 6, 14),
    catchup=False,
) as dag:

    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
        op_args=[task_extract.output],
    )

    task_load = PythonOperator(
        task_id='load',
        python_callable=load,
        op_args=[task_transform.output],
    )

    task_extract >> task_transform >> task_load
