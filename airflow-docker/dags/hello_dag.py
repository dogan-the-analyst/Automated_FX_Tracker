from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def say_hello():
    print("Hello, Airflow! Başarılı bir şekilde çalışıyor.")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='hello_world_dag',
    default_args=default_args,
    description='Test amaçlı basit bir DAG',
    start_date=datetime(2025, 6, 13),
    schedule_interval='@daily',
    catchup=False,
    tags=['test'],
) as dag:
    
    task1 = PythonOperator(
        task_id='print_hello',
        python_callable=say_hello,
    )

    task1
