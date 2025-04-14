from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from etl.coordinator import ETLCoordinator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_etl():
    coordinator = ETLCoordinator()
    coordinator.run()

with DAG('mpesa_etl_job',
         default_args=default_args,
         schedule_interval='@hourly',
         catchup=False) as dag:

    etl_task = PythonOperator(
        task_id='run_mpesa_etl',
        python_callable=run_etl
    )
