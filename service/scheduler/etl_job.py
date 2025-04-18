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

def run_batch_processing():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("airflow")

    coordinator = None
    try:
        coordinator = CoordinatorActor.start()
        logger.info(f"CoordinatorActor started: {coordinator}")

        if coordinator is None:
            logger.error("Failed to start CoordinatorActor")
            return

        result = coordinator.ask({"command": Command.RUN_BATCH})
        logger.info(f"Batch job result: {result}")

    except Exception as e:
        logger.exception("Error running actor-based ETL job")
        raise e

    finally:
        if coordinator:
            coordinator.stop()
            logger.info("CoordinatorActor stopped")


with DAG('mpesa_etl_batch',
        default_args=default_args,
        description='Hourly ETL job to process M-Pesa transactions',
        schedule_interval='@hourly',
        catchup=False,
        tags=['mpesa', 'etl', 'batch']
    ) as dag:

    etl_task = PythonOperator(
        task_id='run_mpesa_etl',
        python_callable=run_batch_processing
    )
