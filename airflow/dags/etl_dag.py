from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "saaim",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="ecommerce_etl", # name to show in UI it should be unique
    default_args=default_args,
    description="bronze -> silver -> gold",
    schedule="@hourly", # run every hour
    start_date=datetime(2026, 7, 1), #earlist date to start the DAG
    catchup=False, # do not backfill the DAG for missed intervals
    tags=["etl", "spark"], # jsut labels nothing else 
) as dag:

    silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="cd /opt/airflow && python spark/batch/bronze_to_silver.py",
    )

    gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="cd /opt/airflow && python spark/batch/silver_to_gold.py",
    )

    silver >> gold