"""
TP 3 - Display message with retry and retry delay using cron schedule
"""
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator

# DAG Configuration
default_args = {
    "owner": "pypamart",
    "start_date": datetime(2025, 3, 20),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "tp3-dag",
    default_args=default_args,
    description="Télécharge et traite le dataset de fleurs avec cron schedule",
    schedule="0 10 * * *",  # Cron expression: tous les jours à 10h du matin
    start_date=datetime(2026, 10, 4),
    catchup=False,
    tags=["tp3"],
    doc_md=__doc__,
) as dag:
    
    task = BashOperator(
        task_id="display_message",
        bash_command="echo 'Hello, Airflow!'",
    )
    
