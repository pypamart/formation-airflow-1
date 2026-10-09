"""
TP 2 - Display message with retry and retry delai
"""
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator

# DAG Configuration
default_args = {
    "owner": "pypamart",
    "start_date": datetime(2025, 3, 20),
    "retries": 3,
    "retry_delay": timedelta(minutes=60),
}

with DAG(
    "tp2-dag",
    default_args=default_args,
    description="Télécharge et traite le dataset de fleurs",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 10, 4),
    catchup=False,
    tags=["tp2"],
    doc_md=__doc__,
) as dag:
    
    task = BashOperator(
        task_id="display_message",
        bash_command="echo 'Hello, Airflow!'",
    )
