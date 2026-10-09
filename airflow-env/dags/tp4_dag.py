"""
TP 4 - Display message with artificially long command and timeout scenario
"""
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator

# DAG Configuration
default_args = {
    "owner": "pypamart",
    "start_date": datetime(2025, 3, 20),
    "retries": 2,
    "retry_delay": timedelta(seconds=1),
}

with DAG(
    "tp4-dag",
    default_args=default_args,
    description="Démonstration d'un scénario de timeout avec une commande longue",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 10, 4),
    catchup=False,
    tags=["tp4"],
    doc_md=__doc__,
) as dag:
    
    # Tâche avec une commande sleep de 300 secondes (5 minutes)
    # et un timeout de 60 secondes (1 minute) pour créer un scénario de timeout
    task = BashOperator(
        task_id="long_running_task",
        bash_command="sleep 300 && echo 'Task completed'",
        execution_timeout=timedelta(seconds=5),  # Timeout après 5 secondes
        retries=3, # Nombre de réessais pour cette tâche
    )
