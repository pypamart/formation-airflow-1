"""DAG avec TimeDeltaSensor puis tâches parallèles."""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor

logger = logging.getLogger(__name__)


def dummy_task_function(task_name: str, **context) -> str:
    """Log une tâche simulée."""
    logger.info("Tâche %s effectuée", task_name)
    return task_name


with DAG(
    dag_id="dag_time_delta_priority",
    description="Attente de 2 minutes puis exécution de deux tâches en parallèle",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"owner": "airflow", "retries": 0},
    tags=["training", "sensor", "parallel"],
) as dag:

    wait_for_2_minutes = TimeDeltaSensor(
        task_id="wait_for_2_minutes",
        delta=timedelta(minutes=2),
    )

    high_priority_task = PythonOperator(
        task_id="high_priority_task",
        python_callable=dummy_task_function,
        op_kwargs={"task_name": "haute priorité"},
        priority_weight=10,
    )

    low_priority_task = PythonOperator(
        task_id="low_priority_task",
        python_callable=dummy_task_function,
        op_kwargs={"task_name": "basse priorité"},
        priority_weight=1,
    )

    wait_for_2_minutes >> [high_priority_task, low_priority_task]
