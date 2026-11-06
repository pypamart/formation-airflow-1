"""DAG de gestion des métadonnées d'exécution PostgreSQL."""

from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

TABLE_NAME = "dag_execution_metadata"


def get_execution_context(context):
    """Récupère dynamiquement la date et l'identifiant du DAG courant."""
    dag_id = context["dag"].dag_id
    logical_date = context.get("logical_date") or context.get("data_interval_start")
    if logical_date is None:
        raise ValueError("logical_date/data_interval_start est absent dans le contexte Airflow")
    return dag_id, logical_date.isoformat()


def ensure_dag_metadata_table(**context):
    """Vérifie l'existence de la table PostgreSQL et la crée si nécessaire."""
    hook = PostgresHook(postgres_conn_id="postgres_localhost")

    table_exists = hook.get_first(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = %s
        );
        """,
        parameters=(TABLE_NAME,),
    )

    if not table_exists or not table_exists[0]:
        hook.run(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                execution_date TIMESTAMPTZ NOT NULL,
                dag_id VARCHAR(255) NOT NULL,
                PRIMARY KEY (execution_date, dag_id)
            );
            """,
            autocommit=True,
        )


def delete_current_execution_metadata(**context):
    """Supprime les anciens enregistrements pour l'exécution courante."""
    dag_id, execution_date = get_execution_context(context)
    hook = PostgresHook(postgres_conn_id="postgres_localhost")

    hook.run(
        f"""
        DELETE FROM {TABLE_NAME}
        WHERE execution_date = %s
          AND dag_id = %s;
        """,
        parameters=(execution_date, dag_id),
        autocommit=True,
    )


def insert_current_execution_metadata(**context):
    """Insère la métadonnée correspondant à l'exécution courante du DAG."""
    dag_id, execution_date = get_execution_context(context)
    hook = PostgresHook(postgres_conn_id="postgres_localhost")

    hook.run(
        f"""
        INSERT INTO {TABLE_NAME} (execution_date, dag_id)
        VALUES (%s, %s);
        """,
        parameters=(execution_date, dag_id),
        autocommit=True,
    )


def extract_and_filter_metadata(**context):
    """Récupère les données de métadonnées, les affiche puis filtre par DAG ID."""
    target_dag_id = "dag_postgres_metadata"
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    records = hook.get_records(f"SELECT execution_date, dag_id FROM {TABLE_NAME} ORDER BY execution_date;")

    df = pd.DataFrame(records, columns=["execution_date", "dag_id"])
    print("Données complètes :")
    print(df.to_string(index=False))

    filtered_df = df[df["dag_id"] == target_dag_id].copy()
    print(f"\nDonnées filtrées pour dag_id='{target_dag_id}' :")
    print(filtered_df.to_string(index=False))


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=1),
    "execution_timeout": timedelta(minutes=30),
}


with DAG(
    dag_id="dag_postgres_metadata",
    description="Vérifie et initialise la table des métadonnées d'exécution PostgreSQL.",
    schedule="0 12 * * *",
    start_date=datetime(2026, 11, 1),
    end_date=datetime(2026, 11, 6),
    catchup=True,
    default_args=default_args,
    tags=["postgres", "metadata", "example"],
    doc_md="""\
    # DAG PostgreSQL Metadata

    Ce DAG vérifie l'existence de la table `dag_execution_metadata` dans PostgreSQL
    et la crée si nécessaire avant d'insérer le couple `(execution_date, dag_id)`
    associé à chaque exécution.
    """,
) as dag:

    ensure_metadata_table = PythonOperator(
        task_id="ensure_metadata_table",
        python_callable=ensure_dag_metadata_table,
    )

    delete_execution_metadata = PythonOperator(
        task_id="delete_execution_metadata",
        python_callable=delete_current_execution_metadata,
    )

    insert_execution_metadata = PythonOperator(
        task_id="insert_execution_metadata",
        python_callable=insert_current_execution_metadata,
    )

    extract_metadata = PythonOperator(
        task_id="extract_metadata",
        python_callable=extract_and_filter_metadata,
    )

    ensure_metadata_table >> delete_execution_metadata >> insert_execution_metadata >> extract_metadata
