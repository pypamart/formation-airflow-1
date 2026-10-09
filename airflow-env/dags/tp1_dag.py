"""
TP 1 - Flower Dataset Processing DAG

Ce DAG télécharge un dataset de fleurs et calcule un volume basé sur
les mesures de sépales.
"""

import logging
import textwrap
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd

# Configuration
logger = logging.getLogger(__name__)

# Constantes de configuration
CSV_PATH = "/tmp/data.csv"
DATA_URL = "https://raw.githubusercontent.com/CourseMaterial/DataWrangling/main/flowerdataset.csv"
SEPAL_LENGTH_COL = "sepal_length"
SEPAL_WIDTH_COL = "sepal_width"
VOLUME_COL = "volume"

# DAG Configuration
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 20),
    "retries": 4,
    "retry_delay": timedelta(minutes=1),
}


def compute_volume() -> None:
    """
    Lit le fichier CSV et ajoute une colonne 'volume' calculée comme la somme
    de sepal_length et sepal_width.
    
    Raises:
        FileNotFoundError: Si le fichier CSV n'existe pas
        KeyError: Si les colonnes attendues ne sont pas présentes
    """
    try:
        logger.info(f"Lecture du fichier CSV: {CSV_PATH}")
        df = pd.read_csv(CSV_PATH)
        
        # Vérifier que les colonnes requises existent
        required_cols = [SEPAL_LENGTH_COL, SEPAL_WIDTH_COL]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise KeyError(f"Colonnes manquantes: {missing_cols}")
        
        # Calculer le volume
        df[VOLUME_COL] = df[SEPAL_LENGTH_COL] + df[SEPAL_WIDTH_COL]
        logger.info(f"Colonne '{VOLUME_COL}' ajoutée avec succès")
        
        # Sauvegarder le fichier
        df.to_csv(CSV_PATH, index=False)
        logger.info(f"Fichier sauvegardé: {CSV_PATH}")
        
    except FileNotFoundError:
        logger.error(f"Fichier CSV non trouvé: {CSV_PATH}")
        raise
    except KeyError as e:
        logger.error(f"Erreur de colonne: {e}")
        raise
    except Exception as e:
        logger.error(f"Erreur lors du traitement du fichier: {e}")
        raise


with DAG(
    "tp1-dag",
    default_args=default_args,
    description="Télécharge et traite le dataset de fleurs",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "data-processing"],
    doc_md=__doc__,
) as dag:
    
    # Tâche 1: Télécharger le CSV
    download_csv_task = BashOperator(
        task_id="download_csv",
        bash_command=f"curl -o {CSV_PATH} {DATA_URL}",
        doc_md=textwrap.dedent(
            f"""\
            #### Téléchargement du Dataset
            Télécharge le fichier CSV depuis l'URL:
            `{DATA_URL}`
            
            Destination: `{CSV_PATH}`
            """
        ),
    )
    
    # Tâche 2: Calculer le volume
    volume_computation_task = PythonOperator(
        task_id="compute_volume",
        python_callable=compute_volume,
        doc_md=textwrap.dedent(
            f"""\
            #### Calcul du Volume
            Ajoute une colonne `{VOLUME_COL}` calculée comme:
            `{VOLUME_COL} = {SEPAL_LENGTH_COL} + {SEPAL_WIDTH_COL}`
            """
        ),
    )
    
    # Dépendances
    download_csv_task >> volume_computation_task
