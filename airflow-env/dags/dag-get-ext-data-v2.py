"""
DAG Get External Data - Flower Dataset Processing with Branching

Ce DAG télécharge un dataset de fleurs, calcule un volume basé sur
les mesures de sépales, puis effectue un branchement conditionnel basé
sur la taille du dataset pour afficher "Gros dataset" ou "Petit dataset".
"""

import logging
import textwrap
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
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


def count_rows(ti) -> None:
    """
    Compte le nombre de lignes dans le fichier CSV et pousse le résultat en XCom.
    
    Cette fonction démontre le parameter passing via XCom en utilisant
    xcom_push() pour envoyer les données à d'autres tâches.
    
    Args:
        ti: task_instance - Objet Airflow pour accéder aux données de la tâche
    """
    try:
        logger.info(f"Comptage des lignes du CSV: {CSV_PATH}")
        df = pd.read_csv(CSV_PATH)
        num_rows = len(df)
        num_cols = len(df.columns)
        
        logger.info(f"Nombre de lignes trouvées: {num_rows}")
        logger.info(f"Nombre de colonnes: {num_cols}")
        
        # Parameter passing: Pousser les données en XCom avec des clés explicites
        ti.xcom_push(key='dataset_row_count', value=num_rows)
        ti.xcom_push(key='dataset_col_count', value=num_cols)
        
        logger.info("Données sauvegardées en XCom pour les tâches suivantes")
        
    except Exception as e:
        logger.error(f"Erreur lors du comptage des lignes: {e}")
        raise


def branch_condition(ti) -> str:
    """
    Détermine la branche à exécuter en fonction du nombre de lignes du dataset.
    Retourne le task_id de la tâche à exécuter.
    
    Cette fonction démontre le parameter passing via XCom en utilisant
    xcom_pull() pour récupérer les données des tâches précédentes et prendre
    une décision de branchement basée sur ces données.
    
    Args:
        ti: task_instance - Objet Airflow pour accéder aux données d'autres tâches
    
    Returns:
        str: ID de la tâche à exécuter ("task_large_dataset" ou "task_small_dataset")
    """
    # Parameter passing: Récupérer les données depuis la tâche 'count_rows' en XCom
    num_rows = ti.xcom_pull(task_ids='count_rows', key='dataset_row_count')
    num_cols = ti.xcom_pull(task_ids='count_rows', key='dataset_col_count')
    
    logger.info(f"Données reçues en XCom: {num_rows} lignes, {num_cols} colonnes")
    
    # Décision de branchement basée sur les données reçues
    if num_rows > 1000:
        choice = "task_large_dataset"
        logger.info(f"Dataset volumineux détecté ({num_rows} lignes) -> {choice}")
    else:
        choice = "task_small_dataset"
        logger.info(f"Petit dataset détecté ({num_rows} lignes) -> {choice}")
    
    return choice


def print_large_dataset(ti) -> None:
    """
    Affiche les statistiques du dataset pour un gros dataset.
    Démontre le parameter passing en récupérant les données d'une tâche amont.
    """
    # Parameter passing: Récupérer les données depuis XCom
    num_rows = ti.xcom_pull(task_ids='count_rows', key='dataset_row_count')
    num_cols = ti.xcom_pull(task_ids='count_rows', key='dataset_col_count')
    
    message = f"Gros dataset: {num_rows} lignes et {num_cols} colonnes"
    logger.info(f"📊 {message} 📊")
    print(message)


def print_small_dataset(ti) -> None:
    """
    Affiche les statistiques du dataset pour un petit dataset.
    Démontre le parameter passing en récupérant les données d'une tâche amont.
    """
    # Parameter passing: Récupérer les données depuis XCom
    num_rows = ti.xcom_pull(task_ids='count_rows', key='dataset_row_count')
    num_cols = ti.xcom_pull(task_ids='count_rows', key='dataset_col_count')
    
    message = f"Petit dataset: {num_rows} lignes et {num_cols} colonnes"
    logger.info(f"📈 {message} 📈")
    print(message)


with DAG(
    "dag-get-ext-data-v2",
    default_args=default_args,
    description="Télécharge et traite le dataset de fleurs avec branchement conditionnel",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "data-processing", "branching"],
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
    
    # Tâche 3: Compter les lignes du dataset
    count_rows_task = PythonOperator(
        task_id="count_rows",
        python_callable=count_rows,
        doc_md=textwrap.dedent(
            """\
            #### Comptage des Lignes (Parameter Passing)
            Compte le nombre de lignes et colonnes dans le dataset CSV.
            
            Démontre le **parameter passing via XCom**:
            - Utilise `ti.xcom_push()` pour envoyer les données
            - Les clés utilisées sont: `dataset_row_count` et `dataset_col_count`
            - Ces données seront récupérées par les tâches aval avec `ti.xcom_pull()`
            """
        ),
    )
    
    # Tâche 4: Branchement conditionnel
    branching_task = BranchPythonOperator(
        task_id="branch_by_size",
        python_callable=branch_condition,
        doc_md=textwrap.dedent(
            """\
            #### Branchement Conditionnel (Avec Parameter Passing)
            Détermine quelle tâche exécuter en fonction du nombre de lignes.
            
            Démontre le **parameter passing pour le branchement**:
            - Récupère les données via `ti.xcom_pull()` depuis la tâche "count_rows"
            - Utilise `ti.xcom_pull(task_ids='count_rows', key='dataset_row_count')`
            - Décide de la branche à suivre basé sur les données reçues:
              - Plus de 1000 lignes: "Gros dataset"
              - 1000 lignes ou moins: "Petit dataset"
            """
        ),
    )
    
    # Tâche 5a: Gros dataset
    large_dataset_task = PythonOperator(
        task_id="task_large_dataset",
        python_callable=print_large_dataset,
        doc_md=textwrap.dedent(
            """\Parameter passing
Parameter passing : utiliser un résultat de type paramètre/variable produit par une tâche dans une autre
tâche.
Pour cela, on utilise la fonctionnalité XCom (Cross-Communication), pour peu que les données soient
« petites » (on ne va pas transmettre des tables entières de cette manière, mais plutôt écrire quelque
part les résultats).
Au niveau de la tâche amont, on peut utiliser l'objet task_instance (ou ti) pour accéder aux données de la
tâche en cours d'exécution et pour pousser des données vers Xcom :
ti.xcom_push(key, value)
Dans la (fonction de la) tâche aval, on accède aux données passées en utilisant cette même ti :
ti.xcom_pull(task_ids='task_id', key='key')
On trouve notamment une utilité au parameter passing pour le branching. La fonction de branching peut
utiliser des paramètres et décider en fonction d’eux de la branche à suivre (et renvoyer le nom de la tâche
correspondante).
            #### Traitement Gros Dataset (Parameter Passing)
            Affiche les statistiques du dataset volumineux.
            
            Démontre le **parameter passing dans les tâches finales**:
            - Récupère les données via `ti.xcom_pull()` depuis la tâche "count_rows"
            - Affiche les statistiques reçues (nombre de lignes et colonnes)
            """
        ),
    )
    
    # Tâche 5b: Petit dataset
    small_dataset_task = PythonOperator(
        task_id="task_small_dataset",
        python_callable=print_small_dataset,
        doc_md=textwrap.dedent(
            """\
            #### Traitement Petit Dataset (Parameter Passing)
            Affiche les statistiques du petit dataset.
            
            Démontre le **parameter passing dans les tâches finales**:
            - Récupère les données via `ti.xcom_pull()` depuis la tâche "count_rows"
            - Affiche les statistiques reçues (nombre de lignes et colonnes)
            """
        ),
    )
    
    # Dépendances
    download_csv_task >> volume_computation_task >> count_rows_task >> branching_task
    branching_task >> [large_dataset_task, small_dataset_task]
