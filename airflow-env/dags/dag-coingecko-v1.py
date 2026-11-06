"""Version précédente du DAG CoinGecko, conservée pour référence."""

from datetime import datetime, timedelta
import logging

import requests
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"


@task
def extract_bitcoin_price():
    """Récupère le prix du Bitcoin depuis l'API CoinGecko."""
    params = {
        "ids": "bitcoin",
        "vs_currencies": "usd",
        "include_24hr_change": "true",
    }

    response = requests.get(COINGECKO_URL, params=params, timeout=20)
    response.raise_for_status()

    payload = response.json()
    bitcoin_data = payload.get("bitcoin")

    if bitcoin_data is None:
        raise ValueError("La clé 'bitcoin' est absente dans la réponse CoinGecko.")

    return bitcoin_data


@task
def process_data(bitcoin_data: dict):
    """Transforme le payload CoinGecko en dictionnaire avec usd et change."""
    return {
        "usd": bitcoin_data["usd"],
        "change": bitcoin_data.get("usd_24h_change", 0),
    }


@task
def store_data(processed_data: dict):
    """Simule l'enregistrement des données traitées en loggant le résultat."""
    logger.info("Données Bitcoin enregistrées : %s", processed_data)
    return processed_data


@dag(
    dag_id="taskflow_coingecko_bitcoin_prices_v1",
    description="Version précédente du DAG CoinGecko avec TaskFlow",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["coin", "crypto", "taskflow", "tp", "previous"],
)
def taskflow_coingecko_bitcoin_prices_previous():
    bitcoin_data = extract_bitcoin_price()
    processed_data = process_data(bitcoin_data)
    store_data(processed_data)


dag_instance = taskflow_coingecko_bitcoin_prices_previous()
