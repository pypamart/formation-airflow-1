"""DAG TaskFlow utilisant l'API CoinGecko pour récupérer le prix du Bitcoin."""

from datetime import datetime, timedelta
import logging

import requests
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"

@task
def extract_bitcoin_price():
    """Récupère les données historiques du Bitcoin pour la date d'exécution."""
    execution_date = datetime.now().strftime("%d-%m-%Y")
    params = {
        "date": execution_date,
        "localization": "false",
    }

    response = requests.get(
        f"{COINGECKO_URL.replace('/simple/price', '')}/coins/bitcoin/history",
        params=params,
        timeout=20,
    )
    response.raise_for_status()

    payload = response.json()
    market_data = payload.get("market_data")

    if market_data is None:
        raise ValueError("La clé 'market_data' est absente dans la réponse CoinGecko.")

    return payload


@task
def process_data(bitcoin_data: dict):
    """Parse les valeurs de prix et de volume en USD."""
    market_data = bitcoin_data.get("market_data", {})

    return {
        "price_usd": market_data.get("current_price", {}).get("usd"),
        "volume_usd": market_data.get("total_volume", {}).get("usd"),
    }


@task
def store_data(processed_data: dict):
    """Simule l'enregistrement des données traitées en loggant le résultat."""
    logger.info("Données Bitcoin historiques enregistrées : %s", processed_data)
    return processed_data


@dag(
    dag_id="taskflow_coingecko_bitcoin_prices_v2",
    description="Récupère le prix du Bitcoin via l'API CoinGecko avec TaskFlow",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["coin", "crypto", "taskflow", "tp"],
)
def taskflow_coingecko_bitcoin_prices():
    bitcoin_data = extract_bitcoin_price()
    processed_data = process_data(bitcoin_data)
    store_data(processed_data)



dag_instance = taskflow_coingecko_bitcoin_prices()

