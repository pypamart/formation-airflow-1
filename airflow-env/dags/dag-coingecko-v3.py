"""DAG TaskFlow CoinGecko v3 : données historiques réelles + calcul de RSI."""

from datetime import datetime, timedelta
import logging

import requests
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

COINGECKO_MARKET_CHART_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"


@task
def extract_bitcoin_price():
    """Récupère les 10 derniers points de prix journalier du Bitcoin."""
    params = {
        "vs_currency": "usd",
        "days": "10",
        "interval": "daily",
    }

    response = requests.get(COINGECKO_MARKET_CHART_URL, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()
    prices = payload.get("prices", [])

    if not prices:
        raise ValueError("Aucun prix historique retourné pour le Bitcoin sur la période demandée.")

    return prices


@task
def process_data(prices: list):
    """Calcule le RSI sur les derniers points historiques et renvoie le dernier prix + le RSI."""
    if len(prices) < 2:
        raise ValueError("Il faut au moins deux points pour calculer le RSI.")

    closes = [float(point[1]) for point in prices]
    deltas = [closes[index] - closes[index - 1] for index in range(1, len(closes))]

    gains = [max(delta, 0) for delta in deltas]
    losses = [abs(min(delta, 0)) for delta in deltas]

    avg_gain = sum(gains) / len(gains)
    avg_loss = sum(losses) / len(losses)

    if avg_loss == 0:
        rsi = 100.0
    else:
        relative_strength = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + relative_strength))

    return {
        "price_usd": round(closes[-1], 2),
        "rsi": round(rsi, 2),
    }


@task
def store_data(processed_data: dict):
    """Simule l'enregistrement des données traitées."""
    logger.info("Données Bitcoin enregistrées : %s", processed_data)
    return processed_data


@dag(
    dag_id="taskflow_coingecko_bitcoin_prices_v3",
    description="Données historiques Bitcoin réelles + calcul du RSI",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["coin", "crypto", "taskflow", "tp", "rsi"],
)
def taskflow_coingecko_bitcoin_prices_v3():
    prices = extract_bitcoin_price()
    processed_data = process_data(prices)
    store_data(processed_data)


dag_instance = taskflow_coingecko_bitcoin_prices_v3()
