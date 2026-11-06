"""DAG TaskFlow CoinGecko v4 : données historiques, RSI et moyennes hebdomadaires."""

from datetime import datetime, timedelta
import logging

import pandas as pd
import requests
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

COINGECKO_MARKET_CHART_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"


@task
def extract_bitcoin_price():
    """Récupère 20 jours d'historique pour calculer un RSI basé sur les valeurs précédentes."""
    params = {
        "vs_currency": "usd",
        "days": "20",
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
    """Calcule un RSI par date en utilisant une moyenne glissante sur les variations précédentes."""
    if len(prices) < 2:
        raise ValueError("Il faut au moins deux points pour calculer le RSI.")

    df = pd.DataFrame(prices, columns=["timestamp", "price_usd"])
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms")
    df["price_usd"] = df["price_usd"].astype(float)
    df["daily_change"] = df["price_usd"].diff()
    df["gain"] = df["daily_change"].clip(lower=0)
    df["loss"] = (-df["daily_change"]).clip(lower=0)

    window = 14
    df["avg_gain"] = df["gain"].rolling(window=window, min_periods=1).mean()
    df["avg_loss"] = df["loss"].rolling(window=window, min_periods=1).mean()

    df["rsi"] = df.apply(
        lambda row: 100.0 if row["avg_loss"] == 0 else 100 - (100 / (1 + row["avg_gain"] / row["avg_loss"])),
        axis=1,
    )

    df["rsi"] = df["rsi"].round(2)
    return df.tail(10)


@task
def build_dataframe(prices: list, processed_data: pd.DataFrame):
    """Construit un DataFrame exploitable à partir des dates, prix et RSI calculés par date."""
    df = processed_data[["date", "price_usd", "rsi"]].copy()
    df["date"] = pd.to_datetime(df["date"])
    return df


@task
def compute_weekly_average_price(df: pd.DataFrame):
    """Calcule la moyenne hebdomadaire du prix à partir du DataFrame."""
    weekly_mean = (
        df.assign(week=df["date"].dt.to_period("W-MON").astype(str))
        .groupby("week", as_index=False)["price_usd"]
        .mean()
        .rename(columns={"price_usd": "weekly_mean_price_usd"})
    )

    logger.info("Moyennes hebdomadaires calculées : %s", weekly_mean.to_dict(orient="records"))
    return weekly_mean.to_dict(orient="records")


@task
def store_data(weekly_summary: list):
    """Simule l'enregistrement des données traitées."""
    logger.info("Données hebdomadaires enregistrées : %s", weekly_summary)
    return weekly_summary


@dag(
    dag_id="taskflow_coingecko_bitcoin_prices_v4",
    description="Prix BTC, RSI glissant et moyennes hebdomadaires à partir des données CoinGecko",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["coin", "crypto", "taskflow", "tp", "rsi", "pandas"],
)
def taskflow_coingecko_bitcoin_prices_v4():
    prices = extract_bitcoin_price()
    processed_data = process_data(prices)
    df = build_dataframe(prices, processed_data)
    weekly_summary = compute_weekly_average_price(df)
    store_data(weekly_summary)


dag_instance = taskflow_coingecko_bitcoin_prices_v4()
