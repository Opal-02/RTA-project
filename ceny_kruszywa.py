import json
import random
import time
from datetime import datetime
from time import sleep
from kafka import KafkaProducer
import yfinance as yf
from zoneinfo import ZoneInfo

KAFKA_SERVER = "kafka:9092"
TOPIC = 'ceny_kruszywa'
LAG = 2

def create_producer(server):
    return KafkaProducer(
        bootstrap_servers=[server],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        api_version=(3, 7, 0),
    )

def get_price(ticker):
    """Pobiera aktualną cenę danego surowca z Yahoo Finance."""
    try:
        data = yf.Ticker(ticker)
        price = data.info.get("regularMarketPrice")
        return price
    except Exception as e:
        print(f"Błąd pobierania ceny dla {ticker}: {e}")
        return None

if __name__ == "__main__":
    producer = create_producer(KAFKA_SERVER)
    try:
        while True:
            base_prices = {
                "Gold": get_price("GC=F"),
                "Silver": get_price("SI=F"),
                "Platinum": get_price("PL=F"),
                "Palad": get_price("PA=F"),
                "Copper": get_price("HG=F")
            }

            base_prices = {k: v for k, v in base_prices.items() if v is not None}

            random_prices = {
                k: round(v * random.uniform(0.98, 1.02), 2) for k, v in base_prices.items()
            }

            message = {
                "Time": datetime.now(ZoneInfo("Europe/Warsaw")).strftime("%Y-%m-%d %H:%M:%S"),
                "Prices": random_prices
            }

            producer.send(TOPIC, value=message)
            print(f"Wysłano: {message}")
            sleep(LAG)
    except KeyboardInterrupt:
        producer.close()
