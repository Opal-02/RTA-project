import json
import sqlite3
from kafka import KafkaConsumer
from datetime import datetime
from zoneinfo import ZoneInfo

DB = 'database.db'
TOPIC = 'ceny_kruszywa'
KAFKA_SERVER = 'kafka:9092'  # w docker-compose Kafka nazywa się "kafka"

def init_db():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity TEXT,
            price REAL,
            timestamp TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity TEXT,
            price REAL,
            change_percent REAL,
            action TEXT,
            timestamp TEXT
        )
    ''')
    conn.commit()
    conn.close()

def insert_price(commodity, price):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('INSERT INTO prices (commodity, price, timestamp) VALUES (?, ?, ?)',
              (commodity, price, datetime.now(ZoneInfo("Europe/Warsaw")).isoformat()))
    conn.commit()
    conn.close()

def insert_alert(commodity, price, change, action):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('INSERT INTO alerts (commodity, price, change_percent, action, timestamp) VALUES (?, ?, ?, ?, ?)',
              (commodity, price, change, action, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def main():
    init_db()
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    last_prices = {}

    for message in consumer:
        data = message.value
        prices = data.get("Prices", {})
        for commodity, price in prices.items():
            insert_price(commodity, price)

            last_price = last_prices.get(commodity)
            if last_price:
                change = ((price - last_price) / last_price) * 100
                if abs(change) >= 3:
                    action = "Rozważ sprzedaż" if change > 0 else "Rozważ kupno"
                    insert_alert(commodity, price, change, action)
                    print(f"ALERT: {commodity} zmiana {change:.2f}% -> {action}")
            last_prices[commodity] = price

if __name__ == "__main__":
    main()
