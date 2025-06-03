import json
import random
import time
import socket
import logging
from datetime import datetime
from confluent_kafka import Producer, Consumer
from multiprocessing import Process

KAFKA_BROKER = 'broker:9092'
TRANSACTION_TOPIC = 'ceny_kruszywa'
SIGNAL_TOPIC = 'buy_signal'
NUM_PARTITIONS = 3
TRANSACTION_CG = 'ceny_kruszywa'
ALERT_THRESHOLD = 0.03  # 3%
CHECK_INTERVAL = 3  # co 3 sekundy
DISPLAY_INTERVAL = 15  # co 15 sekund

def create_producer():
    try:
        producer = Producer({
            "bootstrap.servers": KAFKA_BROKER,
            "client.id": socket.gethostname(),
            "enable.idempotence": True,
            "acks": "all",
        })
        logging.info("Producer created successfully")
        return producer
    except Exception as e:
        logging.exception("Cannot create producer")
        return None

def create_consumer(topic, group_id):
    try:
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": group_id,
            "client.id": socket.gethostname(),
            "auto.offset.reset": "latest",
            "enable.auto.commit": False
        })
        consumer.subscribe([topic])
        logging.info("Consumer created and subscribed successfully")
        return consumer
    except Exception as e:
        logging.exception("Cannot create consumer")
        return None

def interpret_change(change_percent):
    if change_percent >= 5:
        return "Zalecana sprzedaż"
    elif change_percent >= 3:
        return "Rozważ sprzedaż"
    elif change_percent <= -5:
        return "Zalecana kupno"
    elif change_percent <= -3:
        return "Rozważ kupno"
    else:
        return ""

def signals_detection():
    consumer = create_consumer(topic=TRANSACTION_TOPIC, group_id=TRANSACTION_CG)
    producer = create_producer()
    if not consumer or not producer:
        logging.error("Consumer or Producer creation failed, exiting process.")
        return

    logging.info("Starting signals detection process...")

    reference_prices = {}
    last_prices = {}
    last_alert_time = {}

    # Poczekaj na pierwszą wiadomość (dane referencyjne)
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error(f"CONSUMER error: {msg.error()}")
            continue

        record = json.loads(msg.value().decode('utf-8'))
        prices = record.get("Prices", {})
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        reference_prices = prices.copy()
        last_prices = prices.copy()
        last_alert_time = {k: current_time for k in prices.keys()}

        print("\n=== [ Dane początkowe ] ===")
        for commodity, price in prices.items():
            print(f"{commodity.strip()}: {price:.2f}")
        print("=====================================\n")

        break  # przechodzimy dalej

    last_display_time = time.time()

    while True:
        message = consumer.poll(timeout=1.0)
        if message is None:
            continue
        if message.error():
            logging.error(f"CONSUMER error: {message.error()}")
            continue

        record = json.loads(message.value().decode('utf-8'))
        prices = record.get("Prices", {})
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Co 15 sekund: wyświetl zmiany
        if time.time() - last_display_time >= DISPLAY_INTERVAL:
            print(f"\n=== [ {current_time} ] Aktualne ceny i zmiany ===")
            for commodity, current_price in prices.items():
                ref_price = reference_prices.get(commodity, current_price)
                last_price = last_prices.get(commodity, current_price)
                change_since_last = (current_price - last_price) / last_price * 100
                change_since_ref = (current_price - ref_price) / ref_price * 100
                print(f"{commodity.strip()}: {current_price:.2f} | "
                      f"Zmiana od ostatniego wyświetlenia: {change_since_last:+.2f}% (od {last_price:.2f}) | "
                      f"Zmiana od startu: {change_since_ref:+.2f}% (od {ref_price:.2f})")
            last_prices = prices.copy()
            last_display_time = time.time()

        # Co 3 sekundy: sprawdzamy zmiany względem ostatniego alertu
        for commodity, current_price in prices.items():
            last_alert_price = reference_prices.get(commodity, current_price)
            price_change = (current_price - last_alert_price) / last_alert_price * 100

            if abs(price_change) >= ALERT_THRESHOLD * 100:
                previous_alert_time = last_alert_time.get(commodity, current_time)
                action_suggestion = interpret_change(price_change)

                alert_message = (
                    f"\n*** ALERT dla {commodity.strip()} ***\n"
                    f"Aktualna cena: {current_price:.2f}\n"
                    f"Zmiana od ostatniego alertu wysłanego o godzinie {previous_alert_time}: {price_change:+.2f}% (od {last_alert_price:.2f})\n"
                    f"{action_suggestion}\n"
                    f"Godzina aktualnego alertu: {current_time}\n"
                )
                print(alert_message)

                alert_json = {
                    "commodity": commodity.strip(),
                    "current_price": current_price,
                    "previous_alert_price": last_alert_price,
                    "previous_alert_time": previous_alert_time,
                    "alert_time": current_time,
                    "change_percent": round(price_change, 2),
                    "action": action_suggestion
                }
                producer.produce(SIGNAL_TOPIC, value=json.dumps(alert_json).encode("utf-8"))
                producer.flush()

                # Aktualizuj referencyjne i czas
                reference_prices[commodity] = current_price
                last_alert_time[commodity] = current_time

        time.sleep(CHECK_INTERVAL)

    consumer.close()

for _ in range(NUM_PARTITIONS):
    p = Process(target=signals_detection)
    p.start()
