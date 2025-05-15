import psycopg2
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
from datetime import datetime, timedelta
from decimal import Decimal
import time

def serialize(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    return obj

producer = None
for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',  # host kafka w dockerze
            value_serializer=lambda v: json.dumps(v, default=serialize).encode('utf-8')
        )
        print("Połączono z Kafka")
        break
    except NoBrokersAvailable:
        print(f"Próba połączenia {i+1}/10 nieudana, czekam 3 sekundy...")
        time.sleep(3)

if producer is None:
    raise Exception("Nie udało się połączyć z brokerem Kafka po 10 próbach")

conn = psycopg2.connect(
    host="pg_kafka",           # host Postgresa w dockerze
    database="kafka_database",
    user="kafka",
    password="kafka"
)
cur = conn.cursor()

cur.execute("SELECT * FROM kafka_dane ORDER BY event_time ASC LIMIT 1000;")
rows = cur.fetchall()
cols = [desc[0] for desc in cur.description]

previous_time = None
seen_users = set()
cart_activity = {}  # user_id -> {"last_cart_time": datetime, "reminder_sent": False}

REMINDER_DELAY = timedelta(minutes=5)  # po ilu minutach wysłać przypomnienie

for row in rows:
    record = dict(zip(cols, row))
    current_time = record.get("event_time")
    user_id = record.get("user_id")
    event_type = record.get("event_type")

    if previous_time and current_time:
        delta = (current_time - previous_time).total_seconds()
        time.sleep(min(delta, 5))

    # Reguła 1: nowy użytkownik
    if user_id not in seen_users:
        print(f"Popup dla nowego usera {user_id}: 'Zarejestruj się i odbierz 10% zniżki!'")
        seen_users.add(user_id)

    # Reguła 2: dodanie do koszyka
    if event_type == "cart":
        cart_activity[user_id] = {"last_cart_time": current_time, "reminder_sent": False}
    print(f"Reguła 2: Użytkownik {user_id} dodał produkt do koszyka.")

    # Reguła 3: przypomnienie o dokończeniu zakupów
    if event_type != "cart" and user_id in cart_activity:
        last_cart_time = cart_activity[user_id]["last_cart_time"]
        reminder_sent = cart_activity[user_id]["reminder_sent"]
        if not reminder_sent and (current_time - last_cart_time) > REMINDER_DELAY:
            print(f"Przypomnienie dla usera {user_id}: 'Dokończ zakupy i odbierz rabat 5%'")
            cart_activity[user_id]["reminder_sent"] = True

    # Wysyłanie eventu do Kafka
    producer.send("moj_topic", value=record)
    print(f"✅ [{current_time}] Wysłano: {record}")

    previous_time = current_time

cur.close()
conn.close()
producer.flush()
