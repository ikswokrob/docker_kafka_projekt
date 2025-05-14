import psycopg2
from kafka import KafkaProducer
import json
from datetime import datetime
from decimal import Decimal
import time

# Funkcja do serializacji niestandardowych typów
def serialize(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    return obj

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=serialize).encode('utf-8')
)

# Połączenie z bazą
conn = psycopg2.connect(
    host="localhost",
    database="kafka_database",
    user="kafka",
    password="kafka"
)
cur = conn.cursor()

# Załaduj dane
cur.execute("SELECT * FROM kafka_dane ORDER BY event_time ASC LIMIT 1000;")
rows = cur.fetchall()
cols = [desc[0] for desc in cur.description]

# Symulacja czasu
previous_time = None

for row in rows:
    record = dict(zip(cols, row))
    current_time = record.get("event_time")

    if previous_time and current_time:
        delta = (current_time - previous_time).total_seconds()
        # Zastosuj maksymalny interwał np. 5 sekund, żeby nie czekać godzin
        time.sleep(min(delta, 5))

    producer.send("moj_topic", value=record)
    print(f"✅ [{current_time}] Wysłano: {record}")

    previous_time = current_time

cur.close()
conn.close()
producer.flush()