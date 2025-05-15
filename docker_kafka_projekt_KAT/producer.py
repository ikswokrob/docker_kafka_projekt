import psycopg2
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
from datetime import datetime, timedelta
from decimal import Decimal
import time

#  Serializacja specjalnych typów (np. datetime, Decimal)
def serialize(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    return obj

# 🔌 Połączenie z Kafka z retry
producer = None
for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',  # adres kontenera Kafka
            value_serializer=lambda v: json.dumps(v, default=serialize).encode('utf-8')
        )
        print("✅ Połączono z Kafka")
        break
    except NoBrokersAvailable:
        print(f"⚠️ Próba połączenia {i+1}/10 nieudana, czekam 3 sekundy...")
        time.sleep(3)

if producer is None:
    raise Exception("❌ Nie udało się połączyć z Kafka po 10 próbach")

# 🔌 Połączenie z Postgres
conn = psycopg2.connect(
    host="pg_kafka",           # nazwa kontenera Postgresa
    database="kafka_database",
    user="kafka",
    password="kafka"
)
cur = conn.cursor()

# 📥 Pobieranie danych z bazy
cur.execute("SELECT * FROM kafka_dane ORDER BY event_time ASC LIMIT 1000;")
rows = cur.fetchall()
cols = [desc[0] for desc in cur.description]

previous_time = None

for row in rows:
    record = dict(zip(cols, row))
    current_time = record.get("event_time")

    if previous_time and current_time:
        delta = (current_time - previous_time).total_seconds()
        time.sleep(min(delta, 1))  # maks. 1 sekunda, żeby nie było za wolno

    # Wysyłanie rekordu do Kafka
    producer.send("moj_topic", value=record)
    print(f"📤 [{current_time}] Wysłano do topicu: {record}")

    previous_time = current_time

cur.close()
conn.close()
producer.flush()
