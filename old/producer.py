import pandas as pd
from kafka import KafkaProducer
import json
from datetime import datetime, timedelta
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

# Wczytaj dane z pliku CSV
df = pd.read_csv(r'C:\Users\kiksw\Mój dysk\dane_2.csv')
df = df.sort_values('event_time')


# Ustaw zegar syntetyczny
synt_clock = datetime(2019, 12, 1, 0, 0, 0)

# Iteruj przez rekordy
for _, row in df.iterrows():
    record = row.to_dict()	
    record['synt_time'] = synt_clock.isoformat()
    synt_clock += timedelta(seconds=10)
    print(f"📤 Wysyłam record dla user_id={record.get('user_id')} @ {record['synt_time']}")
    producer.send("topic", value=record)
    time.sleep(0.1)

# Zakończenie
producer.flush()