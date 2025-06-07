from datetime import datetime, timedelta
import pandas as pd

# Wczytaj dane
df = pd.read_csv(r'C:\Users\kiksw\Mój dysk\dane_2.csv')
df = df.sort_values('event_time').reset_index(drop=True)

# Dodaj syntetyczny czas co 10 sekund
start_time = datetime(2019, 12, 1, 0, 0, 0)
df['synt_time'] = pd.date_range(start=start_time, periods=len(df), freq='10S')

from kafka import KafkaProducer
import json
from decimal import Decimal
import time
from datetime import datetime

# Serializacja
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

# Iteracja przez dane z gotowym synt_time
for _, row in df.iterrows():
    record = row.to_dict()
    print(f"📤 Wysyłam record dla user_id={record.get('user_id')} @ {record['synt_time']}")
    producer.send("topic", value=record)
    time.sleep(0.1)  # nadal symulujemy przyspieszone wysyłanie

producer.flush()