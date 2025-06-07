from kafka import KafkaConsumer
import json
import time
from datetime import datetime
import pandas as pd

# Consumer Kafka
consumer = KafkaConsumer(
    'moj_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',     # zaczyna od początku jeśli to możliwe
    enable_auto_commit=True,
    group_id='moja_grupa',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

okno = []
okno_start = time.time()
okno_czas = 15



print("Nasłuchuję wiadomości z topicu 'moj_topic'...\n")

for message in consumer:
    record = message.value
    okno.append(record)

    cur_time = time.time()
    if cur_time - okno_start >= okno_czas:
        
        
        df = pd.DataFrame(okno)
        if not df.empty:
            dfg = df.groupby(['user_id', 'event_type']).agg({
                'event_type' : 'count',
                'price' : ['sum','mean']
            }).fillna(0)


        print(f"\n Podsumowanie z ostatniej minuty")
        print(f" Liczba transakcji: {len(okno)}")
        print(dfg)
        print("-" * 40)

        okno = []
        okno_start = cur_time



