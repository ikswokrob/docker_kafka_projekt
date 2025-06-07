from kafka import KafkaConsumer
import json
from datetime import timedelta
from collections import defaultdict
import pandas as pd

consumer = KafkaConsumer(
    'moj_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='moja_grupa_1',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Bufor sesji per user
sessions = defaultdict(list)
last_seen = {}

# Ustawienia
SESSION_TIMEOUT = timedelta(seconds=600)

def process_session(user_id, events, user_session):
    df = pd.DataFrame(events)

    try:
        df['event_time'] = pd.to_datetime(df['event_time'], errors='coerce')
        df['synt_time'] = pd.to_datetime(df['synt_time'], errors='coerce')

        session_duration = (df['synt_time'].max() - df['synt_time'].min()).total_seconds()
        if session_duration == 0 or pd.isna(session_duration):
            return  # 👈 całkowicie pomiń sesję
    except Exception:
        return  # 👈 pomiń, jeśli nie da się policzyć czasu sesji

    print(f"\n📊 Sesja użytkownika {user_id}")
    print(f"Czas trwania sesji: {session_duration:.1f} sekund")

    summary = df['event_type'].value_counts()
    print("📌 Typy zdarzeń w sesji:\n", summary)

    try:
        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
        value_added = df.loc[df['event_type'] == 'cart', 'price'].sum()
        value_removed = df.loc[df['event_type'] == 'remove_from_cart', 'price'].sum()
        cart_value = value_added - value_removed
        print(f"💰 Wartość końcowa koszyka: {cart_value:.2f} zł")
    except Exception as e:
        print(f"❌ Błąd przy liczeniu wartości koszyka: {e}")

    if 'purchase' not in summary:
        print(f"🚨 Użytkownik {user_id} w trakcie sesji {user_session} nie kupił nic → wyślij kod rabatowy")

for message in consumer:
    record = message.value
    user_id = record['user_id']
    user_session = record['user_session']

    try:
        event_time = pd.to_datetime(record['synt_time'], errors='coerce')
    except Exception:
        continue

    if user_id in last_seen:
        if event_time - last_seen[user_id] > SESSION_TIMEOUT:
            process_session(user_id, sessions[user_id], user_session)
            sessions[user_id] = []

    sessions[user_id].append(record)
    last_seen[user_id] = event_time