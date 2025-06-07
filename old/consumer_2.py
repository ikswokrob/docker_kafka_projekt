from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
import random
import string

consumer = KafkaConsumer(
    'moj_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='moja_grupa_1',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

sessions = defaultdict(list)
last_seen = {}
user_discounts = defaultdict(dict)
COOLDOWN = timedelta(hours=24)
SESSION_TIMEOUT = timedelta(seconds=600)

def now_str():
    return f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"

def generate_code(prefix, length=4):
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return prefix + suffix

def can_give_discount(user_id, discount_type):
    now = datetime.now()
    last_given = user_discounts[user_id].get(discount_type)
    if last_given and now - last_given < COOLDOWN:
        remaining = COOLDOWN - (now - last_given)
        print(f"{now_str()} ⏳ {discount_type}: użytkownik {user_id} już otrzymał ten rabat. Następny możliwy za {remaining}.")
        return False
    return True

def register_discount(user_id, discount_type):
    user_discounts[user_id][discount_type] = datetime.now()

def process_session(user_id, events, user_session):
    df = pd.DataFrame(events)

    try:
        df['event_time'] = pd.to_datetime(df['event_time'], errors='coerce')
        df['synt_time'] = pd.to_datetime(df['synt_time'], errors='coerce')
        session_duration = (df['synt_time'].max() - df['synt_time'].min()).total_seconds()
        if session_duration == 0 or pd.isna(session_duration):
            return
    except Exception:
        return

    try:
        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
        value_added = df.loc[df['event_type'] == 'cart', 'price'].sum()
        value_removed = df.loc[df['event_type'] == 'remove_from_cart', 'price'].sum()
        cart_value = value_added - value_removed
        if cart_value < 0:
            return
    except Exception:
        return

    print(f"\n{now_str()} 📊 Sesja użytkownika {user_id}")
    print(f"{now_str()} Czas trwania sesji: {session_duration:.1f} sekund")

    summary = df['event_type'].value_counts()
    print(f"{now_str()} 📌 Typy zdarzeń w sesji:\n{summary}")
    print(f"{now_str()} 💰 Wartość końcowa koszyka: {cart_value:.2f} zł")

    # Reguła 1: 5% rabatu – oglądał ≥ 5, nie dodał, nie kupił
    if 'purchase' not in summary:
        if summary.get('view', 0) >= 5 and summary.get('cart', 0) == 0:
            discount_type = '5PERCENT'
            if can_give_discount(user_id, discount_type):
                code = generate_code("DISCOUNT")
                print(f"{now_str()} 🎁 Użytkownikowi {user_id} przyznano rabat 5%")
                print(f"{now_str()} 🔐 Kod rabatowy: {code}")
                register_discount(user_id, discount_type)

    # Reguła 2: koszyk 15–99.99 → darmowa dostawa
    if 15 <= cart_value < 100:
        discount_type = 'FREESHIPPING'
        if can_give_discount(user_id, discount_type):
            code = generate_code("FREESHIPPING")
            print(f"{now_str()} 🚚 Użytkownik {user_id} otrzymuje kod na darmową dostawę!")
            print(f"{now_str()} 🔐 Kod: {code}")
            register_discount(user_id, discount_type)

    # Reguła 3: koszyk ≥ 100 → próbki gratis
    if cart_value >= 100:
        discount_type = 'FREESAMPLES'
        if can_give_discount(user_id, discount_type):
            code = generate_code("FREESAMPLES")
            print(f"{now_str()} 🎉 Użytkownik {user_id} otrzymuje kod na darmowe próbki!")
            print(f"{now_str()} 🔐 Kod: {code}")
            register_discount(user_id, discount_type)

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