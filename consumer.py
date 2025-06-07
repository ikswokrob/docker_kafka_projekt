from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
import random
import string
import os
from openpyxl import load_workbook

consumer = KafkaConsumer(
    'topic',
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

EXCEL_PATH = r'C:\Users\kiksw\vs_projects\docker_kafka_producer_projekt\rabat_log.xlsx'


def generate_code(prefix, length=4):
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return prefix + suffix

def can_give_discount(user_id, discount_type, current_time):
    last_given = user_discounts[user_id].get(discount_type)
    if last_given and current_time - last_given < COOLDOWN:
        remaining = COOLDOWN - (current_time - last_given)
        print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] ⏳ {discount_type}: użytkownik {user_id} już otrzymał ten rabat. Następny możliwy za {remaining}.")
        return False
    return True

def register_discount(user_id, discount_type, code, current_time):
    user_discounts[user_id][discount_type] = current_time
    save_discount_to_excel(user_id, discount_type, code, current_time)

def save_discount_to_excel(user_id, discount_type, code, current_time):
    row = {
        'user_id': user_id,
        'discount_code': code,
        'discount_type': discount_type,
        'timestamp': current_time.strftime('%Y-%m-%d %H:%M:%S')
    }

    df_new = pd.DataFrame([row])

    if os.path.exists(EXCEL_PATH):
        with pd.ExcelWriter(EXCEL_PATH, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
            reader = pd.read_excel(EXCEL_PATH)
            startrow = len(reader) + 1
            df_new.to_excel(writer, index=False, header=False, startrow=startrow)
    else:
        df_new.to_excel(EXCEL_PATH, index=False)

def process_session(user_id, events, user_session):
    df = pd.DataFrame(events)

    try:
        df['synt_time'] = pd.to_datetime(df['synt_time'], errors='coerce')
        session_start = df['synt_time'].min()
        session_end = df['synt_time'].max()
        session_duration = (session_end - session_start).total_seconds()
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
	
    print("-" * 40)	
    print(f"Sesja użytkownika {user_id}")
    print(f"Początek sesji: {session_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Koniec sesji:   {session_end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Czas trwania sesji: {session_duration:.1f} sekund")

    summary = df['event_type'].value_counts()
    print(f"Typy zdarzeń w sesji:\n{summary.to_string()}")
    print(f"Wartość końcowa koszyka: {cart_value:.2f} zł")

    if 'purchase' not in summary:
        if summary.get('view', 0) >= 5 and summary.get('cart', 0) == 0:
            discount_type = '5PERCENT'
            if can_give_discount(user_id, discount_type, session_end):
                code = generate_code("DISCOUNT")
                print(f"Użytkownikowi {user_id} przyznano rabat 5%")
                print(f"Kod rabatowy: {code}")
                register_discount(user_id, discount_type, code, session_end)

    if 15 <= cart_value < 100:
        discount_type = 'FREESHIPPING'
        if can_give_discount(user_id, discount_type, session_end):
            code = generate_code("FREESHIPPING")
            print(f"Użytkownik {user_id} otrzymuje kod na darmową dostawę!")
            print(f"Kod: {code}")
            register_discount(user_id, discount_type, code, session_end)

    if cart_value >= 100:
        discount_type = 'FREESAMPLES'
        if can_give_discount(user_id, discount_type, session_end):
            code = generate_code("FREESAMPLES")
            print(f"Użytkownik {user_id} otrzymuje kod na darmowe próbki!")
            print(f"Kod: {code}")
            register_discount(user_id, discount_type, code, session_end)

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
