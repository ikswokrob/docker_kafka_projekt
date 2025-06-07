import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import os
import time

# Połączenie
DATABASE_URL = "postgresql+psycopg2://kafka:kafka@pg_kafka:5432/kafka_database"

# Próbuj połączyć się z bazą
max_retries = 10
for i in range(max_retries):
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            print("✅ Połączono z PostgreSQL")
            break
    except Exception as e:
        print(f"⏳ Próba {i+1}/{max_retries} - PostgreSQL niegotowe... Czekam 5s")
        time.sleep(5)
else:
    print("❌ Nie udało się połączyć z PostgreSQL")
    exit(1)

# Wczytanie danych
if not os.path.exists("dane.csv"):
    print("❌ Brak pliku dane.csv.")
    exit(1)

chunksize = 10000
i = 0

for chunk in pd.read_csv("dane.csv", chunksize=chunksize):
    chunk.to_sql("kafka_dane", con=engine, if_exists="append", index=False)
    i += len(chunk)
    print(f"✅ Załadowano {i:,} rekordów...")

print("🎉 Wszystko załadowane!")