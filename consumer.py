from kafka import KafkaConsumer
import json

# Consumer Kafka
consumer = KafkaConsumer(
    'moj_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',     # zaczyna od początku jeśli to możliwe
    enable_auto_commit=True,
    group_id='moja_grupa',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🔁 Nasłuchuję wiadomości z topicu 'moj_topic'...\n")

for message in consumer:
    print("📥 Odebrano wiadomość:")
    for k, v in message.value.items():
        print(f"  {k}: {v}")
    print("-" * 40)