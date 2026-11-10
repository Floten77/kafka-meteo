import json
from kafka import KafkaConsumer

TOPIC = "weather_stream"
BOOTSTRAP_SERVERS = "kafka:9092"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id="weather-consumer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print(f"En attente de messages sur le topic {TOPIC}...")

for message in consumer:
    print(message.value)