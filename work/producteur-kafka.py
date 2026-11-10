import json
from kafka import KafkaProducer

TOPIC = "weather_stream"
BOOTSTRAP_SERVERS = "kafka:9092"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Producteur Kafka démarré (tape 'exit' pour quitter)")

while True:
    message = input("Entrez votre message : ")

    if message.lower() == "exit":
        break

    producer.send(TOPIC, {"msg": message})
    producer.flush()

    print(f"Message envoyé : {message}")

producer.close()