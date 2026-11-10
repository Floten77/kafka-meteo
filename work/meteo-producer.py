import json
from kafka import KafkaProducer
from time import sleep
import requests
data = {
    "city": ["Paris", "Seville", "Dubai", "Reykjavik", "Wellington"],
    "country": ["France", "Spain", "United Arab Emirates", "Iceland", "New Zealand"],
    "latitude": [48.8566, 37.3891, 25.2048, 64.1466, -41.2865],
    "longitude": [2.3522, -5.9845, 55.2708, -21.9426, 174.7762]
}

TOPIC = "weather_stream"
BOOTSTRAP_SERVERS = "kafka:9092"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

cpt = 0
try:
    while True:
        API_URL = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={data['latitude'][cpt]}"
            f"&longitude={data['longitude'][cpt]}"
            f"&current=temperature_2m,wind_speed_10m,weather_code"
            f"&wind_speed_unit=ms"
        )
        response = requests.get(API_URL,timeout=10)
        message = {
            "city": data.get("city")[cpt],
            "country": data.get("country")[cpt],
            "weather": response.json()
        }
        producer.send(TOPIC, {"msg": message})
    
        print(f"Message envoyé")
        if cpt == 4:
            cpt = 0
            sleep(30)
        else:
            cpt+=1
            sleep(5)

except KeyboardInterrupt:
    print("\nArrêt du producteur demandé.")

finally:
    producer.flush()
    producer.close()
    print("Producteur Kafka fermé.")