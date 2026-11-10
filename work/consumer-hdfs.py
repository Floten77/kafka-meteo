
import json
import re
import time
import requests
from kafka import KafkaConsumer

HDFS_URL = "http://namenode:9870/webhdfs/v1"
HDFS_USER = "root"

consumer = KafkaConsumer(
    "weather_transformed",
    bootstrap_servers="kafka:9092",
    group_id="weather-hdfs-consumer",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda v: v.decode("utf-8")
)

session = requests.Session()


def safe_path(value):
    return re.sub(r"[^a-zA-Z0-9 _-]", "_", value).strip()


def hdfs_request(method, path, operation, **kwargs):
    url = f"{HDFS_URL}{path}"

    params = {
        "op": operation,
        "user.name": HDFS_USER
    }


    response = session.request(
        method,
        url,
        params=params,
        allow_redirects=True,
        timeout=30,
        **kwargs
    )

    if not response.ok:
        print("Code HTTP :", response.status_code)
        print("Réponse HDFS :", response.text)
        response.raise_for_status()
    return response


def save_to_hdfs(message, country, city):
    directory = f"/hdfs-data/{country}/{city}"
    filepath = f"{directory}/alerts.json"


    hdfs_request("PUT", directory, "MKDIRS")

    response = session.get(
        f"{HDFS_URL}{filepath}",
        params={
            "op": "GETFILESTATUS",
            "user.name": HDFS_USER
        },
        timeout=30
    )

    if response.status_code == 404:
        operation = "CREATE"
        method = "PUT"
    else:
        response.raise_for_status()
        operation = "APPEND"
        method = "POST"


    hdfs_request(
        method,
        filepath,
        operation,
        data=(message + "\n").encode("utf-8")
    )

    return filepath


print("Consommateur Kafka → HDFS démarré")

try:
    for record in consumer:
        message = record.value

        try:
            data = json.loads(message)

            if "country" not in data or "city" not in data:
                print(f"Message invalide : {message}")
                continue

            country = safe_path(data["country"])
            city = safe_path(data["city"])

            filepath = save_to_hdfs(message, country, city)


            consumer.commit()

            print(f"Message enregistré : {filepath}")

        except requests.RequestException as e:
            print(f"Erreur HDFS : {e}")
            print("Arrêt pour éviter de valider un message non enregistré")
            break

except KeyboardInterrupt:
    print("\nArrêt du consommateur")

finally:
    consumer.close()
    session.close()
    print("Consommateur fermé")
