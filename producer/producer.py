import os
import time
import json
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# define all paths here
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "github_events")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")


# wait till Kafka is ready
def wait_for_kafka(max_retries=10, delay=3):
    retries = 0
    while retries < max_retries:
        try:
            print(f"Attempting to connect to Kafka ({KAFKA_BROKER})... try {retries+1}")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka is ready.")
            return producer
        except NoBrokersAvailable:
            print("Kafka not ready, waiting...")
            time.sleep(delay)
            retries += 1
    raise RuntimeError("Kafka not available after retries")


# define request
def fetch_github_events():
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    url = "https://api.github.com/events"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    producer = wait_for_kafka()
    while True:
        print("Fetching GitHub events...")
        try:
            events = fetch_github_events()
            for event in events:
                producer.send(TOPIC, event)
            print(f"Sent {len(events)} events to Kafka topic '{TOPIC}'")
        except Exception as e:
            print(f"Error fetching or sending events: {e}")
        time.sleep(5)
