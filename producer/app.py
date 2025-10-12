import os
import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from prometheus_client import start_http_server, Counter

# -----------------------------
# Environment Variables
# -----------------------------
KAFKA_SERVER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather")
CITY = os.getenv("CITY", "Hyderabad")
API_KEY = os.getenv("API_KEY", "")  # Replace with your API key

# -----------------------------
# Prometheus Metrics
# -----------------------------
start_http_server(8003)  # Expose metrics on port 8000
messages_sent = Counter('weather_messages_sent_total', 'Total weather messages sent to Kafka')

# -----------------------------
# Retry Kafka Connection
# -----------------------------
for attempt in range(10):
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)
        print("✅ Connected to Kafka!")
        break
    except NoBrokersAvailable:
        print(f"⏳ Kafka not ready, retrying... ({attempt + 1}/10)")
        time.sleep(5)
else:
    raise Exception("❌ Could not connect to Kafka after retries")

# -----------------------------
# Fetch Weather Function
# -----------------------------
def fetch_weather():
    url = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={CITY}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# -----------------------------
# Sending Loop
# -----------------------------
while True:
    try:
        weather = fetch_weather()

        data = json.dumps({
            "city": CITY,
            "temperature": weather["current"]["temp_c"],
            "humidity": weather["current"]["humidity"],
            "timestamp": weather["location"]["localtime_epoch"]
        })

        producer.send(KAFKA_TOPIC, value=data.encode('utf-8'))
        messages_sent.inc()  # Increment Prometheus metric
        print(f"Sent: {data}")

    except Exception as e:
        print(f"Error fetching/sending data: {e}")

    time.sleep(30)  # Fetch every 30 seconds

