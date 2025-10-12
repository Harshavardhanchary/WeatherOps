import os
import json
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import psycopg2
from psycopg2 import OperationalError
from prometheus_client import start_http_server, Counter

# -----------------------------
# Environment Variables
# -----------------------------
KAFKA_SERVER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather")

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "weatherdb")
DB_USER = os.getenv("POSTGRES_USER", "user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "password")

# -----------------------------
# Start Prometheus metrics server
# -----------------------------
start_http_server(8001)
messages_consumed = Counter('weather_messages_consumed_total', 'Total weather messages consumed')

# -----------------------------
# Retry Kafka Connection
# -----------------------------
for attempt in range(10):
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='weather-group'
        )
        print("✅ Connected to Kafka!")
        break
    except NoBrokersAvailable:
        print(f"⏳ Kafka not ready, retrying... ({attempt + 1}/10)")
        time.sleep(5)
else:
    raise Exception("❌ Could not connect to Kafka after retries")

# -----------------------------
# Retry PostgreSQL Connection
# -----------------------------
for attempt in range(10):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()
        print("✅ Connected to PostgreSQL!")
        break
    except OperationalError:
        print(f"⏳ PostgreSQL not ready, retrying... ({attempt + 1}/10)")
        time.sleep(5)
else:
    raise Exception("❌ Could not connect to PostgreSQL after retries")

# -----------------------------
# Create Table if Not Exists
# -----------------------------
cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        city TEXT,
        temperature FLOAT,
        humidity FLOAT,
        timestamp BIGINT
    );
""")
conn.commit()

print("Consumer is running...")

# -----------------------------
# Start Consuming Messages
# -----------------------------
for msg in consumer:
    try:
        data = json.loads(msg.value.decode('utf-8'))
        cursor.execute("""
            INSERT INTO weather_data (city, temperature, humidity, timestamp)
            VALUES (%s, %s, %s, %s);
        """, (
            data['city'],
            data['temperature'],
            data['humidity'],
            data['timestamp']
        ))
        conn.commit()

        # Increment Prometheus counter
        messages_consumed.inc()

        print(f"Stored in DB: {data}")
    except Exception as e:
        print(f"Error inserting to DB: {e}")

