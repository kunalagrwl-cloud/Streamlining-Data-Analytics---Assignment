import json
import time
from datetime import datetime
from kafka import KafkaConsumer
import mysql.connector

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
KAFKA_SERVER = "localhost:9092"
TOPICS = [
    "air-quality-stream",
    "weather-stream",
    "traffic-stream",
]

# ---------------------------------------------------------
# Connect to MySQL & Create Tables
# ---------------------------------------------------------
print("Connecting to MySQL...")
db_conn = None
for _ in range(10):
    try:
        db_conn = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="root",
            database="smart_city_db"
        )
        if db_conn.is_connected():
            break
    except Exception as e:
        print(f"Connection failed: {e}")
        time.sleep(2)

if not db_conn or not db_conn.is_connected():
    raise Exception("Could not connect to MySQL container. Ensure Docker is running.")

cursor = db_conn.cursor()
print("✅ Connected to MySQL")

# Create tables matching the JSON schemas
cursor.execute("""
CREATE TABLE IF NOT EXISTS air_quality (
    id INT AUTO_INCREMENT PRIMARY KEY,
    event_time DATETIME,
    location_id VARCHAR(50),
    location_name VARCHAR(100),
    aqi INT,
    pm2_5 FLOAT,
    pm10 FLOAT,
    co FLOAT,
    no2 FLOAT,
    o3 FLOAT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS weather (
    id INT AUTO_INCREMENT PRIMARY KEY,
    event_time DATETIME,
    location_id VARCHAR(50),
    location_name VARCHAR(100),
    temperature_c FLOAT,
    humidity_pct INT,
    wind_speed_kmh FLOAT,
    wind_direction_deg INT,
    precipitation_mm FLOAT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS traffic (
    id INT AUTO_INCREMENT PRIMARY KEY,
    event_time DATETIME,
    location_id VARCHAR(50),
    location_name VARCHAR(100),
    current_speed_kmh FLOAT,
    free_flow_speed_kmh FLOAT,
    travel_time_sec INT,
    traffic_flow_condition VARCHAR(50)
)
""")
db_conn.commit()

# ---------------------------------------------------------
# Connect to Kafka
# ---------------------------------------------------------
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_SERVER,
    group_id="assignment3-mysql-group",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("✅ Connected to Kafka")
print("📡 Listening and streaming into MySQL...")

# ---------------------------------------------------------
# Consume messages & Insert
# ---------------------------------------------------------
try:
    for message in consumer:
        topic = message.topic
        d = message.value
        
        # Standardize timestamp
        raw_time = d.get("event_time", "")
        formatted_time = raw_time.replace("T", " ") if "T" in raw_time else raw_time

        if topic == "air-quality-stream":
            query = """
            INSERT INTO air_quality (event_time, location_id, location_name, aqi, pm2_5, pm10, co, no2, o3)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                formatted_time, d.get("location_id"), d.get("location_name"),
                d.get("aqi"), d.get("pm2_5"), d.get("pm10"),
                d.get("co"), d.get("no2"), d.get("o3")
            ))

        elif topic == "weather-stream":
            query = """
            INSERT INTO weather (event_time, location_id, location_name, temperature_c, humidity_pct, wind_speed_kmh, wind_direction_deg, precipitation_mm)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                formatted_time, d.get("location_id"), d.get("location_name"),
                d.get("temperature_c"), d.get("humidity_pct"), d.get("wind_speed_kmh"),
                d.get("wind_direction_deg"), d.get("precipitation_mm")
            ))

        elif topic == "traffic-stream":
            query = """
            INSERT INTO traffic (event_time, location_id, location_name, current_speed_kmh, free_flow_speed_kmh, travel_time_sec, traffic_flow_condition)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                formatted_time, d.get("location_id"), d.get("location_name"),
                d.get("current_speed_kmh"), d.get("free_flow_speed_kmh"), d.get("travel_time_sec"),
                d.get("traffic_flow_condition")
            ))

        db_conn.commit()
        print(f"✅ STORED IN MYSQL | {topic:<20} | {d.get('location_name', 'Unknown'):<15} | {formatted_time}")

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped.")
finally:
    cursor.close()
    db_conn.close()
    consumer.close()