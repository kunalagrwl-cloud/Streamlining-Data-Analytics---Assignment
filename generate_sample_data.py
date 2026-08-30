"""
Assignment 2 - Sample Data Generator
Generates correlated sample data for the 3 sources chosen in Assignment 1:
  1) Air Quality (Open-Meteo Air Quality API)
  2) Weather      (Open-Meteo Weather API)
  3) Traffic      (TomTom Traffic Flow API)

All 3 files share the same set of monitoring zones (location_id) so records
across sources can later be correlated by location + time, the same way
Parv kept user_id consistent across his clickstream sources.
"""

import json
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# Shared monitoring zones across all 3 sources
LOCATIONS = [
    {"location_id": "ZONE_01", "name": "Connaught Place"},
    {"location_id": "ZONE_02", "name": "Dwarka Sector 21"},
    {"location_id": "ZONE_03", "name": "Rohini"},
    {"location_id": "ZONE_04", "name": "Saket"},
    {"location_id": "ZONE_05", "name": "Karol Bagh"},
]

RECORDS_PER_SOURCE = 150
START_TIME = datetime(2026, 8, 25, 6, 0, 0)


def ts(base, index):
    return (base + timedelta(minutes=index * 4)).strftime("%Y-%m-%dT%H:%M:%S")


def generate_air_quality(n):
    out = []
    for i in range(n):
        loc = random.choice(LOCATIONS)
        out.append({
            "event_time": ts(START_TIME, i),
            "location_id": loc["location_id"],
            "location_name": loc["name"],
            "aqi": random.randint(35, 320),
            "pm2_5": round(random.uniform(8, 220), 1),
            "pm10": round(random.uniform(15, 300), 1),
            "co": round(random.uniform(0.2, 4.5), 2),
            "no2": round(random.uniform(5, 90), 1),
            "o3": round(random.uniform(10, 120), 1),
        })
    return out


def generate_weather(n):
    out = []
    for i in range(n):
        loc = random.choice(LOCATIONS)
        out.append({
            "event_time": ts(START_TIME, i),
            "location_id": loc["location_id"],
            "location_name": loc["name"],
            "temperature_c": round(random.uniform(24, 41), 1),
            "humidity_pct": random.randint(20, 85),
            "wind_speed_kmh": round(random.uniform(0, 28), 1),
            "wind_direction_deg": random.randint(0, 359),
            "precipitation_mm": round(random.uniform(0, 12), 1),
        })
    return out


def generate_traffic(n):
    out = []
    for i in range(n):
        loc = random.choice(LOCATIONS)
        free_flow = round(random.uniform(35, 60), 1)
        current_speed = round(free_flow * random.uniform(0.25, 1.0), 1)
        out.append({
            "event_time": ts(START_TIME, i),
            "location_id": loc["location_id"],
            "location_name": loc["name"],
            "current_speed_kmh": current_speed,
            "free_flow_speed_kmh": free_flow,
            "travel_time_sec": random.randint(60, 900),
            "traffic_flow_condition": random.choice(
                ["free_flow", "moderate", "congested", "heavy_congestion"]
            ),
        })
    return out


def write_jsonl(filename, records):
    with open(filename, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"Wrote {len(records)} records to {filename}")


if __name__ == "__main__":
    write_jsonl("air_quality.jsonl", generate_air_quality(RECORDS_PER_SOURCE))
    write_jsonl("weather.jsonl", generate_weather(RECORDS_PER_SOURCE))
    write_jsonl("traffic.jsonl", generate_traffic(RECORDS_PER_SOURCE))
