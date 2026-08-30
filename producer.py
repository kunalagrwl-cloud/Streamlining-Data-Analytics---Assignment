"""
Assignment 2 - Kafka Producer
Reads each sample .jsonl file line by line and streams the records to its
corresponding Kafka topic, with a small delay between messages to simulate
a live event stream rather than a bulk dump.
"""

import json
import time
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
DELAY_SECONDS = 0.5

SOURCES = [
    {"file": "air_quality.jsonl", "topic": "air-quality-stream"},
    {"file": "weather.jsonl", "topic": "weather-stream"},
    {"file": "traffic.jsonl", "topic": "traffic-stream"},
]


def build_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def stream_source(producer, filepath, topic):
    with open(filepath, "r") as f:
        for line in f:
            record = json.loads(line)
            key = record.get("location_id")
            producer.send(topic, key=key, value=record)
            print(f"[{topic}] sent record for {key} at {record.get('event_time')}")
            time.sleep(DELAY_SECONDS)


if __name__ == "__main__":
    producer = build_producer()
    for source in SOURCES:
        print(f"\nStreaming {source['file']} -> topic '{source['topic']}'")
        stream_source(producer, source["file"], source["topic"])
    producer.flush()
    print("\nAll records sent.")