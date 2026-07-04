"""
Deliverable 1: Ingestion layer — Kafka Producer.

Simulates a live stream of incoming support tickets by reading the sample
dataset and publishing each ticket to Kafka with a small delay, as if new
tickets were arriving in real time. In production this producer would sit
behind a webhook/API instead of reading from a static file.
"""
import json
import os
import time

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "support_tickets_raw")
DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "sample_tickets.json")


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def stream_tickets(delay_seconds: float = 0.02) -> None:
    producer = build_producer()
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        tickets = json.load(f)

    for ticket in tickets:
        producer.send(TOPIC_RAW, key=ticket["ticket_id"], value=ticket)
        print(f"[producer] sent {ticket['ticket_id']} -> topic '{TOPIC_RAW}'")
        producer.flush()
        time.sleep(delay_seconds)

    print("[producer] finished streaming all tickets.")


if __name__ == "__main__":
    stream_tickets()