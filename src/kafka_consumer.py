"""
Deliverable 1: Ingestion layer — Kafka Consumer with schema validation.

Reads raw tickets from Kafka, validates each one against TICKET_SCHEMA.
- Valid tickets  -> written as newline-delimited JSON to the Bronze landing
                    zone (picked up later by delta_lakehouse.py).
- Invalid tickets -> routed to a Dead Letter Queue (DLQ) topic/file instead
                    of silently dropped or crashing the pipeline.
"""
import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

from schema import validate_ticket

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "support_tickets_raw")
TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "support_tickets_dlq")
BRONZE_LANDING_FILE = os.path.join(
    os.path.dirname(__file__), "..", "lakehouse", "bronze", "_landing", "tickets.jsonl"
)


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="ticket-ingestion-group",
    )


def build_dlq_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def consume_and_validate(max_messages: int | None = None) -> None:
    os.makedirs(os.path.dirname(BRONZE_LANDING_FILE), exist_ok=True)
    consumer = build_consumer()
    dlq_producer = build_dlq_producer()

    count = 0
    with open(BRONZE_LANDING_FILE, "a", encoding="utf-8") as bronze_file:
        for message in consumer:
            payload = message.value
            is_valid, error = validate_ticket(payload)

            if is_valid:
                bronze_file.write(json.dumps(payload) + "\n")
                bronze_file.flush()
                print(f"[consumer] VALID   {payload['ticket_id']} -> bronze landing zone")
            else:
                dlq_producer.send(TOPIC_DLQ, value={"payload": payload, "error": error})
                dlq_producer.flush()
                print(f"[consumer] INVALID {payload.get('ticket_id', '?')} -> DLQ ({error})")

            count += 1
            if max_messages and count >= max_messages:
                break


if __name__ == "__main__":
    consume_and_validate()