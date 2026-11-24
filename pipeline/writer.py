"""Persist one Kafka consumer group's events in TimescaleDB."""

import json
import os

from ingestion_normalization.validation import validate_event
from pipeline.publisher import TOPICS

INSERT = """
INSERT INTO market_events
    (received_at, event_id, source, event_type, source_instrument_id,
     market_segment, feed_time, payload)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
ON CONFLICT (received_at, event_id) DO NOTHING
"""


def persist_batch(connection, consumer, messages, topic_partition):
    rows = []
    offsets = {}
    for message in messages:
        location = (message.topic(), message.partition())
        try:
            if message.error():
                raise RuntimeError(str(message.error()))
            payload = message.value().decode("utf-8")
            event = json.loads(payload)
            validate_event(event)
            if TOPICS[event["event_type"]] != message.topic():
                raise ValueError("Event type does not match topic")
            rows.append((event["received_at"], event["event_id"], event["source"],
                         event["event_type"], event.get("source_instrument_id"),
                         event["market_segment"], event.get("feed_time"), payload))
        except Exception as exc:
            raise RuntimeError(f"Invalid message at {location} offset {message.offset()}") from exc
        offsets[location] = message.offset() + 1

    with connection.transaction(), connection.cursor() as cursor:
        cursor.executemany(INSERT, rows)
    committed = consumer.commit(
        offsets=[topic_partition(topic, partition, offset)
                 for (topic, partition), offset in offsets.items()],
        asynchronous=False,
    )
    for partition in committed:
        if partition.error:
            raise RuntimeError(str(partition.error))


def main():
    import psycopg
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                         "group.id": "timescale-writer", "auto.offset.reset": "earliest",
                         "enable.auto.commit": False, "enable.auto.offset.store": False})
    try:
        with psycopg.connect(connect_timeout=10, autocommit=True) as connection:
            consumer.subscribe(list(TOPICS.values()))
            while True:
                messages = consumer.consume(num_messages=500, timeout=1)
                if messages:
                    persist_batch(connection, consumer, messages, TopicPartition)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
