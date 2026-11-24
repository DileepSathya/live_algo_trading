"""Follow the collector's append-only JSONL capture and publish canonical events."""

import json
import os
import time
from pathlib import Path

from ingestion_normalization.normalization import normalize_upstox

TOPICS = {"quote_update": "market.quotes.v1", "market_status": "market.status.v1"}


def create_producer(brokers):
    from confluent_kafka import Producer

    return Producer({"bootstrap.servers": brokers, "enable.idempotence": True,
                     "acks": "all", "delivery.timeout.ms": 45000})


def publish_capture(producer, record):
    errors = []

    def delivered(error, message):
        if error is not None:
            errors.append(str(error))

    for event in normalize_upstox(record):
        entity = event.get("source_instrument_id", event["market_segment"])
        key = f"{event['source']}:{entity}"
        value = json.dumps(event, ensure_ascii=False, allow_nan=False)
        while True:
            try:
                producer.produce(TOPICS[event["event_type"]], key=key,
                                 value=value, on_delivery=delivered)
                break
            except BufferError:
                producer.poll(0.1)
    if producer.flush(60) or errors:
        raise RuntimeError(f"Kafka capture delivery incomplete: {errors[:1]}")


def publish_batch(stream, producer, limit=100):
    start = stream.tell()
    for _ in range(limit):
        position = stream.tell()
        line = stream.readline()
        if not line.endswith(b"\n"):
            stream.seek(position)
            break
        try:
            publish_capture(producer, json.loads(line))
        except Exception as exc:
            raise RuntimeError(f"Capture processing failed at byte {position}") from exc
    if stream.tell() == start:
        return False
    return True


def main():
    source = Path(os.environ["CAPTURE_FILE"])
    checkpoint = Path(os.environ["CHECKPOINT_FILE"])
    producer = create_producer(os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    with source.open("rb") as stream:
        if checkpoint.exists():
            state = json.loads(checkpoint.read_text())
            if state["source"] != str(source) or not 0 <= state["offset"] <= source.stat().st_size:
                raise ValueError("Checkpoint does not match the append-only capture")
            stream.seek(state["offset"])
        while True:
            if publish_batch(stream, producer):
                temp = checkpoint.with_suffix(".tmp")
                temp.write_text(json.dumps({"source": str(source), "offset": stream.tell()}))
                temp.replace(checkpoint)
            else:
                time.sleep(0.2)


if __name__ == "__main__":
    main()
