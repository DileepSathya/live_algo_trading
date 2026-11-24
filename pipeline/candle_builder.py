"""Build event-time OHLC from market quotes with PostgreSQL-owned checkpoints."""

import json
import logging
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pipeline.publisher import TOPICS

TOPIC = TOPICS["quote_update"]
WINDOW = timedelta(minutes=5)

MERGE = """
INSERT INTO candle_state_5m AS c
    (source, source_instrument_id, candle_start, partition_id, open, high, low, close,
     first_time, first_offset, last_time, last_offset)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (source, source_instrument_id, candle_start) DO UPDATE SET
    high = greatest(c.high, excluded.high), low = least(c.low, excluded.low),
    open = CASE WHEN (excluded.first_time, excluded.first_offset) < (c.first_time, c.first_offset)
                THEN excluded.open ELSE c.open END,
    close = CASE WHEN (excluded.last_time, excluded.last_offset) > (c.last_time, c.last_offset)
                 THEN excluded.close ELSE c.close END,
    first_offset = CASE WHEN (excluded.first_time, excluded.first_offset) < (c.first_time, c.first_offset)
                        THEN excluded.first_offset ELSE c.first_offset END,
    last_offset = CASE WHEN (excluded.last_time, excluded.last_offset) > (c.last_time, c.last_offset)
                       THEN excluded.last_offset ELSE c.last_offset END,
    first_time = least(c.first_time, excluded.first_time),
    last_time = greatest(c.last_time, excluded.last_time)
"""


def utc(value):
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.utcoffset() is None:
        raise ValueError("timestamp needs timezone")
    return stamp.astimezone(timezone.utc)


def add_quote(batch, event, partition, offset, correction):
    if event.get("event_type") != "quote_update" or event.get("schema_version") != 1:
        raise ValueError("unexpected event type or version")
    price = event.get("last_trade_price")
    if price is None or not event.get("last_trade_time"):
        return False
    if type(price) not in (int, float) or not math.isfinite(price) or price <= 0:
        raise ValueError("invalid last_trade_price")
    trade = utc(event["last_trade_time"])
    start = trade.replace(minute=trade.minute // 5 * 5, second=0, microsecond=0)
    if utc(event["received_at"]) > start + WINDOW + correction:
        raise ValueError("outside correction period")
    key = (event["source"], event["source_instrument_id"], start, partition)
    order = (trade, offset)
    if key not in batch:
        batch[key] = [price, price, price, price, order, order]
    else:
        bar = batch[key]
        bar[1] = max(bar[1], price)
        bar[2] = min(bar[2], price)
        if order < bar[4]:
            bar[0], bar[4] = price, order
        if order > bar[5]:
            bar[3], bar[5] = price, order
    return True


def persist(db, messages, correction):
    bars, offsets = {}, {}
    skipped = 0
    for msg in messages:
        if msg.error():
            raise RuntimeError(str(msg.error()))
        partition, offset = msg.partition(), msg.offset()
        try:
            if not add_quote(bars, json.loads(msg.value()), partition, offset, correction):
                skipped += 1
        except (KeyError, TypeError, AttributeError, ValueError, OverflowError) as exc:
            logging.warning("Rejected quote partition=%s offset=%s: %s", partition, offset, exc)
        offsets[partition] = offset + 1
    if skipped:
        logging.info("Skipped %d quotes without trade data", skipped)
    with db.transaction(), db.cursor() as cur:
        cur.executemany(MERGE, [(*key, op, hi, lo, cl, *first, *last)
                               for key, (op, hi, lo, cl, first, last) in bars.items()])
        cur.executemany("""INSERT INTO candle_offsets VALUES (%s,%s)
                        ON CONFLICT (partition_id) DO UPDATE SET next_offset=excluded.next_offset""",
                        offsets.items())
    return offsets


def main():
    import psycopg
    from confluent_kafka import Consumer

    logging.basicConfig(level=logging.INFO)
    grace = timedelta(seconds=int(os.getenv("CANDLE_GRACE_SECONDS", "10")))
    correction = timedelta(seconds=int(os.getenv("CANDLE_CORRECTION_SECONDS", "86400")))
    if grace < timedelta(0) or correction < grace:
        raise ValueError("correction period must cover nonnegative grace period")
    with psycopg.connect(autocommit=True) as db:
        # This service is a single writer, including while restoring its offsets.
        db.execute("SELECT pg_advisory_lock(583510995)")
        db.execute(Path(__file__).with_name("candles.sql").read_text())
        consumer = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                             "group.id": "candle-builder-5m-v1", "enable.auto.commit": False,
                             "auto.offset.reset": "error"})
        positions = {}

        def assign(client, parts):
            positions.clear()
            for part in parts:
                low, high = client.get_watermark_offsets(part, timeout=10)
                row = db.execute("SELECT next_offset FROM candle_offsets WHERE partition_id=%s",
                                 (part.partition,)).fetchone()
                part.offset = row[0] if row else low
                if not low <= part.offset <= high:
                    raise RuntimeError("Candle checkpoint outside retained Kafka history")
                positions[part.partition] = part.offset
            client.assign(parts)

        try:
            consumer.subscribe([TOPIC], on_assign=assign)
            while True:
                messages = consumer.consume(500, timeout=1)
                if messages:
                    positions.update(persist(db, messages, correction))
                for part in consumer.assignment():
                    _, high = consumer.get_watermark_offsets(part, timeout=10)
                    if positions.get(part.partition) == high:
                        db.execute("""UPDATE candle_state_5m SET complete=true
                                   WHERE partition_id=%s AND NOT complete
                                   AND candle_start <= now() - %s""", (part.partition, WINDOW + grace))
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
