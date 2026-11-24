"""Normalize decoded Upstox market data and replay raw JSONL captures."""

import argparse
import hashlib
import json
import re
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


if __package__:
    from .validation import validate_event
else:
    from validation import validate_event


DATA_ERRORS = (ValueError, TypeError, KeyError, AttributeError, OverflowError)
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
# Persist this namespace across releases to keep replay identities stable.
EVENT_NAMESPACE = uuid.UUID("d53b2750-56dc-4ef9-89de-b6574db1b128")


def capture_identity(record):
    """Use the stored receipt identity, with a deterministic legacy fallback."""
    if "capture_id" in record:
        return str(uuid.UUID(record["capture_id"]))
    payload = json.dumps(
        record["data"], sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    name = json.dumps(["legacy", record["received_at"], digest], separators=(",", ":"))
    return str(uuid.uuid5(EVENT_NAMESPACE, name))


def assign_event_id(event, capture_id):
    entity = event.get("source_instrument_id", event["market_segment"])
    name = json.dumps(
        [event["source"], capture_id, event["event_type"], entity],
        separators=(",", ":"), ensure_ascii=False,
    )
    event["capture_id"] = capture_id
    event["event_id"] = str(uuid.uuid5(EVENT_NAMESPACE, name))


def utc_milliseconds(value):
    return (EPOCH + timedelta(milliseconds=int(value))).isoformat(
        timespec="milliseconds"
    )


def mapped_fields(data, fields):
    if not isinstance(data, dict):
        raise ValueError("expected an object containing market fields")
    result = {}
    for source, target, convert in fields:
        value = data.get(source)
        if value is None:
            continue
        if isinstance(value, bool) or (
            convert in (int, utc_milliseconds) and isinstance(value, float)
            and not value.is_integer()
        ):
            raise ValueError(f"invalid {target}")
        if target == "interval" and not isinstance(value, str):
            raise ValueError("invalid candle interval")
        result[target] = convert(value)
    return result


TRADE_FIELDS = (
    ("ltp", "last_trade_price", float),
    ("ltt", "last_trade_time", utc_milliseconds),
    ("ltq", "last_trade_quantity", int),
    ("cp", "previous_close", float),
)
SESSION_FIELDS = (
    ("atp", "average_traded_price", float),
    ("vtt", "session_volume", int),
    ("tbq", "total_bid_quantity", float),
    ("tsq", "total_ask_quantity", float),
)
CANDLE_FIELDS = (
    ("interval", "interval", lambda value: {"I1": "1m", "I30": "30m"}.get(value, value)),
    ("ts", "start_time", utc_milliseconds),
    ("open", "open", float),
    ("high", "high", float),
    ("low", "low", float),
    ("close", "close", float),
    ("vol", "volume", int),
)


def normalize_upstox(record, context="Upstox"):
    """Yield canonical events from one decoded capture record."""
    data = record["data"]
    received_at = datetime.fromisoformat(record["received_at"].replace("Z", "+00:00"))
    if received_at.utcoffset() is None:
        raise ValueError("received_at requires a timezone")
    capture_id = capture_identity(record)
    base = {
        "source": "upstox",
        "schema_version": 1,
        "received_at": received_at.astimezone(timezone.utc).isoformat(),
        **mapped_fields(data, (("currentTs", "feed_time", utc_milliseconds),)),
    }
    for kind, entries in (("market_status", data.get("marketInfo", {})),
                          ("quote_update", data.get("feeds", {}))):
        if kind == "market_status" and isinstance(entries, dict):
            entries = entries.get("segmentStatus", {})
        if not isinstance(entries, dict):
            print(f"{context}: rejected {kind}: expected an object", file=sys.stderr)
            continue
        for key, payload in entries.items():
            try:
                if kind == "market_status":
                    event = dict(base, event_type=kind, market_segment=key, status=payload)
                else:
                    event = normalize_quote(base, key, payload)
                validate_event(event)
                assign_event_id(event, capture_id)
            except DATA_ERRORS as exc:
                print(f"{context}: rejected {key!r}: {exc}", file=sys.stderr)
                continue
            yield event


def normalize_quote(base, instrument, feed):
    full = feed.get("fullFeed", {})
    quote = full.get("marketFF", full.get("indexFF", feed.get("firstLevelWithGreeks", feed)))
    event = dict(base, event_type="quote_update", source_instrument_id=instrument)
    segment, separator, identifier = instrument.partition("|")
    if not identifier.strip():
        raise ValueError("missing instrument identifier")
    if separator:
        event["market_segment"] = segment
        if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}[0-9]", identifier):
            event["isin"] = identifier

    event.update(mapped_fields(quote.get("ltpc", {}), TRADE_FIELDS))
    event.update(mapped_fields(quote, SESSION_FIELDS))
    depth = quote.get("marketLevel", {}).get("bidAskQuote")
    if depth is None and "firstDepth" in quote:
        depth = [quote["firstDepth"]]
    if depth is not None:
        for side, prefix in (("bids", "bid"), ("asks", "ask")):
            fields = ((prefix + "P", "price", float), (prefix + "Q", "quantity", int))
            levels = [mapped_fields(level, fields) for level in depth]
            event[side] = levels
    candles = quote.get("marketOHLC", {}).get("ohlc")
    if candles is not None:
        event["candles"] = [mapped_fields(candle, CANDLE_FIELDS) for candle in candles]
    return event


def encode_events(record, context="Upstox"):
    """Encode a record's events for either live storage or replay."""
    return "".join(
        json.dumps(event, ensure_ascii=False, allow_nan=False) + "\n"
        for event in normalize_upstox(record, context)
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Raw Upstox JSONL capture")
    parser.add_argument("output", type=Path, help="New canonical JSONL file")
    args = parser.parse_args()
    with args.input.open(encoding="utf-8") as source, args.output.open(
        "x", encoding="utf-8"
    ) as output:
        for line_number, line in enumerate(source, 1):
            context = f"{args.input}:{line_number}"
            try:
                output.write(encode_events(json.loads(line), context))
            except DATA_ERRORS as exc:
                print(f"{context}: rejected record: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
