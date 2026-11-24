"""Normalize decoded Upstox market data and replay raw JSONL captures."""

import argparse
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path


EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def utc_milliseconds(value):
    return (EPOCH + timedelta(milliseconds=int(value))).isoformat(
        timespec="milliseconds"
    )


def mapped_fields(data, fields):
    return {
        target: convert(data[source])
        for source, target, convert in fields
        if source in data and data[source] is not None
    }


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


def normalize_upstox(record):
    """Yield canonical events from one decoded capture record."""
    data = record["data"]
    received_at = datetime.fromisoformat(record["received_at"].replace("Z", "+00:00"))
    base = {
        "source": "upstox",
        "schema_version": 1,
        "received_at": received_at.astimezone(timezone.utc).isoformat(),
        **mapped_fields(data, (("currentTs", "feed_time", utc_milliseconds),)),
    }
    for segment, status in data.get("marketInfo", {}).get("segmentStatus", {}).items():
        yield dict(base, event_type="market_status", market_segment=segment, status=status)

    for instrument, feed in data.get("feeds", {}).items():
        full = feed.get("fullFeed", {})
        quote = full.get("marketFF", full.get("indexFF", feed.get("firstLevelWithGreeks", feed)))
        event = dict(base, event_type="quote_update", source_instrument_id=instrument)
        segment, separator, identifier = instrument.partition("|")
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
                event[side] = [level for level in levels if level]
        candles = quote.get("marketOHLC", {}).get("ohlc")
        if candles is not None:
            event["candles"] = [mapped_fields(candle, CANDLE_FIELDS) for candle in candles]
        yield event


def encode_events(record):
    """Encode a record's events for either live storage or replay."""
    return "".join(
        json.dumps(event, ensure_ascii=False, allow_nan=False) + "\n"
        for event in normalize_upstox(record)
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Raw Upstox JSONL capture")
    parser.add_argument("output", type=Path, help="New canonical JSONL file")
    args = parser.parse_args()
    with args.input.open(encoding="utf-8") as source, args.output.open(
        "x", encoding="utf-8"
    ) as output:
        try:
            for line_number, line in enumerate(source, 1):
                output.write(encode_events(json.loads(line)))
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as exc:
            parser.exit(1, f"{args.input}:{line_number}: {exc}\n")


if __name__ == "__main__":
    main()
