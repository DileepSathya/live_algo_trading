"""Validate canonical market events before publishing them downstream."""

import math
from datetime import datetime


def required_text(record, key):
    value = record.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"missing or invalid {key}")
    return value


def timestamp(record, key):
    value = datetime.fromisoformat(required_text(record, key).replace("Z", "+00:00"))
    if value.utcoffset() is None:
        raise ValueError(f"{key} requires a timezone")


def number(record, key, nonnegative=False):
    value = record.get(key)
    if type(value) not in (float, int) or not math.isfinite(value):
        raise ValueError(f"missing or invalid {key}")
    if nonnegative and value < 0:
        raise ValueError(f"negative {key}")


def validate_event(event):
    """Raise ValueError for an unusable canonical market event."""
    if type(event.get("schema_version")) is not int or event["schema_version"] != 1:
        raise ValueError("unsupported schema_version")
    required_text(event, "source")
    required_text(event, "market_segment")
    timestamp(event, "received_at")
    if "feed_time" in event:
        timestamp(event, "feed_time")
    if event.get("event_type") == "market_status":
        required_text(event, "status")
        return
    if event.get("event_type") != "quote_update":
        raise ValueError("unsupported event_type")
    required_text(event, "source_instrument_id")
    if "last_trade_time" in event:
        timestamp(event, "last_trade_time")

    prices = ("last_trade_price", "previous_close", "average_traded_price")
    quantities = ("session_volume", "total_bid_quantity", "total_ask_quantity")
    for key in prices + quantities + ("last_trade_quantity",):
        if key in event:
            number(event, key, nonnegative=key not in prices)

    for side in ("bids", "asks"):
        for level in event.get(side, []):
            number(level, "price")
            number(level, "quantity", nonnegative=True)
    for candle in event.get("candles", []):
        required_text(candle, "interval")
        timestamp(candle, "start_time")
        for key in ("open", "high", "low", "close"):
            number(candle, key)
        if "volume" in candle:
            number(candle, "volume", nonnegative=True)
        if not all(candle["low"] <= candle[key] <= candle["high"] for key in ("open", "close")):
            raise ValueError("inconsistent candle OHLC")
    if not (any(key in event for key in prices + quantities)
            or any(event.get(key) for key in ("bids", "asks", "candles"))):
        raise ValueError("quote has no usable payload")
