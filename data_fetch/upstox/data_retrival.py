
import sys
import json
import uuid
import time
import asyncio
import socket
import ssl

from pathlib import Path
from datetime import datetime, timezone

import requests
import websockets

from google.protobuf.json_format import MessageToDict
from upstox_client.feeder.proto import MarketDataFeedV3_pb2


# ==========================================
# CONFIGURATION
# ==========================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

TOKEN_FILE = PROJECT_ROOT / "private_data" / "access_token.txt"

DATA_DIR = PROJECT_ROOT / "market_data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = DATA_DIR / "live_market_data.jsonl"

AUTH_URL = (
    "https://api.upstox.com/v3/feed/market-data-feed/authorize"
)

VALIDATION_URL = (
    "https://api.upstox.com/v2/user/get-funds-and-margin"
)

INSTRUMENT_KEYS = [
    "NSE_EQ|INE002A01018",  # RELIANCE
    "NSE_EQ|INE467B01029",  # TCS
    "NSE_EQ|INE848E01016",  # NHPC
]

MODE = "full"

RECONNECT_INTERVAL = 5


# ==========================================
# ACCESS TOKEN
# ==========================================

def load_access_token():

    if not TOKEN_FILE.is_file():
        raise FileNotFoundError(
            f"Token file not found: {TOKEN_FILE}"
        )

    token = TOKEN_FILE.read_text(
        encoding="utf-8"
    ).strip()

    if not token:
        raise ValueError("Access token file is empty.")

    return token


# ==========================================
# TOKEN VALIDATION
# ==========================================

def validate_access_token(token):

    response = requests.get(
        VALIDATION_URL,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
        timeout=20
    )

    response.raise_for_status()

    print("Access token validated successfully.")


# ==========================================
# GET AUTHORIZED WEBSOCKET URL
# ==========================================

def get_authorized_websocket_url(token):

    response = requests.get(
        AUTH_URL,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
        timeout=20
    )

    response.raise_for_status()

    result = response.json()

    ws_url = result["data"]["authorized_redirect_uri"]

    if not ws_url.startswith("wss://"):
        raise RuntimeError(
            "Invalid WebSocket authorization URL."
        )

    return ws_url


# ==========================================
# SAVE MARKET DATA
# ==========================================

def save_market_data(message):

    record = {
        "received_at": datetime.now(
            timezone.utc
        ).isoformat(),

        "data": message
    }

    try:

        with OUTPUT_FILE.open(
            "a",
            encoding="utf-8"
        ) as file:

            file.write(
                json.dumps(
                    record,
                    ensure_ascii=False
                ) + "\n"
            )

            file.flush()

    except Exception as exc:

        print("JSON storage error:", repr(exc))


# ==========================================
# DECODE PROTOBUF
# ==========================================

def decode_protobuf(binary_message):

    feed_response = MarketDataFeedV3_pb2.FeedResponse()

    feed_response.ParseFromString(binary_message)

    return MessageToDict(feed_response)


# ==========================================
# WEBSOCKET CONNECTION
# ==========================================

async def connect_websocket(token):

    # Obtain a fresh authorized URL for every connection.
    ws_url = get_authorized_websocket_url(token)

    print("\nAuthorized WebSocket URL received.")
    print("Connecting using IPv4...")

    ssl_context = ssl.create_default_context()

    async with websockets.connect(
        ws_url,
        ssl=ssl_context,
        open_timeout=20,
        ping_interval=20,
        ping_timeout=10,
        family=socket.AF_INET
    ) as ws:

        print("\nWEBSOCKET CONNECTED SUCCESSFULLY")

        # Subscribe to instruments.
        subscribe_request = {
            "guid": str(uuid.uuid4()),
            "method": "sub",
            "data": {
                "mode": MODE,
                "instrumentKeys": INSTRUMENT_KEYS
            }
        }

        await ws.send(
            json.dumps(subscribe_request).encode("utf-8")
        )

        print("Subscription request sent.")
        print("Mode:", MODE)
        print("Instruments:", INSTRUMENT_KEYS)
        print("Waiting for market data...\n")

        # Receive market data continuously.
        async for message in ws:

            try:

                if isinstance(message, str):
                    message = message.encode("latin-1")

                decoded_data = decode_protobuf(message)

                save_market_data(decoded_data)

                print(
                    "LIVE DATA:",
                    json.dumps(
                        decoded_data,
                        ensure_ascii=False
                    )
                )

            except Exception as exc:

                print(
                    "Message decoding error:",
                    repr(exc)
                )

    print("\nWebSocket connection closed.")


# ==========================================
# MAIN APPLICATION
# ==========================================

async def main():

    token = load_access_token()

    print("Token file:", TOKEN_FILE)

    validate_access_token(token)

    print("\nOutput file:", OUTPUT_FILE)

    print("\nStarting Upstox Market Data Feed V3...")
    print("Press CTRL+C to stop.\n")

    while True:

        try:

            await connect_websocket(token)

            print(
                "\nConnection ended. "
                "Preparing to reconnect..."
            )

        except requests.exceptions.HTTPError as exc:

            print(
                "\nAuthorization HTTP error:",
                repr(exc)
            )

            if exc.response is not None:

                print(
                    "Response:",
                    exc.response.text[:500]
                )

                if exc.response.status_code in (401, 403):
                    raise RuntimeError(
                        "Authorization rejected. "
                        "Check access token."
                    ) from exc

        except Exception as exc:

            print(
                "\nConnection error:",
                repr(exc)
            )

        print(
            f"Reconnecting in {RECONNECT_INTERVAL} seconds..."
        )

        await asyncio.sleep(RECONNECT_INTERVAL)


# ==========================================
# ENTRY POINT
# ==========================================

if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        print("\nCTRL+C detected.")
        print("Stopping market data collector...")

    except Exception as exc:

        print(
            "\nFATAL ERROR:",
            repr(exc),
            file=sys.stderr
        )

        sys.exit(1)

    finally:

        print("\nApplication terminated.")
        print("Data file:", OUTPUT_FILE)
