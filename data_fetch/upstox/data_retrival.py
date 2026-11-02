
import sys
from pathlib import Path

import requests
import upstox_client


# Project layout:
# live_algo_trading/
#   data/access_token.txt
#   data_fetch/upstox/data_retrival.py

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TOKEN_FILE = PROJECT_ROOT / "private_data" / "access_token.txt"

# Replace these with the Upstox instrument keys for your stocks.
INSTRUMENT_KEYS = [
    "NSE_EQ|INE002A01018",  # RELIANCE
    "NSE_EQ|INE467B01029",  # TCS
    "NSE_EQ|INE848E01016",  # NHPC
]

MODE = "full"


def load_access_token():
    if not TOKEN_FILE.is_file():
        raise FileNotFoundError(
            f"Access token file not found: {TOKEN_FILE}"
        )

    token = TOKEN_FILE.read_text(encoding="utf-8").strip()

    if not token:
        raise ValueError(f"Access token file is empty: {TOKEN_FILE}")

    return token


def validate_access_token(token):
    """Check the token against an Upstox REST API endpoint."""
    response = requests.get(
        "https://api.upstox.com/v2/user/get-funds-and-margin",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
        timeout=20,
    )

    if response.status_code == 401:
        raise RuntimeError(
            "Upstox rejected the access token (401). "
            "Generate a fresh token using the official OAuth flow."
        )

    if response.status_code != 200:
        raise RuntimeError(
            f"Token validation returned HTTP {response.status_code}: "
            f"{response.text[:500]}"
        )

    print("Access token validated successfully.")


def main():
    token = load_access_token()

    print("Token file:", TOKEN_FILE)
    print("Token length:", len(token))

    # Never print the full access token.
    validate_access_token(token)

    configuration = upstox_client.Configuration()
    configuration.access_token = token

    api_client = upstox_client.ApiClient(configuration)

    streamer = upstox_client.MarketDataStreamerV3(
        api_client,
        INSTRUMENT_KEYS,
        MODE,
    )

    def on_open():
        print("WebSocket connected.")
        print("Subscribed instruments:", INSTRUMENT_KEYS)

    def on_message(message):
        print("LIVE DATA:", message)

    def on_error(error):
        print("WebSocket error:", error)

    def on_close(*args):
        print("WebSocket closed:", args)

    streamer.on("open", on_open)
    streamer.on("message", on_message)
    streamer.on("error", on_error)
    streamer.on("close", on_close)

    streamer.auto_reconnect(True, 5, 10)

    try:
        print("Connecting to Upstox Market Data Feed V3...")
        streamer.connect()
    except KeyboardInterrupt:
        print("\nStopping feed...")
        streamer.disconnect()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
