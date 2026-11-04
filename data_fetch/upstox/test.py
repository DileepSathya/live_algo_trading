
import asyncio
import ssl
import requests
from pathlib import Path
import websockets
import socket

# Project root: D:\FM\live_algo_trading
PROJECT_ROOT = Path(__file__).resolve().parents[2]

TOKEN_FILE = PROJECT_ROOT / "private_data" / "access_token.txt"

AUTH_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"


def get_authorized_url():
    if not TOKEN_FILE.exists():
        raise FileNotFoundError(f"Access token not found: {TOKEN_FILE}")

    token = TOKEN_FILE.read_text(encoding="utf-8").strip()

    response = requests.get(
        AUTH_URL,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}"
        },
        timeout=20
    )

    response.raise_for_status()

    return response.json()["data"]["authorized_redirect_uri"]


async def test_connection():
    ws_url = get_authorized_url()

    print("Authorization successful.")
    print("Testing WebSocket handshake...")

    ssl_context = ssl.create_default_context()

    try:
        async with websockets.connect(
        ws_url,
        ssl=ssl_context,
        open_timeout=20,
        ping_interval=20,
        ping_timeout=10,
        family=socket.AF_INET
        ) as ws:

            print("\nSUCCESS: WebSocket connected!")
            print("Connection state:", ws.state)

            await ws.close()
            print("WebSocket closed successfully.")

    except Exception as exc:
        print("\nWEBSOCKET CONNECTION FAILED")
        print("Error type:", type(exc).__name__)
        print("Error:", repr(exc))


if __name__ == "__main__":
    asyncio.run(test_connection())
