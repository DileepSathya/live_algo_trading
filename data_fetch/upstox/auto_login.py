"""
Upstox OAuth login automation with a local callback server.

Required .env variables:
    API_KEY=your_upstox_client_id
    SECRET_KEY=your_upstox_client_secret
    URL=http://127.0.0.1:8080/
    MOBILE_NO=your_registered_mobile_number
    TOTP_KEY=your_totp_secret
    PIN=your_upstox_pin

Optional:
    TOKEN_FILE=access_token.txt

Use only with your own Upstox account and in accordance with Upstox's terms.
"""

import asyncio
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse

import pyotp
import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright
# auto_login.py -> upstox -> data_fetch -> live_algo_trading (project root)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PRIVATE_DIR = PROJECT_ROOT / "private_data"
PRIVATE_DIR.mkdir(exist_ok=True)

load_dotenv(PRIVATE_DIR / ".env")

API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
REDIRECT_URI = os.getenv("URL", "http://127.0.0.1:8080/")
MOBILE_NO = os.getenv("MOBILE_NO")
TOTP_KEY = os.getenv("TOTP_KEY")
PIN = os.getenv("PIN")

TOKEN_FILE = PRIVATE_DIR / os.getenv("TOKEN_FILE", "access_token.txt")

AUTH_URL = (
    "https://api.upstox.com/v2/login/authorization/dialog"
    f"?response_type=code&client_id={quote(API_KEY or '', safe='')}"
    f"&redirect_uri={quote(REDIRECT_URI or '', safe='')}"
)

_callback_event = threading.Event()
_callback_lock = threading.Lock()
_callback_code = None
_callback_error = None


def reset_callback_state():
    global _callback_code, _callback_error
    with _callback_lock:
        _callback_code = None
        _callback_error = None
    _callback_event.clear()


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """Receive the OAuth authorization code at the registered redirect URI."""

    def do_GET(self):
        global _callback_code, _callback_error

        expected = urlparse(REDIRECT_URI)
        actual = urlparse(self.path)
        query = parse_qs(actual.query)

        # Only accept the path configured in the registered redirect URI.
        expected_path = expected.path or "/"
        if actual.path != expected_path:
            self.send_error(404, "Unexpected callback path")
            return

        error = query.get("error", [None])[0]
        code = query.get("code", [None])[0]

        if error:
            _callback_error = (
                f"Upstox authorization failed: {error}. "
                f"{query.get('error_description', [''])[0]}"
            )
            self._respond(400, "Upstox authorization failed. You may close this tab.")
            _callback_event.set()
            return

        if not code:
            self._respond(
                400,
                "No authorization code was received. Return to the terminal for details.",
            )
            return

        with _callback_lock:
            _callback_code = code

        self._respond(
            200,
            "Upstox authorization received. You may close this browser tab.",
        )
        _callback_event.set()

    def _respond(self, status, message):
        body = (
            "<!doctype html><html><head><meta charset='utf-8'>"
            "<title>Upstox login</title></head><body>"
            f"<h2>{message}</h2></body></html>"
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format_string, *args):
        # Avoid printing authorization callback URLs (which contain the code).
        pass


def start_callback_server():
    parsed = urlparse(REDIRECT_URI)
    if parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "localhost"):
        raise RuntimeError(
            "This script's local callback server requires an HTTP localhost URL, "
            "such as http://127.0.0.1:8080/."
        )

    try:
        server = HTTPServer((parsed.hostname, parsed.port or 80), OAuthCallbackHandler)
    except OSError as exc:
        raise RuntimeError(
            f"Could not start callback server at {parsed.hostname}:{parsed.port or 80}. "
            "Check whether another process is using the port."
        ) from exc

    server.timeout = 0.5
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"Callback server listening at {REDIRECT_URI}")
    return server


def get_access_token(code: str) -> str:
    """Exchange the one-time OAuth authorization code for an access token."""
    url = "https://api.upstox.com/v2/login/authorization/token"
    headers = {
        "accept": "application/json",
        "Api-Version": "2.0",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "code": code,
        "client_id": API_KEY,
        "client_secret": SECRET_KEY,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    }

    response = requests.post(url, headers=headers, data=data, timeout=30)
    if not response.ok:
        raise RuntimeError(
            f"Upstox token exchange failed (HTTP {response.status_code}): "
            f"{response.text[:1000]}"
        )

    payload = response.json()
    token = payload.get("access_token")
    if not token:
        raise RuntimeError(
            f"Token response did not contain access_token: {payload}"
        )
    return token


def check_token_validity(access_token: str) -> bool:
    """Check a cached token against Upstox's funds-and-margin endpoint."""
    url = "https://api.upstox.com/v2/user/get-funds-and-margin"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
    }

    try:
        response = requests.get(url, headers=headers, timeout=20)
    except requests.RequestException as exc:
        print(f"Token validation request failed: {exc}")
        return False

    if response.status_code == 200:
        print("Cached access token is valid.")
        return True

    print(
        f"Cached token validation failed: HTTP {response.status_code}. "
        "A new login will be attempted."
    )
    return False


def load_cached_token():
    try:
        token = TOKEN_FILE.read_text(encoding="utf-8").strip()
        return token or None
    except FileNotFoundError:
        return None
    except OSError as exc:
        print(f"Could not read cached token: {exc}")
        return None


def save_cached_token(access_token: str):
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(access_token, encoding="utf-8")
    try:
        os.chmod(TOKEN_FILE, 0o600)
    except OSError:
        pass


async def automate_upstox_login(playwright):
    """Complete the browser login and wait for the local OAuth callback."""
    required = {
        "API_KEY": API_KEY,
        "SECRET_KEY": SECRET_KEY,
        "URL": REDIRECT_URI,
        "MOBILE_NO": MOBILE_NO,
        "TOTP_KEY": TOTP_KEY,
        "PIN": PIN,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError(
            "Missing environment variables: " + ", ".join(missing)
            + ". Check your .env file."
        )

    browser = await playwright.firefox.launch(headless=False)
    context = await browser.new_context()
    page = await context.new_page()

    try:
        print("Opening Upstox authorization page...")
        await page.goto(AUTH_URL, wait_until="domcontentloaded", timeout=60000)

        await page.locator("#mobileNum").fill(MOBILE_NO)
        await page.get_by_role("button", name="Get OTP").click()

        # Generate the current TOTP only after requesting the OTP page.
        otp = pyotp.TOTP(TOTP_KEY).now()
        await page.locator("#otpNum").fill(otp)
        await page.get_by_role("button", name="Continue").click()

        # This selector may change if Upstox updates its login UI.
        pin_field = page.get_by_label("Enter 6-digit PIN")
        await pin_field.wait_for(state="visible", timeout=30000)
        await pin_field.fill(PIN)
        await page.get_by_role("button", name="Continue").click()

        print("Waiting for Upstox to redirect to the local callback...")
        # Do not wait for page.wait_for_url(REDIRECT_URI): the browser's
        # localhost navigation may fail if the callback server is unavailable.
        # The server thread receives the callback independently.
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if _callback_event.wait(timeout=0.25):
                break
            if page.is_closed():
                raise RuntimeError("The login browser closed before callback arrived.")
        else:
            raise TimeoutError(
                "Timed out waiting for the OAuth callback. Check the login page, "
                "redirect URI registration, and browser automation selectors."
            )

        if _callback_error:
            raise RuntimeError(_callback_error)

        with _callback_lock:
            code = _callback_code

        if not code:
            raise RuntimeError(
                "Callback was received without an authorization code. "
                "Check the Upstox authorization result."
            )

        return get_access_token(code)

    finally:
        await context.close()
        await browser.close()


async def main():
    cached_token = load_cached_token()
    if cached_token and check_token_validity(cached_token):
        print(f"Valid cached access token found in {TOKEN_FILE}.")
        return

    reset_callback_state()
    server = start_callback_server()

    try:
        async with async_playwright() as playwright:
            token = await automate_upstox_login(playwright)

        save_cached_token(token)
        print(f"Access token saved to: {TOKEN_FILE.resolve()}")
        print("Do not share your access token or commit it to Git.")
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nLogin cancelled.")
    except Exception as exc:
        print(f"\nERROR: {exc}")
        raise
