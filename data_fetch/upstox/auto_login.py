"""
Reconstructed from the user's screenshots of the Data for Algo Traders tutorial.

This is a best-effort runnable reconstruction, not a verbatim transcript:
some lines in the video screenshots were not visible. Use only with your own
Upstox API credentials and in accordance with Upstox's terms.
"""

import asyncio
import os
from urllib.parse import parse_qs, quote, urlparse

import pyotp
import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright


load_dotenv()

API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
REDIRECT_URI = os.getenv("URL")
MOBILE_NO = os.getenv("MOBILE_NO")
TOTP_KEY = os.getenv("TOTP_KEY")
PIN = os.getenv("PIN")
TOKEN_FILE = os.getenv("TOKEN_FILE", "access_token.txt")

AUTH_URL = (
    "https://api.upstox.com/v2/login/authorization/dialog"
    f"?response_type=code&client_id={quote(API_KEY or '', safe='')}"
    f"&redirect_uri={quote(REDIRECT_URI or '', safe='')}"
)


def get_access_token(code: str) -> str:
    """Exchange the OAuth authorization code for an Upstox access token."""
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
    response.raise_for_status()
    payload = response.json()

    if "access_token" not in payload:
        raise RuntimeError(f"Token response did not contain access_token: {payload}")

    return payload["access_token"]


def check_token_validity(access_token: str) -> bool:
    """Check a cached token against the Upstox funds-and-margin endpoint."""
    url = "https://api.upstox.com/v2/user/get-funds-and-margin"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
    except requests.RequestException as exc:
        print(f"Token validation request failed: {exc}")
        return False

    if response.status_code != 200:
        print(f"Token validation failed: {response.status_code} {response.reason}")
        print(response.text)
        return False

    print("Token validation response:")
    print(response.json())
    return True


def load_cached_token():
    if not os.path.exists(TOKEN_FILE):
        return None

    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as token_file:
            token = token_file.read().strip()
            return token or None
    except OSError as exc:
        print(f"Failed to read cached token: {exc}")
        return None


def save_cached_token(access_token: str):
    try:
        with open(TOKEN_FILE, "w", encoding="utf-8") as token_file:
            token_file.write(access_token)
    except OSError as exc:
        print(f"Failed to save access token: {exc}")


async def run(playwright) -> str:
    """Log in through the Upstox web flow and return the access token."""
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
            + ". Fill them in your .env file."
        )

    browser = await playwright.firefox.launch(headless=False)
    context = await browser.new_context()
    page = await context.new_page()

    try:
        # The redirect URL receives the authorization code after successful login.
        await page.goto(AUTH_URL, wait_until="domcontentloaded")

        await page.locator("#mobileNum").fill(MOBILE_NO)
        await page.get_by_role("button", name="Get OTP").click()

        otp = pyotp.TOTP(TOTP_KEY).now()
        await page.locator("#otpNum").fill(otp)
        await page.get_by_role("button", name="Continue").click()

        await page.get_by_label("Enter 6-digit PIN").fill(PIN)
        await page.get_by_label("Enter 6-digit PIN").press("Tab")
        # Some versions of the login page expose the PIN fields/labels differently.
        # If the first selector fails, inspect the current page and update selectors.
        await page.get_by_role("button", name="Continue").click()

        # Wait for the redirect URI and extract its ?code=... parameter.
        await page.wait_for_url(
            lambda url: url.startswith(REDIRECT_URI),
            timeout=120_000,
        )
        parsed = urlparse(page.url)
        query = parse_qs(parsed.query)
        code_values = query.get("code")
        if not code_values:
            raise RuntimeError(
                "Redirect completed but no authorization code was found in the URL."
            )

        token = get_access_token(code_values[0])
        save_cached_token(token)
        return token
    finally:
        await context.close()
        await browser.close()


async def main():
    cached_token = load_cached_token()
    if cached_token and check_token_validity(cached_token):
        print(f"Valid cached access token found in {TOKEN_FILE}.")
        return

    async with async_playwright() as playwright:
        access_token = await run(playwright)
        print(f"Access token saved to {TOKEN_FILE}.")
        print(access_token)


if __name__ == "__main__":
    asyncio.run(main())
