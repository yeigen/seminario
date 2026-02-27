import os
import json
import sys
from pathlib import Path
from urllib.parse import urlencode
from dotenv import load_dotenv

load_dotenv()

TOKEN_PATH = Path(__file__).parent / "token.json"
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def step1_get_auth_url():
    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
    }
    url = f"https://accounts.google.com/o/oauth2/auth?{urlencode(params)}"
    print("Abre esta URL en tu navegador y autoriza la app:")
    print()
    print(url)
    print()
    print("Después pega el código de autorización y ejecuta:")
    print(f"  python {__file__} <CODIGO>")


def step2_exchange_code(code: str):
    import requests

    response = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "code": code,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "redirect_uri": REDIRECT_URI,
            "grant_type": "authorization_code",
        },
    )
    data = response.json()
    if "error" in data:
        print(f"Error: {data}")
        return
    token_data = {
        "token": data["access_token"],
        "refresh_token": data.get("refresh_token", ""),
        "token_uri": "https://oauth2.googleapis.com/token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scopes": SCOPES,
        "universe_domain": "googleapis.com",
        "account": "",
    }
    TOKEN_PATH.write_text(json.dumps(token_data))
    print(f"Token guardado en: {TOKEN_PATH}")
    print(f"Scopes: {SCOPES}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        step2_exchange_code(sys.argv[1])
    else:
        step1_get_auth_url()
