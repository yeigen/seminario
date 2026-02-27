import json
import sys
from urllib.parse import urlencode

from config.globals import (
    TOKEN_PATH,
    CLIENT_ID,
    CLIENT_SECRET,
    GOOGLE_REDIRECT_URI_OOB,
    GOOGLE_AUTH_URI,
    GOOGLE_TOKEN_URI,
    GOOGLE_UNIVERSE_DOMAIN,
    SCOPES,
)

def step1_get_auth_url():
    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI_OOB,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
    }
    url = f"{GOOGLE_AUTH_URI}?{urlencode(params)}"
    print("Abre esta URL en tu navegador y autoriza la app:")
    print()
    print(url)
    print()
    print("Después pega el código de autorización y ejecuta:")
    print(f"  python {__file__} <CODIGO>")


def step2_exchange_code(code: str):
    import requests

    response = requests.post(
        GOOGLE_TOKEN_URI,
        data={
            "code": code,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "redirect_uri": GOOGLE_REDIRECT_URI_OOB,
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
        "token_uri": GOOGLE_TOKEN_URI,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scopes": SCOPES,
        "universe_domain": GOOGLE_UNIVERSE_DOMAIN,
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
