import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
TOKEN_PATH = Path(__file__).parent / "token.json"

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")


def reauth():
    if TOKEN_PATH.exists():
        TOKEN_PATH.unlink()
        print(f"Token anterior eliminado: {TOKEN_PATH}")

    client_config = {
        "installed": {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    }
    flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    creds = flow.run_local_server(port=0)
    TOKEN_PATH.write_text(creds.to_json())
    print(f"Nuevo token guardado en: {TOKEN_PATH}")
    print(f"Scopes: {creds.scopes}")

    service = build("drive", "v3", credentials=creds)
    results = service.files().list(pageSize=3, fields="files(id, name)").execute()
    files = results.get("files", [])
    print(f"\nVerificación - archivos encontrados: {len(files)}")
    for f in files:
        print(f"  - {f['name']}")
    print("\n--- RE-AUTENTICACIÓN EXITOSA ---")


if __name__ == "__main__":
    reauth()
