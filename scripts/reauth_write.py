import sys
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import (
    TOKEN_PATH,
    SCOPES_READWRITE,
    DRIVE_API_SERVICE,
    DRIVE_API_VERSION,
    build_oauth_client_config,
)


def reauth_write():
    if TOKEN_PATH.exists():
        TOKEN_PATH.unlink()
        print(f"Token anterior eliminado: {TOKEN_PATH}")

    client_config = build_oauth_client_config()
    flow = InstalledAppFlow.from_client_config(client_config, SCOPES_READWRITE)
    creds = flow.run_local_server(port=0)
    TOKEN_PATH.write_text(creds.to_json())
    print(f"Nuevo token guardado en: {TOKEN_PATH}")
    print(f"Scopes: {creds.scopes}")

    service = build(DRIVE_API_SERVICE, DRIVE_API_VERSION, credentials=creds)
    results = service.files().list(pageSize=3, fields="files(id, name)").execute()
    files = results.get("files", [])
    print(f"\nVerificación - archivos encontrados: {len(files)}")
    for f in files:
        print(f"  - {f['name']}")
    print("\n--- RE-AUTENTICACIÓN CON ESCRITURA EXITOSA ---")


if __name__ == "__main__":
    reauth_write()
