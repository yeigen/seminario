import sys
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from config.globals import (
    SCOPES_METADATA,
    TOKEN_PATH,
    CLIENT_ID,
    CLIENT_SECRET,
    DRIVE_API_SERVICE,
    DRIVE_API_VERSION,
    build_oauth_client_config,
)

def get_credentials() -> Credentials:
    creds = None

    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES_METADATA)

    if creds and creds.valid:
        return creds

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        client_config = build_oauth_client_config()
        flow = InstalledAppFlow.from_client_config(client_config, SCOPES_METADATA)
        creds = flow.run_local_server(port=0)

    TOKEN_PATH.write_text(creds.to_json())
    return creds

def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("ERROR: CLIENT_ID o CLIENT_SECRET no encontrados en .env")
        sys.exit(1)

    if CLIENT_ID == "your_client_id_here" or CLIENT_SECRET == "your_client_secret_here":
        print(
            "ERROR: Reemplaza las credenciales placeholder en .env con tus credenciales reales de Google Cloud Console"
        )
        sys.exit(1)

    print("Autenticando con Google Drive API...")
    creds = get_credentials()

    service = build(DRIVE_API_SERVICE, DRIVE_API_VERSION, credentials=creds)

    print("Listando los primeros 5 archivos en Drive:\n")
    results = (
        service.files().list(pageSize=5, fields="files(id, name, mimeType)").execute()
    )
    files = results.get("files", [])

    if not files:
        print("No se encontraron archivos (el Drive puede estar vacío).")
    else:
        for f in files:
            print(f"  - {f['name']} ({f['mimeType']})")

    print("\n--- CONEXIÓN EXITOSA ---")
    print(f"Token guardado en: {TOKEN_PATH}")

if __name__ == "__main__":
    main()
