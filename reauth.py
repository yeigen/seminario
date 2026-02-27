import json
import sys

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from config.globals import (
    TOKEN_PATH,
    SCOPES,
    DRIVE_API_SERVICE,
    DRIVE_API_VERSION,
    build_oauth_client_config,
)


def check_token():
    if not TOKEN_PATH.exists():
        print(f"Token no encontrado en: {TOKEN_PATH}")
        return False

    try:
        token_data = json.loads(TOKEN_PATH.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        print(f"Error leyendo token: {exc}")
        return False

    has_refresh = bool(token_data.get("refresh_token"))
    has_client_id = bool(token_data.get("client_id"))
    has_client_secret = bool(token_data.get("client_secret"))
    expiry = token_data.get("expiry", "desconocido")

    print(f"Token path:         {TOKEN_PATH}")
    print(f"Refresh token:      {'presente' if has_refresh else 'AUSENTE'}")
    print(f"Client ID:          {'presente' if has_client_id else 'AUSENTE'}")
    print(f"Client Secret:      {'presente' if has_client_secret else 'AUSENTE'}")
    print(f"Expiry:             {expiry}")
    print(f"Scopes:             {token_data.get('scopes', [])}")

    if not has_refresh:
        print("\nSIN REFRESH TOKEN: Los contenedores no podrán refrescar")
        print("automáticamente. Ejecuta: python reauth.py")
        return False

    try:
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
        if creds.valid:
            print("\nToken vigente (no necesita refresh)")
            return True

        if creds.expired and creds.refresh_token:
            print("\nToken expirado, intentando refresh...")
            creds.refresh(Request())
            TOKEN_PATH.write_text(creds.to_json())
            print("Token refrescado exitosamente")
            print(f"Nuevo expiry: {creds.expiry}")
            return True
    except Exception as exc:
        print(f"\nError al validar/refrescar: {exc}")
        return False

    return False


def reauth():
    if TOKEN_PATH.exists():
        TOKEN_PATH.unlink()
        print(f"Token anterior eliminado: {TOKEN_PATH}")

    client_config = build_oauth_client_config()
    flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    creds = flow.run_local_server(port=0)
    TOKEN_PATH.write_text(creds.to_json())
    print(f"\nNuevo token guardado en: {TOKEN_PATH}")
    print(f"Scopes:         {creds.scopes}")
    print(f"Refresh token:  {'presente' if creds.refresh_token else 'AUSENTE'}")
    print(f"Expiry:         {creds.expiry}")

    service = build(DRIVE_API_SERVICE, DRIVE_API_VERSION, credentials=creds)
    results = service.files().list(pageSize=3, fields="files(id, name)").execute()
    files = results.get("files", [])
    print(f"\nVerificación — archivos encontrados: {len(files)}")
    for f in files:
        print(f"   - {f['name']}")

    print("\n" + "=" * 50)
    print("  RE-AUTENTICACIÓN EXITOSA")
    print("=" * 50)
    print("\nPróximo paso: reinicia los contenedores de Airflow:")
    print("  docker compose -f airflow/docker-compose.yaml restart")


if __name__ == "__main__":
    if "--check" in sys.argv:
        ok = check_token()
        sys.exit(0 if ok else 1)
    else:
        reauth()
