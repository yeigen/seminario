import sys
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import (
    TOKEN_PATH,
    SCOPES_READWRITE,
    DRIVE_API_SERVICE,
    DRIVE_API_VERSION,
    DRIVE_UPLOAD_FOLDER_ID,
    SQLITE_DB_FILES,
    build_oauth_client_config,
)
from utils.logger import logger

def _get_write_credentials() -> Credentials:
    """Obtiene credenciales con scope de escritura para Google Drive.

    Re-usa token.json si tiene el scope necesario. Si el token
    existente solo tiene scope de lectura, solicita re-autenticacion.
    """
    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES_READWRITE)
    if creds and creds.valid:
        return creds
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            TOKEN_PATH.write_text(creds.to_json())
            return creds
        except Exception:
            logger.warning(
                "No se pudo refrescar el token existente, re-autenticando..."
            )
    # Flujo completo de autenticacion
    client_config = build_oauth_client_config()
    flow = InstalledAppFlow.from_client_config(client_config, SCOPES_READWRITE)
    creds = flow.run_local_server(port=0)
    TOKEN_PATH.write_text(creds.to_json())
    return creds


def _build_drive_service():
    """Construye el servicio de Google Drive con permisos de escritura."""
    creds = _get_write_credentials()
    return build(DRIVE_API_SERVICE, DRIVE_API_VERSION, credentials=creds)


def _find_existing_file(service, filename: str, folder_id: str) -> str | None:
    """Busca un archivo por nombre en la carpeta destino. Retorna file_id o None."""
    query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
    results = (
        service.files().list(q=query, fields="files(id, name)", pageSize=5).execute()
    )
    files = results.get("files", [])
    if files:
        return files[0]["id"]
    return None


def _upload_file(service, local_path: Path, folder_id: str) -> dict:
    filename = local_path.name
    mime_type = "application/x-sqlite3"

    media = MediaFileUpload(str(local_path), mimetype=mime_type, resumable=True)

    existing_id = _find_existing_file(service, filename, folder_id)

    if existing_id:
        # Actualizar archivo existente
        logger.info("  [UPDATE] %s (id=%s)", filename, existing_id)
        file_metadata = {"name": filename}
        result = (
            service.files()
            .update(
                fileId=existing_id,
                body=file_metadata,
                media_body=media,
                fields="id, name, size, modifiedTime",
            )
            .execute()
        )
    else:
        # Crear nuevo archivo
        logger.info("  [CREATE] %s", filename)
        file_metadata = {
            "name": filename,
            "parents": [folder_id],
        }
        result = (
            service.files()
            .create(
                body=file_metadata,
                media_body=media,
                fields="id, name, size, modifiedTime",
            )
            .execute()
        )

    return result


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────


def upload_databases() -> list[dict]:
    """Sube las 3 bases de datos SQLite a Google Drive.

    Retorna lista de metadatos de los archivos subidos.
    """
    logger.info("=" * 60)
    logger.info("Upload de bases de datos a Google Drive")
    logger.info("Carpeta destino: %s", DRIVE_UPLOAD_FOLDER_ID)
    logger.info("=" * 60)

    # Verificar que los archivos existen
    missing = [p for p in SQLITE_DB_FILES if not p.exists()]
    if missing:
        for p in missing:
            logger.warning("Archivo no encontrado: %s", p)

    files_to_upload = [p for p in SQLITE_DB_FILES if p.exists()]
    if not files_to_upload:
        logger.error("No hay archivos para subir")
        return []

    logger.info("Archivos a subir: %d", len(files_to_upload))
    for p in files_to_upload:
        size_mb = p.stat().st_size / (1024 * 1024)
        logger.info("  %s (%.1f MB)", p.name, size_mb)

    # Autenticar y construir servicio
    service = _build_drive_service()

    # Subir cada archivo
    results: list[dict] = []
    for local_path in files_to_upload:
        try:
            result = _upload_file(service, local_path, DRIVE_UPLOAD_FOLDER_ID)
            size_mb = int(result.get("size", 0)) / (1024 * 1024)
            logger.info(
                "  OK: %s (%.1f MB, id=%s)",
                result["name"],
                size_mb,
                result["id"],
            )
            results.append(result)
        except Exception as e:
            logger.error("  ERROR subiendo %s: %s", local_path.name, e)

    logger.info("=" * 60)
    logger.info("Upload completado: %d/%d archivos", len(results), len(files_to_upload))
    logger.info("=" * 60)

    return results


if __name__ == "__main__":
    upload_databases()
