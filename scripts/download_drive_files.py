import argparse
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from config.globals import (
    TOKEN_PATH,
    RAW_SNIES_DIR,
    SCOPES,
    DRIVE_API_SERVICE,
    DRIVE_API_VERSION,
    SNIES_CATEGORIES,
    INPUT_EXTENSION_SNIES,
    build_oauth_client_config,
)
from config.sources import PRIORITY_FILES
from utils.logger import logger


def get_credentials() -> Credentials:
    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if creds and creds.valid:
        return creds
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        TOKEN_PATH.write_text(creds.to_json())
        return creds
    client_config = build_oauth_client_config()
    flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    creds = flow.run_local_server(port=0)
    TOKEN_PATH.write_text(creds.to_json())
    return creds


def build_drive_service():
    creds = get_credentials()
    return build(DRIVE_API_SERVICE, DRIVE_API_VERSION, credentials=creds)


def get_file_size(service, file_id: str) -> int:
    try:
        meta = service.files().get(fileId=file_id, fields="size").execute()
        return int(meta.get("size", 0))
    except Exception:
        return 0


def download_file(service, file_id: str, dest_path: Path) -> int:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            pct = int(status.progress() * 100)
            logger.debug("  Progreso: %d%%", pct)
    buffer.seek(0)
    with open(dest_path, "wb") as f:
        f.write(buffer.read())
    return dest_path.stat().st_size


def dest_path_for(category: str, year: str) -> Path:
    return RAW_SNIES_DIR / category / f"{category}-{year}{INPUT_EXTENSION_SNIES}"


def download_category(service, category: str) -> tuple[int, int]:
    years_config = PRIORITY_FILES.get("snies", {}).get(category)
    if not years_config:
        logger.warning("No hay archivos configurados para la categoría '%s'", category)
        return 0, 0

    downloaded = 0
    skipped = 0

    logger.info("[%s] %d archivos configurados", category, len(years_config))

    for year, file_id in sorted(years_config.items()):
        dest = dest_path_for(category, year)

        if dest.exists():
            logger.info(
                "  [EXISTE] %s (%.1f MB)",
                dest.name,
                dest.stat().st_size / (1024 * 1024),
            )
            skipped += 1
            continue

        size_bytes = get_file_size(service, file_id)
        size_mb = size_bytes / (1024 * 1024) if size_bytes else 0

        logger.info("  [DESCARGANDO] %s (%.1f MB)...", dest.name, size_mb)
        try:
            final_size = download_file(service, file_id, dest)
            logger.info("  [OK] %s (%.1f MB)", dest.name, final_size / (1024 * 1024))
            downloaded += 1
        except Exception as e:
            logger.error("  [ERROR] %s: %s", dest.name, e)

    return downloaded, skipped


def download_all(service) -> tuple[int, int]:
    total_downloaded = 0
    total_skipped = 0

    for category in SNIES_CATEGORIES:
        downloaded, skipped = download_category(service, category)
        total_downloaded += downloaded
        total_skipped += skipped

    return total_downloaded, total_skipped


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Descarga archivos SNIES desde Google Drive.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--all",
        action="store_true",
        dest="download_all",
        help="Descargar todas las categorías SNIES.",
    )
    group.add_argument(
        "--category",
        type=str,
        choices=SNIES_CATEGORIES,
        help="Descargar una categoría específica.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logger.info("=" * 60)
    logger.info("DESCARGA DE ARCHIVOS SNIES DESDE GOOGLE DRIVE")
    logger.info("=" * 60)

    service = build_drive_service()
    logger.info("Autenticación exitosa con Google Drive")

    if args.download_all:
        logger.info("Modo: TODAS las categorías (%d)", len(SNIES_CATEGORIES))
        downloaded, skipped = download_all(service)
    else:
        logger.info("Modo: categoría '%s'", args.category)
        downloaded, skipped = download_category(service, args.category)

    logger.info("=" * 60)
    logger.info("Descarga finalizada: %d nuevos, %d ya existentes", downloaded, skipped)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
