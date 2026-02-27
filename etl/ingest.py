import io
import json
import hashlib
import time
from pathlib import Path
from datetime import datetime, timezone

from config.globals import (
    RAW_DATA_DIR,
    MANIFEST_PATH,
    HASH_CHUNK_SIZE,
    MAX_SNIES_FILE_SIZE_MB,
    DATASET_PND,
    DATASET_ICFES_SABER,
    raw_csv_path,
)
from utils.google_auth import build_drive_service
from utils.logger import logger
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

_DOWNLOAD_MAX_RETRIES = 3
_DOWNLOAD_BASE_DELAY_S = 2.0

def file_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(HASH_CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()

def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {}

def save_manifest(manifest: dict):
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))

def download_file(service, file_id: str, dest_path: Path) -> dict:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")

    last_error: Exception | None = None
    buffer: io.FileIO | None = None
    for attempt in range(_DOWNLOAD_MAX_RETRIES):
        buffer = None
        try:
            request = service.files().get_media(fileId=file_id)
            buffer = io.FileIO(str(tmp_path), mode="wb")
            downloader = MediaIoBaseDownload(buffer, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            buffer.close()
            buffer = None
            tmp_path.rename(dest_path)

            md5 = file_md5(dest_path)
            return {
                "file_id": file_id,
                "local_path": str(dest_path),
                "local_md5": md5,
                "size_bytes": dest_path.stat().st_size,
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
            }

        except (HttpError, OSError, TimeoutError, ConnectionError) as exc:
            last_error = exc
            if buffer is not None and not buffer.closed:
                buffer.close()
            if tmp_path.exists():
                tmp_path.unlink()
            delay = _DOWNLOAD_BASE_DELAY_S * (2**attempt)
            logger.warning(
                "Descarga intento %d/%d fallo para %s: %s. Reintentando en %.1f s...",
                attempt + 1,
                _DOWNLOAD_MAX_RETRIES,
                file_id,
                exc,
                delay,
            )
            time.sleep(delay)

    if tmp_path.exists():
        tmp_path.unlink()
    raise RuntimeError(
        f"No se pudo descargar {file_id} tras "
        f"{_DOWNLOAD_MAX_RETRIES} intentos. Ultimo error: {last_error}"
    )

def get_file_size(service, file_id: str) -> int:
    try:
        meta = service.files().get(fileId=file_id, fields="size").execute()
        return int(meta.get("size", 0))
    except Exception:
        return 0

def ingest_snies(
    service,
    sources: dict,
    manifest: dict,
    max_size_mb: float = MAX_SNIES_FILE_SIZE_MB,
) -> dict:
    snies_config = sources["snies"]
    total_files = sum(len(years) for years in snies_config.values())
    current = 0

    for category, years in snies_config.items():
        for year, file_id in years.items():
            current += 1
            key = f"snies/{category}/{year}"
            dest = RAW_DATA_DIR / "snies" / category / f"{category}-{year}.xlsx"

            if dest.exists() and key in manifest:
                logger.info("  [SKIP] (%d/%d) %s", current, total_files, key)
                continue

            size_bytes = get_file_size(service, file_id)
            size_mb = size_bytes / (1024 * 1024)

            if size_mb > max_size_mb:
                logger.info(
                    "  [SKIP] (%d/%d) %s (%.1f MB > %.0f MB)",
                    current,
                    total_files,
                    key,
                    size_mb,
                    max_size_mb,
                )
                continue

            logger.info(
                "  [DL] (%d/%d) %s (%.1f MB)",
                current,
                total_files,
                key,
                size_mb,
            )
            info = download_file(service, file_id, dest)
            manifest[key] = info
            save_manifest(manifest)

    return manifest

def ingest_csv_file(
    service,
    file_id: str,
    dest_path: Path,
    key: str,
    manifest: dict,
) -> dict:
    if dest_path.exists() and key in manifest:
        logger.info("  [SKIP] %s", key)
        return manifest

    size_bytes = get_file_size(service, file_id)
    size_mb = size_bytes / (1024 * 1024)
    logger.info("  [DL] %s (%.1f MB)", key, size_mb)

    info = download_file(service, file_id, dest_path)
    manifest[key] = info
    save_manifest(manifest)
    return manifest

def ingest_all(
    sources: dict,
    max_snies_size_mb: float = MAX_SNIES_FILE_SIZE_MB,
):
    logger.info("=== INGESTION DE DATOS DESDE GOOGLE DRIVE ===")
    service = build_drive_service()
    manifest = load_manifest()

    logger.info("[1/3] Descargando SNIES (bases consolidadas)...")
    manifest = ingest_snies(service, sources, manifest, max_snies_size_mb)

    logger.info("[2/3] Descargando Seguimiento PND...")
    pnd_dest = raw_csv_path(DATASET_PND)
    manifest = ingest_csv_file(
        service,
        sources["pnd"]["seguimiento_pnd"],
        pnd_dest,
        DATASET_PND,
        manifest,
    )

    logger.info("[3/3] Descargando Saber 3-5-9...")
    saber_dest = raw_csv_path(DATASET_ICFES_SABER)
    manifest = ingest_csv_file(
        service,
        sources["icfes"]["saber_359"],
        saber_dest,
        DATASET_ICFES_SABER,
        manifest,
    )

    logger.info("=== INGESTION COMPLETA: %d archivos ===", len(manifest))
    return manifest
