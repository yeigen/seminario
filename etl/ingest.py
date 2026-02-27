import io
import os
import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

load_dotenv()

TOKEN_PATH = Path(__file__).parents[1] / "token.json"
RAW_DATA_DIR = Path(__file__).parents[1] / "data" / "raw"
MANIFEST_PATH = RAW_DATA_DIR / "_manifest.json"

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")


def get_credentials():
    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if creds and creds.valid:
        return creds
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        TOKEN_PATH.write_text(creds.to_json())
        return creds
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
    return creds


def build_drive_service():
    creds = get_credentials()
    return build("drive", "v3", credentials=creds)


def file_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
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
    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    buffer.seek(0)
    with open(dest_path, "wb") as f:
        f.write(buffer.read())
    md5 = file_md5(dest_path)
    return {
        "file_id": file_id,
        "local_path": str(dest_path),
        "local_md5": md5,
        "size_bytes": dest_path.stat().st_size,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }


def get_file_size(service, file_id: str) -> int:
    try:
        meta = service.files().get(fileId=file_id, fields="size").execute()
        return int(meta.get("size", 0))
    except Exception:
        return 0


def ingest_snies(service, sources: dict, manifest: dict, max_size_mb: float = 15.0):
    snies_config = sources["snies"]
    for category, years in snies_config.items():
        for year, file_id in years.items():
            key = f"snies/{category}/{year}"
            dest = RAW_DATA_DIR / "snies" / category / f"{category}-{year}.xlsx"
            if dest.exists() and key in manifest:
                print(f"  [SKIP] {key}")
                continue
            size_bytes = get_file_size(service, file_id)
            size_mb = size_bytes / (1024 * 1024)
            if size_mb > max_size_mb:
                print(f"  [SKIP] {key} ({size_mb:.1f} MB > {max_size_mb} MB)")
                continue
            print(f"  [DL] {key} ({size_mb:.1f} MB)")
            info = download_file(service, file_id, dest)
            manifest[key] = info
            save_manifest(manifest)
    return manifest


def ingest_csv_file(service, file_id: str, dest_path: Path, key: str, manifest: dict):
    if dest_path.exists() and key in manifest:
        print(f"  [SKIP] {key}")
        return manifest
    size_bytes = get_file_size(service, file_id)
    size_mb = size_bytes / (1024 * 1024)
    print(f"  [DL] {key} ({size_mb:.1f} MB)")
    info = download_file(service, file_id, dest_path)
    manifest[key] = info
    save_manifest(manifest)
    return manifest


def ingest_all(sources: dict, max_snies_size_mb: float = 15.0):
    print("=== INGESTIÓN DE DATOS DESDE GOOGLE DRIVE ===\n")
    service = build_drive_service()
    manifest = load_manifest()

    print("[1/3] Descargando SNIES (bases consolidadas)...")
    manifest = ingest_snies(service, sources, manifest, max_snies_size_mb)

    print("\n[2/3] Descargando Seguimiento PND...")
    pnd_dest = RAW_DATA_DIR / "pnd" / "seguimiento_pnd.csv"
    manifest = ingest_csv_file(
        service,
        sources["pnd"]["seguimiento_pnd"],
        pnd_dest,
        "pnd/seguimiento_pnd",
        manifest,
    )

    print("\n[3/3] Descargando Saber 3-5-9...")
    saber_dest = RAW_DATA_DIR / "icfes" / "saber_359.csv"
    manifest = ingest_csv_file(
        service, sources["icfes"]["saber_359"], saber_dest, "icfes/saber_359", manifest
    )

    print(f"\n=== INGESTIÓN COMPLETA: {len(manifest)} archivos ===")
    return manifest
