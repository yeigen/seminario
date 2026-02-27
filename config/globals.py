import os
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]

LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "seminario.log"

DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
DICTIONARIES_DIR = DATA_DIR / "dictionaries"

POSTGRES_USER: str = os.getenv("POSTGRES_USER", "yeigen")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "LavidaEsbella16*#")
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "seminario")
POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5433")

_pg_password_encoded = quote_plus(POSTGRES_PASSWORD)
DATABASE_URL: str = (
    f"postgresql://{POSTGRES_USER}:{_pg_password_encoded}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
DATABASE_URL_ASYNC: str = (
    f"postgresql+asyncpg://{POSTGRES_USER}:{_pg_password_encoded}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

PG_SCHEMA_RAW = "raw"
PG_SCHEMA_UNIFIED = "unified"
PG_SCHEMA_FACTS = "facts"
PG_SCHEMAS: list[str] = [PG_SCHEMA_RAW, PG_SCHEMA_UNIFIED, PG_SCHEMA_FACTS]

RAW_SNIES_DIR = RAW_DATA_DIR / "snies"
RAW_PND_DIR = RAW_DATA_DIR / "pnd"
RAW_ICFES_DIR = RAW_DATA_DIR / "icfes"

PROCESSED_SNIES_DIR = PROCESSED_DIR / "snies"

MANIFEST_PATH = RAW_DATA_DIR / "_manifest.json"
LINEAGE_PATH = PROCESSED_DIR / "_lineage.json"
QUALITY_REPORTS_DIR = PROCESSED_DIR / "_quality_reports"
QUALITY_REPORT_PATH = QUALITY_REPORTS_DIR / "quality_report.json"
DATA_DICTIONARY_JSON_PATH = DICTIONARIES_DIR / "data_dictionary.json"
DATA_DICTIONARY_MD_PATH = DICTIONARIES_DIR / "data_dictionary.md"

TOKEN_PATH = PROJECT_ROOT / "token.json"

CLIENT_ID: str | None = os.getenv("CLIENT_ID")
CLIENT_SECRET: str | None = os.getenv("CLIENT_SECRET")
FOLDER_ID: str = os.getenv("FOLDER_ID", "1tcRwkHfaMpu2TUoNgU_GU8SwUGz9WBGK")

GOOGLE_AUTH_URI = "https://accounts.google.com/o/oauth2/auth"
GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"
GOOGLE_REDIRECT_URIS = ["http://localhost"]
GOOGLE_REDIRECT_URI_OOB = "urn:ietf:wg:oauth:2.0:oob"
GOOGLE_UNIVERSE_DOMAIN = "googleapis.com"

SCOPES_READONLY = ["https://www.googleapis.com/auth/drive.readonly"]
SCOPES_METADATA = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
SCOPES_READWRITE = ["https://www.googleapis.com/auth/drive"]
SCOPES = SCOPES_READWRITE

DRIVE_API_SERVICE = "drive"
DRIVE_API_VERSION = "v3"

def build_oauth_client_config(
    client_id: str | None = None,
    client_secret: str | None = None,
) -> dict:
    return {
        "installed": {
            "client_id": client_id or CLIENT_ID,
            "client_secret": client_secret or CLIENT_SECRET,
            "auth_uri": GOOGLE_AUTH_URI,
            "token_uri": GOOGLE_TOKEN_URI,
            "redirect_uris": GOOGLE_REDIRECT_URIS,
        }
    }

DRIVE_ROOT_FOLDER_ID = FOLDER_ID
SNIES_FOLDER_ID = "1ti0l0DsQm3ct8IE37OhMEwRJI2vY-WRE"
MATRICULA_TOTAL_FOLDER_ID = "1fTj8K_or6h5rYICnFbcdeB2x0DjWksc-"
ICFES_FOLDER_ID = "17ZdyGTKGF-7lqYt5U41Wsb4j0C7XUsjW"
PND_FOLDER_ID = "1dDAr1pTdVX3swGKC5G5rNSqTKW1JT8G7"

DRIVE_UPLOAD_FOLDER_ID: str = os.getenv("DRIVE_UPLOAD_FOLDER_ID", FOLDER_ID)

PG_EXPORT_DIR = DATA_DIR / "exports"
PG_EXPORT_FILES: list[Path] = [
    PG_EXPORT_DIR / "seminario_raw.sql",
    PG_EXPORT_DIR / "seminario_unified.sql",
    PG_EXPORT_DIR / "seminario_facts.sql",
]

SNIES_CATEGORIES = [
    "administrativos",
    "admitidos",
    "docentes",
    "graduados",
    "inscritos",
    "matriculados",
    "matriculados_primer_curso",
]

DATASET_PND = "pnd/seguimiento_pnd"
DATASET_ICFES_SABER = "icfes/saber_359"
CSV_DATASETS = [DATASET_PND, DATASET_ICFES_SABER]

TOKEN_FILENAME = "token.json"
MANIFEST_FILENAME = "_manifest.json"
LINEAGE_FILENAME = "_lineage.json"
QUALITY_REPORT_FILENAME = "quality_report.json"
DATA_DICTIONARY_JSON_FILENAME = "data_dictionary.json"
DATA_DICTIONARY_MD_FILENAME = "data_dictionary.md"

MIME_TYPE_EXTENSIONS: dict[str, str] = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "text/csv": ".csv",
    "text/plain": ".txt",
    "application/json": ".json",
}

GOOGLE_FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"

INPUT_EXTENSION_SNIES = ".xlsx"
OUTPUT_EXTENSION = ".parquet"

MAX_SNIES_FILE_SIZE_MB: float = 15.0
DRIVE_LIST_PAGE_SIZE: int = 100
HASH_CHUNK_SIZE: int = 8192

CSV_ENCODINGS = ["utf-8", "latin-1", "cp1252"]

QUALITY_NULL_THRESHOLD_PCT: float = 50.0
QUALITY_MIN_COLUMNS: int = 2

YEAR_START = 2018
YEAR_END = 2024
YEARS = list(range(YEAR_START, YEAR_END + 1))

PROJECT_NAME = "seminario"
PROJECT_TITLE = "Seminario Ingeniería de Datos"
PROJECT_VERSION = "0.1.0"

def raw_snies_path(category: str, year: int | str) -> Path:
    return RAW_SNIES_DIR / category / f"{category}-{year}{INPUT_EXTENSION_SNIES}"

def processed_snies_path(category: str, year: int | str) -> Path:
    return PROCESSED_SNIES_DIR / category / f"{category}-{year}{OUTPUT_EXTENSION}"

def raw_csv_path(dataset_key: str) -> Path:
    return RAW_DATA_DIR / f"{dataset_key}.csv"

def processed_parquet_path(dataset_key: str) -> Path:
    return PROCESSED_DIR / f"{dataset_key}{OUTPUT_EXTENSION}"
