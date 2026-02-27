"""
Configuración global centralizada del proyecto Seminario Ingeniería de Datos.

Todas las constantes, paths, variables de entorno y valores repetidos
se definen aquí para evitar duplicación en el resto del código.

Uso:
    from config.globals import PROJECT_ROOT, SCOPES, build_oauth_client_config
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────────────────────
# 1. PATHS BASE DEL PROYECTO
# ──────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Logs
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "seminario.log"

# Datos
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
DICTIONARIES_DIR = DATA_DIR / "dictionaries"
SQLITE_DB_PATH = DATA_DIR / "seminario.db"
SQLITE_UNIFIED_DB_PATH = DATA_DIR / "seminario_unified.db"
SQLITE_FACTS_DB_PATH = DATA_DIR / "seminario_facts.db"

# Subdirectorios de datos crudos
RAW_SNIES_DIR = RAW_DATA_DIR / "snies"
RAW_PND_DIR = RAW_DATA_DIR / "pnd"
RAW_ICFES_DIR = RAW_DATA_DIR / "icfes"

# Subdirectorios de datos procesados
PROCESSED_SNIES_DIR = PROCESSED_DIR / "snies"

# Archivos de metadatos y reportes
MANIFEST_PATH = RAW_DATA_DIR / "_manifest.json"
LINEAGE_PATH = PROCESSED_DIR / "_lineage.json"
QUALITY_REPORTS_DIR = PROCESSED_DIR / "_quality_reports"
QUALITY_REPORT_PATH = QUALITY_REPORTS_DIR / "quality_report.json"
DATA_DICTIONARY_JSON_PATH = DICTIONARIES_DIR / "data_dictionary.json"
DATA_DICTIONARY_MD_PATH = DICTIONARIES_DIR / "data_dictionary.md"

# Autenticación
TOKEN_PATH = PROJECT_ROOT / "token.json"

# ──────────────────────────────────────────────────────────────
# 2. VARIABLES DE ENTORNO (.env)
# ──────────────────────────────────────────────────────────────
CLIENT_ID: str | None = os.getenv("CLIENT_ID")
CLIENT_SECRET: str | None = os.getenv("CLIENT_SECRET")
FOLDER_ID: str = os.getenv("FOLDER_ID", "1tcRwkHfaMpu2TUoNgU_GU8SwUGz9WBGK")

# ──────────────────────────────────────────────────────────────
# 3. GOOGLE DRIVE / OAUTH
# ──────────────────────────────────────────────────────────────
GOOGLE_AUTH_URI = "https://accounts.google.com/o/oauth2/auth"
GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"
GOOGLE_REDIRECT_URIS = ["http://localhost"]
GOOGLE_REDIRECT_URI_OOB = "urn:ietf:wg:oauth:2.0:oob"
GOOGLE_UNIVERSE_DOMAIN = "googleapis.com"

# Scopes — el proyecto usa drive.readonly para producción
SCOPES_READONLY = ["https://www.googleapis.com/auth/drive.readonly"]
SCOPES_METADATA = ["https://www.googleapis.com/auth/drive.metadata.readonly"]

# Scope de escritura para upload
SCOPES_READWRITE = ["https://www.googleapis.com/auth/drive"]

# Scope por defecto para el pipeline ETL
SCOPES = SCOPES_READONLY

# Google Drive API
DRIVE_API_SERVICE = "drive"
DRIVE_API_VERSION = "v3"


def build_oauth_client_config(
    client_id: str | None = None,
    client_secret: str | None = None,
) -> dict:
    """Construye el dict de configuración OAuth para InstalledAppFlow."""
    return {
        "installed": {
            "client_id": client_id or CLIENT_ID,
            "client_secret": client_secret or CLIENT_SECRET,
            "auth_uri": GOOGLE_AUTH_URI,
            "token_uri": GOOGLE_TOKEN_URI,
            "redirect_uris": GOOGLE_REDIRECT_URIS,
        }
    }


# ──────────────────────────────────────────────────────────────
# 4. IDs DE CARPETAS EN GOOGLE DRIVE
# ──────────────────────────────────────────────────────────────
DRIVE_ROOT_FOLDER_ID = FOLDER_ID
SNIES_FOLDER_ID = "1ti0l0DsQm3ct8IE37OhMEwRJI2vY-WRE"
MATRICULA_TOTAL_FOLDER_ID = "1fTj8K_or6h5rYICnFbcdeB2x0DjWksc-"
ICFES_FOLDER_ID = "17ZdyGTKGF-7lqYt5U41Wsb4j0C7XUsjW"
PND_FOLDER_ID = "1dDAr1pTdVX3swGKC5G5rNSqTKW1JT8G7"

# Carpeta destino para upload de bases de datos procesadas
DRIVE_UPLOAD_FOLDER_ID: str = os.getenv("DRIVE_UPLOAD_FOLDER_ID", FOLDER_ID)

# Bases de datos que se suben a Google Drive
SQLITE_DB_FILES: list[Path] = [
    SQLITE_DB_PATH,
    SQLITE_UNIFIED_DB_PATH,
    SQLITE_FACTS_DB_PATH,
]

# ──────────────────────────────────────────────────────────────
# 5. DATASETS — Claves lógicas usadas en el pipeline
# ──────────────────────────────────────────────────────────────
# Categorías SNIES disponibles
SNIES_CATEGORIES = [
    "administrativos",
    "admitidos",
    "docentes",
    "graduados",
    "inscritos",
    "matriculados",
    "matriculados_primer_curso",
]

# Datasets CSV externos (clave lógica → nombre de archivo sin extensión)
DATASET_PND = "pnd/seguimiento_pnd"
DATASET_ICFES_SABER = "icfes/saber_359"

CSV_DATASETS = [DATASET_PND, DATASET_ICFES_SABER]

# ──────────────────────────────────────────────────────────────
# 6. NOMBRES DE ARCHIVOS Y EXTENSIONES
# ──────────────────────────────────────────────────────────────
TOKEN_FILENAME = "token.json"
MANIFEST_FILENAME = "_manifest.json"
LINEAGE_FILENAME = "_lineage.json"
QUALITY_REPORT_FILENAME = "quality_report.json"
DATA_DICTIONARY_JSON_FILENAME = "data_dictionary.json"
DATA_DICTIONARY_MD_FILENAME = "data_dictionary.md"

# Extensiones de archivo mapeadas por MIME type de Google Drive
MIME_TYPE_EXTENSIONS: dict[str, str] = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "text/csv": ".csv",
    "text/plain": ".txt",
    "application/json": ".json",
}

GOOGLE_FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"

# Extensiones de entrada/salida del pipeline
INPUT_EXTENSION_SNIES = ".xlsx"
OUTPUT_EXTENSION = ".parquet"

# ──────────────────────────────────────────────────────────────
# 7. PARÁMETROS DEL PIPELINE ETL
# ──────────────────────────────────────────────────────────────
MAX_SNIES_FILE_SIZE_MB: float = 15.0
DRIVE_LIST_PAGE_SIZE: int = 100
HASH_CHUNK_SIZE: int = 8192

# Encodings a intentar al leer CSV
CSV_ENCODINGS = ["utf-8", "latin-1", "cp1252"]

# Umbral de calidad: porcentaje máximo de nulos por columna
QUALITY_NULL_THRESHOLD_PCT: float = 50.0
QUALITY_MIN_COLUMNS: int = 2

# ──────────────────────────────────────────────────────────────
# 8. RANGO DE AÑOS DEL PROYECTO
# ──────────────────────────────────────────────────────────────
YEAR_START = 2018
YEAR_END = 2024
YEARS = list(range(YEAR_START, YEAR_END + 1))

# ──────────────────────────────────────────────────────────────
# 9. PROYECTO
# ──────────────────────────────────────────────────────────────
PROJECT_NAME = "seminario"
PROJECT_TITLE = "Seminario Ingeniería de Datos"
PROJECT_VERSION = "0.1.0"

# ──────────────────────────────────────────────────────────────
# 10. HELPERS DE PATHS
# ──────────────────────────────────────────────────────────────


def raw_snies_path(category: str, year: int | str) -> Path:
    """Retorna el path esperado de un archivo SNIES crudo."""
    return RAW_SNIES_DIR / category / f"{category}-{year}{INPUT_EXTENSION_SNIES}"


def processed_snies_path(category: str, year: int | str) -> Path:
    """Retorna el path esperado de un archivo SNIES procesado."""
    return PROCESSED_SNIES_DIR / category / f"{category}-{year}{OUTPUT_EXTENSION}"


def raw_csv_path(dataset_key: str) -> Path:
    """Retorna el path de un CSV crudo a partir de su clave lógica."""
    return RAW_DATA_DIR / f"{dataset_key}.csv"


def processed_parquet_path(dataset_key: str) -> Path:
    """Retorna el path de un parquet procesado a partir de su clave lógica."""
    return PROCESSED_DIR / f"{dataset_key}{OUTPUT_EXTENSION}"
