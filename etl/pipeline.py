"""
Pipeline ETL — Seminario Ingeniería de Datos.

Flujo de 10 pasos:
    1. ingest           — Descarga de datos desde Google Drive  (--skip-ingest)
    2. create_sqlite_db — Excel/CSV → seminario.db
    3. transform        — Limpieza y transformación de datos
    4. normalize_data   — Normalización de texto (minúsculas, sin tildes)
    5. unify_by_year    — Unificación por año → seminario_unified.db
    6. create_dimensions — Dimensiones del star schema → seminario_facts.db
    7. create_facts      — Tablas de hechos del star schema → seminario_facts.db
    8. upload_to_drive   — Subida de DBs a Google Drive  (--skip-upload)
    9. dictionaries      — Diccionarios de datos
   10. quality           — Verificación de calidad

Uso:
    uv run python -m etl.pipeline
    uv run python -m etl.pipeline --skip-ingest
    uv run python -m etl.pipeline --skip-ingest --skip-upload
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from config.globals import MAX_SNIES_FILE_SIZE_MB, SQLITE_DB_PATH
from config.sources import PRIORITY_FILES
from etl.ingest import ingest_all
from etl.transform import transform_all
from etl.upload import upload_databases
from etl.dictionary import generate_all_dictionaries
from etl.quality import run_quality_checks
from scripts.create_sqlite_db import main as create_sqlite_db
from scripts.normalize_data import main as normalize_data
from scripts.unify_by_year import main as unify_all
from scripts.create_dimensions import main as create_all_dimensions
from scripts.create_facts import main as create_all_facts

from utils.logger import logger

TOTAL_STEPS = 10


def run_pipeline(skip_ingest: bool = False, skip_upload: bool = False):
    logger.info("=" * 60)
    logger.info("  PIPELINE ETL - SEMINARIO INGENIERIA DE DATOS")
    logger.info("=" * 60)
    logger.info("")

    pipeline_start = time.time()

    # ── 1. Ingest (descarga) [opcional] ──────────────────────
    if not skip_ingest:
        logger.info("=== PASO 1/%d: INGESTA DE DATOS ===", TOTAL_STEPS)
        ingest_all(PRIORITY_FILES, max_snies_size_mb=MAX_SNIES_FILE_SIZE_MB)
        logger.info("")
    else:
        logger.info("=== PASO 1/%d: INGESTA OMITIDA (--skip-ingest) ===", TOTAL_STEPS)
        logger.info("")

    # ── 2. Excel → SQLite ────────────────────────────────────
    logger.info("=== PASO 2/%d: CREANDO BASE DE DATOS SQLite ===", TOTAL_STEPS)
    create_sqlite_db()
    logger.info("  DB: %s", SQLITE_DB_PATH)
    logger.info("")

    # ── 3. Transform (limpieza) ──────────────────────────────
    logger.info("=== PASO 3/%d: TRANSFORMACION Y LIMPIEZA ===", TOTAL_STEPS)
    transform_all()
    print()

    generate_all_dictionaries()
    logger.info("")

    # ── 10. Quality checks ───────────────────────────────────
    logger.info("=== PASO 10/%d: VERIFICACION DE CALIDAD ===", TOTAL_STEPS)
    run_quality_checks()
    logger.info("")

    elapsed = time.time() - pipeline_start

    logger.info("=" * 60)
    logger.info("  PIPELINE COMPLETO (%d pasos) en %.1f segundos", TOTAL_STEPS, elapsed)
    logger.info("=" * 60)


if __name__ == "__main__":
    skip_ingest = "--skip-ingest" in sys.argv
    skip_upload = "--skip-upload" in sys.argv
    run_pipeline(skip_ingest=skip_ingest, skip_upload=skip_upload)
