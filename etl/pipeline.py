import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from config.globals import MAX_SNIES_FILE_SIZE_MB, SQLITE_DB_PATH
from config.sources import PRIORITY_FILES
from etl.ingest import ingest_all
from etl.transform import transform_all
from etl.dictionary import generate_all_dictionaries
from etl.quality import run_quality_checks
from scripts.create_sqlite_db import main as create_sqlite_db


def run_pipeline(skip_ingest: bool = False):
    print("=" * 60)
    print("  PIPELINE ETL - SEMINARIO INGENIERÍA DE DATOS")
    print("=" * 60)
    print()

    if not skip_ingest:
        ingest_all(PRIORITY_FILES, max_snies_size_mb=MAX_SNIES_FILE_SIZE_MB)
        print()

    print("=== CREANDO BASE DE DATOS SQLite ===\n")
    create_sqlite_db()
    print(f"  DB: {SQLITE_DB_PATH}")
    print()

    transform_all()
    print()

    print("=== CREANDO BASE DE DATOS SQLite ===\n")
    create_sqlite_db()
    print(f"  DB: {SQLITE_DB_PATH}")
    print()

    generate_all_dictionaries()
    print()

    run_quality_checks()
    print()

    print("=" * 60)
    print("  PIPELINE COMPLETO")
    print("=" * 60)


if __name__ == "__main__":
    skip = "--skip-ingest" in sys.argv
    run_pipeline(skip_ingest=skip)
