from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import PG_SCHEMA_RAW, PG_SCHEMA_UNIFIED, PG_SCHEMA_FACTS
from utils.db import (
    ensure_schemas,
    get_column_names,
    get_row_count,
    list_tables,
)
from utils.logger import logger

SCHEMAS = (PG_SCHEMA_RAW, PG_SCHEMA_UNIFIED, PG_SCHEMA_FACTS)


def print_schema_summary(schema: str) -> None:
    tables = list_tables(schema)
    logger.info("")
    logger.info("Schema '%s' (%d tablas):", schema, len(tables))

    if not tables:
        logger.info("  (vacio)")
        return

    for table in tables:
        row_count = get_row_count(schema, table)
        col_count = len(get_column_names(schema, table))
        logger.info("  %-40s %8d filas | %d cols", table, row_count, col_count)


def main() -> None:
    logger.info("=" * 60)
    logger.info("VERIFICACION DE SCHEMAS POSTGRESQL")
    logger.info("=" * 60)

    ensure_schemas()
    logger.info("Schemas creados/verificados: %s", list(SCHEMAS))

    for schema in SCHEMAS:
        print_schema_summary(schema)

    logger.info("")
    logger.info("=" * 60)
    logger.info("Verificacion completada.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
