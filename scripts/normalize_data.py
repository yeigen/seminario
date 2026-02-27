import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import PG_SCHEMA_RAW
from utils.db import (
    get_columns,
    get_row_count,
    list_tables,
    managed_connection,
)
from utils.logger import logger
from utils.text import create_pg_normalize_function

EXCLUDED_COLUMNS = {"created_at", "updated_at"}
NULL_THRESHOLD_PCT = 90.0
TARGET_SCHEMA: str = PG_SCHEMA_RAW


def get_all_tables(schema: str) -> list[str]:
    return list_tables(schema)


def get_text_columns(schema: str, table_name: str) -> list[str]:
    cols = get_columns(schema, table_name)
    return [
        col["column_name"]
        for col in cols
        if col["data_type"].upper() in ("TEXT", "CHARACTER VARYING")
        and col["column_name"] not in EXCLUDED_COLUMNS
    ]


def get_all_columns_info(schema: str, table_name: str) -> list[dict]:
    return [
        {
            "name": c["column_name"],
            "type": c["data_type"],
            "ordinal": c["ordinal_position"],
        }
        for c in get_columns(schema, table_name)
    ]


def _create_normalize_function(schema: str) -> None:
    with managed_connection(schema=schema) as conn:
        create_pg_normalize_function(conn)


def drop_high_null_columns(
    schema: str, table_name: str, threshold: float = NULL_THRESHOLD_PCT
) -> list[str]:
    row_count = get_row_count(schema, table_name)
    if row_count == 0:
        return []

    candidate_cols = [
        col_info["name"]
        for col_info in get_all_columns_info(schema, table_name)
        if col_info["name"] != "id" and col_info["name"] not in EXCLUDED_COLUMNS
    ]

    if not candidate_cols:
        return []

    with managed_connection(schema=schema) as conn:
        null_counts_sql = ", ".join(
            f'COUNT(*) FILTER (WHERE "{col}" IS NULL)' for col in candidate_cols
        )
        with conn.cursor() as cur:
            cur.execute(f'SELECT {null_counts_sql} FROM "{table_name}"')
            null_counts = cur.fetchone()

        cols_to_drop: list[str] = []
        for i, col_name in enumerate(candidate_cols):
            null_pct = (null_counts[i] / row_count) * 100
            if null_pct > threshold:
                cols_to_drop.append(col_name)
                logger.debug(
                    "[%s] Columna '%s' tiene %.1f%% nulos (umbral: %.1f%%)",
                    table_name,
                    col_name,
                    null_pct,
                    threshold,
                )

        if not cols_to_drop:
            return []

        drop_clauses = ", ".join(f'DROP COLUMN "{col}"' for col in cols_to_drop)
        with conn.cursor() as cur:
            cur.execute(f'ALTER TABLE "{table_name}" {drop_clauses}')

    logger.info(
        "[%s] Eliminadas %d columnas con >%.0f%% nulos: %s",
        table_name,
        len(cols_to_drop),
        threshold,
        cols_to_drop,
    )

    return cols_to_drop


def normalize_table(schema: str, table_name: str) -> dict:
    text_cols = get_text_columns(schema, table_name)
    if not text_cols:
        return {
            "table": table_name,
            "status": "skipped",
            "reason": "sin columnas TEXT",
            "rows": 0,
            "text_cols": 0,
            "nullified": 0,
        }

    row_count = get_row_count(schema, table_name)
    if row_count == 0:
        return {
            "table": table_name,
            "status": "skipped",
            "reason": "tabla vacia",
            "rows": 0,
            "text_cols": len(text_cols),
            "nullified": 0,
        }

    logger.info(
        "[%s] Normalizando %d columnas TEXT en %d filas...",
        table_name,
        len(text_cols),
        row_count,
    )

    with managed_connection(schema=schema) as conn:
        with conn.cursor() as cur:
            null_counts_sql = ", ".join(
                f'COUNT(*) FILTER (WHERE "{col}" IS NULL)' for col in text_cols
            )
            cur.execute(f'SELECT {null_counts_sql} FROM "{table_name}"')
            nulls_before = cur.fetchone()

            set_clauses = ", ".join(
                f'"{col}" = pg_normalize_text("{col}")' for col in text_cols
            )
            where_clauses = " OR ".join(f'"{col}" IS NOT NULL' for col in text_cols)
            cur.execute(
                f'UPDATE "{table_name}" SET {set_clauses} WHERE {where_clauses}'
            )

            cur.execute(f'SELECT {null_counts_sql} FROM "{table_name}"')
            nulls_after = cur.fetchone()

    total_nullified = sum(
        (nulls_after[i] - nulls_before[i]) for i in range(len(text_cols))
    )

    final_count = get_row_count(schema, table_name)
    logger.info(
        "[%s] Completado: %d filas, %d nuevos NULL (vacios->NULL)",
        table_name,
        final_count,
        total_nullified,
    )

    return {
        "table": table_name,
        "status": "ok",
        "reason": "",
        "rows": final_count,
        "text_cols": len(text_cols),
        "nullified": total_nullified,
    }


def verify_normalization(schema: str, tables: list[str]) -> bool:
    logger.info("Verificando normalizacion...")
    issues: list[str] = []

    with managed_connection(schema=schema) as conn:
        with conn.cursor() as cur:
            for table_name in tables:
                text_cols = get_text_columns(schema, table_name)
                if not text_cols:
                    continue

                accent_counts_sql = ", ".join(
                    f"COUNT(*) FILTER (WHERE \"{col}\" ~ '[áéíóúÁÉÍÓÚñÑàèìòùÀÈÌÒÙ]')"
                    for col in text_cols
                )
                cur.execute(f'SELECT {accent_counts_sql} FROM "{table_name}"')
                accent_results = cur.fetchone()

                for i, col in enumerate(text_cols):
                    if accent_results[i] > 0:
                        issues.append(
                            f"  {table_name}.{col}: {accent_results[i]} filas con tildes"
                        )

                space_counts_sql = ", ".join(
                    f"COUNT(*) FILTER (WHERE \"{col}\" LIKE '%%  %%')"
                    for col in text_cols
                )
                empty_counts_sql = ", ".join(
                    f"COUNT(*) FILTER (WHERE \"{col}\" = '' OR TRIM(\"{col}\") = '')"
                    for col in text_cols
                )
                cur.execute(
                    f'SELECT {space_counts_sql}, {empty_counts_sql} FROM "{table_name}"'
                )
                combined = cur.fetchone()
                n = len(text_cols)

                for i, col in enumerate(text_cols):
                    if combined[i] > 0:
                        issues.append(
                            f"  {table_name}.{col}: {combined[i]} filas con espacios multiples"
                        )
                    if combined[n + i] > 0:
                        issues.append(
                            f"  {table_name}.{col}: {combined[n + i]} filas con strings vacios"
                        )

                for col in text_cols:
                    cur.execute(
                        f'SELECT "{col}" FROM "{table_name}" '
                        f'WHERE "{col}" IS NOT NULL LIMIT 100'
                    )
                    sample = cur.fetchall()
                    upper_count = sum(
                        1 for (val,) in sample if val and val != val.lower()
                    )
                    if upper_count > 0:
                        issues.append(
                            f"  {table_name}.{col}: {upper_count}/100 muestras con mayusculas"
                        )

    if issues:
        logger.warning("Se encontraron problemas de normalizacion:")
        for issue in issues:
            logger.warning(issue)
        return False

    logger.info("Verificacion exitosa: todos los textos normalizados correctamente")
    return True


def process_schema(schema: str) -> dict:
    logger.info("-" * 60)
    logger.info("Procesando schema: %s", schema)
    logger.info("-" * 60)

    all_tables = get_all_tables(schema)
    if not all_tables:
        logger.warning("Schema '%s' no tiene tablas", schema)
        return {"db": schema, "status": "empty", "results": []}

    _create_normalize_function(schema)

    dim_tables = sorted(t for t in all_tables if t.startswith("dim_"))
    fact_tables = sorted(t for t in all_tables if t.startswith("fact_"))
    other_tables = sorted(
        t for t in all_tables if not t.startswith("dim_") and not t.startswith("fact_")
    )

    logger.info("Tablas encontradas en '%s':", schema)
    logger.info("  Dimensiones: %s", dim_tables)
    logger.info("  Hechos:      %s", fact_tables)
    logger.info("  Otras:       %s", other_tables)

    tables_to_process = dim_tables + fact_tables + other_tables

    total_dropped: dict[str, list[str]] = {}
    for table_name in tables_to_process:
        dropped = drop_high_null_columns(schema, table_name)
        if dropped:
            total_dropped[table_name] = dropped

    if total_dropped:
        logger.info(
            "Columnas eliminadas (>%.0f%% nulos) en '%s':",
            NULL_THRESHOLD_PCT,
            schema,
        )
        for tbl, cols in total_dropped.items():
            logger.info("  %s: %s", tbl, cols)

    results: list[dict] = []
    for table_name in tables_to_process:
        t0 = time.time()
        result = normalize_table(schema, table_name)
        result["elapsed_s"] = round(time.time() - t0, 2)
        results.append(result)

    processed_tables = [r["table"] for r in results if r["status"] == "ok"]
    verify_normalization(schema, processed_tables)

    return {
        "db": schema,
        "status": "ok",
        "results": results,
        "dropped_columns": total_dropped,
    }


def print_summary(db_result: dict) -> None:
    schema_name = db_result["db"]
    results = db_result.get("results", [])

    if db_result["status"] in ("not_found", "empty"):
        logger.info("  %-30s VACIO / NO ENCONTRADO", schema_name)
        return

    total_rows = 0
    total_nullified = 0
    processed = 0
    skipped = 0

    for r in results:
        if r["status"] == "ok":
            processed += 1
            total_rows += r["rows"]
            total_nullified += r["nullified"]
            logger.info(
                "  %-40s %8d filas | %2d cols TEXT | %5d ->NULL | %.1fs",
                r["table"],
                r["rows"],
                r["text_cols"],
                r["nullified"],
                r.get("elapsed_s", 0),
            )
        else:
            skipped += 1
            logger.info("  %-40s SALTADA (%s)", r["table"], r["reason"])

    dropped = db_result.get("dropped_columns", {})
    total_cols_dropped = sum(len(v) for v in dropped.values())

    logger.info("  --- schema: %s ---", schema_name)
    logger.info("  Tablas procesadas:    %d", processed)
    logger.info("  Tablas saltadas:      %d", skipped)
    logger.info("  Filas totales:        %d", total_rows)
    logger.info("  Nuevos NULL:          %d (vacios -> NULL)", total_nullified)
    logger.info(
        "  Columnas eliminadas:  %d (>%.0f%% nulos)",
        total_cols_dropped,
        NULL_THRESHOLD_PCT,
    )


def main():
    logger.info("=" * 60)
    logger.info("Normalizacion de datos de texto - PostgreSQL")
    logger.info("Schema objetivo: %s", TARGET_SCHEMA)
    logger.info("=" * 60)

    pipeline_start = time.time()
    db_result = process_schema(TARGET_SCHEMA)
    total_elapsed = time.time() - pipeline_start

    logger.info("=" * 60)
    logger.info("Resumen de normalizacion")
    logger.info("=" * 60)

    print_summary(db_result)

    logger.info("Tiempo total: %.1f s", total_elapsed)
    logger.info("=" * 60)
    logger.info("Normalizacion completada exitosamente")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
