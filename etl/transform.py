import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from config.globals import (
    LINEAGE_PATH,
    PG_SCHEMA_RAW,
    PROCESSED_SNIES_DIR,
    OUTPUT_EXTENSION,
)
from utils.db import get_engine, list_tables, managed_connection
from utils.logger import logger
from utils.text import (
    create_pg_normalize_function,
    normalize_column_name,
    normalize_columns,
)

_INTERNAL_TABLES = {"_import_log"}

def load_lineage() -> list:
    if LINEAGE_PATH.exists():
        return json.loads(LINEAGE_PATH.read_text())
    return []

def save_lineage(lineage: list):
    LINEAGE_PATH.parent.mkdir(parents=True, exist_ok=True)
    LINEAGE_PATH.write_text(json.dumps(lineage, indent=2, ensure_ascii=False))

def record_lineage(
    source: str, dest: str, operation: str, rows_in: int, rows_out: int, cols: int
):
    lineage = load_lineage()
    lineage.append(
        {
            "source": source,
            "destination": dest,
            "operation": operation,
            "rows_input": rows_in,
            "rows_output": rows_out,
            "columns": cols,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )
    save_lineage(lineage)

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)
    unnamed = [c for c in df.columns if c.startswith("unnamed")]
    if unnamed:
        df = df.drop(columns=unnamed)
    full_null = [c for c in df.columns if df[c].isna().all()]
    if full_null:
        df = df.drop(columns=full_null)
    obj_cols = df.select_dtypes(include=["object"]).columns
    if len(obj_cols) > 0:
        df[obj_cols] = (
            df[obj_cols]
            .apply(lambda col: col.str.strip(), axis=0)
            .replace(r"^\s*$", pd.NA, regex=True)
        )
    df = df.dropna(how="all").reset_index(drop=True)
    return df

def clean_snies_file(path: Path, category: str, year: str) -> pd.DataFrame | None:
    try:
        df = pd.read_excel(path, engine="openpyxl")
    except Exception as e:
        logger.error("No se pudo leer %s: %s", path, e)
        return None

    rows_in = len(df)
    df = clean_dataframe(df)

    dest = PROCESSED_SNIES_DIR / category / f"{category}-{year}{OUTPUT_EXTENSION}"
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dest, index=False)

    record_lineage(
        str(path),
        str(dest),
        f"clean_snies_{category}",
        rows_in,
        len(df),
        len(df.columns),
    )
    return df

def _rename_columns_sql(cur, schema: str, table: str) -> list[str]:
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s "
        "ORDER BY ordinal_position",
        (schema, table),
    )
    original_cols = [row[0] for row in cur.fetchall()]
    renamed = []
    for col in original_cols:
        new_name = normalize_column_name(col)
        if new_name != col:
            cur.execute(
                f'ALTER TABLE {schema}."{table}" RENAME COLUMN "{col}" TO "{new_name}"'
            )
            renamed.append(f"{col} -> {new_name}")
    return renamed

def _drop_junk_columns_sql(cur, schema: str, table: str) -> list[str]:
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s "
        "ORDER BY ordinal_position",
        (schema, table),
    )
    columns = [row[0] for row in cur.fetchall()]

    unnamed = [c for c in columns if c.startswith("unnamed")]

    remaining = [c for c in columns if c not in unnamed]
    all_null_cols = []
    if remaining:
        null_checks = ", ".join(
            f'COUNT(*) FILTER (WHERE "{c}" IS NOT NULL)' for c in remaining
        )
        cur.execute(f'SELECT {null_checks} FROM {schema}."{table}"')
        counts = cur.fetchone()
        for i, c in enumerate(remaining):
            if counts[i] == 0:
                all_null_cols.append(c)

    cols_to_drop = unnamed + all_null_cols
    if cols_to_drop:
        drop_clauses = ", ".join(f'DROP COLUMN "{c}"' for c in cols_to_drop)
        cur.execute(f'ALTER TABLE {schema}."{table}" {drop_clauses}')

    return cols_to_drop

def _clean_text_columns_sql(cur, schema: str, table: str) -> int:
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s "
        "AND data_type IN ('text', 'character varying') "
        "ORDER BY ordinal_position",
        (schema, table),
    )
    text_cols = [row[0] for row in cur.fetchall()]
    if not text_cols:
        return 0

    set_clauses = ", ".join(
        f'"{col}" = pg_normalize_text("{col}")' for col in text_cols
    )
    where_clauses = " OR ".join(f'"{col}" IS NOT NULL' for col in text_cols)
    cur.execute(f'UPDATE {schema}."{table}" SET {set_clauses} WHERE {where_clauses}')
    return cur.rowcount

def _delete_empty_rows_sql(cur, schema: str, table: str) -> int:
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s "
        "ORDER BY ordinal_position",
        (schema, table),
    )
    columns = [row[0] for row in cur.fetchall()]
    if not columns:
        return 0

    all_null_condition = " AND ".join(f'"{c}" IS NULL' for c in columns)
    cur.execute(f'DELETE FROM {schema}."{table}" WHERE {all_null_condition}')
    return cur.rowcount

def _ensure_pg_normalize_function(schema: str):
    with managed_connection(schema=schema) as conn:
        create_pg_normalize_function(conn)

def _get_table_meta(cur, schema: str, table: str) -> tuple[int, list[str]]:
    cur.execute(f'SELECT COUNT(*) FROM {schema}."{table}"')
    row_count = cur.fetchone()[0]
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s "
        "ORDER BY ordinal_position",
        (schema, table),
    )
    columns = [row[0] for row in cur.fetchall()]
    return row_count, columns

def transform_table(table: str, engine) -> dict | None:
    schema = PG_SCHEMA_RAW

    with managed_connection(schema=schema) as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM {schema}."{table}"')
            rows_in = cur.fetchone()[0]

            if rows_in == 0:
                logger.info("  %s: tabla vacía, omitiendo", table)
                return None

            logger.info("  %s: %d filas leídas", table, rows_in)

            renamed = _rename_columns_sql(cur, schema, table)
            if renamed:
                logger.info("  %s: columnas renombradas: %s", table, renamed)

            dropped = _drop_junk_columns_sql(cur, schema, table)
            if dropped:
                logger.info("  %s: columnas eliminadas: %s", table, dropped)

            updated = _clean_text_columns_sql(cur, schema, table)
            if updated:
                logger.info("  %s: %d filas actualizadas (texto)", table, updated)

            deleted = _delete_empty_rows_sql(cur, schema, table)
            if deleted:
                logger.info("  %s: %d filas vacías eliminadas", table, deleted)

            rows_out, final_columns = _get_table_meta(cur, schema, table)

    record_lineage(
        f"pg:{schema}.{table}",
        f"pg:{schema}.{table}",
        f"transform_{table}",
        rows_in,
        rows_out,
        len(final_columns),
    )

    logger.info(
        "  %s: %d → %d filas, %d columnas",
        table,
        rows_in,
        rows_out,
        len(final_columns),
    )

    return {
        "rows": rows_out,
        "cols": len(final_columns),
        "columns": final_columns,
    }

def transform_all():
    logger.info("=" * 50)
    logger.info("TRANSFORMACIÓN Y LIMPIEZA (PostgreSQL)")
    logger.info("Schema objetivo: %s", PG_SCHEMA_RAW)
    logger.info("=" * 50)

    raw_tables = list_tables(PG_SCHEMA_RAW)
    tables_to_process = sorted(t for t in raw_tables if t not in _INTERNAL_TABLES)

    if not tables_to_process:
        logger.warning("Schema '%s' no tiene tablas para transformar", PG_SCHEMA_RAW)
        return {}

    logger.info(
        "Tablas encontradas en '%s': %d %s",
        PG_SCHEMA_RAW,
        len(tables_to_process),
        tables_to_process,
    )

    _ensure_pg_normalize_function(PG_SCHEMA_RAW)

    engine = get_engine(schema=PG_SCHEMA_RAW)
    results: dict[str, dict] = {}

    for table in tables_to_process:
        result = transform_table(table, engine)
        if result is not None:
            results[table] = result

    logger.info("=" * 50)
    logger.info("TRANSFORMACIÓN COMPLETA: %d datasets procesados", len(results))
    logger.info("=" * 50)
    return results
