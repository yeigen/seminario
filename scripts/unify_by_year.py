import re
import sys
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import PG_SCHEMA_RAW, PG_SCHEMA_UNIFIED
from utils.db import (
    ensure_schemas,
    get_column_names,
    get_engine,
    get_row_count,
    list_tables,
    managed_connection,
    table_exists,
)
from utils.logger import logger

YEAR_PATTERN = re.compile(r"^(.+)[_\-](\d{4})$")
READ_CHUNK_SIZE = 50_000
INSERT_PAGE_SIZE = 5_000

def get_all_tables(schema: str) -> list[str]:
    return list_tables(schema)

def classify_tables(tables: list[str]) -> dict[str, list[tuple[str, int | None]]]:
    categories: dict[str, list[tuple[str, int | None]]] = {}
    for table in tables:
        if table.endswith("_unified"):
            continue
        match = YEAR_PATTERN.match(table)
        if match:
            category = match.group(1)
            year = int(match.group(2))
            categories.setdefault(category, []).append((table, year))
        else:
            categories.setdefault(table, []).append((table, None))
    return categories

def normalize_year(value: object) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip().replace("\ufeff", "")
    try:
        return int(float(text))
    except (ValueError, OverflowError):
        return None

def _read_table_chunked(schema: str, table_name: str) -> pd.DataFrame:
    engine = get_engine(schema=schema)
    chunks: list[pd.DataFrame] = []
    for chunk in pd.read_sql_query(
        f'SELECT * FROM "{table_name}"',
        engine,
        chunksize=READ_CHUNK_SIZE,
    ):
        chunks.append(chunk)
    if not chunks:
        return pd.DataFrame()
    return pd.concat(chunks, ignore_index=True)

def read_table(schema: str, table_name: str) -> pd.DataFrame:
    return _read_table_chunked(schema, table_name)

def unify_category(
    schema: str,
    category: str,
    table_entries: list[tuple[str, int | None]],
) -> pd.DataFrame | None:
    logger.info("[%s] Iniciando unificación (%d tablas)", category, len(table_entries))
    frames: list[pd.DataFrame] = []
    for table_name, year_from_name in table_entries:
        df = read_table(schema, table_name)
        if "id" in df.columns:
            df = df.drop(columns=["id"])
        if year_from_name is not None and "ano" not in df.columns:
            df["ano"] = year_from_name
            logger.debug(
                "[%s] Columna 'ano' añadida con valor %d desde nombre de tabla",
                category,
                year_from_name,
            )
        if "ano" in df.columns:
            df["ano"] = df["ano"].apply(normalize_year)
        if year_from_name is not None:
            missing_mask = (
                df["ano"].isna() if "ano" in df.columns else pd.Series([True] * len(df))
            )
            if missing_mask.any():
                df.loc[missing_mask, "ano"] = year_from_name
        logger.info("[%s] Tabla '%s': %d filas leídas", category, table_name, len(df))
        frames.append(df)
    if not frames:
        logger.warning("[%s] Sin datos para unificar", category)
        return None
    unified = pd.concat(frames, ignore_index=True)
    rows_before = len(unified)
    subset_cols = [c for c in unified.columns if c not in ("id",)]
    unified = unified.drop_duplicates(subset=subset_cols).reset_index(drop=True)
    rows_after = len(unified)
    duplicates_removed = rows_before - rows_after
    if duplicates_removed > 0:
        logger.info(
            "[%s] Duplicados eliminados: %d (%d → %d filas)",
            category,
            duplicates_removed,
            rows_before,
            rows_after,
        )
    logger.info(
        "[%s] Unificación completada: %d filas, %d columnas",
        category,
        len(unified),
        len(unified.columns),
    )
    return unified

def _ensure_table_schema(
    category: str,
    df: pd.DataFrame,
    table_name: str,
) -> None:
    if table_exists(PG_SCHEMA_UNIFIED, table_name):
        return
    logger.info("[%s] Tabla '%s' no existe, creándola", category, table_name)
    engine = get_engine(schema=PG_SCHEMA_UNIFIED)
    df.head(0).to_sql(
        table_name,
        engine,
        schema=PG_SCHEMA_UNIFIED,
        if_exists="fail",
        index=False,
    )

def _truncate_table(table_name: str) -> None:
    with managed_connection(schema=PG_SCHEMA_UNIFIED) as conn:
        with conn.cursor() as cur:
            cur.execute(f'TRUNCATE TABLE {PG_SCHEMA_UNIFIED}."{table_name}"')

def _bulk_insert(df: pd.DataFrame, table_name: str) -> None:
    columns = list(df.columns)
    quoted_cols = ", ".join(f'"{c}"' for c in columns)
    insert_sql = (
        f'INSERT INTO {PG_SCHEMA_UNIFIED}."{table_name}" ({quoted_cols}) VALUES %s'
    )
    records = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df.itertuples(index=False, name=None)
    ]
    with managed_connection(schema=PG_SCHEMA_UNIFIED) as conn:
        with conn.cursor() as cur:
            for offset in range(0, len(records), INSERT_PAGE_SIZE):
                batch = records[offset : offset + INSERT_PAGE_SIZE]
                execute_values(cur, insert_sql, batch, page_size=INSERT_PAGE_SIZE)

def save_unified_table(category: str, df: pd.DataFrame) -> None:
    table_name = f"{category}_unified"
    try:
        _ensure_table_schema(category, df, table_name)
        _truncate_table(table_name)
        _bulk_insert(df, table_name)
    except Exception:
        logger.exception("[%s] Error al guardar tabla '%s'", category, table_name)
        raise
    row_count = get_row_count(PG_SCHEMA_UNIFIED, table_name)
    col_count = len(get_column_names(PG_SCHEMA_UNIFIED, table_name))
    logger.info(
        "[%s] Tabla '%s' guardada: %d filas, %d columnas",
        category,
        table_name,
        row_count,
        col_count,
    )

def main():
    logger.info("=" * 60)
    logger.info("Unificación de tablas por año")
    logger.info("Fuente: schema '%s'", PG_SCHEMA_RAW)
    logger.info("Destino: schema '%s'", PG_SCHEMA_UNIFIED)
    logger.info("=" * 60)
    ensure_schemas()
    all_tables = get_all_tables(PG_SCHEMA_RAW)
    if not all_tables:
        logger.error("No se encontraron tablas en schema '%s'", PG_SCHEMA_RAW)
        sys.exit(1)
    logger.info("Tablas encontradas: %s", all_tables)
    categories = classify_tables(all_tables)
    logger.info("Categorías detectadas: %s", list(categories.keys()))
    unified_count = 0
    for category, table_entries in sorted(categories.items()):
        table_names = [t[0] for t in table_entries]
        logger.info("[%s] Tablas a unificar: %s", category, table_names)
        unified_df = unify_category(PG_SCHEMA_RAW, category, table_entries)
        if unified_df is not None and not unified_df.empty:
            save_unified_table(category, unified_df)
            unified_count += 1
        else:
            logger.warning("[%s] Sin datos, tabla unificada no creada", category)
    logger.info("=" * 60)
    logger.info("Resumen de unificación")
    logger.info("=" * 60)
    final_tables = get_all_tables(PG_SCHEMA_UNIFIED)
    unified_tables = [t for t in final_tables if t.endswith("_unified")]
    for table_name in unified_tables:
        row_count = get_row_count(PG_SCHEMA_UNIFIED, table_name)
        cols = get_column_names(PG_SCHEMA_UNIFIED, table_name)
        col_count = len(cols)
        if "ano" in cols:
            engine = get_engine(schema=PG_SCHEMA_UNIFIED)
            anos_df = pd.read_sql_query(
                f'SELECT DISTINCT ano FROM "{table_name}" WHERE ano IS NOT NULL ORDER BY ano',
                engine,
            )
            anos_list = anos_df["ano"].tolist()
        else:
            anos_list = ["N/A (sin columna año)"]
        logger.info(
            "  %-40s %8d filas | %d cols | años: %s",
            table_name,
            row_count,
            col_count,
            anos_list,
        )
    logger.info(
        "Unificación finalizada: %d tablas creadas en schema '%s'",
        unified_count,
        PG_SCHEMA_UNIFIED,
    )

if __name__ == "__main__":
    main()
