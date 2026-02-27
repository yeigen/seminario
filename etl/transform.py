import json
import sqlite3
from datetime import datetime, timezone

import pandas as pd

from config.globals import (
    LINEAGE_PATH,
    SQLITE_DB_PATH,
    SNIES_CATEGORIES,
    CSV_DATASETS,
    CSV_ENCODINGS,
    raw_csv_path,
)
from utils.logger import logger


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


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(r"[áà]", "a", regex=True)
        .str.replace(r"[éè]", "e", regex=True)
        .str.replace(r"[íì]", "i", regex=True)
        .str.replace(r"[óò]", "o", regex=True)
        .str.replace(r"[úù]", "u", regex=True)
        .str.replace(r"[ñ]", "n", regex=True)
        .str.replace(r"[\s\-\.]+", "_", regex=True)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    return df


def drop_junk_columns(df: pd.DataFrame) -> pd.DataFrame:
    unnamed = [c for c in df.columns if c.startswith("unnamed")]
    if unnamed:
        df = df.drop(columns=unnamed)

    full_null = [c for c in df.columns if df[c].isna().all()]
    if full_null:
        df = df.drop(columns=full_null)

    return df


def clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.strip()
        df[col] = df[col].replace(r"^\s*$", pd.NA, regex=True)
    return df


def drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(how="all").reset_index(drop=True)


def drop_junk_columns(df: pd.DataFrame) -> pd.DataFrame:
    unnamed = [c for c in df.columns if c.startswith("unnamed")]
    if unnamed:
        df = df.drop(columns=unnamed)
    return df


def clean_snies_file(path: Path, category: str, year: str) -> pd.DataFrame | None:
    try:
        df = pd.read_excel(path, engine="openpyxl")
    except Exception as e:
        print(f"  [ERROR] No se pudo leer {path}: {e}")
        return None

    rows_in = len(df)
    df = normalize_columns(df)
    df = drop_junk_columns(df)
    df = clean_text_columns(df)
    df = drop_empty_rows(df)
    df = standardize_year_column(df, int(year))

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


def table_name_from_dataset_key(dataset_key: str) -> str:
    return dataset_key.split("/")[-1]


def load_csv_into_sqlite(conn: sqlite3.Connection, dataset_key: str) -> bool:
    path = raw_csv_path(dataset_key)
    if not path.exists():
        logger.warning("CSV no encontrado: %s", path)
        return False

    table = table_name_from_dataset_key(dataset_key)
    sep = ","

    for encoding in CSV_ENCODINGS:
        try:
            df = pd.read_csv(path, sep=sep, encoding=encoding, low_memory=False)
            break
        except UnicodeDecodeError:
            continue
    else:
        logger.error("No se pudo decodificar %s", path)
        return False

    df.to_sql(table, conn, if_exists="replace", index=False)
    logger.info("CSV cargado en SQLite tabla '%s': %d filas", table, len(df))
    return True


def get_all_tables(conn: sqlite3.Connection) -> list[str]:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    return [row[0] for row in cursor.fetchall()]


def transform_all():
    logger.info("=" * 50)
    logger.info("TRANSFORMACIÓN Y LIMPIEZA")
    logger.info("=" * 50)

    if not SQLITE_DB_PATH.exists():
        logger.error("Base de datos no encontrada: %s", SQLITE_DB_PATH)
        return {}

    conn = sqlite3.connect(SQLITE_DB_PATH)
    results = {}

    logger.info("[1/2] Transformando tablas SNIES...")
    for category in SNIES_CATEGORIES:
        try:
            df = pd.read_sql(f'SELECT * FROM "{category}"', conn)
        except Exception as e:
            logger.warning("No se pudo leer tabla '%s': %s", category, e)
            continue

        rows_in = len(df)
        if rows_in == 0:
            logger.info("  %s: tabla vacía, omitiendo", category)
            continue

        logger.info("  %s: %d filas leídas", category, rows_in)
        df = clean_snies_dataframe(df)

        df.to_sql(category, conn, if_exists="replace", index=False)

        record_lineage(
            f"sqlite:{category}",
            f"sqlite:{category}",
            f"transform_snies_{category}",
            rows_in,
            len(df),
            len(df.columns),
        )

        results[category] = {
            "rows": len(df),
            "cols": len(df.columns),
            "columns": list(df.columns),
        }
        logger.info(
            "  %s: %d → %d filas, %d columnas",
            category,
            rows_in,
            len(df),
            len(df.columns),
        )

    logger.info("[2/2] Cargando y transformando datasets CSV...")
    for dataset_key in CSV_DATASETS:
        table = table_name_from_dataset_key(dataset_key)
        loaded = load_csv_into_sqlite(conn, dataset_key)

        if not loaded:
            logger.warning("  %s: no se pudo cargar, omitiendo", dataset_key)
            continue

        try:
            df = pd.read_sql(f'SELECT * FROM "{table}"', conn)
        except Exception as e:
            logger.warning("No se pudo leer tabla '%s': %s", table, e)
            continue

        rows_in = len(df)
        logger.info("  %s: %d filas leídas", table, rows_in)
        df = clean_csv_dataframe(df)

        df.to_sql(table, conn, if_exists="replace", index=False)

        record_lineage(
            f"sqlite:{table}",
            f"sqlite:{table}",
            f"transform_csv_{table}",
            rows_in,
            len(df),
            len(df.columns),
        )

        results[table] = {
            "rows": len(df),
            "cols": len(df.columns),
            "columns": list(df.columns),
        }
        logger.info(
            "  %s: %d → %d filas, %d columnas",
            table,
            rows_in,
            len(df),
            len(df.columns),
        )

    conn.close()

    logger.info("=" * 50)
    logger.info("TRANSFORMACIÓN COMPLETA: %d datasets", len(results))
    logger.info("=" * 50)
    return results
