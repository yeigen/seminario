import re
import sqlite3
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import SQLITE_DB_PATH, SQLITE_UNIFIED_DB_PATH, DATA_DIR
from utils.logger import logger

YEAR_PATTERN = re.compile(r"^(.+)[_\-](\d{4})$")


def get_all_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence' ORDER BY name"
    ).fetchall()
    return [row[0] for row in rows]


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


def read_table(conn: sqlite3.Connection, table_name: str) -> pd.DataFrame:
    return pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)


def unify_category(
    conn: sqlite3.Connection,
    category: str,
    table_entries: list[tuple[str, int | None]],
) -> pd.DataFrame | None:
    logger.info("[%s] Iniciando unificación (%d tablas)", category, len(table_entries))

    frames: list[pd.DataFrame] = []

    for table_name, year_from_name in table_entries:
        df = read_table(conn, table_name)

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


def save_unified_table(conn: sqlite3.Connection, category: str, df: pd.DataFrame):
    table_name = f"{category}_unified"

    conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.commit()

    row_count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
    col_count = len(conn.execute(f'PRAGMA table_info("{table_name}")').fetchall())

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
    logger.info("Fuente: %s", SQLITE_DB_PATH)
    logger.info("Destino: %s", SQLITE_UNIFIED_DB_PATH)
    logger.info("=" * 60)

    if not SQLITE_DB_PATH.exists():
        logger.error("Base de datos no encontrada: %s", SQLITE_DB_PATH)
        sys.exit(1)

    src_conn = sqlite3.connect(SQLITE_DB_PATH)
    src_conn.execute("PRAGMA journal_mode=WAL")

    all_tables = get_all_tables(src_conn)
    logger.info("Tablas encontradas: %s", all_tables)

    categories = classify_tables(all_tables)
    logger.info("Categorías detectadas: %s", list(categories.keys()))

    # Crear la base de datos destino para tablas unificadas
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if SQLITE_UNIFIED_DB_PATH.exists():
        SQLITE_UNIFIED_DB_PATH.unlink()
        logger.info("Base de datos unificada anterior eliminada")

    dst_conn = sqlite3.connect(SQLITE_UNIFIED_DB_PATH)
    dst_conn.execute("PRAGMA journal_mode=WAL")
    dst_conn.execute("PRAGMA synchronous=NORMAL")

    unified_count = 0

    for category, table_entries in sorted(categories.items()):
        table_names = [t[0] for t in table_entries]
        logger.info("[%s] Tablas a unificar: %s", category, table_names)

        unified_df = unify_category(src_conn, category, table_entries)

        if unified_df is not None and not unified_df.empty:
            save_unified_table(dst_conn, category, unified_df)
            unified_count += 1
        else:
            logger.warning("[%s] Sin datos, tabla unificada no creada", category)

    logger.info("=" * 60)
    logger.info("Resumen de unificación")
    logger.info("=" * 60)

    final_tables = get_all_tables(dst_conn)
    unified_tables = [t for t in final_tables if t.endswith("_unified")]

    for table_name in unified_tables:
        row_count = dst_conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[
            0
        ]
        col_count = len(
            dst_conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
        )
        anos = dst_conn.execute(
            f'SELECT DISTINCT ano FROM "{table_name}" WHERE ano IS NOT NULL ORDER BY ano'
        ).fetchall()
        anos_list = [row[0] for row in anos]
        logger.info(
            "  %-40s %8d filas | %d cols | años: %s",
            table_name,
            row_count,
            col_count,
            anos_list,
        )

    src_conn.close()
    dst_conn.close()

    size_mb = SQLITE_UNIFIED_DB_PATH.stat().st_size / (1024 * 1024)
    logger.info(
        "Unificación finalizada: %d tablas creadas en %s (%.1f MB)",
        unified_count,
        SQLITE_UNIFIED_DB_PATH,
        size_mb,
    )


if __name__ == "__main__":
    main()
