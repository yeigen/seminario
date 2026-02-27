"""
Normaliza los datos de texto en la base de datos SQLite.

Aplica las siguientes transformaciones a todas las columnas TEXT:
    1. Minúsculas
    2. Eliminar tildes (á→a, é→e, í→i, ó→o, ú→u, ñ→n)
    3. Trim + colapsar espacios múltiples
    4. Strings vacíos o solo espacios → NULL

Se procesan:
    - Tablas de dimensiones (dim_*)
    - Tablas de hechos (fact_*) si contienen columnas TEXT relevantes
    - Tablas fuente (*_unified y tablas base)

Columnas excluidas: created_at, updated_at (timestamps).

Uso:
    uv run python scripts/normalize_data.py
"""

import re
import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import SQLITE_DB_PATH
from utils.logger import logger

# ──────────────────────────────────────────────────────────────
# Columnas que NO se deben normalizar (timestamps, IDs técnicos)
# ──────────────────────────────────────────────────────────────
EXCLUDED_COLUMNS = {"created_at", "updated_at"}

# Mapeo explícito de caracteres con tilde y ñ
ACCENT_MAP = str.maketrans(
    "áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜñÑ",
    "aeiouAEIOUaeiouAEIOUaeiouAEIOUnN",
)

# Regex para colapsar espacios múltiples
MULTI_SPACE_RE = re.compile(r"\s{2,}")


# ──────────────────────────────────────────────────────────────
# Funciones de normalización
# ──────────────────────────────────────────────────────────────


def remove_accents(text: str) -> str:
    """Elimina tildes y convierte ñ→n usando mapeo directo."""
    return text.translate(ACCENT_MAP)


def normalize_text(text: str) -> str | None:
    """Aplica todas las normalizaciones a un string.

    1. Strip de espacios al inicio y final.
    2. Si queda vacío → None (se convertirá a NULL en SQL).
    3. Minúsculas.
    4. Eliminar tildes.
    5. Colapsar espacios múltiples a uno solo.
    """
    text = text.strip()
    if not text:
        return None
    text = text.lower()
    text = remove_accents(text)
    text = MULTI_SPACE_RE.sub(" ", text)
    return text


# ──────────────────────────────────────────────────────────────
# Funciones de introspección de la base de datos
# ──────────────────────────────────────────────────────────────


def get_all_tables(conn: sqlite3.Connection) -> list[str]:
    """Retorna todas las tablas de la base de datos (excepto sqlite_sequence)."""
    rows = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name != 'sqlite_sequence' "
        "ORDER BY name"
    ).fetchall()
    return [row[0] for row in rows]


def get_text_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    """Retorna las columnas TEXT de una tabla, excluyendo las protegidas."""
    cols = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    return [
        col[1]
        for col in cols
        if col[2].upper() == "TEXT" and col[1] not in EXCLUDED_COLUMNS
    ]


def get_row_count(conn: sqlite3.Connection, table_name: str) -> int:
    """Retorna el número de filas de una tabla."""
    return conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]


# ──────────────────────────────────────────────────────────────
# Normalización vía SQL puro (UPDATE in-place)
# ──────────────────────────────────────────────────────────────


# Registrar función personalizada en SQLite para usar en UPDATE
def _register_normalize_function(conn: sqlite3.Connection) -> None:
    """Registra la función normalize() como UDF de SQLite."""
    conn.create_function("normalize", 1, normalize_text, deterministic=True)


def normalize_table(conn: sqlite3.Connection, table_name: str) -> dict:
    """Normaliza todas las columnas TEXT de una tabla usando UPDATE directo.

    Estrategia:
        Para cada columna TEXT ejecuta:
          UPDATE tabla SET col = normalize(col) WHERE col IS NOT NULL;
        Luego convierte strings vacíos a NULL:
          UPDATE tabla SET col = NULL WHERE TRIM(col) = '' OR col = '';

    Retorna un dict con estadísticas de la operación.
    """
    text_cols = get_text_columns(conn, table_name)
    if not text_cols:
        return {
            "table": table_name,
            "status": "skipped",
            "reason": "sin columnas TEXT",
            "rows": 0,
            "text_cols": 0,
            "nullified": 0,
        }

    row_count = get_row_count(conn, table_name)
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

    total_nullified = 0

    for col in text_cols:
        # Contar NULLs antes
        nulls_before = conn.execute(
            f'SELECT COUNT(*) FROM "{table_name}" WHERE "{col}" IS NULL'
        ).fetchone()[0]

        # Aplicar normalización vía UDF registrada en SQLite
        conn.execute(
            f'UPDATE "{table_name}" SET "{col}" = normalize("{col}") '
            f'WHERE "{col}" IS NOT NULL'
        )

        # Contar NULLs después (normalize() retorna None para vacíos)
        nulls_after = conn.execute(
            f'SELECT COUNT(*) FROM "{table_name}" WHERE "{col}" IS NULL'
        ).fetchone()[0]

        total_nullified += nulls_after - nulls_before

    conn.commit()

    final_count = get_row_count(conn, table_name)
    logger.info(
        "[%s] Completado: %d filas, %d nuevos NULL (vacios→NULL)",
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


# ──────────────────────────────────────────────────────────────
# Verificación post-normalización
# ──────────────────────────────────────────────────────────────


def verify_normalization(conn: sqlite3.Connection, tables: list[str]) -> bool:
    """Verifica que no queden textos con tildes, mayúsculas o espacios extra."""
    logger.info("Verificando normalizacion...")

    issues: list[str] = []

    for table_name in tables:
        text_cols = get_text_columns(conn, table_name)
        if not text_cols:
            continue

        for col in text_cols:
            # Verificar tildes
            rows_with_accents = conn.execute(
                f'SELECT COUNT(*) FROM "{table_name}" '
                f"WHERE \"{col}\" GLOB '*[áéíóúÁÉÍÓÚñÑàèìòùÀÈÌÒÙ]*'"
            ).fetchone()[0]

            if rows_with_accents > 0:
                issues.append(
                    f"  {table_name}.{col}: {rows_with_accents} filas con tildes"
                )

            # Verificar mayúsculas (sample check para rendimiento)
            sample = conn.execute(
                f'SELECT "{col}" FROM "{table_name}" '
                f'WHERE "{col}" IS NOT NULL LIMIT 100'
            ).fetchall()

            upper_count = sum(1 for (val,) in sample if val and val != val.lower())
            if upper_count > 0:
                issues.append(
                    f"  {table_name}.{col}: {upper_count}/100 muestras con mayusculas"
                )

            # Verificar espacios múltiples
            rows_multi_space = conn.execute(
                f'SELECT COUNT(*) FROM "{table_name}" WHERE "{col}" LIKE \'%  %\''
            ).fetchone()[0]

            if rows_multi_space > 0:
                issues.append(
                    f"  {table_name}.{col}: {rows_multi_space} filas con espacios multiples"
                )

            # Verificar strings vacíos
            rows_empty = conn.execute(
                f'SELECT COUNT(*) FROM "{table_name}" '
                f"WHERE \"{col}\" = '' OR TRIM(\"{col}\") = ''"
            ).fetchone()[0]

            if rows_empty > 0:
                issues.append(
                    f"  {table_name}.{col}: {rows_empty} filas con strings vacios"
                )

    if issues:
        logger.warning("Se encontraron problemas de normalizacion:")
        for issue in issues:
            logger.warning(issue)
        return False

    logger.info("Verificacion exitosa: todos los textos normalizados correctamente")
    return True


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────


def main():
    logger.info("=" * 60)
    logger.info("Normalizacion de datos de texto — SQLite")
    logger.info("Base de datos: %s", SQLITE_DB_PATH)
    logger.info("=" * 60)

    if not SQLITE_DB_PATH.exists():
        logger.error("Base de datos no encontrada: %s", SQLITE_DB_PATH)
        sys.exit(1)

    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Registrar UDF de normalización
    _register_normalize_function(conn)

    all_tables = get_all_tables(conn)

    # Clasificar tablas
    dim_tables = sorted([t for t in all_tables if t.startswith("dim_")])
    fact_tables = sorted([t for t in all_tables if t.startswith("fact_")])
    other_tables = sorted(
        [
            t
            for t in all_tables
            if not t.startswith("dim_") and not t.startswith("fact_")
        ]
    )

    logger.info("Tablas encontradas:")
    logger.info("  Dimensiones: %s", dim_tables)
    logger.info("  Hechos:      %s", fact_tables)
    logger.info("  Otras:       %s", other_tables)

    # Orden de procesamiento: dimensiones → hechos → otras
    tables_to_process = dim_tables + fact_tables + other_tables
    results: list[dict] = []

    start_time = time.time()

    for table_name in tables_to_process:
        t0 = time.time()
        result = normalize_table(conn, table_name)
        elapsed = time.time() - t0
        result["elapsed_s"] = round(elapsed, 2)
        results.append(result)

    total_elapsed = time.time() - start_time

    # ──────────────────────────────────────────────────────
    # Resumen
    # ──────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Resumen de normalizacion")
    logger.info("=" * 60)

    total_rows = 0
    total_nullified = 0
    processed = 0
    skipped = 0

    for r in results:
        status = r["status"]
        name = r["table"]
        rows = r["rows"]
        text_cols = r["text_cols"]
        nullified = r["nullified"]
        elapsed = r.get("elapsed_s", 0)

        if status == "ok":
            processed += 1
            total_rows += rows
            total_nullified += nullified
            logger.info(
                "  %-40s %8d filas | %2d cols TEXT | %5d ->NULL | %.1fs",
                name,
                rows,
                text_cols,
                nullified,
                elapsed,
            )
        else:
            skipped += 1
            logger.info(
                "  %-40s SALTADA (%s)",
                name,
                r["reason"],
            )

    logger.info("-" * 60)
    logger.info("  Tablas procesadas:  %d", processed)
    logger.info("  Tablas saltadas:    %d", skipped)
    logger.info("  Filas totales:      %d", total_rows)
    logger.info("  Nuevos NULL:        %d (vacios -> NULL)", total_nullified)
    logger.info("  Tiempo total:       %.1f s", total_elapsed)

    # ──────────────────────────────────────────────────────
    # Verificación
    # ──────────────────────────────────────────────────────
    logger.info("")
    processed_tables = [r["table"] for r in results if r["status"] == "ok"]
    verify_normalization(conn, processed_tables)

    # ──────────────────────────────────────────────────────
    # Muestras post-normalización
    # ──────────────────────────────────────────────────────
    logger.info("")
    logger.info("Muestras post-normalizacion:")

    # Muestras dinámicas: tomar 3 columnas TEXT del primer registro de cada tabla procesada
    sample_queries = []
    for r in results:
        if r["status"] != "ok":
            continue
        tname = r["table"]
        tcols = get_text_columns(conn, tname)[:3]
        if tcols:
            cols_sql = ", ".join(f'"{c}"' for c in tcols)
            sample_queries.append((tname, f'SELECT {cols_sql} FROM "{tname}" LIMIT 3'))

    for label, query in sample_queries:
        try:
            rows = conn.execute(query).fetchall()
            logger.info("  [%s]", label)
            for row in rows:
                logger.info("    %s", row)
        except Exception as e:
            logger.warning("  [%s] Error al consultar: %s", label, e)

    conn.close()

    logger.info("=" * 60)
    logger.info("Normalizacion completada exitosamente")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
