"""
Separación de la base de datos monolítica en 3 bases especializadas.

Lee `data/seminario.db` y distribuye las tablas en:
  - data/seminario.db          → tablas originales (crudas por categoría SNIES)
  - data/seminario_unified.db  → tablas con sufijo _unified
  - data/seminario_facts.db    → dimensiones (dim_*) y hechos (fact_*)

Uso:
    python scripts/split_databases.py
"""

from __future__ import annotations

import shutil
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import DATA_DIR, SQLITE_DB_PATH
from utils.logger import logger

# ──────────────────────────────────────────────────────────────
# PATHS DE LAS 3 BASES DE DATOS
# ──────────────────────────────────────────────────────────────
UNIFIED_DB_PATH = DATA_DIR / "seminario_unified.db"
FACTS_DB_PATH = DATA_DIR / "seminario_facts.db"
BACKUP_DB_PATH = DATA_DIR / "seminario_backup.db"

# ──────────────────────────────────────────────────────────────
# CLASIFICACIÓN DE TABLAS
# ──────────────────────────────────────────────────────────────
# Tablas del sistema SQLite que nunca se copian/eliminan
SYSTEM_TABLES = {"sqlite_sequence"}


def classify_table(name: str) -> str:
    """
    Determina a cuál de las 3 DBs pertenece una tabla.

    Returns:
        "original"  → seminario.db       (tablas crudas SNIES, PND, ICFES)
        "unified"   → seminario_unified.db
        "facts"     → seminario_facts.db  (dim_* y fact_*)
    """
    if name in SYSTEM_TABLES:
        return "system"
    if name.endswith("_unified"):
        return "unified"
    if name.startswith("dim_") or name.startswith("fact_"):
        return "facts"
    return "original"


# ──────────────────────────────────────────────────────────────
# FUNCIONES DE COPIA
# ──────────────────────────────────────────────────────────────
def get_table_ddl(conn: sqlite3.Connection, table_name: str) -> str:
    """Obtiene el CREATE TABLE original de una tabla."""
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Tabla '{table_name}' no encontrada en sqlite_master")
    return row[0]


def get_table_indexes(conn: sqlite3.Connection, table_name: str) -> list[str]:
    """Obtiene todos los CREATE INDEX de una tabla."""
    rows = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
        (table_name,),
    ).fetchall()
    return [row[0] for row in rows]


def copy_table(
    src_conn: sqlite3.Connection,
    dst_conn: sqlite3.Connection,
    table_name: str,
) -> int:
    """
    Copia estructura + datos + índices de una tabla entre conexiones.

    Returns:
        Número de filas copiadas.
    """
    # 1. Crear la tabla en destino
    ddl = get_table_ddl(src_conn, table_name)
    dst_conn.execute(ddl)

    # 2. Copiar datos en lotes para eficiencia con tablas grandes
    batch_size = 50_000
    cursor = src_conn.execute(f'SELECT * FROM "{table_name}"')
    col_count = len(cursor.description)
    placeholders = ", ".join(["?"] * col_count)
    insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'

    total_rows = 0
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        dst_conn.executemany(insert_sql, batch)
        total_rows += len(batch)

    dst_conn.commit()

    # 3. Recrear índices
    for idx_sql in get_table_indexes(src_conn, table_name):
        dst_conn.execute(idx_sql)
    dst_conn.commit()

    return total_rows


def drop_tables(conn: sqlite3.Connection, tables: list[str]) -> None:
    """Elimina una lista de tablas de una conexión."""
    for table_name in tables:
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        logger.debug("  Eliminada tabla '%s' de la fuente", table_name)
    conn.execute("VACUUM")
    conn.commit()


# ──────────────────────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ──────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("=" * 60)
    logger.info("SEPARACIÓN DE BASE DE DATOS EN 3 ARCHIVOS")
    logger.info("=" * 60)

    # ── Validar que existe la DB fuente ──
    if not SQLITE_DB_PATH.exists():
        logger.error("Base de datos fuente no encontrada: %s", SQLITE_DB_PATH)
        sys.exit(1)

    src_size_mb = SQLITE_DB_PATH.stat().st_size / (1024 * 1024)
    logger.info("Fuente: %s (%.1f MB)", SQLITE_DB_PATH, src_size_mb)

    # ── Backup preventivo ──
    if BACKUP_DB_PATH.exists():
        logger.info("Backup ya existe, se conserva: %s", BACKUP_DB_PATH)
    else:
        logger.info("Creando backup: %s", BACKUP_DB_PATH)
        shutil.copy2(SQLITE_DB_PATH, BACKUP_DB_PATH)
    backup_size_mb = BACKUP_DB_PATH.stat().st_size / (1024 * 1024)
    logger.info("Backup: %.1f MB", backup_size_mb)

    # ── Conectar a la fuente (solo lectura para la fase de copia) ──
    src_conn = sqlite3.connect(SQLITE_DB_PATH)
    src_conn.execute("PRAGMA journal_mode=WAL")

    # ── Inventariar y clasificar tablas ──
    all_tables = [
        row[0]
        for row in src_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    ]

    classification: dict[str, list[str]] = {
        "original": [],
        "unified": [],
        "facts": [],
    }

    logger.info("")
    logger.info("Clasificación de tablas:")
    logger.info("-" * 50)

    for table_name in all_tables:
        category = classify_table(table_name)
        if category == "system":
            continue
        classification[category].append(table_name)
        row_count = src_conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[
            0
        ]
        logger.info("  %-40s → %-10s (%d filas)", table_name, category, row_count)

    logger.info("")
    logger.info("Resumen de clasificación:")
    for cat, tables in classification.items():
        logger.info("  %-10s: %d tablas → %s", cat, len(tables), [t for t in tables])

    # ── Crear bases de datos destino ──
    # Eliminar destinos previos si existen
    for db_path in (UNIFIED_DB_PATH, FACTS_DB_PATH):
        if db_path.exists():
            db_path.unlink()
            logger.info("Eliminada DB previa: %s", db_path)

    # ── Fase 1: Copiar tablas unified ──
    logger.info("")
    logger.info("=" * 60)
    logger.info("FASE 1: Copiando tablas _unified → %s", UNIFIED_DB_PATH.name)
    logger.info("=" * 60)

    t0 = time.perf_counter()
    unified_conn = sqlite3.connect(UNIFIED_DB_PATH)
    unified_conn.execute("PRAGMA journal_mode=WAL")
    unified_conn.execute("PRAGMA synchronous=NORMAL")

    for table_name in classification["unified"]:
        t_start = time.perf_counter()
        rows = copy_table(src_conn, unified_conn, table_name)
        elapsed = time.perf_counter() - t_start
        logger.info("  %-40s %8d filas copiadas (%.1fs)", table_name, rows, elapsed)

    unified_conn.close()
    elapsed_total = time.perf_counter() - t0
    unified_size_mb = UNIFIED_DB_PATH.stat().st_size / (1024 * 1024)
    logger.info(
        "Fase 1 completada: %s (%.1f MB) en %.1fs",
        UNIFIED_DB_PATH.name,
        unified_size_mb,
        elapsed_total,
    )

    # ── Fase 2: Copiar tablas facts/dimensions (dims primero, luego facts) ──
    logger.info("")
    logger.info("=" * 60)
    logger.info("FASE 2: Copiando tablas dim_*/fact_* → %s", FACTS_DB_PATH.name)
    logger.info("=" * 60)

    t0 = time.perf_counter()
    facts_conn = sqlite3.connect(FACTS_DB_PATH)
    facts_conn.execute("PRAGMA journal_mode=WAL")
    facts_conn.execute("PRAGMA synchronous=NORMAL")

    # Copiar dimensiones primero, luego hechos (para que FKs sean válidas)
    dim_tables = [t for t in classification["facts"] if t.startswith("dim_")]
    fact_tables = [t for t in classification["facts"] if t.startswith("fact_")]
    ordered_facts = dim_tables + fact_tables

    for table_name in ordered_facts:
        t_start = time.perf_counter()
        rows = copy_table(src_conn, facts_conn, table_name)
        elapsed = time.perf_counter() - t_start
        # Verificar integridad de IDs para dimensiones
        if table_name.startswith("dim_"):
            src_range = src_conn.execute(
                f'SELECT MIN(id), MAX(id) FROM "{table_name}"'
            ).fetchone()
            dst_range = facts_conn.execute(
                f'SELECT MIN(id), MAX(id) FROM "{table_name}"'
            ).fetchone()
            if src_range != dst_range:
                logger.error(
                    "  ¡ERROR DE IDs! %s: fuente=[%s,%s] destino=[%s,%s]",
                    table_name,
                    src_range[0],
                    src_range[1],
                    dst_range[0],
                    dst_range[1],
                )
                raise RuntimeError(
                    f"IDs no coinciden para {table_name}: "
                    f"src={src_range}, dst={dst_range}"
                )
        logger.info("  %-40s %8d filas copiadas (%.1fs)", table_name, rows, elapsed)

    # Verificar integridad referencial
    logger.info("  Verificando integridad referencial...")
    orphans = facts_conn.execute(
        """
        SELECT COUNT(*)
        FROM fact_estudiantes f
        LEFT JOIN dim_institucion d ON f.institucion_id = d.id
        WHERE d.id IS NULL
        """
    ).fetchone()[0]
    if orphans > 0:
        logger.error(
            "  ¡%d FKs huérfanas en fact_estudiantes→dim_institucion!", orphans
        )
    else:
        logger.info("  Integridad referencial OK (0 huérfanos)")

    facts_conn.close()
    elapsed_total = time.perf_counter() - t0
    facts_size_mb = FACTS_DB_PATH.stat().st_size / (1024 * 1024)
    logger.info(
        "Fase 2 completada: %s (%.1f MB) en %.1fs",
        FACTS_DB_PATH.name,
        facts_size_mb,
        elapsed_total,
    )

    # ── Fase 3: Eliminar tablas copiadas del original ──
    logger.info("")
    logger.info("=" * 60)
    logger.info("FASE 3: Limpiando tablas migradas de %s", SQLITE_DB_PATH.name)
    logger.info("=" * 60)

    tables_to_remove = classification["unified"] + classification["facts"]
    logger.info("Eliminando %d tablas de la DB original...", len(tables_to_remove))

    t0 = time.perf_counter()
    drop_tables(src_conn, tables_to_remove)
    elapsed_total = time.perf_counter() - t0
    logger.info("Limpieza completada en %.1fs", elapsed_total)

    # ── Verificación final ──
    logger.info("")
    logger.info("=" * 60)
    logger.info("VERIFICACIÓN FINAL")
    logger.info("=" * 60)

    # Verificar seminario.db
    remaining = [
        row[0]
        for row in src_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence' ORDER BY name"
        ).fetchall()
    ]
    logger.info("")
    logger.info("seminario.db (%d tablas):", len(remaining))
    for t in remaining:
        count = src_conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        logger.info("  %-40s %8d filas", t, count)

    src_conn.close()
    final_size_mb = SQLITE_DB_PATH.stat().st_size / (1024 * 1024)

    # Verificar seminario_unified.db
    unified_conn = sqlite3.connect(UNIFIED_DB_PATH)
    unified_tables = [
        row[0]
        for row in unified_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence' ORDER BY name"
        ).fetchall()
    ]
    logger.info("")
    logger.info("seminario_unified.db (%d tablas):", len(unified_tables))
    for t in unified_tables:
        count = unified_conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        logger.info("  %-40s %8d filas", t, count)
    unified_conn.close()

    # Verificar seminario_facts.db
    facts_conn = sqlite3.connect(FACTS_DB_PATH)
    facts_tables = [
        row[0]
        for row in facts_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence' ORDER BY name"
        ).fetchall()
    ]
    logger.info("")
    logger.info("seminario_facts.db (%d tablas):", len(facts_tables))
    for t in facts_tables:
        count = facts_conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        logger.info("  %-40s %8d filas", t, count)
    facts_conn.close()

    # Resumen de tamaños
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESUMEN DE TAMAÑOS")
    logger.info("=" * 60)
    logger.info(
        "  %-30s %8.1f MB (antes: %.1f MB)", "seminario.db", final_size_mb, src_size_mb
    )
    logger.info("  %-30s %8.1f MB", "seminario_unified.db", unified_size_mb)
    logger.info("  %-30s %8.1f MB", "seminario_facts.db", facts_size_mb)
    logger.info(
        "  %-30s %8.1f MB",
        "TOTAL",
        final_size_mb + unified_size_mb + facts_size_mb,
    )
    logger.info("  %-30s %8.1f MB", "seminario_backup.db", backup_size_mb)
    logger.info("")
    logger.info("Separación completada exitosamente.")


if __name__ == "__main__":
    main()
