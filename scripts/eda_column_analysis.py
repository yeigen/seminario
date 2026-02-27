"""
EDA — Análisis exploratorio de columnas en la base de datos SQLite.

Lee todas las tablas de seminario.db, identifica columnas, compara
estructuras entre tablas similares y genera un reporte Markdown + JSON.

Uso:
    python scripts/eda_column_analysis.py
"""

import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import DATA_DIR, SQLITE_DB_PATH
from utils.logger import logger

# ──────────────────────────────────────────────────────────────
# Paths de salida
# ──────────────────────────────────────────────────────────────
EDA_DIR = DATA_DIR / "eda"
REPORT_MD_PATH = EDA_DIR / "column_analysis.md"
REPORT_JSON_PATH = EDA_DIR / "column_analysis.json"

# ──────────────────────────────────────────────────────────────
# Grupos de tablas similares para comparación cruzada
# ──────────────────────────────────────────────────────────────
TABLE_GROUPS: dict[str, list[str]] = {
    "poblacion_estudiantil": [
        "admitidos",
        "inscritos",
        "matriculados",
        "matriculados_primer_curso",
        "graduados",
    ],
    "recurso_humano": [
        "docentes",
        "administrativos",
    ],
}


# ──────────────────────────────────────────────────────────────
# 1. Lectura de metadatos desde SQLite
# ──────────────────────────────────────────────────────────────
def get_all_tables(conn: sqlite3.Connection) -> list[str]:
    """Retorna la lista de tablas de usuario (excluye sqlite internas)."""
    query = """
        SELECT name FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """
    rows = conn.execute(query).fetchall()
    return [r[0] for r in rows]


def get_table_info(conn: sqlite3.Connection, table: str) -> list[dict]:
    """Retorna info de columnas via PRAGMA table_info."""
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    columns = []
    for row in rows:
        columns.append(
            {
                "cid": row[0],
                "name": row[1],
                "type": row[2] or "TEXT",
                "notnull": bool(row[3]),
                "default_value": row[4],
                "pk": bool(row[5]),
            }
        )
    return columns


def get_row_count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]


def get_sample_values(
    conn: sqlite3.Connection, table: str, column: str, limit: int = 5
) -> list:
    """Obtiene valores de ejemplo no-nulos de una columna."""
    query = f"""
        SELECT DISTINCT "{column}" FROM "{table}"
        WHERE "{column}" IS NOT NULL AND "{column}" != ''
        LIMIT {limit}
    """
    rows = conn.execute(query).fetchall()
    return [r[0] for r in rows]


def get_null_stats(
    conn: sqlite3.Connection, table: str, column: str, total_rows: int
) -> dict:
    """Calcula estadísticas de nulos para una columna."""
    null_count = conn.execute(
        f'SELECT COUNT(*) FROM "{table}" WHERE "{column}" IS NULL OR "{column}" = \'\''
    ).fetchone()[0]
    null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0.0
    return {
        "null_count": null_count,
        "total_rows": total_rows,
        "null_pct": round(null_pct, 2),
    }


# ──────────────────────────────────────────────────────────────
# 2. Análisis de comparación entre tablas
# ──────────────────────────────────────────────────────────────
def compare_table_group(
    tables_info: dict[str, list[dict]], group_tables: list[str]
) -> dict:
    """Compara columnas entre un grupo de tablas similares."""
    present_tables = [t for t in group_tables if t in tables_info]

    if len(present_tables) < 2:
        return {"error": "Menos de 2 tablas disponibles para comparación"}

    # Conjuntos de nombres de columna por tabla (sin 'id' autoincrement)
    col_sets: dict[str, set[str]] = {}
    for table in present_tables:
        cols = {c["name"] for c in tables_info[table] if c["name"] != "id"}
        col_sets[table] = cols

    # Columnas comunes a TODAS las tablas del grupo
    all_sets = list(col_sets.values())
    common = all_sets[0]
    for s in all_sets[1:]:
        common = common & s

    # Columnas únicas de cada tabla (no están en ninguna otra)
    unique_per_table: dict[str, list[str]] = {}
    for table in present_tables:
        others = set()
        for other_table in present_tables:
            if other_table != table:
                others = others | col_sets[other_table]
        unique = col_sets[table] - others
        unique_per_table[table] = sorted(unique)

    # Columnas que existen en ALGUNAS pero no en TODAS
    union_all = set()
    for s in all_sets:
        union_all = union_all | s
    partial = union_all - common
    # Para cada columna parcial, indicar en qué tablas aparece
    partial_detail: dict[str, list[str]] = {}
    for col in sorted(partial):
        present_in = [t for t in present_tables if col in col_sets[t]]
        if len(present_in) < len(present_tables):
            partial_detail[col] = present_in

    return {
        "tables_compared": present_tables,
        "total_tables": len(present_tables),
        "common_columns": sorted(common),
        "common_count": len(common),
        "unique_per_table": unique_per_table,
        "partial_columns": partial_detail,
        "partial_count": len(partial_detail),
    }


# ──────────────────────────────────────────────────────────────
# 3. Generación de reporte JSON
# ──────────────────────────────────────────────────────────────
def build_report(conn: sqlite3.Connection) -> dict:
    """Construye la estructura completa del reporte EDA."""
    logger.info("Iniciando análisis EDA de columnas...")

    tables = get_all_tables(conn)
    logger.info("Tablas encontradas: %d — %s", len(tables), tables)

    # Info por tabla
    tables_detail: dict[str, dict] = {}
    tables_info: dict[str, list[dict]] = {}

    for table in tables:
        logger.info("Analizando tabla: %s", table)
        columns = get_table_info(conn, table)
        tables_info[table] = columns
        row_count = get_row_count(conn, table)

        col_details = []
        for col in columns:
            col_name = col["name"]
            null_stats = get_null_stats(conn, table, col_name, row_count)
            samples = get_sample_values(conn, table, col_name)

            col_details.append(
                {
                    "name": col_name,
                    "type": col["type"],
                    "is_pk": col["pk"],
                    "notnull": col["notnull"],
                    "null_pct": null_stats["null_pct"],
                    "null_count": null_stats["null_count"],
                    "sample_values": [str(v) for v in samples],
                }
            )

        tables_detail[table] = {
            "row_count": row_count,
            "column_count": len(columns),
            "columns": col_details,
            "column_names": [c["name"] for c in columns],
        }
        logger.info(
            "  → %s: %d filas, %d columnas, null_max=%.1f%%",
            table,
            row_count,
            len(columns),
            max((c["null_pct"] for c in col_details), default=0),
        )

    # Comparación entre grupos
    logger.info("Comparando grupos de tablas similares...")
    group_comparisons: dict[str, dict] = {}
    for group_name, group_tables in TABLE_GROUPS.items():
        comparison = compare_table_group(tables_info, group_tables)
        group_comparisons[group_name] = comparison
        if "common_count" in comparison:
            logger.info(
                "  Grupo '%s': %d comunes, %d parciales entre %d tablas",
                group_name,
                comparison["common_count"],
                comparison["partial_count"],
                comparison["total_tables"],
            )

    # Estadísticas globales
    all_columns: set[str] = set()
    col_frequency: defaultdict[str, int] = defaultdict(int)
    for table, info in tables_detail.items():
        for col_name in info["column_names"]:
            if col_name != "id":
                all_columns.add(col_name)
                col_frequency[col_name] += 1

    # Columnas por frecuencia (en cuántas tablas aparecen)
    freq_sorted = sorted(col_frequency.items(), key=lambda x: -x[1])

    report = {
        "metadata": {
            "db_path": str(SQLITE_DB_PATH),
            "total_tables": len(tables),
            "total_unique_columns": len(all_columns),
            "total_rows_all_tables": sum(
                t["row_count"] for t in tables_detail.values()
            ),
        },
        "tables": tables_detail,
        "group_comparisons": group_comparisons,
        "column_frequency": {col: freq for col, freq in freq_sorted},
    }

    return report


# ──────────────────────────────────────────────────────────────
# 4. Generación de reporte Markdown
# ──────────────────────────────────────────────────────────────
def generate_markdown(report: dict) -> str:
    """Genera un reporte Markdown legible a partir del dict."""
    lines: list[str] = []
    meta = report["metadata"]

    lines.append("# EDA — Análisis de Columnas de `seminario.db`")
    lines.append("")
    lines.append("## Resumen General")
    lines.append("")
    lines.append(f"| Métrica | Valor |")
    lines.append(f"|---|---|")
    lines.append(f"| Base de datos | `{meta['db_path']}` |")
    lines.append(f"| Total tablas | {meta['total_tables']} |")
    lines.append(f"| Columnas únicas (sin `id`) | {meta['total_unique_columns']} |")
    lines.append(
        f"| Total filas (todas las tablas) | {meta['total_rows_all_tables']:,} |"
    )
    lines.append("")

    # ── Detalle por tabla ──
    lines.append("---")
    lines.append("")
    lines.append("## Detalle por Tabla")
    lines.append("")

    for table_name, table_info in report["tables"].items():
        lines.append(f"### `{table_name}`")
        lines.append("")
        lines.append(
            f"**Filas:** {table_info['row_count']:,} | "
            f"**Columnas:** {table_info['column_count']}"
        )
        lines.append("")
        lines.append("| # | Columna | Tipo | PK | Nulos (%) | Valores de ejemplo |")
        lines.append("|---|---|---|---|---|---|")

        for i, col in enumerate(table_info["columns"], 1):
            samples = (
                ", ".join(col["sample_values"][:3]) if col["sample_values"] else "—"
            )
            # Limitar longitud de samples para la tabla
            if len(samples) > 60:
                samples = samples[:57] + "..."
            pk_mark = "✅" if col["is_pk"] else ""
            null_bar = f"{col['null_pct']:.1f}%"
            lines.append(
                f"| {i} | `{col['name']}` | {col['type']} | {pk_mark} | {null_bar} | {samples} |"
            )

        lines.append("")

    # ── Comparación de grupos ──
    lines.append("---")
    lines.append("")
    lines.append("## Comparación entre Grupos de Tablas")
    lines.append("")

    for group_name, comp in report["group_comparisons"].items():
        lines.append(f"### Grupo: `{group_name}`")
        lines.append("")

        if "error" in comp:
            lines.append(f"> ⚠️ {comp['error']}")
            lines.append("")
            continue

        tables_str = ", ".join(f"`{t}`" for t in comp["tables_compared"])
        lines.append(f"**Tablas comparadas ({comp['total_tables']}):** {tables_str}")
        lines.append("")

        # Columnas comunes
        lines.append(f"#### Columnas comunes a TODAS ({comp['common_count']})")
        lines.append("")
        if comp["common_columns"]:
            for col in comp["common_columns"]:
                lines.append(f"- `{col}`")
        else:
            lines.append("_Ninguna columna es común a todas las tablas._")
        lines.append("")

        # Columnas únicas
        lines.append("#### Columnas únicas por tabla")
        lines.append("")
        for table, unique_cols in comp["unique_per_table"].items():
            if unique_cols:
                cols_str = ", ".join(f"`{c}`" for c in unique_cols)
                lines.append(f"- **`{table}`** ({len(unique_cols)}): {cols_str}")
            else:
                lines.append(f"- **`{table}`**: _ninguna columna exclusiva_")
        lines.append("")

        # Columnas parciales
        if comp["partial_columns"]:
            lines.append(
                f"#### Columnas parciales (en algunas pero no todas) ({comp['partial_count']})"
            )
            lines.append("")
            lines.append("| Columna | Presente en |")
            lines.append("|---|---|")
            for col, present_in in comp["partial_columns"].items():
                tables_list = ", ".join(f"`{t}`" for t in present_in)
                lines.append(f"| `{col}` | {tables_list} |")
            lines.append("")

    # ── Frecuencia global de columnas ──
    lines.append("---")
    lines.append("")
    lines.append("## Frecuencia Global de Columnas")
    lines.append("")
    lines.append(
        "Cuántas tablas contienen cada columna (excluyendo `id` autoincrement):"
    )
    lines.append("")
    lines.append("| Columna | Aparece en N tablas |")
    lines.append("|---|---|")
    for col, freq in report["column_frequency"].items():
        lines.append(f"| `{col}` | {freq} |")
    lines.append("")

    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("EDA — Análisis de columnas de seminario.db")
    logger.info("=" * 60)

    # Verificar que la DB existe
    if not SQLITE_DB_PATH.exists():
        logger.error(
            "La base de datos no existe en %s. "
            "Ejecuta primero: python scripts/create_sqlite_db.py",
            SQLITE_DB_PATH,
        )
        sys.exit(1)

    logger.info("DB encontrada: %s", SQLITE_DB_PATH)
    db_size_mb = SQLITE_DB_PATH.stat().st_size / (1024 * 1024)
    logger.info("Tamaño: %.1f MB", db_size_mb)

    # Conexión de solo lectura
    conn = sqlite3.connect(f"file:{SQLITE_DB_PATH}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only = ON")

    try:
        # Construir reporte
        report = build_report(conn)

        # Crear directorio de salida
        EDA_DIR.mkdir(parents=True, exist_ok=True)

        # Guardar JSON
        with open(REPORT_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        logger.info("Reporte JSON guardado: %s", REPORT_JSON_PATH)

        # Guardar Markdown
        md_content = generate_markdown(report)
        with open(REPORT_MD_PATH, "w", encoding="utf-8") as f:
            f.write(md_content)
        logger.info("Reporte Markdown guardado: %s", REPORT_MD_PATH)

        # Resumen rápido en consola
        logger.info("=" * 60)
        logger.info("RESUMEN EDA")
        logger.info("=" * 60)
        logger.info("Tablas: %d", report["metadata"]["total_tables"])
        logger.info("Columnas únicas: %d", report["metadata"]["total_unique_columns"])
        logger.info(
            "Filas totales: %s",
            f"{report['metadata']['total_rows_all_tables']:,}",
        )
        for table, info in report["tables"].items():
            logger.info(
                "  %-30s %8d filas | %3d columnas",
                table,
                info["row_count"],
                info["column_count"],
            )

    finally:
        conn.close()

    logger.info("EDA completado correctamente.")


if __name__ == "__main__":
    main()
