import json
import sys
from collections import defaultdict
from functools import reduce
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import DATA_DIR, PG_SCHEMA_RAW
from utils.db import (
    get_columns,
    get_row_count,
    list_tables,
    managed_connection,
)
from utils.logger import logger

EDA_DIR = DATA_DIR / "eda"
REPORT_MD_PATH = EDA_DIR / "column_analysis.md"
REPORT_JSON_PATH = EDA_DIR / "column_analysis.json"

ANALYSIS_SCHEMA = PG_SCHEMA_RAW

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


def get_all_tables(schema: str) -> list[str]:
    return list_tables(schema)


def get_table_info(schema: str, table: str) -> list[dict]:
    return [
        {
            "cid": col["ordinal_position"],
            "name": col["column_name"],
            "type": col["data_type"] or "text",
            "notnull": False,
            "default_value": None,
            "pk": False,
        }
        for col in get_columns(schema, table)
    ]


def _get_row_count(schema: str, table: str) -> int:
    return get_row_count(schema, table)


def get_sample_values(schema: str, table: str, column: str, limit: int = 5) -> list:
    with managed_connection(schema=schema) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT DISTINCT "{column}" FROM "{table}" '
                f'WHERE "{column}" IS NOT NULL AND "{column}" != \'\' '
                f"LIMIT %s",
                (limit,),
            )
            return [r[0] for r in cur.fetchall()]


def get_null_stats(schema: str, table: str, column: str, total_rows: int) -> dict:
    with managed_connection(schema=schema) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT COUNT(*) FROM "{table}" '
                f'WHERE "{column}" IS NULL OR CAST("{column}" AS TEXT) = \'\'',
            )
            row = cur.fetchone()
            null_count = row[0] if row else 0
    null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0.0
    return {
        "null_count": null_count,
        "total_rows": total_rows,
        "null_pct": round(null_pct, 2),
    }


def compare_table_group(
    tables_info: dict[str, list[dict]], group_tables: list[str]
) -> dict:
    present_tables = [t for t in group_tables if t in tables_info]

    if len(present_tables) < 2:
        return {"error": "Menos de 2 tablas disponibles para comparación"}

    col_sets = {
        table: {c["name"] for c in tables_info[table] if c["name"] != "id"}
        for table in present_tables
    }

    all_sets = list(col_sets.values())
    common = reduce(set.intersection, all_sets)
    union_all = reduce(set.union, all_sets)

    unique_per_table = {
        table: sorted(
            col_sets[table]
            - reduce(set.union, (col_sets[t] for t in present_tables if t != table))
        )
        for table in present_tables
    }

    partial_detail = {
        col: present_in
        for col in sorted(union_all - common)
        if len(present_in := [t for t in present_tables if col in col_sets[t]])
        < len(present_tables)
    }

    return {
        "tables_compared": present_tables,
        "total_tables": len(present_tables),
        "common_columns": sorted(common),
        "common_count": len(common),
        "unique_per_table": unique_per_table,
        "partial_columns": partial_detail,
        "partial_count": len(partial_detail),
    }


def build_report(schema: str) -> dict:
    logger.info("Iniciando análisis EDA de columnas...")

    tables = get_all_tables(schema)
    logger.info("Tablas encontradas: %d — %s", len(tables), tables)

    tables_detail: dict[str, dict] = {}
    tables_info: dict[str, list[dict]] = {}

    for table in tables:
        logger.info("Analizando tabla: %s.%s", schema, table)
        columns = get_table_info(schema, table)
        tables_info[table] = columns
        row_count = _get_row_count(schema, table)

        col_details = [
            {
                "name": col["name"],
                "type": col["type"],
                "is_pk": col["pk"],
                "notnull": col["notnull"],
                "null_pct": (
                    ns := get_null_stats(schema, table, col["name"], row_count)
                )["null_pct"],
                "null_count": ns["null_count"],
                "sample_values": [
                    str(v) for v in get_sample_values(schema, table, col["name"])
                ],
            }
            for col in columns
        ]

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

    logger.info("Comparando grupos de tablas similares...")
    group_comparisons = {
        group_name: compare_table_group(tables_info, group_tables)
        for group_name, group_tables in TABLE_GROUPS.items()
    }
    for group_name, comparison in group_comparisons.items():
        if "common_count" in comparison:
            logger.info(
                "  Grupo '%s': %d comunes, %d parciales entre %d tablas",
                group_name,
                comparison["common_count"],
                comparison["partial_count"],
                comparison["total_tables"],
            )

    col_frequency: defaultdict[str, int] = defaultdict(int)
    for info in tables_detail.values():
        for col_name in info["column_names"]:
            if col_name != "id":
                col_frequency[col_name] += 1

    freq_sorted = sorted(col_frequency.items(), key=lambda x: -x[1])

    return {
        "metadata": {
            "database": f"PostgreSQL schema '{schema}'",
            "total_tables": len(tables),
            "total_unique_columns": len(col_frequency),
            "total_rows_all_tables": sum(
                t["row_count"] for t in tables_detail.values()
            ),
        },
        "tables": tables_detail,
        "group_comparisons": group_comparisons,
        "column_frequency": dict(freq_sorted),
    }


def generate_markdown(report: dict) -> str:
    lines: list[str] = []
    meta = report["metadata"]

    lines.append("# EDA — Análisis de Columnas (PostgreSQL)")
    lines.append("")
    lines.append("## Resumen General")
    lines.append("")
    lines.append("| Métrica | Valor |")
    lines.append("|---|---|")
    lines.append(f"| Base de datos | `{meta['database']}` |")
    lines.append(f"| Total tablas | {meta['total_tables']} |")
    lines.append(f"| Columnas únicas (sin `id`) | {meta['total_unique_columns']} |")
    lines.append(
        f"| Total filas (todas las tablas) | {meta['total_rows_all_tables']:,} |"
    )
    lines.append("")

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
            if len(samples) > 60:
                samples = samples[:57] + "..."
            pk_mark = "PK" if col["is_pk"] else ""
            lines.append(
                f"| {i} | `{col['name']}` | {col['type']} | {pk_mark} | {col['null_pct']:.1f}% | {samples} |"
            )

        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("## Comparación entre Grupos de Tablas")
    lines.append("")

    for group_name, comp in report["group_comparisons"].items():
        lines.append(f"### Grupo: `{group_name}`")
        lines.append("")

        if "error" in comp:
            lines.append(f"> {comp['error']}")
            lines.append("")
            continue

        tables_str = ", ".join(f"`{t}`" for t in comp["tables_compared"])
        lines.append(f"**Tablas comparadas ({comp['total_tables']}):** {tables_str}")
        lines.append("")

        lines.append(f"#### Columnas comunes a TODAS ({comp['common_count']})")
        lines.append("")
        if comp["common_columns"]:
            for col in comp["common_columns"]:
                lines.append(f"- `{col}`")
        else:
            lines.append("_Ninguna columna es común a todas las tablas._")
        lines.append("")

        lines.append("#### Columnas únicas por tabla")
        lines.append("")
        for table, unique_cols in comp["unique_per_table"].items():
            if unique_cols:
                cols_str = ", ".join(f"`{c}`" for c in unique_cols)
                lines.append(f"- **`{table}`** ({len(unique_cols)}): {cols_str}")
            else:
                lines.append(f"- **`{table}`**: _ninguna columna exclusiva_")
        lines.append("")

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


def main():
    logger.info("=" * 60)
    logger.info("EDA — Análisis de columnas (PostgreSQL, schema: %s)", ANALYSIS_SCHEMA)
    logger.info("=" * 60)

    tables = get_all_tables(ANALYSIS_SCHEMA)
    if not tables:
        logger.error(
            "No se encontraron tablas en el schema '%s'. "
            "Ejecuta primero: python scripts/create_db.py",
            ANALYSIS_SCHEMA,
        )
        sys.exit(1)

    logger.info("Schema '%s': %d tablas encontradas", ANALYSIS_SCHEMA, len(tables))

    report = build_report(ANALYSIS_SCHEMA)

    EDA_DIR.mkdir(parents=True, exist_ok=True)

    with open(REPORT_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    logger.info("Reporte JSON guardado: %s", REPORT_JSON_PATH)

    md_content = generate_markdown(report)
    with open(REPORT_MD_PATH, "w", encoding="utf-8") as f:
        f.write(md_content)
    logger.info("Reporte Markdown guardado: %s", REPORT_MD_PATH)

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

    logger.info("EDA completado correctamente.")


if __name__ == "__main__":
    main()
