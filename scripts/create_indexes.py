from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import PG_SCHEMA_RAW, PG_SCHEMA_UNIFIED
from utils.db import (
    get_column_names,
    list_tables,
    managed_connection,
)
from utils.logger import logger


@dataclass(frozen=True)
class IndexDef:
    name: str
    table: str
    columns: list[str]
    unique: bool = False
    where: str | None = None

    def sql(self, schema: str) -> str:
        unique = "UNIQUE " if self.unique else ""
        cols = ", ".join(f'"{c}"' for c in self.columns)
        where = f" WHERE {self.where}" if self.where else ""
        return (
            f'CREATE {unique}INDEX IF NOT EXISTS "{self.name}" '
            f'ON "{schema}"."{self.table}" ({cols}){where}'
        )


STUDENT_TABLES = [
    "admitidos",
    "graduados",
    "inscritos",
    "matriculados",
    "matriculados_primer_curso",
]

STUDENT_INDEX_COLUMNS: list[tuple[str, list[str], bool, str | None]] = [
    ("ano", ["ano"], False, None),
    ("ano_semestre", ["ano", "semestre"], False, None),
    ("codigo_ies", ["codigo_de_la_institucion"], False, None),
    ("codigo_snies", ["codigo_snies_del_programa"], False, None),
    ("mun_ies", ["codigo_del_municipio_ies"], False, None),
    ("mun_prog", ["codigo_del_municipio_programa"], False, None),
    ("id_sexo", ["id_sexo"], False, None),
    ("id_nivel_acad", ["id_nivel_academico"], False, None),
    ("id_area", ["id_area"], False, None),
    ("id_metodologia", ["id_metodologia"], False, None),
    ("id_nucleo", ["id_nucleo"], False, None),
    ("id_sector", ["id_sector_ies"], False, None),
    ("id_caracter", ["id_caracter"], False, None),
    (
        "fact_lookup",
        ["codigo_de_la_institucion", "codigo_snies_del_programa", "ano", "semestre"],
        False,
        None,
    ),
]

DOCENTES_INDEX_DEFS: list[tuple[str, list[str], bool, str | None]] = [
    ("ano", ["ano"], False, None),
    ("ano_semestre", ["ano", "semestre"], False, None),
    ("codigo_ies", ["codigo_de_la_institucion"], False, None),
    ("mun_ies", ["codigo_del_municipio_ies"], False, None),
    ("id_sexo", ["id_sexo"], False, None),
    ("id_nivel_form", ["id_maximo_nivel_de_formacion_del_docente"], False, None),
    ("id_dedicacion", ["id_tiempo_de_dedicacion"], False, None),
    ("id_contrato", ["id_tipo_de_contrato"], False, None),
    (
        "fact_lookup",
        ["codigo_de_la_institucion", "id_sexo", "ano", "semestre"],
        False,
        None,
    ),
    (
        "dedicacion_contrato",
        ["id_tiempo_de_dedicacion", "id_tipo_de_contrato"],
        False,
        None,
    ),
]

ADMIN_INDEX_DEFS: list[tuple[str, list[str], bool, str | None]] = [
    ("ano", ["ano"], False, None),
    ("ano_semestre", ["ano", "semestre"], False, None),
    ("codigo_ies", ["codigo_de_la_institucion"], False, None),
    ("mun_ies", ["codigo_del_municipio_ies"], False, None),
    (
        "fact_lookup",
        ["codigo_de_la_institucion", "ano", "semestre"],
        False,
        None,
    ),
]


def _build_index_defs(
    table: str,
    index_specs: list[tuple[str, list[str], bool, str | None]],
) -> list[IndexDef]:
    defs = []
    for suffix, columns, unique, where in index_specs:
        name = f"idx_{table}_{suffix}"
        defs.append(
            IndexDef(
                name=name,
                table=table,
                columns=columns,
                unique=unique,
                where=where,
            )
        )
    return defs


def build_all_index_defs() -> list[IndexDef]:
    all_defs: list[IndexDef] = []

    for table in STUDENT_TABLES:
        all_defs.extend(_build_index_defs(table, STUDENT_INDEX_COLUMNS))

    all_defs.extend(_build_index_defs("docentes", DOCENTES_INDEX_DEFS))
    all_defs.extend(_build_index_defs("administrativos", ADMIN_INDEX_DEFS))

    return all_defs


def build_unified_index_defs() -> list[IndexDef]:
    all_defs: list[IndexDef] = []

    for table in STUDENT_TABLES:
        unified_table = f"{table}_unified"
        all_defs.extend(
            [
                IndexDef(
                    name=f"idx_{unified_table}_{suffix}",
                    table=unified_table,
                    columns=columns,
                    unique=unique,
                    where=where,
                )
                for suffix, columns, unique, where in STUDENT_INDEX_COLUMNS
            ]
        )

    all_defs.extend(
        [
            IndexDef(
                name=f"idx_docentes_unified_{suffix}",
                table="docentes_unified",
                columns=columns,
                unique=unique,
                where=where,
            )
            for suffix, columns, unique, where in DOCENTES_INDEX_DEFS
        ]
    )

    all_defs.extend(
        [
            IndexDef(
                name=f"idx_administrativos_unified_{suffix}",
                table="administrativos_unified",
                columns=columns,
                unique=unique,
                where=where,
            )
            for suffix, columns, unique, where in ADMIN_INDEX_DEFS
        ]
    )

    return all_defs


def _get_existing_tables(schema: str) -> set[str]:
    return set(list_tables(schema))


def _get_existing_columns(schema: str, table: str) -> set[str]:
    return set(get_column_names(schema, table))


def _get_existing_index_names(schema: str) -> set[str]:
    query = (
        "SELECT i.relname "
        "FROM pg_index ix "
        "JOIN pg_class i ON i.oid = ix.indexrelid "
        "JOIN pg_class t ON t.oid = ix.indrelid "
        "JOIN pg_namespace n ON n.oid = t.relnamespace "
        "WHERE n.nspname = %s AND NOT ix.indisprimary"
    )
    try:
        with managed_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (schema,))
                return {row[0] for row in cur.fetchall()}
    except Exception as e:
        logger.warning("[%s] Error obteniendo índices existentes: %s", schema, e)
        return set()


def create_indexes_for_schema(
    schema: str,
    index_defs: list[IndexDef],
) -> dict[str, int]:
    stats = {
        "created": 0,
        "skipped_table": 0,
        "skipped_column": 0,
        "already_exists": 0,
        "errors": 0,
    }

    existing_tables = _get_existing_tables(schema)
    existing_indexes = _get_existing_index_names(schema)
    column_cache: dict[str, set[str]] = {}

    for idx_def in index_defs:
        if idx_def.table not in existing_tables:
            logger.debug("[%s] SKIP tabla '%s' no existe", schema, idx_def.table)
            stats["skipped_table"] += 1
            continue

        if idx_def.name in existing_indexes:
            logger.debug("[%s] Índice ya existe: %s", schema, idx_def.name)
            stats["already_exists"] += 1
            continue

        if idx_def.table not in column_cache:
            column_cache[idx_def.table] = _get_existing_columns(schema, idx_def.table)

        table_cols = column_cache[idx_def.table]
        missing_cols = [c for c in idx_def.columns if c not in table_cols]

        if missing_cols:
            logger.debug(
                "[%s] SKIP índice '%s' — columnas faltantes: %s",
                schema,
                idx_def.name,
                missing_cols,
            )
            stats["skipped_column"] += 1
            continue

        sql = idx_def.sql(schema)
        try:
            with managed_connection(schema=schema, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
            stats["created"] += 1
            logger.debug("[%s] Índice creado: %s", schema, idx_def.name)
        except Exception as exc:
            error_msg = str(exc).strip()
            if "already exists" in error_msg.lower():
                stats["already_exists"] += 1
                logger.debug("[%s] Índice ya existe: %s", schema, idx_def.name)
            else:
                stats["errors"] += 1
                logger.warning(
                    "[%s] Error creando índice '%s': %s",
                    schema,
                    idx_def.name,
                    error_msg,
                )

    return stats


def run_analyze(schema: str) -> None:
    tables = list_tables(schema)
    if not tables:
        return

    logger.info("[%s] Ejecutando ANALYZE en %d tablas...", schema, len(tables))
    try:
        with managed_connection(schema=schema) as conn:
            with conn.cursor() as cur:
                for table in tables:
                    cur.execute(f'ANALYZE "{schema}"."{table}"')
        logger.info("[%s] ANALYZE completado", schema)
    except Exception as e:
        logger.warning("[%s] Error durante ANALYZE: %s", schema, e)


def create_indexes(target: str = "all") -> None:
    logger.info("=" * 60)
    logger.info("CREACIÓN DE ÍNDICES PARA OPTIMIZACIÓN DE QUERIES")
    logger.info("=" * 60)

    start_time = time.time()
    total_stats: dict[str, int] = {
        "created": 0,
        "skipped_table": 0,
        "skipped_column": 0,
        "already_exists": 0,
        "errors": 0,
    }

    if target in ("raw", "all"):
        logger.info("")
        logger.info("─" * 40)
        logger.info("Schema: %s", PG_SCHEMA_RAW)
        logger.info("─" * 40)

        raw_defs = build_all_index_defs()
        logger.info(
            "[%s] %d índices definidos para %d tablas",
            PG_SCHEMA_RAW,
            len(raw_defs),
            len({d.table for d in raw_defs}),
        )

        stats = create_indexes_for_schema(PG_SCHEMA_RAW, raw_defs)
        _log_stats(PG_SCHEMA_RAW, stats)
        _merge_stats(total_stats, stats)

        run_analyze(PG_SCHEMA_RAW)

    if target in ("unified", "all"):
        logger.info("")
        logger.info("─" * 40)
        logger.info("Schema: %s", PG_SCHEMA_UNIFIED)
        logger.info("─" * 40)

        unified_defs = build_unified_index_defs()
        logger.info(
            "[%s] %d índices definidos para %d tablas",
            PG_SCHEMA_UNIFIED,
            len(unified_defs),
            len({d.table for d in unified_defs}),
        )

        stats = create_indexes_for_schema(PG_SCHEMA_UNIFIED, unified_defs)
        _log_stats(PG_SCHEMA_UNIFIED, stats)
        _merge_stats(total_stats, stats)

        run_analyze(PG_SCHEMA_UNIFIED)

    elapsed = time.time() - start_time

    logger.info("")
    logger.info("=" * 60)
    logger.info("RESUMEN DE ÍNDICES")
    logger.info("=" * 60)
    logger.info("  Creados:                %d", total_stats["created"])
    logger.info("  Ya existían:            %d", total_stats["already_exists"])
    logger.info("  Saltados (tabla):       %d", total_stats["skipped_table"])
    logger.info("  Saltados (columna):     %d", total_stats["skipped_column"])
    logger.info("  Errores:                %d", total_stats["errors"])
    logger.info("  Tiempo total:           %.1f s", elapsed)
    logger.info("=" * 60)


def _log_stats(schema: str, stats: dict[str, int]) -> None:
    logger.info(
        "[%s] Resultado: %d creados, %d ya existían, %d saltados, %d errores",
        schema,
        stats["created"],
        stats["already_exists"],
        stats["skipped_table"] + stats["skipped_column"],
        stats["errors"],
    )


def _merge_stats(total: dict[str, int], partial: dict[str, int]) -> None:
    for key in total:
        total[key] += partial.get(key, 0)


def list_indexes(schema: str) -> list[dict]:
    query = """
        SELECT
            i.relname AS index_name,
            t.relname AS table_name,
            pg_size_pretty(pg_relation_size(i.oid)) AS index_size,
            ix.indisunique AS is_unique,
            array_to_string(
                array_agg(a.attname ORDER BY k.ordinality), ', '
            ) AS columns
        FROM pg_index ix
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        CROSS JOIN LATERAL unnest(ix.indkey)
            WITH ORDINALITY AS k(attnum, ordinality)
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
        WHERE n.nspname = %s
          AND NOT ix.indisprimary
        GROUP BY i.relname, t.relname, i.oid, ix.indisunique
        ORDER BY t.relname, i.relname
    """
    try:
        with managed_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (schema,))
                cols = [desc[0] for desc in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        logger.warning("[%s] Error listando índices: %s", schema, e)
        return []


def print_index_report(schema: str) -> None:
    indexes = list_indexes(schema)
    if not indexes:
        logger.info("[%s] Sin índices (excluyendo PKs)", schema)
        return

    logger.info("[%s] %d índices encontrados:", schema, len(indexes))
    for idx in indexes:
        unique_str = "UNIQUE " if idx["is_unique"] else ""
        logger.info(
            "  %-50s %-30s %s(%s) — %s",
            idx["index_name"],
            idx["table_name"],
            unique_str,
            idx["columns"],
            idx["index_size"],
        )


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Crea índices en PostgreSQL para optimizar el pipeline ETL."
    )
    parser.add_argument(
        "--schema",
        choices=["raw", "unified", "all"],
        default="all",
        help="Schema donde crear índices (default: all).",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Solo mostrar índices existentes (no crear nuevos).",
    )
    args = parser.parse_args()

    if args.report:
        logger.info("=" * 60)
        logger.info("REPORTE DE ÍNDICES EXISTENTES")
        logger.info("=" * 60)
        if args.schema in ("raw", "all"):
            print_index_report(PG_SCHEMA_RAW)
        if args.schema in ("unified", "all"):
            print_index_report(PG_SCHEMA_UNIFIED)
        return

    create_indexes(target=args.schema)


if __name__ == "__main__":
    main()
