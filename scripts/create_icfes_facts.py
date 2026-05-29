from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import PG_SCHEMA_FACTS, PG_SCHEMA_RAW
from utils.db import ensure_schemas, get_row_count, managed_connection, table_exists
from utils.logger import logger

NOW = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

RAW_SABER11 = "icfes_saber11_puntajes_trimestral"
RAW_SABERPRO = "icfes_saberpro_puntajes_trimestral"

DDL = [
    """
    CREATE TABLE IF NOT EXISTS dim_icfes_periodo (
        id                  SERIAL PRIMARY KEY,
        ano                 SMALLINT NOT NULL,
        trimestre           SMALLINT CHECK (trimestre IS NULL OR trimestre BETWEEN 1 AND 4),
        periodo_componente  SMALLINT NOT NULL,
        ano_periodo         TEXT NOT NULL,
        created_at          TEXT NOT NULL,
        updated_at          TEXT NOT NULL,
        UNIQUE (ano, periodo_componente)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_icfes_geografia (
        id                      SERIAL PRIMARY KEY,
        codigo_departamento     INTEGER,
        nombre_departamento     TEXT,
        codigo_municipio        INTEGER NOT NULL,
        nombre_municipio        TEXT NOT NULL,
        created_at              TEXT NOT NULL,
        updated_at              TEXT NOT NULL,
        UNIQUE (codigo_municipio)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_icfes_colegio (
        id                  SERIAL PRIMARY KEY,
        codigo_icfes        INTEGER NOT NULL,
        nombre_colegio      TEXT NOT NULL,
        naturaleza          TEXT,
        created_at          TEXT NOT NULL,
        updated_at          TEXT NOT NULL,
        UNIQUE (codigo_icfes)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_icfes_institucion (
        id                      SERIAL PRIMARY KEY,
        codigo_institucion      INTEGER NOT NULL,
        nombre_institucion      TEXT NOT NULL,
        tipo_institucion        TEXT,
        origen_institucion      TEXT,
        created_at              TEXT NOT NULL,
        updated_at              TEXT NOT NULL,
        UNIQUE (codigo_institucion)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_icfes_saber11 (
        id                          SERIAL PRIMARY KEY,
        periodo_id                  INTEGER NOT NULL REFERENCES dim_icfes_periodo(id),
        geografia_id                INTEGER NOT NULL REFERENCES dim_icfes_geografia(id),
        colegio_id                  INTEGER NOT NULL REFERENCES dim_icfes_colegio(id),
        observaciones               INTEGER NOT NULL DEFAULT 0,
        promedio_puntaje_global     NUMERIC(10,2),
        created_at                  TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_icfes_saberpro (
        id                                      SERIAL PRIMARY KEY,
        periodo_id                              INTEGER NOT NULL REFERENCES dim_icfes_periodo(id),
        geografia_id                            INTEGER NOT NULL REFERENCES dim_icfes_geografia(id),
        institucion_id                          INTEGER NOT NULL REFERENCES dim_icfes_institucion(id),
        observaciones                           INTEGER NOT NULL DEFAULT 0,
        promedio_razonamiento_cuantitativo      NUMERIC(10,2),
        promedio_comunicacion_escrita           NUMERIC(10,2),
        promedio_lectura_critica                NUMERIC(10,2),
        promedio_ingles                         NUMERIC(10,2),
        promedio_competencias_ciudadanas        NUMERIC(10,2),
        promedio_modulos                        NUMERIC(10,2),
        created_at                              TEXT NOT NULL
    )
    """,
]

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_dim_icfes_periodo_ano ON dim_icfes_periodo (ano, trimestre)",
    "CREATE INDEX IF NOT EXISTS idx_dim_icfes_geo_depto ON dim_icfes_geografia (codigo_departamento)",
    "CREATE INDEX IF NOT EXISTS idx_fact_icfes_s11_periodo ON fact_icfes_saber11 (periodo_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_icfes_s11_geo ON fact_icfes_saber11 (geografia_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_icfes_s11_colegio ON fact_icfes_saber11 (colegio_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_icfes_spro_periodo ON fact_icfes_saberpro (periodo_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_icfes_spro_geo ON fact_icfes_saberpro (geografia_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_icfes_spro_inst ON fact_icfes_saberpro (institucion_id)",
]


def _raw_ready() -> bool:
    return table_exists(PG_SCHEMA_RAW, RAW_SABER11) and table_exists(PG_SCHEMA_RAW, RAW_SABERPRO)


def _prepare_schema() -> None:
    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            for ddl in DDL:
                cur.execute(ddl)
            for table in [
                "fact_icfes_saber11",
                "fact_icfes_saberpro",
                "dim_icfes_colegio",
                "dim_icfes_institucion",
                "dim_icfes_geografia",
                "dim_icfes_periodo",
            ]:
                cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")


def _load_dimensions() -> None:
    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO dim_icfes_periodo
                    (ano, trimestre, periodo_componente, ano_periodo, created_at, updated_at)
                SELECT DISTINCT
                    ano,
                    trimestre,
                    periodo_componente,
                    ano::TEXT || '-' || periodo_componente::TEXT,
                    %s,
                    %s
                FROM (
                    SELECT ano, trimestre, periodo_componente FROM {PG_SCHEMA_RAW}.{RAW_SABER11}
                    UNION
                    SELECT ano, trimestre, periodo_componente FROM {PG_SCHEMA_RAW}.{RAW_SABERPRO}
                ) s
                WHERE ano IS NOT NULL AND periodo_componente IS NOT NULL
                ORDER BY ano, periodo_componente
                """,
                (NOW, NOW),
            )

            cur.execute(
                f"""
                INSERT INTO dim_icfes_geografia
                    (codigo_departamento, nombre_departamento, codigo_municipio, nombre_municipio, created_at, updated_at)
                SELECT DISTINCT ON (COALESCE(municipio_codigo, -1))
                    departamento_codigo,
                    COALESCE(departamento, 'desconocido'),
                    COALESCE(municipio_codigo, -1),
                    COALESCE(municipio, 'desconocido'),
                    %s,
                    %s
                FROM (
                    SELECT departamento_codigo, departamento, municipio_codigo, municipio
                    FROM {PG_SCHEMA_RAW}.{RAW_SABER11}
                    UNION ALL
                    SELECT NULL::INTEGER, departamento, municipio_codigo, municipio
                    FROM {PG_SCHEMA_RAW}.{RAW_SABERPRO}
                ) s
                ORDER BY COALESCE(municipio_codigo, -1), departamento_codigo NULLS LAST
                """,
                (NOW, NOW),
            )

            cur.execute(
                f"""
                INSERT INTO dim_icfes_colegio
                    (codigo_icfes, nombre_colegio, naturaleza, created_at, updated_at)
                SELECT DISTINCT ON (COALESCE(colegio_icfes_codigo, -1))
                    COALESCE(colegio_icfes_codigo, -1),
                    COALESCE(colegio, 'desconocido'),
                    COALESCE(colegio_naturaleza, 'desconocido'),
                    %s,
                    %s
                FROM {PG_SCHEMA_RAW}.{RAW_SABER11}
                ORDER BY COALESCE(colegio_icfes_codigo, -1), observaciones DESC
                """,
                (NOW, NOW),
            )

            cur.execute(
                f"""
                INSERT INTO dim_icfes_institucion
                    (codigo_institucion, nombre_institucion, tipo_institucion, origen_institucion, created_at, updated_at)
                SELECT DISTINCT ON (institucion_codigo)
                    institucion_codigo,
                    institucion,
                    tipo_institucion,
                    origen_institucion,
                    %s,
                    %s
                FROM {PG_SCHEMA_RAW}.{RAW_SABERPRO}
                WHERE institucion_codigo IS NOT NULL AND institucion IS NOT NULL
                ORDER BY institucion_codigo, observaciones DESC
                """,
                (NOW, NOW),
            )


def _load_facts() -> None:
    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO fact_icfes_saber11
                    (periodo_id, geografia_id, colegio_id, observaciones, promedio_puntaje_global, created_at)
                SELECT
                    dp.id,
                    dg.id,
                    dc.id,
                    r.observaciones,
                    r.promedio_puntaje_global,
                    %s
                FROM {PG_SCHEMA_RAW}.{RAW_SABER11} r
                JOIN dim_icfes_periodo dp
                  ON dp.ano = r.ano AND dp.periodo_componente = r.periodo_componente
                JOIN dim_icfes_geografia dg ON dg.codigo_municipio = COALESCE(r.municipio_codigo, -1)
                JOIN dim_icfes_colegio dc ON dc.codigo_icfes = COALESCE(r.colegio_icfes_codigo, -1)
                """,
                (NOW,),
            )

            cur.execute(
                f"""
                INSERT INTO fact_icfes_saberpro
                    (
                        periodo_id,
                        geografia_id,
                        institucion_id,
                        observaciones,
                        promedio_razonamiento_cuantitativo,
                        promedio_comunicacion_escrita,
                        promedio_lectura_critica,
                        promedio_ingles,
                        promedio_competencias_ciudadanas,
                        promedio_modulos,
                        created_at
                    )
                SELECT
                    dp.id,
                    dg.id,
                    di.id,
                    r.observaciones,
                    r.promedio_razonamiento_cuantitativo,
                    r.promedio_comunicacion_escrita,
                    r.promedio_lectura_critica,
                    r.promedio_ingles,
                    r.promedio_competencias_ciudadanas,
                    r.promedio_modulos,
                    %s
                FROM {PG_SCHEMA_RAW}.{RAW_SABERPRO} r
                JOIN dim_icfes_periodo dp
                  ON dp.ano = r.ano AND dp.periodo_componente = r.periodo_componente
                JOIN dim_icfes_geografia dg ON dg.codigo_municipio = COALESCE(r.municipio_codigo, -1)
                JOIN dim_icfes_institucion di ON di.codigo_institucion = r.institucion_codigo
                """,
                (NOW,),
            )

            for idx in INDEXES:
                cur.execute(idx)


def main() -> None:
    logger.info("=" * 60)
    logger.info("CREACION DE FACTS ICFES")
    logger.info("Fuente: PostgreSQL schema '%s'", PG_SCHEMA_RAW)
    logger.info("Destino: PostgreSQL schema '%s'", PG_SCHEMA_FACTS)
    logger.info("=" * 60)
    start = time.time()
    ensure_schemas()

    if not _raw_ready():
        logger.warning("Tablas raw de ICFES no encontradas; omitiendo facts ICFES")
        return

    _prepare_schema()
    _load_dimensions()
    _load_facts()

    for table in [
        "dim_icfes_periodo",
        "dim_icfes_geografia",
        "dim_icfes_colegio",
        "dim_icfes_institucion",
        "fact_icfes_saber11",
        "fact_icfes_saberpro",
    ]:
        logger.info("  %-30s %8d filas", table, get_row_count(PG_SCHEMA_FACTS, table))

    logger.info("Facts ICFES listas en %.1f segundos", time.time() - start)


if __name__ == "__main__":
    main()
