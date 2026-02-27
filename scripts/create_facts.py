from __future__ import annotations

import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import SQLITE_UNIFIED_DB_PATH, SQLITE_FACTS_DB_PATH, DATA_DIR
from utils.logger import logger

NOW = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


COLUMN_ALIASES = {
    "codigo_del_municipio_programa": [
        "cdigo_del_municipio_programa",
        "codigo_del_municipio",
    ],
    "municipio_de_oferta_del_programa": [
        "municipio_de_domicilio_de_la_ies",
    ],
}


def get_column_name(cols_info: set, preferred: str) -> str | None:
    if preferred in cols_info:
        return preferred
    for alias in COLUMN_ALIASES.get(preferred, []):
        if alias in cols_info:
            return alias
    return None


STUDENT_CATEGORIES: dict[str, dict] = {
    "inscritos": {
        "tipo_evento": "inscritos",
        "metric_cols": ["inscritos", "inscripciones_2018"],
    },
    "admitidos": {
        "tipo_evento": "admitidos",
        "metric_cols": ["admitidos", "admisiones_2018"],
    },
    "matriculados": {
        "tipo_evento": "matriculados",
        "metric_cols": ["matriculados", "matriculados_2018"],
    },
    "matriculados_primer_curso": {
        "tipo_evento": "primer_curso",
        "metric_cols": [
            "matriculados_primer_curso",
            "primer_curso",
            "primer_curso_2018",
            "primer_curso_2019",
        ],
    },
    "graduados": {
        "tipo_evento": "graduados",
        "metric_cols": ["graduados"],
    },
}

# Columnas fuente → columna destino para municipio IES
# (algunas tablas usan 'codigo_del_municipio_ies', otras 'codigo_del_municipio')
MUNICIPIO_IES_COLS = ["codigo_del_municipio_ies", "codigo_del_municipio"]


# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────
def safe_int(value: object) -> int | None:
    """Convierte un valor a int de forma segura."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError, OverflowError):
        return None


def coalesce_column(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    """Retorna la primera columna no-nula de una lista de candidatos."""
    result = pd.Series([None] * len(df), dtype=object)
    for col in reversed(candidates):
        if col in df.columns:
            mask = df[col].notna()
            result[mask] = df.loc[mask, col]
    return result


# ──────────────────────────────────────────────────────────────
# DDL — CREACIÓN DE TABLAS DE DIMENSIONES Y HECHOS
# ──────────────────────────────────────────────────────────────
DDL_DIMENSIONS = """
-- ═══════════════════════════════════════════════════════════
-- DIMENSIONES
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dim_institucion (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    codigo_ies              INTEGER NOT NULL,
    codigo_ies_padre        INTEGER,
    nombre_ies              TEXT    NOT NULL,
    principal_o_seccional   TEXT,
    id_sector_ies           INTEGER,
    sector_ies              TEXT,
    id_caracter             INTEGER,
    caracter_ies            TEXT,
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_institucion_codigo
    ON dim_institucion (codigo_ies);

CREATE TABLE IF NOT EXISTS dim_geografia (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    codigo_departamento     INTEGER NOT NULL,
    nombre_departamento     TEXT    NOT NULL,
    codigo_municipio        INTEGER NOT NULL,
    nombre_municipio        TEXT    NOT NULL,
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_geografia_municipio
    ON dim_geografia (codigo_municipio);

CREATE TABLE IF NOT EXISTS dim_programa (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    codigo_snies_programa   INTEGER NOT NULL,
    nombre_programa         TEXT    NOT NULL,
    id_nivel_academico      INTEGER,
    nivel_academico         TEXT,
    id_nivel_formacion      INTEGER,
    nivel_formacion         TEXT,
    id_metodologia          INTEGER,
    metodologia             TEXT,
    id_area                 INTEGER,
    area_conocimiento       TEXT,
    id_nucleo               INTEGER,
    nucleo_basico           TEXT,
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_programa_snies
    ON dim_programa (codigo_snies_programa);

CREATE TABLE IF NOT EXISTS dim_tiempo (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    ano                     INTEGER NOT NULL,
    semestre                INTEGER NOT NULL CHECK (semestre IN (1, 2)),
    ano_semestre            TEXT    NOT NULL,
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_tiempo_periodo
    ON dim_tiempo (ano, semestre);

CREATE TABLE IF NOT EXISTS dim_sexo (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    id_sexo                 INTEGER NOT NULL,
    sexo                    TEXT    NOT NULL,
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_sexo
    ON dim_sexo (id_sexo);

CREATE TABLE IF NOT EXISTS dim_nivel_formacion_docente (
    id                              INTEGER PRIMARY KEY AUTOINCREMENT,
    id_nivel_formacion_docente      INTEGER NOT NULL,
    nivel_formacion_docente         TEXT    NOT NULL,
    created_at                      TEXT NOT NULL,
    updated_at                      TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_nivel_form_doc
    ON dim_nivel_formacion_docente (id_nivel_formacion_docente);

CREATE TABLE IF NOT EXISTS dim_dedicacion_docente (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    id_tiempo_dedicacion    INTEGER NOT NULL,
    tiempo_dedicacion       TEXT    NOT NULL,
    id_tipo_contrato        INTEGER NOT NULL,
    tipo_contrato           TEXT    NOT NULL,
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_dedicacion
    ON dim_dedicacion_docente (id_tiempo_dedicacion, id_tipo_contrato);
"""

DDL_FACTS = """
-- ═══════════════════════════════════════════════════════════
-- TABLAS DE HECHOS
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS fact_estudiantes (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo_evento             TEXT    NOT NULL
        CHECK (tipo_evento IN (
            'inscritos', 'admitidos', 'matriculados',
            'primer_curso', 'graduados'
        )),
    institucion_id          INTEGER NOT NULL REFERENCES dim_institucion(id),
    programa_id             INTEGER NOT NULL REFERENCES dim_programa(id),
    geografia_ies_id        INTEGER NOT NULL REFERENCES dim_geografia(id),
    geografia_programa_id   INTEGER NOT NULL REFERENCES dim_geografia(id),
    sexo_id                 INTEGER NOT NULL REFERENCES dim_sexo(id),
    tiempo_id               INTEGER NOT NULL REFERENCES dim_tiempo(id),
    cantidad                INTEGER NOT NULL DEFAULT 0,
    created_at              TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_est_institucion   ON fact_estudiantes (institucion_id);
CREATE INDEX IF NOT EXISTS idx_fact_est_programa      ON fact_estudiantes (programa_id);
CREATE INDEX IF NOT EXISTS idx_fact_est_geo_ies       ON fact_estudiantes (geografia_ies_id);
CREATE INDEX IF NOT EXISTS idx_fact_est_geo_prog      ON fact_estudiantes (geografia_programa_id);
CREATE INDEX IF NOT EXISTS idx_fact_est_sexo          ON fact_estudiantes (sexo_id);
CREATE INDEX IF NOT EXISTS idx_fact_est_tiempo        ON fact_estudiantes (tiempo_id);
CREATE INDEX IF NOT EXISTS idx_fact_est_tipo_tiempo   ON fact_estudiantes (tipo_evento, tiempo_id);
CREATE INDEX IF NOT EXISTS idx_fact_est_tipo_inst_tiempo
    ON fact_estudiantes (tipo_evento, institucion_id, tiempo_id);

CREATE TABLE IF NOT EXISTS fact_docentes (
    id                              INTEGER PRIMARY KEY AUTOINCREMENT,
    institucion_id                  INTEGER NOT NULL REFERENCES dim_institucion(id),
    geografia_ies_id                INTEGER NOT NULL REFERENCES dim_geografia(id),
    sexo_id                         INTEGER NOT NULL REFERENCES dim_sexo(id),
    nivel_formacion_docente_id      INTEGER NOT NULL REFERENCES dim_nivel_formacion_docente(id),
    dedicacion_docente_id           INTEGER NOT NULL REFERENCES dim_dedicacion_docente(id),
    tiempo_id                       INTEGER NOT NULL REFERENCES dim_tiempo(id),
    cantidad_docentes               INTEGER NOT NULL DEFAULT 0,
    created_at                      TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_doc_institucion   ON fact_docentes (institucion_id);
CREATE INDEX IF NOT EXISTS idx_fact_doc_geo_ies       ON fact_docentes (geografia_ies_id);
CREATE INDEX IF NOT EXISTS idx_fact_doc_sexo          ON fact_docentes (sexo_id);
CREATE INDEX IF NOT EXISTS idx_fact_doc_nivel         ON fact_docentes (nivel_formacion_docente_id);
CREATE INDEX IF NOT EXISTS idx_fact_doc_dedicacion    ON fact_docentes (dedicacion_docente_id);
CREATE INDEX IF NOT EXISTS idx_fact_doc_tiempo        ON fact_docentes (tiempo_id);
CREATE INDEX IF NOT EXISTS idx_fact_doc_inst_tiempo   ON fact_docentes (institucion_id, tiempo_id);

CREATE TABLE IF NOT EXISTS fact_administrativos (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    institucion_id          INTEGER NOT NULL REFERENCES dim_institucion(id),
    geografia_ies_id        INTEGER NOT NULL REFERENCES dim_geografia(id),
    tiempo_id               INTEGER NOT NULL REFERENCES dim_tiempo(id),
    auxiliar                INTEGER NOT NULL DEFAULT 0,
    tecnico                 INTEGER NOT NULL DEFAULT 0,
    profesional             INTEGER NOT NULL DEFAULT 0,
    directivo               INTEGER NOT NULL DEFAULT 0,
    total                   INTEGER NOT NULL DEFAULT 0,
    created_at              TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_adm_institucion   ON fact_administrativos (institucion_id);
CREATE INDEX IF NOT EXISTS idx_fact_adm_geo_ies       ON fact_administrativos (geografia_ies_id);
CREATE INDEX IF NOT EXISTS idx_fact_adm_tiempo        ON fact_administrativos (tiempo_id);
CREATE INDEX IF NOT EXISTS idx_fact_adm_inst_tiempo   ON fact_administrativos (institucion_id, tiempo_id);
"""


# ──────────────────────────────────────────────────────────────
# FUNCIONES DE CARGA DE DIMENSIONES
# ──────────────────────────────────────────────────────────────
def load_dim_institucion(conn: sqlite3.Connection) -> dict[int, int]:
    """
    Extrae instituciones únicas de TODAS las tablas _unified y las
    inserta en dim_institucion. Retorna mapping: codigo_ies → id.
    """
    logger.info("[dim_institucion] Extrayendo instituciones únicas...")

    tables = [
        "matriculados_unified",
        "inscritos_unified",
        "admitidos_unified",
        "graduados_unified",
        "matriculados_primer_curso_unified",
        "docentes_unified",
        "administrativos_unified",
    ]

    all_inst: dict[int, dict] = {}

    for table in tables:
        # id_caracter puede llamarse id_caracter o id_caracter_ies
        query = f"""
            SELECT DISTINCT
                codigo_de_la_institucion,
                ies_padre,
                institucion_de_educacion_superior_ies,
                principal_o_seccional,
                id_sector_ies,
                sector_ies
            FROM "{table}"
            WHERE codigo_de_la_institucion IS NOT NULL
        """
        rows = conn.execute(query).fetchall()

        # Intentar obtener id_caracter y caracter_ies
        cols_info = {
            row[1] for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()
        }
        has_id_caracter = "id_caracter" in cols_info
        has_id_caracter_ies = "id_caracter_ies" in cols_info
        has_caracter_ies = "caracter_ies" in cols_info

        caracter_query = f"""
            SELECT DISTINCT
                codigo_de_la_institucion,
                {"id_caracter" if has_id_caracter else ("id_caracter_ies" if has_id_caracter_ies else "NULL")} as id_caracter,
                {"caracter_ies" if has_caracter_ies else "NULL"} as caracter_ies
            FROM "{table}"
            WHERE codigo_de_la_institucion IS NOT NULL
        """
        caracter_rows = {
            safe_int(r[0]): (safe_int(r[1]), r[2])
            for r in conn.execute(caracter_query).fetchall()
            if safe_int(r[0]) is not None
        }

        for row in rows:
            codigo = safe_int(row[0])
            if codigo is None:
                continue
            if codigo not in all_inst:
                car = caracter_rows.get(codigo, (None, None))
                all_inst[codigo] = {
                    "codigo_ies": codigo,
                    "codigo_ies_padre": safe_int(row[1]),
                    "nombre_ies": str(row[2]).strip() if row[2] else "DESCONOCIDA",
                    "principal_o_seccional": str(row[3]).strip() if row[3] else None,
                    "id_sector_ies": safe_int(row[4]),
                    "sector_ies": str(row[5]).strip() if row[5] else None,
                    "id_caracter": car[0],
                    "caracter_ies": str(car[1]).strip() if car[1] else None,
                }

    # Insertar en dim_institucion
    count = 0
    for inst in all_inst.values():
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO dim_institucion
                    (codigo_ies, codigo_ies_padre, nombre_ies, principal_o_seccional,
                     id_sector_ies, sector_ies, id_caracter, caracter_ies,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    inst["codigo_ies"],
                    inst["codigo_ies_padre"],
                    inst["nombre_ies"],
                    inst["principal_o_seccional"],
                    inst["id_sector_ies"],
                    inst["sector_ies"],
                    inst["id_caracter"],
                    inst["caracter_ies"],
                    NOW,
                    NOW,
                ),
            )
            count += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()

    # Construir mapping
    mapping = {
        row[0]: row[1]
        for row in conn.execute("SELECT codigo_ies, id FROM dim_institucion").fetchall()
    }
    logger.info(
        "[dim_institucion] %d instituciones insertadas (%d en mapping)",
        count,
        len(mapping),
    )
    return mapping


def load_dim_geografia(conn: sqlite3.Connection) -> dict[int, int]:
    """
    Extrae combinaciones departamento/municipio únicas y las inserta
    en dim_geografia. Retorna mapping: codigo_municipio → id.
    """
    logger.info("[dim_geografia] Extrayendo geografías únicas...")

    # Fuentes: municipio IES + municipio programa de todas las tablas
    geo_data: dict[int, dict] = {}

    # Tablas de estudiantes: tienen municipio IES y municipio programa
    student_tables = [
        "matriculados_unified",
        "inscritos_unified",
        "admitidos_unified",
        "graduados_unified",
        "matriculados_primer_curso_unified",
    ]

    for table in student_tables:
        cols_info = {
            row[1] for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()
        }

        # Municipio IES
        mun_ies_col = (
            "codigo_del_municipio_ies"
            if "codigo_del_municipio_ies" in cols_info
            else (
                "codigo_del_municipio" if "codigo_del_municipio" in cols_info else None
            )
        )

        if mun_ies_col:
            rows = conn.execute(
                f"""
                SELECT DISTINCT
                    codigo_del_departamento_ies,
                    departamento_de_domicilio_de_la_ies,
                    {mun_ies_col},
                    municipio_de_domicilio_de_la_ies
                FROM "{table}"
                WHERE {mun_ies_col} IS NOT NULL
                """
            ).fetchall()
            for r in rows:
                cod_mun = safe_int(r[2])
                if cod_mun is not None and cod_mun not in geo_data:
                    geo_data[cod_mun] = {
                        "codigo_departamento": safe_int(r[0]) or 0,
                        "nombre_departamento": str(r[1]).strip()
                        if r[1]
                        else "DESCONOCIDO",
                        "codigo_municipio": cod_mun,
                        "nombre_municipio": str(r[3]).strip()
                        if r[3]
                        else "DESCONOCIDO",
                    }

        # Municipio programa
        if "codigo_del_municipio_programa" in cols_info:
            rows = conn.execute(
                f"""
                SELECT DISTINCT
                    codigo_del_departamento_programa,
                    departamento_de_oferta_del_programa,
                    codigo_del_municipio_programa,
                    municipio_de_oferta_del_programa
                FROM "{table}"
                WHERE codigo_del_municipio_programa IS NOT NULL
                """
            ).fetchall()
            for r in rows:
                cod_mun = safe_int(r[2])
                if cod_mun is not None and cod_mun not in geo_data:
                    geo_data[cod_mun] = {
                        "codigo_departamento": safe_int(r[0]) or 0,
                        "nombre_departamento": str(r[1]).strip()
                        if r[1]
                        else "DESCONOCIDO",
                        "codigo_municipio": cod_mun,
                        "nombre_municipio": str(r[3]).strip()
                        if r[3]
                        else "DESCONOCIDO",
                    }

    # Docentes y administrativos: solo tienen municipio IES
    for table in ["docentes_unified", "administrativos_unified"]:
        cols_info = {
            row[1] for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()
        }
        mun_col = (
            "codigo_del_municipio_ies"
            if "codigo_del_municipio_ies" in cols_info
            else (
                "codigo_del_municipio" if "codigo_del_municipio" in cols_info else None
            )
        )
        if mun_col:
            rows = conn.execute(
                f"""
                SELECT DISTINCT
                    codigo_del_departamento_ies,
                    departamento_de_domicilio_de_la_ies,
                    {mun_col},
                    municipio_de_domicilio_de_la_ies
                FROM "{table}"
                WHERE {mun_col} IS NOT NULL
                """
            ).fetchall()
            for r in rows:
                cod_mun = safe_int(r[2])
                if cod_mun is not None and cod_mun not in geo_data:
                    geo_data[cod_mun] = {
                        "codigo_departamento": safe_int(r[0]) or 0,
                        "nombre_departamento": str(r[1]).strip()
                        if r[1]
                        else "DESCONOCIDO",
                        "codigo_municipio": cod_mun,
                        "nombre_municipio": str(r[3]).strip()
                        if r[3]
                        else "DESCONOCIDO",
                    }

    # Insertar
    count = 0
    for geo in geo_data.values():
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO dim_geografia
                    (codigo_departamento, nombre_departamento,
                     codigo_municipio, nombre_municipio,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    geo["codigo_departamento"],
                    geo["nombre_departamento"],
                    geo["codigo_municipio"],
                    geo["nombre_municipio"],
                    NOW,
                    NOW,
                ),
            )
            count += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()

    mapping = {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT codigo_municipio, id FROM dim_geografia"
        ).fetchall()
    }
    logger.info(
        "[dim_geografia] %d municipios insertados (%d en mapping)",
        count,
        len(mapping),
    )
    return mapping


def load_dim_programa(conn: sqlite3.Connection) -> dict[int, int]:
    """
    Extrae programas únicos de tablas de estudiantes.
    Retorna mapping: codigo_snies_programa → id.
    """
    logger.info("[dim_programa] Extrayendo programas únicos...")

    tables = [
        "matriculados_unified",
        "inscritos_unified",
        "admitidos_unified",
        "graduados_unified",
        "matriculados_primer_curso_unified",
    ]

    prog_data: dict[int, dict] = {}

    for table in tables:
        rows = conn.execute(
            f"""
            SELECT DISTINCT
                codigo_snies_del_programa,
                programa_academico,
                id_nivel_academico,
                nivel_academico,
                id_nivel_de_formacion,
                nivel_de_formacion,
                id_metodologia,
                metodologia,
                id_area,
                area_de_conocimiento,
                id_nucleo,
                nucleo_basico_del_conocimiento_nbc
            FROM "{table}"
            WHERE codigo_snies_del_programa IS NOT NULL
            """
        ).fetchall()

        for r in rows:
            codigo = safe_int(r[0])
            if codigo is not None and codigo not in prog_data:
                prog_data[codigo] = {
                    "codigo_snies_programa": codigo,
                    "nombre_programa": str(r[1]).strip() if r[1] else "DESCONOCIDO",
                    "id_nivel_academico": safe_int(r[2]),
                    "nivel_academico": str(r[3]).strip() if r[3] else None,
                    "id_nivel_formacion": safe_int(r[4]),
                    "nivel_formacion": str(r[5]).strip() if r[5] else None,
                    "id_metodologia": safe_int(r[6]),
                    "metodologia": str(r[7]).strip() if r[7] else None,
                    "id_area": safe_int(r[8]),
                    "area_conocimiento": str(r[9]).strip() if r[9] else None,
                    "id_nucleo": safe_int(r[10]),
                    "nucleo_basico": str(r[11]).strip() if r[11] else None,
                }

    count = 0
    for prog in prog_data.values():
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO dim_programa
                    (codigo_snies_programa, nombre_programa,
                     id_nivel_academico, nivel_academico,
                     id_nivel_formacion, nivel_formacion,
                     id_metodologia, metodologia,
                     id_area, area_conocimiento,
                     id_nucleo, nucleo_basico,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    prog["codigo_snies_programa"],
                    prog["nombre_programa"],
                    prog["id_nivel_academico"],
                    prog["nivel_academico"],
                    prog["id_nivel_formacion"],
                    prog["nivel_formacion"],
                    prog["id_metodologia"],
                    prog["metodologia"],
                    prog["id_area"],
                    prog["area_conocimiento"],
                    prog["id_nucleo"],
                    prog["nucleo_basico"],
                    NOW,
                    NOW,
                ),
            )
            count += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()

    mapping = {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT codigo_snies_programa, id FROM dim_programa"
        ).fetchall()
    }
    logger.info(
        "[dim_programa] %d programas insertados (%d en mapping)",
        count,
        len(mapping),
    )
    return mapping


def load_dim_tiempo(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Extrae periodos únicos (ano, semestre) de todas las tablas.
    Retorna mapping: "ano-semestre" → id.
    """
    logger.info("[dim_tiempo] Extrayendo periodos únicos...")

    tables = [
        "matriculados_unified",
        "inscritos_unified",
        "admitidos_unified",
        "graduados_unified",
        "matriculados_primer_curso_unified",
        "docentes_unified",
        "administrativos_unified",
    ]

    periodos: set[tuple[int, int]] = set()
    for table in tables:
        rows = conn.execute(
            f"""
            SELECT DISTINCT ano, semestre
            FROM "{table}"
            WHERE ano IS NOT NULL AND semestre IS NOT NULL
            """
        ).fetchall()
        for r in rows:
            ano = safe_int(r[0])
            sem = safe_int(r[1])
            if ano is not None and sem is not None and sem in (1, 2):
                periodos.add((ano, sem))

    count = 0
    for ano, sem in sorted(periodos):
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO dim_tiempo
                    (ano, semestre, ano_semestre, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (ano, sem, f"{ano}-{sem}", NOW, NOW),
            )
            count += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()

    mapping = {}
    for row in conn.execute("SELECT ano, semestre, id FROM dim_tiempo").fetchall():
        key = f"{int(row[0])}-{int(row[1])}"
        mapping[key] = row[2]

    logger.info(
        "[dim_tiempo] %d periodos insertados (%d en mapping)", count, len(mapping)
    )
    return mapping


def load_dim_sexo(conn: sqlite3.Connection) -> dict[int, int]:
    """
    Extrae sexos únicos. Retorna mapping: id_sexo → id.
    """
    logger.info("[dim_sexo] Extrayendo sexos únicos...")

    sexo_data: dict[int, str] = {}

    # De tablas de estudiantes
    for table in [
        "matriculados_unified",
        "inscritos_unified",
        "admitidos_unified",
        "graduados_unified",
        "matriculados_primer_curso_unified",
    ]:
        rows = conn.execute(
            f"""
            SELECT DISTINCT id_sexo, sexo
            FROM "{table}"
            WHERE id_sexo IS NOT NULL
            """
        ).fetchall()
        for r in rows:
            id_s = safe_int(r[0])
            if id_s is not None and id_s not in sexo_data:
                sexo_data[id_s] = str(r[1]).strip() if r[1] else "DESCONOCIDO"

    # De docentes (columna se llama sexo_del_docente o sexo)
    cols_doc = {
        row[1]
        for row in conn.execute('PRAGMA table_info("docentes_unified")').fetchall()
    }
    sexo_col_doc = "sexo" if "sexo" in cols_doc else "sexo_del_docente"
    rows = conn.execute(
        f"""
        SELECT DISTINCT id_sexo, {sexo_col_doc}
        FROM docentes_unified
        WHERE id_sexo IS NOT NULL
        """
    ).fetchall()
    for r in rows:
        id_s = safe_int(r[0])
        if id_s is not None and id_s not in sexo_data:
            sexo_data[id_s] = str(r[1]).strip() if r[1] else "DESCONOCIDO"

    count = 0
    for id_s, sexo in sexo_data.items():
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO dim_sexo
                    (id_sexo, sexo, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (id_s, sexo, NOW, NOW),
            )
            count += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()

    mapping = {
        row[0]: row[1]
        for row in conn.execute("SELECT id_sexo, id FROM dim_sexo").fetchall()
    }
    logger.info("[dim_sexo] %d sexos insertados (%d en mapping)", count, len(mapping))
    return mapping


def load_dim_nivel_formacion_docente(conn: sqlite3.Connection) -> dict[int, int]:
    """
    Extrae niveles de formación docente únicos.
    Retorna mapping: id_nivel_formacion_docente → id.
    """
    logger.info("[dim_nivel_formacion_docente] Extrayendo niveles de formación...")

    rows = conn.execute(
        """
        SELECT DISTINCT
            id_maximo_nivel_de_formacion_del_docente,
            maximo_nivel_de_formacion_del_docente
        FROM docentes_unified
        WHERE id_maximo_nivel_de_formacion_del_docente IS NOT NULL
        """
    ).fetchall()

    count = 0
    for r in rows:
        id_nf = safe_int(r[0])
        if id_nf is None:
            continue
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO dim_nivel_formacion_docente
                    (id_nivel_formacion_docente, nivel_formacion_docente,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (id_nf, str(r[1]).strip() if r[1] else "DESCONOCIDO", NOW, NOW),
            )
            count += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()

    mapping = {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT id_nivel_formacion_docente, id FROM dim_nivel_formacion_docente"
        ).fetchall()
    }
    logger.info(
        "[dim_nivel_formacion_docente] %d niveles insertados (%d en mapping)",
        count,
        len(mapping),
    )
    return mapping


def load_dim_dedicacion_docente(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Extrae combinaciones de dedicación + contrato únicas.
    Retorna mapping: "id_dedicacion-id_contrato" → id.
    """
    logger.info("[dim_dedicacion_docente] Extrayendo dedicaciones únicas...")

    # La columna de texto puede ser tiempo_de_dedicacion_del_docente o tiempo_de_dedicacion_del_docente_1
    # Y tipo_de_contrato puede ser tipo_de_contrato_del_docente o tipo_de_contrato
    cols_info = {
        row[1]
        for row in conn.execute('PRAGMA table_info("docentes_unified")').fetchall()
    }

    tipo_contrato_col = (
        "tipo_de_contrato_del_docente"
        if "tipo_de_contrato_del_docente" in cols_info
        else "tipo_de_contrato"
    )

    rows = conn.execute(
        f"""
        SELECT DISTINCT
            id_tiempo_de_dedicacion,
            tiempo_de_dedicacion_del_docente,
            id_tipo_de_contrato,
            {tipo_contrato_col}
        FROM docentes_unified
        WHERE id_tiempo_de_dedicacion IS NOT NULL
          AND id_tipo_de_contrato IS NOT NULL
        """
    ).fetchall()

    count = 0
    for r in rows:
        id_ded = safe_int(r[0])
        id_cont = safe_int(r[2])
        if id_ded is None or id_cont is None:
            continue
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO dim_dedicacion_docente
                    (id_tiempo_dedicacion, tiempo_dedicacion,
                     id_tipo_contrato, tipo_contrato,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    id_ded,
                    str(r[1]).strip() if r[1] else "DESCONOCIDO",
                    id_cont,
                    str(r[3]).strip() if r[3] else "DESCONOCIDO",
                    NOW,
                    NOW,
                ),
            )
            count += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()

    mapping = {}
    for row in conn.execute(
        "SELECT id_tiempo_dedicacion, id_tipo_contrato, id FROM dim_dedicacion_docente"
    ).fetchall():
        key = f"{row[0]}-{row[1]}"
        mapping[key] = row[2]

    logger.info(
        "[dim_dedicacion_docente] %d combinaciones insertadas (%d en mapping)",
        count,
        len(mapping),
    )
    return mapping


# ──────────────────────────────────────────────────────────────
# FUNCIONES DE CARGA DE HECHOS
# ──────────────────────────────────────────────────────────────
def load_fact_estudiantes(
    conn: sqlite3.Connection,
    inst_map: dict[int, int],
    geo_map: dict[int, int],
    prog_map: dict[int, int],
    tiempo_map: dict[str, int],
    sexo_map: dict[int, int],
) -> int:
    """
    Carga fact_estudiantes desde las 5 tablas de categorías de estudiantes.
    """
    logger.info("=" * 50)
    logger.info("[fact_estudiantes] Iniciando carga...")

    total_inserted = 0
    total_skipped = 0

    for category, config in STUDENT_CATEGORIES.items():
        table_name = f"{category}_unified"
        tipo_evento = config["tipo_evento"]
        metric_cols = config["metric_cols"]

        logger.info("[fact_estudiantes][%s] Procesando...", category)

        # Determinar columna de municipio IES
        cols_info = {
            row[1]
            for row in conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
        }
        mun_ies_col = (
            "codigo_del_municipio_ies"
            if "codigo_del_municipio_ies" in cols_info
            else (
                "codigo_del_municipio" if "codigo_del_municipio" in cols_info else None
            )
        )

        if mun_ies_col is None:
            logger.warning(
                "[fact_estudiantes][%s] No se encontró columna de municipio IES, saltando",
                category,
            )
            continue

        mun_prog_col = get_column_name(cols_info, "codigo_del_municipio_programa")
        if mun_prog_col is None:
            logger.warning(
                "[fact_estudiantes][%s] No se encontró columna de municipio programa, usando municipio IES",
                category,
            )
            mun_prog_col = mun_ies_col

        # Construir expresión COALESCE para la métrica
        available_metrics = [c for c in metric_cols if c in cols_info]
        if not available_metrics:
            logger.warning(
                "[fact_estudiantes][%s] No se encontró columna de métrica, saltando",
                category,
            )
            continue

        # COALESCE necesita al menos 2 argumentos
        if len(available_metrics) == 1:
            coalesce_expr = f'"{available_metrics[0]}"'
        else:
            coalesce_expr = (
                "COALESCE(" + ", ".join(f'"{c}"' for c in available_metrics) + ")"
            )

        query = f"""
            SELECT
                codigo_de_la_institucion,
                codigo_snies_del_programa,
                {mun_ies_col} as codigo_municipio_ies,
                {mun_prog_col} as codigo_municipio_programa,
                id_sexo,
                CAST(ano AS INTEGER) as ano,
                CAST(semestre AS INTEGER) as semestre,
                {coalesce_expr} as cantidad
            FROM "{table_name}"
            WHERE codigo_de_la_institucion IS NOT NULL
              AND codigo_snies_del_programa IS NOT NULL
              AND {mun_ies_col} IS NOT NULL
              AND id_sexo IS NOT NULL
              AND ano IS NOT NULL
              AND semestre IS NOT NULL
              AND {coalesce_expr} IS NOT NULL
        """

        rows = conn.execute(query).fetchall()
        logger.info(
            "[fact_estudiantes][%s] %d filas leídas de la fuente", category, len(rows)
        )

        batch: list[tuple] = []
        skipped = 0

        for r in rows:
            codigo_ies = safe_int(r[0])
            codigo_prog = safe_int(r[1])
            codigo_mun_ies = safe_int(r[2])
            codigo_mun_prog = safe_int(r[3])
            id_sexo = safe_int(r[4])
            ano = safe_int(r[5])
            semestre = safe_int(r[6])
            cantidad = safe_int(r[7])

            if cantidad is None or cantidad == 0:
                skipped += 1
                continue

            # Lookup dimension IDs
            inst_id = inst_map.get(codigo_ies) if codigo_ies else None
            prog_id = prog_map.get(codigo_prog) if codigo_prog else None
            geo_ies_id = geo_map.get(codigo_mun_ies) if codigo_mun_ies else None
            # Si no hay municipio programa, usar el de la IES
            geo_prog_id = (
                geo_map.get(codigo_mun_prog) if codigo_mun_prog else geo_ies_id
            )
            sexo_id = sexo_map.get(id_sexo) if id_sexo else None
            tiempo_key = f"{ano}-{semestre}" if ano and semestre else None
            tiempo_id = tiempo_map.get(tiempo_key) if tiempo_key else None

            if not all([inst_id, prog_id, geo_ies_id, geo_prog_id, sexo_id, tiempo_id]):
                skipped += 1
                continue

            batch.append(
                (
                    tipo_evento,
                    inst_id,
                    prog_id,
                    geo_ies_id,
                    geo_prog_id,
                    sexo_id,
                    tiempo_id,
                    cantidad,
                    NOW,
                )
            )

        # Insertar en lote
        if batch:
            conn.executemany(
                """
                INSERT INTO fact_estudiantes
                    (tipo_evento, institucion_id, programa_id,
                     geografia_ies_id, geografia_programa_id,
                     sexo_id, tiempo_id, cantidad, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch,
            )
            conn.commit()

        inserted = len(batch)
        total_inserted += inserted
        total_skipped += skipped
        logger.info(
            "[fact_estudiantes][%s] %d filas insertadas, %d saltadas",
            category,
            inserted,
            skipped,
        )

    logger.info(
        "[fact_estudiantes] TOTAL: %d filas insertadas, %d saltadas",
        total_inserted,
        total_skipped,
    )
    return total_inserted


def load_fact_docentes(
    conn: sqlite3.Connection,
    inst_map: dict[int, int],
    geo_map: dict[int, int],
    sexo_map: dict[int, int],
    nivel_form_map: dict[int, int],
    dedicacion_map: dict[str, int],
    tiempo_map: dict[str, int],
) -> int:
    """Carga fact_docentes desde docentes_unified."""
    logger.info("=" * 50)
    logger.info("[fact_docentes] Iniciando carga...")

    cols_info = {
        row[1]
        for row in conn.execute('PRAGMA table_info("docentes_unified")').fetchall()
    }

    mun_ies_col = (
        "codigo_del_municipio_ies"
        if "codigo_del_municipio_ies" in cols_info
        else "codigo_del_municipio"
    )

    # Métrica: no_de_docentes o docentes
    metric_cols = [c for c in ["no_de_docentes", "docentes"] if c in cols_info]
    coalesce_expr = "COALESCE(" + ", ".join(f'"{c}"' for c in metric_cols) + ")"

    tipo_contrato_col = (
        "tipo_de_contrato_del_docente"
        if "tipo_de_contrato_del_docente" in cols_info
        else "tipo_de_contrato"
    )

    query = f"""
        SELECT
            codigo_de_la_institucion,
            {mun_ies_col} as codigo_municipio_ies,
            id_sexo,
            id_maximo_nivel_de_formacion_del_docente,
            id_tiempo_de_dedicacion,
            id_tipo_de_contrato,
            CAST(ano AS INTEGER) as ano,
            CAST(semestre AS INTEGER) as semestre,
            {coalesce_expr} as cantidad
        FROM docentes_unified
        WHERE codigo_de_la_institucion IS NOT NULL
          AND {mun_ies_col} IS NOT NULL
          AND id_sexo IS NOT NULL
          AND id_maximo_nivel_de_formacion_del_docente IS NOT NULL
          AND id_tiempo_de_dedicacion IS NOT NULL
          AND id_tipo_de_contrato IS NOT NULL
          AND ano IS NOT NULL
          AND semestre IS NOT NULL
          AND {coalesce_expr} IS NOT NULL
    """

    rows = conn.execute(query).fetchall()
    logger.info("[fact_docentes] %d filas leídas de la fuente", len(rows))

    batch: list[tuple] = []
    skipped = 0

    for r in rows:
        codigo_ies = safe_int(r[0])
        codigo_mun_ies = safe_int(r[1])
        id_sexo = safe_int(r[2])
        id_nivel_form = safe_int(r[3])
        id_dedicacion = safe_int(r[4])
        id_contrato = safe_int(r[5])
        ano = safe_int(r[6])
        semestre = safe_int(r[7])
        cantidad = safe_int(r[8])

        if cantidad is None or cantidad == 0:
            skipped += 1
            continue

        inst_id = inst_map.get(codigo_ies) if codigo_ies else None
        geo_ies_id = geo_map.get(codigo_mun_ies) if codigo_mun_ies else None
        sexo_id = sexo_map.get(id_sexo) if id_sexo else None
        nivel_form_id = nivel_form_map.get(id_nivel_form) if id_nivel_form else None
        ded_key = (
            f"{id_dedicacion}-{id_contrato}" if id_dedicacion and id_contrato else None
        )
        dedicacion_id = dedicacion_map.get(ded_key) if ded_key else None
        tiempo_key = f"{ano}-{semestre}" if ano and semestre else None
        tiempo_id = tiempo_map.get(tiempo_key) if tiempo_key else None

        if not all(
            [inst_id, geo_ies_id, sexo_id, nivel_form_id, dedicacion_id, tiempo_id]
        ):
            skipped += 1
            continue

        batch.append(
            (
                inst_id,
                geo_ies_id,
                sexo_id,
                nivel_form_id,
                dedicacion_id,
                tiempo_id,
                cantidad,
                NOW,
            )
        )

    if batch:
        conn.executemany(
            """
            INSERT INTO fact_docentes
                (institucion_id, geografia_ies_id, sexo_id,
                 nivel_formacion_docente_id, dedicacion_docente_id,
                 tiempo_id, cantidad_docentes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch,
        )
        conn.commit()

    inserted = len(batch)
    logger.info("[fact_docentes] %d filas insertadas, %d saltadas", inserted, skipped)
    return inserted


def load_fact_administrativos(
    conn: sqlite3.Connection,
    inst_map: dict[int, int],
    geo_map: dict[int, int],
    tiempo_map: dict[str, int],
) -> int:
    """Carga fact_administrativos desde administrativos_unified."""
    logger.info("=" * 50)
    logger.info("[fact_administrativos] Iniciando carga...")

    cols_info = {
        row[1]
        for row in conn.execute(
            'PRAGMA table_info("administrativos_unified")'
        ).fetchall()
    }

    mun_ies_col = (
        "codigo_del_municipio_ies"
        if "codigo_del_municipio_ies" in cols_info
        else "codigo_del_municipio"
    )

    query = f"""
        SELECT
            codigo_de_la_institucion,
            {mun_ies_col} as codigo_municipio_ies,
            CAST(ano AS INTEGER) as ano,
            CAST(semestre AS INTEGER) as semestre,
            auxiliar,
            tecnico,
            profesional,
            directivo,
            total
        FROM administrativos_unified
        WHERE codigo_de_la_institucion IS NOT NULL
          AND {mun_ies_col} IS NOT NULL
          AND ano IS NOT NULL
          AND semestre IS NOT NULL
          AND total IS NOT NULL
    """

    rows = conn.execute(query).fetchall()
    logger.info("[fact_administrativos] %d filas leídas de la fuente", len(rows))

    batch: list[tuple] = []
    skipped = 0

    for r in rows:
        codigo_ies = safe_int(r[0])
        codigo_mun_ies = safe_int(r[1])
        ano = safe_int(r[2])
        semestre = safe_int(r[3])
        auxiliar = safe_int(r[4]) or 0
        tecnico = safe_int(r[5]) or 0
        profesional = safe_int(r[6]) or 0
        directivo = safe_int(r[7]) or 0
        total_val = safe_int(r[8])

        if total_val is None or total_val == 0:
            skipped += 1
            continue

        inst_id = inst_map.get(codigo_ies) if codigo_ies else None
        geo_ies_id = geo_map.get(codigo_mun_ies) if codigo_mun_ies else None
        tiempo_key = f"{ano}-{semestre}" if ano and semestre else None
        tiempo_id = tiempo_map.get(tiempo_key) if tiempo_key else None

        if not all([inst_id, geo_ies_id, tiempo_id]):
            skipped += 1
            continue

        batch.append(
            (
                inst_id,
                geo_ies_id,
                tiempo_id,
                auxiliar,
                tecnico,
                profesional,
                directivo,
                total_val,
                NOW,
            )
        )

    if batch:
        conn.executemany(
            """
            INSERT INTO fact_administrativos
                (institucion_id, geografia_ies_id, tiempo_id,
                 auxiliar, tecnico, profesional, directivo, total,
                 created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch,
        )
        conn.commit()

    inserted = len(batch)
    logger.info(
        "[fact_administrativos] %d filas insertadas, %d saltadas", inserted, skipped
    )
    return inserted


# ──────────────────────────────────────────────────────────────
# VALIDACIÓN
# ──────────────────────────────────────────────────────────────
def validate_star_schema(conn: sqlite3.Connection):
    """Ejecuta validaciones básicas sobre el star schema creado."""
    logger.info("=" * 50)
    logger.info("VALIDACIÓN DEL STAR SCHEMA")
    logger.info("=" * 50)

    # Conteos de dimensiones
    dims = [
        "dim_institucion",
        "dim_geografia",
        "dim_programa",
        "dim_tiempo",
        "dim_sexo",
        "dim_nivel_formacion_docente",
        "dim_dedicacion_docente",
    ]
    for dim in dims:
        count = conn.execute(f"SELECT COUNT(*) FROM {dim}").fetchone()[0]
        logger.info("  %-35s %8d registros", dim, count)

    # Conteos de hechos
    facts = ["fact_estudiantes", "fact_docentes", "fact_administrativos"]
    for fact in facts:
        count = conn.execute(f"SELECT COUNT(*) FROM {fact}").fetchone()[0]
        logger.info("  %-35s %8d registros", fact, count)

    # Desglose de fact_estudiantes por tipo_evento
    logger.info("")
    logger.info("  Desglose fact_estudiantes por tipo_evento:")
    rows = conn.execute(
        """
        SELECT tipo_evento, COUNT(*), SUM(cantidad)
        FROM fact_estudiantes
        GROUP BY tipo_evento
        ORDER BY tipo_evento
        """
    ).fetchall()
    for r in rows:
        logger.info(
            "    %-20s %8d filas | SUM(cantidad) = %s",
            r[0],
            r[1],
            f"{r[2]:,}" if r[2] else "0",
        )

    # Validar integridad referencial (FKs huérfanas)
    logger.info("")
    logger.info("  Validación de integridad referencial:")

    fk_checks = [
        ("fact_estudiantes", "institucion_id", "dim_institucion", "id"),
        ("fact_estudiantes", "programa_id", "dim_programa", "id"),
        ("fact_estudiantes", "geografia_ies_id", "dim_geografia", "id"),
        ("fact_estudiantes", "geografia_programa_id", "dim_geografia", "id"),
        ("fact_estudiantes", "sexo_id", "dim_sexo", "id"),
        ("fact_estudiantes", "tiempo_id", "dim_tiempo", "id"),
        ("fact_docentes", "institucion_id", "dim_institucion", "id"),
        ("fact_docentes", "geografia_ies_id", "dim_geografia", "id"),
        ("fact_docentes", "sexo_id", "dim_sexo", "id"),
        (
            "fact_docentes",
            "nivel_formacion_docente_id",
            "dim_nivel_formacion_docente",
            "id",
        ),
        ("fact_docentes", "dedicacion_docente_id", "dim_dedicacion_docente", "id"),
        ("fact_docentes", "tiempo_id", "dim_tiempo", "id"),
        ("fact_administrativos", "institucion_id", "dim_institucion", "id"),
        ("fact_administrativos", "geografia_ies_id", "dim_geografia", "id"),
        ("fact_administrativos", "tiempo_id", "dim_tiempo", "id"),
    ]

    all_ok = True
    for fact, fk_col, dim, dim_pk in fk_checks:
        orphans = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {fact} f
            LEFT JOIN {dim} d ON f.{fk_col} = d.{dim_pk}
            WHERE d.{dim_pk} IS NULL
            """
        ).fetchone()[0]
        if orphans > 0:
            logger.warning(
                "    HUÉRFANOS: %s.%s → %s: %d registros sin referencia",
                fact,
                fk_col,
                dim,
                orphans,
            )
            all_ok = False

    if all_ok:
        logger.info("    Todas las FKs son válidas (0 huérfanos)")

    # Ejemplo: top 5 IES por matriculados
    logger.info("")
    logger.info("  Top 5 IES por matriculados:")
    rows = conn.execute(
        """
        SELECT di.nombre_ies, SUM(fe.cantidad) as total
        FROM fact_estudiantes fe
        JOIN dim_institucion di ON fe.institucion_id = di.id
        WHERE fe.tipo_evento = 'matriculados'
        GROUP BY di.nombre_ies
        ORDER BY total DESC
        LIMIT 5
        """
    ).fetchall()
    for r in rows:
        logger.info("    %-50s %s", r[0][:50], f"{r[1]:,}")


# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────
def _copy_unified_tables(
    src_conn: sqlite3.Connection, dst_conn: sqlite3.Connection
) -> None:
    """Copia las tablas _unified de src a dst para que las funciones de hechos las lean."""
    tables = [
        row[0]
        for row in src_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_unified' ORDER BY name"
        ).fetchall()
    ]
    logger.info("Copiando %d tablas _unified a facts.db temporalmente...", len(tables))
    for table_name in tables:
        df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', src_conn)
        df.to_sql(table_name, dst_conn, if_exists="replace", index=False)
        logger.debug("  Copiada %s (%d filas)", table_name, len(df))
    dst_conn.commit()


def _drop_unified_tables(conn: sqlite3.Connection) -> None:
    """Elimina las tablas _unified temporales de facts.db."""
    tables = [
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_unified' ORDER BY name"
        ).fetchall()
    ]
    for table_name in tables:
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    conn.execute("VACUUM")
    conn.commit()
    logger.info("Tablas _unified temporales eliminadas de facts.db")


def main():
    logger.info("=" * 60)
    logger.info("CREACIÓN DE TABLAS DE HECHOS — Star Schema SNIES")
    logger.info("Fuente: %s", SQLITE_UNIFIED_DB_PATH)
    logger.info("Destino: %s", SQLITE_FACTS_DB_PATH)
    logger.info("=" * 60)

    if not SQLITE_UNIFIED_DB_PATH.exists():
        logger.error(
            "Base de datos unificada no encontrada: %s", SQLITE_UNIFIED_DB_PATH
        )
        sys.exit(1)

    if not SQLITE_FACTS_DB_PATH.exists():
        logger.error(
            "Base de datos de hechos no encontrada: %s "
            "(debe haberse creado en el paso create_dimensions)",
            SQLITE_FACTS_DB_PATH,
        )
        sys.exit(1)

    # Leer datos unificados desde seminario_unified.db
    src_conn = sqlite3.connect(SQLITE_UNIFIED_DB_PATH)
    src_conn.execute("PRAGMA journal_mode=WAL")

    # Abrir la base de datos de hechos (ya contiene dimensiones)
    conn = sqlite3.connect(str(SQLITE_FACTS_DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")

    start_time = time.time()

    # Copiar tablas _unified al destino para que las funciones las lean
    _copy_unified_tables(src_conn, conn)
    src_conn.close()

    # ── Paso 1: Eliminar tablas de hechos existentes (idempotente) ──
    logger.info("Eliminando tablas de hechos anteriores (si existen)...")
    conn.execute("PRAGMA foreign_keys=OFF")
    for table in [
        "fact_estudiantes",
        "fact_docentes",
        "fact_administrativos",
    ]:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()
    conn.execute("PRAGMA foreign_keys=ON")

    # ── Paso 2: Crear estructura DDL de hechos ──
    logger.info("Creando estructura de tablas de hechos...")
    conn.executescript(DDL_FACTS)
    logger.info("Estructura DDL de hechos creada correctamente")

    # ── Paso 3: Poblar dimensiones (re-cargar mappings de las dims existentes) ──
    logger.info("")
    logger.info("CARGANDO DIMENSIONES (para obtener mappings)...")
    logger.info("-" * 50)

    inst_map = load_dim_institucion(conn)
    geo_map = load_dim_geografia(conn)
    prog_map = load_dim_programa(conn)
    tiempo_map = load_dim_tiempo(conn)
    sexo_map = load_dim_sexo(conn)
    nivel_form_map = load_dim_nivel_formacion_docente(conn)
    dedicacion_map = load_dim_dedicacion_docente(conn)

    # ── Paso 4: Poblar tablas de hechos ──
    logger.info("")
    logger.info("CARGANDO TABLAS DE HECHOS...")
    logger.info("-" * 50)

    n_est = load_fact_estudiantes(
        conn, inst_map, geo_map, prog_map, tiempo_map, sexo_map
    )
    n_doc = load_fact_docentes(
        conn, inst_map, geo_map, sexo_map, nivel_form_map, dedicacion_map, tiempo_map
    )
    n_adm = load_fact_administrativos(conn, inst_map, geo_map, tiempo_map)

    # ── Paso 5: Validar ──
    validate_star_schema(conn)

    # Eliminar tablas _unified temporales
    _drop_unified_tables(conn)

    elapsed = time.time() - start_time
    conn.close()

    # Tamaño final
    size_mb = SQLITE_FACTS_DB_PATH.stat().st_size / (1024 * 1024)

    logger.info("")
    logger.info("=" * 60)
    logger.info("STAR SCHEMA CREADO EXITOSAMENTE")
    logger.info("  fact_estudiantes:     %8d filas", n_est)
    logger.info("  fact_docentes:        %8d filas", n_doc)
    logger.info("  fact_administrativos: %8d filas", n_adm)
    logger.info("  Tiempo total:         %.1f segundos", elapsed)
    logger.info("  Tamaño BD:            %.1f MB", size_mb)
    logger.info("  Destino: %s", SQLITE_FACTS_DB_PATH)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
