"""
queries.py — Consultas SQL contra el star schema (facts.*) para el análisis de Hito 3.

Todas las funciones retornan pandas DataFrames listos para análisis.
Requieren una conexión SQLAlchemy activa (DATABASE_URL en config/globals.py).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import DATABASE_URL
from sqlalchemy import create_engine

_engine = None


def _get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    return _engine


# ---------------------------------------------------------------------------
# 1. Serie temporal de matrícula por sector y semestre
# ---------------------------------------------------------------------------

SQL_MATRICULA_SECTOR = """
SELECT
    dt.ano,
    dt.semestre,
    di.sector_ies,
    SUM(fe.cantidad) AS total
FROM facts.fact_estudiantes fe
JOIN facts.dim_tiempo       dt ON fe.tiempo_id       = dt.id
JOIN facts.dim_institucion  di ON fe.institucion_id  = di.id
WHERE fe.tipo_evento = :tipo_evento
GROUP BY dt.ano, dt.semestre, di.sector_ies
ORDER BY dt.ano, dt.semestre, di.sector_ies
"""


def get_matricula_por_sector(tipo_evento: str = "matriculados") -> pd.DataFrame:
    """
    Retorna la matrícula/inscripciones/graduados totales por sector (Oficial/Privada)
    y semestre (2018-S1 a 2024-S2).

    Columnas: ano, semestre, sector_ies, total, periodo (str 'AAAA-SN'), t (int secuencial)
    """
    with _get_engine().connect() as conn:
        df = pd.read_sql(text(SQL_MATRICULA_SECTOR), conn, params={"tipo_evento": tipo_evento})

    df["periodo"] = df["ano"].astype(str) + "-S" + df["semestre"].astype(str)
    df = df.sort_values(["ano", "semestre", "sector_ies"]).reset_index(drop=True)

    # Índice temporal secuencial (para regresiones de series de tiempo)
    periodos_ord = (
        df[["ano", "semestre"]]
        .drop_duplicates()
        .sort_values(["ano", "semestre"])
        .reset_index(drop=True)
    )
    periodos_ord["t"] = range(1, len(periodos_ord) + 1)
    df = df.merge(periodos_ord, on=["ano", "semestre"])
    return df


# ---------------------------------------------------------------------------
# 2. Panel de IES: matrícula por institución y semestre
# ---------------------------------------------------------------------------

SQL_PANEL_IES = """
SELECT
    di.codigo_ies,
    di.nombre_ies,
    di.sector_ies,
    di.caracter_ies,
    dg.nombre_departamento,
    dt.ano,
    dt.semestre,
    SUM(fe.cantidad) AS matriculados
FROM facts.fact_estudiantes fe
JOIN facts.dim_tiempo       dt ON fe.tiempo_id       = dt.id
JOIN facts.dim_institucion  di ON fe.institucion_id  = di.id
JOIN facts.dim_geografia    dg ON fe.geografia_ies_id = dg.id
WHERE fe.tipo_evento = 'matriculados'
GROUP BY
    di.codigo_ies, di.nombre_ies, di.sector_ies, di.caracter_ies,
    dg.nombre_departamento, dt.ano, dt.semestre
ORDER BY di.codigo_ies, dt.ano, dt.semestre
"""


def get_panel_ies() -> pd.DataFrame:
    """
    Panel balanceado (o desbalanceado) de IES con matrícula por semestre.
    Columnas: codigo_ies, nombre_ies, sector_ies, caracter_ies,
              nombre_departamento, ano, semestre, matriculados, periodo, t
    """
    with _get_engine().connect() as conn:
        df = pd.read_sql(text(SQL_PANEL_IES), conn)

    df["periodo"] = df["ano"].astype(str) + "-S" + df["semestre"].astype(str)
    periodos_ord = (
        df[["ano", "semestre"]]
        .drop_duplicates()
        .sort_values(["ano", "semestre"])
        .reset_index(drop=True)
    )
    periodos_ord["t"] = range(1, len(periodos_ord) + 1)
    df = df.merge(periodos_ord, on=["ano", "semestre"])
    return df


# ---------------------------------------------------------------------------
# 3. Serie temporal por departamento y sector
# ---------------------------------------------------------------------------

SQL_MATRICULA_DEPTO = """
SELECT
    dg.codigo_departamento,
    dg.nombre_departamento,
    di.sector_ies,
    dt.ano,
    dt.semestre,
    SUM(fe.cantidad) AS total
FROM facts.fact_estudiantes fe
JOIN facts.dim_tiempo       dt ON fe.tiempo_id        = dt.id
JOIN facts.dim_institucion  di ON fe.institucion_id   = di.id
JOIN facts.dim_geografia    dg ON fe.geografia_ies_id = dg.id
WHERE fe.tipo_evento = 'matriculados'
GROUP BY
    dg.codigo_departamento, dg.nombre_departamento,
    di.sector_ies, dt.ano, dt.semestre
ORDER BY dg.codigo_departamento, dt.ano, dt.semestre
"""


def get_matricula_por_departamento() -> pd.DataFrame:
    """
    Matrícula por departamento, sector y semestre.
    Útil para análisis de heterogeneidad territorial.
    """
    with _get_engine().connect() as conn:
        df = pd.read_sql(text(SQL_MATRICULA_DEPTO), conn)

    df["periodo"] = df["ano"].astype(str) + "-S" + df["semestre"].astype(str)
    return df


# ---------------------------------------------------------------------------
# 4. Múltiples tipos de evento para análisis embudo
# ---------------------------------------------------------------------------

SQL_EMBUDO = """
SELECT
    dt.ano,
    dt.semestre,
    di.sector_ies,
    fe.tipo_evento,
    SUM(fe.cantidad) AS total
FROM facts.fact_estudiantes fe
JOIN facts.dim_tiempo       dt ON fe.tiempo_id      = dt.id
JOIN facts.dim_institucion  di ON fe.institucion_id = di.id
GROUP BY dt.ano, dt.semestre, di.sector_ies, fe.tipo_evento
ORDER BY dt.ano, dt.semestre, di.sector_ies, fe.tipo_evento
"""


def get_embudo_estudiantil() -> pd.DataFrame:
    """
    Retorna inscritos, admitidos, matriculados, primer_curso y graduados
    por sector y semestre. Permite análisis de tasas de conversión.
    """
    with _get_engine().connect() as conn:
        df = pd.read_sql(text(SQL_EMBUDO), conn)
    df["periodo"] = df["ano"].astype(str) + "-S" + df["semestre"].astype(str)
    return df


# ---------------------------------------------------------------------------
# 5. Docentes por sector (variable de inputs de la política)
# ---------------------------------------------------------------------------

SQL_DOCENTES_SECTOR = """
SELECT
    dt.ano,
    dt.semestre,
    di.sector_ies,
    SUM(fd.cantidad_docentes) AS total_docentes
FROM facts.fact_docentes fd
JOIN facts.dim_tiempo       dt ON fd.tiempo_id      = dt.id
JOIN facts.dim_institucion  di ON fd.institucion_id = di.id
GROUP BY dt.ano, dt.semestre, di.sector_ies
ORDER BY dt.ano, dt.semestre, di.sector_ies
"""


def get_docentes_por_sector() -> pd.DataFrame:
    """
    Total docentes por sector y semestre. Proxy de capacidad instalada.
    """
    with _get_engine().connect() as conn:
        df = pd.read_sql(text(SQL_DOCENTES_SECTOR), conn)
    df["periodo"] = df["ano"].astype(str) + "-S" + df["semestre"].astype(str)
    return df
