"""
Crea las tablas de dimensiones del star schema en SQLite.

Lee los datos unificados de las tablas *_unified, extrae valores únicos
y los carga en las tablas dimensionales con surrogate keys.

Dimensiones creadas:
    - dim_institucion
    - dim_geografia
    - dim_programa
    - dim_tiempo
    - dim_sexo
    - dim_nivel_formacion_docente
    - dim_dedicacion_docente

Uso:
    uv run python scripts/create_dimensions.py
"""

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import SQLITE_UNIFIED_DB_PATH, SQLITE_FACTS_DB_PATH, DATA_DIR
from utils.logger import logger

# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

NOW = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# Tablas fuente tipo "estudiantes" (comparten estructura de programa + geo programa)
STUDENT_TABLES = [
    "admitidos_unified",
    "graduados_unified",
    "inscritos_unified",
    "matriculados_unified",
    "matriculados_primer_curso_unified",
]

ALL_UNIFIED_TABLES = STUDENT_TABLES + [
    "docentes_unified",
    "administrativos_unified",
]


def safe_int(value: object) -> int | None:
    """Convierte un valor a int, manejando floats con .0 y strings."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip().replace("\ufeff", "")
    if not text or text.lower() in ("nan", "none", ""):
        return None
    try:
        return int(float(text))
    except (ValueError, OverflowError):
        return None


def safe_str(value: object) -> str | None:
    """Convierte un valor a string limpio."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() in ("nan", "none"):
        return None
    return text


def normalize_text(value: object) -> str | None:
    """Normaliza texto a Title Case para consistencia."""
    text = safe_str(value)
    if text is None:
        return None
    # Mantener acrónimos y nombres propios: usar title() solo si todo es MAYÚSCULAS
    if text.isupper():
        return text.title()
    return text


def read_table(conn: sqlite3.Connection, table_name: str) -> pd.DataFrame:
    """Lee una tabla completa de SQLite en un DataFrame."""
    return pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Verifica si una tabla existe en la base de datos."""
    result = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return result[0] > 0


def drop_if_exists(conn: sqlite3.Connection, table_name: str) -> None:
    """Elimina una tabla si existe."""
    conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')


def get_col(df: pd.DataFrame, *candidates: str) -> pd.Series:
    """Retorna la primera columna que exista en el DataFrame.

    Útil para manejar las variaciones de nombres de columna
    entre las distintas categorías SNIES (ej. id_caracter vs id_caracter_ies).
    """
    for col in candidates:
        if col in df.columns:
            return df[col]
    # Retorna una serie de NaN si ninguna columna existe
    return pd.Series([None] * len(df), name=candidates[0])


# ──────────────────────────────────────────────────────────────
# dim_institucion
# ──────────────────────────────────────────────────────────────


def create_dim_institucion(conn: sqlite3.Connection) -> int:
    """Crea dim_institucion con instituciones únicas de todas las tablas."""
    logger.info("[dim_institucion] Extrayendo instituciones únicas...")

    frames: list[pd.DataFrame] = []

    for table_name in ALL_UNIFIED_TABLES:
        if not table_exists(conn, table_name):
            logger.warning(
                "[dim_institucion] Tabla %s no encontrada, saltando", table_name
            )
            continue

        df = read_table(conn, table_name)

        chunk = pd.DataFrame(
            {
                "codigo_ies": get_col(df, "codigo_de_la_institucion").apply(safe_int),
                "codigo_ies_padre": get_col(df, "ies_padre").apply(safe_int),
                "nombre_ies": get_col(
                    df, "institucion_de_educacion_superior_ies"
                ).apply(normalize_text),
                "principal_o_seccional": get_col(df, "principal_o_seccional").apply(
                    normalize_text
                ),
                "id_sector_ies": get_col(df, "id_sector_ies").apply(safe_int),
                "sector_ies": get_col(df, "sector_ies").apply(normalize_text),
                "id_caracter": get_col(df, "id_caracter", "id_caracter_ies").apply(
                    safe_int
                ),
                "caracter_ies": get_col(df, "caracter_ies").apply(normalize_text),
            }
        )

        chunk = chunk.dropna(subset=["codigo_ies", "nombre_ies"])
        frames.append(chunk)

    if not frames:
        logger.error("[dim_institucion] Sin datos de instituciones")
        return 0

    all_inst = pd.concat(frames, ignore_index=True)

    # Deduplicar por codigo_ies: tomar la primera fila no-nula para cada IES
    # Ordenar para que las filas con más datos completos vayan primero
    all_inst["completeness"] = all_inst.notna().sum(axis=1)
    all_inst = all_inst.sort_values("completeness", ascending=False)
    dim = all_inst.drop_duplicates(subset=["codigo_ies"], keep="first").copy()
    dim = dim.drop(columns=["completeness"])
    dim = dim.sort_values("codigo_ies").reset_index(drop=True)

    # Agregar timestamps
    dim["created_at"] = NOW
    dim["updated_at"] = NOW

    # Crear tabla
    drop_if_exists(conn, "dim_institucion")
    conn.execute("""
        CREATE TABLE dim_institucion (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_ies              INTEGER NOT NULL,
            codigo_ies_padre        INTEGER,
            nombre_ies              TEXT    NOT NULL,
            principal_o_seccional   TEXT,
            id_sector_ies           INTEGER,
            sector_ies              TEXT,
            id_caracter             INTEGER,
            caracter_ies            TEXT,
            created_at              TEXT    NOT NULL,
            updated_at              TEXT    NOT NULL
        )
    """)

    # Insertar datos
    dim.to_sql("dim_institucion", conn, if_exists="append", index=False)

    # Crear índices
    conn.execute("""
        CREATE UNIQUE INDEX uq_dim_institucion_codigo
        ON dim_institucion (codigo_ies)
    """)
    conn.execute("""
        CREATE INDEX idx_dim_institucion_sector
        ON dim_institucion (id_sector_ies)
    """)
    conn.execute("""
        CREATE INDEX idx_dim_institucion_caracter
        ON dim_institucion (id_caracter)
    """)

    conn.commit()

    row_count = conn.execute("SELECT COUNT(*) FROM dim_institucion").fetchone()[0]
    logger.info("[dim_institucion] Tabla creada: %d instituciones únicas", row_count)
    return row_count


# ──────────────────────────────────────────────────────────────
# dim_geografia
# ──────────────────────────────────────────────────────────────


def create_dim_geografia(conn: sqlite3.Connection) -> int:
    """Crea dim_geografia con ubicaciones únicas (departamento + municipio)."""
    logger.info("[dim_geografia] Extrayendo ubicaciones geográficas únicas...")

    frames: list[pd.DataFrame] = []

    for table_name in ALL_UNIFIED_TABLES:
        if not table_exists(conn, table_name):
            continue

        df = read_table(conn, table_name)

        # Geografía IES (presente en todas las tablas)
        geo_ies = pd.DataFrame(
            {
                "codigo_departamento": get_col(df, "codigo_del_departamento_ies").apply(
                    safe_int
                ),
                "nombre_departamento": get_col(
                    df, "departamento_de_domicilio_de_la_ies"
                ).apply(normalize_text),
                "codigo_municipio": get_col(
                    df, "codigo_del_municipio_ies", "codigo_del_municipio"
                ).apply(safe_int),
                "nombre_municipio": get_col(
                    df, "municipio_de_domicilio_de_la_ies"
                ).apply(normalize_text),
            }
        )
        geo_ies = geo_ies.dropna(
            subset=["codigo_municipio", "nombre_departamento", "nombre_municipio"]
        )
        frames.append(geo_ies)

        # Geografía programa (solo tablas de estudiantes)
        if table_name in STUDENT_TABLES:
            if "codigo_del_departamento_programa" in df.columns:
                geo_prog = pd.DataFrame(
                    {
                        "codigo_departamento": get_col(
                            df, "codigo_del_departamento_programa"
                        ).apply(safe_int),
                        "nombre_departamento": get_col(
                            df, "departamento_de_oferta_del_programa"
                        ).apply(normalize_text),
                        "codigo_municipio": get_col(
                            df, "codigo_del_municipio_programa"
                        ).apply(safe_int),
                        "nombre_municipio": get_col(
                            df, "municipio_de_oferta_del_programa"
                        ).apply(normalize_text),
                    }
                )
                geo_prog = geo_prog.dropna(
                    subset=[
                        "codigo_municipio",
                        "nombre_departamento",
                        "nombre_municipio",
                    ]
                )
                frames.append(geo_prog)

    if not frames:
        logger.error("[dim_geografia] Sin datos geográficos")
        return 0

    all_geo = pd.concat(frames, ignore_index=True)

    # Deduplicar por codigo_municipio
    all_geo["completeness"] = all_geo.notna().sum(axis=1)
    all_geo = all_geo.sort_values("completeness", ascending=False)
    dim = all_geo.drop_duplicates(subset=["codigo_municipio"], keep="first").copy()
    dim = dim.drop(columns=["completeness"])
    dim = dim.sort_values("codigo_municipio").reset_index(drop=True)

    dim["created_at"] = NOW
    dim["updated_at"] = NOW

    drop_if_exists(conn, "dim_geografia")
    conn.execute("""
        CREATE TABLE dim_geografia (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_departamento     INTEGER NOT NULL,
            nombre_departamento     TEXT    NOT NULL,
            codigo_municipio        INTEGER NOT NULL,
            nombre_municipio        TEXT    NOT NULL,
            created_at              TEXT    NOT NULL,
            updated_at              TEXT    NOT NULL
        )
    """)

    dim.to_sql("dim_geografia", conn, if_exists="append", index=False)

    conn.execute("""
        CREATE UNIQUE INDEX uq_dim_geografia_municipio
        ON dim_geografia (codigo_municipio)
    """)
    conn.execute("""
        CREATE INDEX idx_dim_geografia_depto
        ON dim_geografia (codigo_departamento)
    """)

    conn.commit()

    row_count = conn.execute("SELECT COUNT(*) FROM dim_geografia").fetchone()[0]
    logger.info("[dim_geografia] Tabla creada: %d municipios únicos", row_count)
    return row_count


# ──────────────────────────────────────────────────────────────
# dim_programa
# ──────────────────────────────────────────────────────────────


def create_dim_programa(conn: sqlite3.Connection) -> int:
    """Crea dim_programa con programas académicos únicos."""
    logger.info("[dim_programa] Extrayendo programas académicos únicos...")

    frames: list[pd.DataFrame] = []

    for table_name in STUDENT_TABLES:
        if not table_exists(conn, table_name):
            continue

        df = read_table(conn, table_name)

        if "codigo_snies_del_programa" not in df.columns:
            logger.warning(
                "[dim_programa] Tabla %s sin columna codigo_snies_del_programa",
                table_name,
            )
            continue

        chunk = pd.DataFrame(
            {
                "codigo_snies_programa": get_col(df, "codigo_snies_del_programa").apply(
                    safe_int
                ),
                "nombre_programa": get_col(df, "programa_academico").apply(
                    normalize_text
                ),
                "id_nivel_academico": get_col(df, "id_nivel_academico").apply(safe_int),
                "nivel_academico": get_col(df, "nivel_academico").apply(normalize_text),
                "id_nivel_formacion": get_col(df, "id_nivel_de_formacion").apply(
                    safe_int
                ),
                "nivel_formacion": get_col(df, "nivel_de_formacion").apply(
                    normalize_text
                ),
                "id_metodologia": get_col(df, "id_metodologia").apply(safe_int),
                "metodologia": get_col(df, "metodologia").apply(normalize_text),
                "id_area": get_col(df, "id_area").apply(safe_int),
                "area_conocimiento": get_col(df, "area_de_conocimiento").apply(
                    normalize_text
                ),
                "id_nucleo": get_col(df, "id_nucleo").apply(safe_int),
                "nucleo_basico": get_col(
                    df, "nucleo_basico_del_conocimiento_nbc"
                ).apply(normalize_text),
            }
        )

        chunk = chunk.dropna(subset=["codigo_snies_programa", "nombre_programa"])
        frames.append(chunk)

    if not frames:
        logger.error("[dim_programa] Sin datos de programas")
        return 0

    all_prog = pd.concat(frames, ignore_index=True)

    # Deduplicar por codigo_snies_programa
    all_prog["completeness"] = all_prog.notna().sum(axis=1)
    all_prog = all_prog.sort_values("completeness", ascending=False)
    dim = all_prog.drop_duplicates(
        subset=["codigo_snies_programa"], keep="first"
    ).copy()
    dim = dim.drop(columns=["completeness"])
    dim = dim.sort_values("codigo_snies_programa").reset_index(drop=True)

    dim["created_at"] = NOW
    dim["updated_at"] = NOW

    drop_if_exists(conn, "dim_programa")
    conn.execute("""
        CREATE TABLE dim_programa (
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
            created_at              TEXT    NOT NULL,
            updated_at              TEXT    NOT NULL
        )
    """)

    dim.to_sql("dim_programa", conn, if_exists="append", index=False)

    conn.execute("""
        CREATE UNIQUE INDEX uq_dim_programa_snies
        ON dim_programa (codigo_snies_programa)
    """)
    conn.execute("""
        CREATE INDEX idx_dim_programa_nivel
        ON dim_programa (id_nivel_academico)
    """)
    conn.execute("""
        CREATE INDEX idx_dim_programa_area
        ON dim_programa (id_area)
    """)
    conn.execute("""
        CREATE INDEX idx_dim_programa_metodologia
        ON dim_programa (id_metodologia)
    """)

    conn.commit()

    row_count = conn.execute("SELECT COUNT(*) FROM dim_programa").fetchone()[0]
    logger.info("[dim_programa] Tabla creada: %d programas únicos", row_count)
    return row_count


# ──────────────────────────────────────────────────────────────
# dim_tiempo
# ──────────────────────────────────────────────────────────────


def create_dim_tiempo(conn: sqlite3.Connection) -> int:
    """Crea dim_tiempo con combinaciones únicas de año + semestre."""
    logger.info("[dim_tiempo] Extrayendo periodos temporales únicos...")

    periods: set[tuple[int, int]] = set()

    for table_name in ALL_UNIFIED_TABLES:
        if not table_exists(conn, table_name):
            continue

        # Usar SQL directo para obtener periodos únicos (mucho más rápido)
        try:
            rows = conn.execute(
                f'SELECT DISTINCT ano, semestre FROM "{table_name}" '
                "WHERE ano IS NOT NULL AND semestre IS NOT NULL"
            ).fetchall()
        except Exception:
            continue

        for row in rows:
            ano = safe_int(row[0])
            semestre = safe_int(row[1])
            if ano is not None and semestre is not None and semestre in (1, 2):
                periods.add((ano, semestre))

    if not periods:
        logger.error("[dim_tiempo] Sin datos temporales")
        return 0

    sorted_periods = sorted(periods)

    dim = pd.DataFrame(
        [
            {
                "ano": ano,
                "semestre": semestre,
                "ano_semestre": f"{ano}-{semestre}",
                "created_at": NOW,
                "updated_at": NOW,
            }
            for ano, semestre in sorted_periods
        ]
    )

    drop_if_exists(conn, "dim_tiempo")
    conn.execute("""
        CREATE TABLE dim_tiempo (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ano             INTEGER NOT NULL,
            semestre        INTEGER NOT NULL CHECK (semestre IN (1, 2)),
            ano_semestre    TEXT    NOT NULL,
            created_at      TEXT    NOT NULL,
            updated_at      TEXT    NOT NULL
        )
    """)

    dim.to_sql("dim_tiempo", conn, if_exists="append", index=False)

    conn.execute("""
        CREATE UNIQUE INDEX uq_dim_tiempo_periodo
        ON dim_tiempo (ano, semestre)
    """)

    conn.commit()

    row_count = conn.execute("SELECT COUNT(*) FROM dim_tiempo").fetchone()[0]
    logger.info("[dim_tiempo] Tabla creada: %d periodos únicos", row_count)
    return row_count


# ──────────────────────────────────────────────────────────────
# dim_sexo
# ──────────────────────────────────────────────────────────────

# Mapeo para normalizar los distintos nombres a un valor canónico
SEXO_CANONICAL: dict[str, str] = {
    "hombre": "Masculino",
    "masculino": "Masculino",
    "mujer": "Femenino",
    "femenino": "Femenino",
    "no binario": "No binario",
    "trans": "Trans",
    "no informa": "No informa",
    "sin información": "Sin información",
    "sin informacion": "Sin información",
}


def create_dim_sexo(conn: sqlite3.Connection) -> int:
    """Crea dim_sexo con valores únicos de sexo."""
    logger.info("[dim_sexo] Extrayendo valores de sexo únicos...")

    id_sexo_map: dict[int, str] = {}

    for table_name in ALL_UNIFIED_TABLES:
        if not table_exists(conn, table_name):
            continue

        # Determinar nombre de columna de sexo según la tabla
        cols_info = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
        col_names = {row[1] for row in cols_info}

        id_col_name = "id_sexo" if "id_sexo" in col_names else None
        sexo_col_name = None
        for candidate in ("sexo", "sexo_del_docente"):
            if candidate in col_names:
                sexo_col_name = candidate
                break

        if id_col_name is None or sexo_col_name is None:
            continue

        rows = conn.execute(
            f'SELECT DISTINCT "{id_col_name}", "{sexo_col_name}" FROM "{table_name}" '
            f'WHERE "{id_col_name}" IS NOT NULL AND "{sexo_col_name}" IS NOT NULL'
        ).fetchall()

        for row in rows:
            id_s = safe_int(row[0])
            sexo_raw = safe_str(row[1])
            if id_s is None or sexo_raw is None:
                continue

            sexo_normalized = SEXO_CANONICAL.get(sexo_raw.lower(), sexo_raw)

            if id_s not in id_sexo_map:
                id_sexo_map[id_s] = sexo_normalized

    if not id_sexo_map:
        logger.error("[dim_sexo] Sin datos de sexo")
        return 0

    dim = pd.DataFrame(
        [
            {
                "id_sexo": id_s,
                "sexo": sexo,
                "created_at": NOW,
                "updated_at": NOW,
            }
            for id_s, sexo in sorted(id_sexo_map.items())
        ]
    )

    drop_if_exists(conn, "dim_sexo")
    conn.execute("""
        CREATE TABLE dim_sexo (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            id_sexo     INTEGER NOT NULL,
            sexo        TEXT    NOT NULL,
            created_at  TEXT    NOT NULL,
            updated_at  TEXT    NOT NULL
        )
    """)

    dim.to_sql("dim_sexo", conn, if_exists="append", index=False)

    conn.execute("""
        CREATE UNIQUE INDEX uq_dim_sexo
        ON dim_sexo (id_sexo)
    """)

    conn.commit()

    row_count = conn.execute("SELECT COUNT(*) FROM dim_sexo").fetchone()[0]
    logger.info("[dim_sexo] Tabla creada: %d valores de sexo", row_count)
    return row_count


# ──────────────────────────────────────────────────────────────
# dim_nivel_formacion_docente
# ──────────────────────────────────────────────────────────────

NIVEL_FORMACION_CANONICAL: dict[str, str] = {
    "posdoctorado": "Posdoctorado",
    "doctorado": "Doctorado",
    "maestría": "Maestría",
    "maestria": "Maestría",
    "especialización universitaria": "Especialización Universitaria",
    "especializacion universitaria": "Especialización Universitaria",
    "especialización técnico profesional": "Especialización Técnico Profesional",
    "especializacion tecnico profesional": "Especialización Técnico Profesional",
    "especialización tecnológica": "Especialización Tecnológica",
    "especializacion tecnologica": "Especialización Tecnológica",
    "especialización médico quirúrgica": "Especialización Médico Quirúrgica",
    "especializacion medico quirurgica": "Especialización Médico Quirúrgica",
    "universitaria": "Universitaria",
    "universitario": "Universitaria",
    "tecnológica": "Tecnológica",
    "tecnologica": "Tecnológica",
    "tecnológico": "Tecnológica",
    "tecnologico": "Tecnológica",
    "formación técnica profesional": "Formación Técnica Profesional",
    "formacion tecnica profesional": "Formación Técnica Profesional",
    "docente sin título": "Docente sin título",
    "docente sin titulo": "Docente sin título",
}


def create_dim_nivel_formacion_docente(conn: sqlite3.Connection) -> int:
    """Crea dim_nivel_formacion_docente con niveles de formación de docentes."""
    logger.info("[dim_nivel_formacion_docente] Extrayendo niveles de formación...")

    table_name = "docentes_unified"
    if not table_exists(conn, table_name):
        logger.error("[dim_nivel_formacion_docente] Tabla %s no encontrada", table_name)
        return 0

    rows = conn.execute(
        f"SELECT DISTINCT id_maximo_nivel_de_formacion_del_docente, "
        f'maximo_nivel_de_formacion_del_docente FROM "{table_name}" '
        f"WHERE id_maximo_nivel_de_formacion_del_docente IS NOT NULL "
        f"AND maximo_nivel_de_formacion_del_docente IS NOT NULL"
    ).fetchall()

    id_nivel_map: dict[int, str] = {}
    for row in rows:
        id_n = safe_int(row[0])
        nivel_raw = safe_str(row[1])
        if id_n is None or nivel_raw is None:
            continue

        nivel_normalized = NIVEL_FORMACION_CANONICAL.get(nivel_raw.lower(), nivel_raw)

        if id_n not in id_nivel_map:
            id_nivel_map[id_n] = nivel_normalized

    if not id_nivel_map:
        logger.error("[dim_nivel_formacion_docente] Sin datos de niveles de formación")
        return 0

    dim = pd.DataFrame(
        [
            {
                "id_nivel_formacion_docente": id_n,
                "nivel_formacion_docente": nivel,
                "created_at": NOW,
                "updated_at": NOW,
            }
            for id_n, nivel in sorted(id_nivel_map.items())
        ]
    )

    drop_if_exists(conn, "dim_nivel_formacion_docente")
    conn.execute("""
        CREATE TABLE dim_nivel_formacion_docente (
            id                              INTEGER PRIMARY KEY AUTOINCREMENT,
            id_nivel_formacion_docente      INTEGER NOT NULL,
            nivel_formacion_docente         TEXT    NOT NULL,
            created_at                      TEXT    NOT NULL,
            updated_at                      TEXT    NOT NULL
        )
    """)

    dim.to_sql("dim_nivel_formacion_docente", conn, if_exists="append", index=False)

    conn.execute("""
        CREATE UNIQUE INDEX uq_dim_nivel_form_doc
        ON dim_nivel_formacion_docente (id_nivel_formacion_docente)
    """)

    conn.commit()

    row_count = conn.execute(
        "SELECT COUNT(*) FROM dim_nivel_formacion_docente"
    ).fetchone()[0]
    logger.info("[dim_nivel_formacion_docente] Tabla creada: %d niveles", row_count)
    return row_count


# ──────────────────────────────────────────────────────────────
# dim_dedicacion_docente
# ──────────────────────────────────────────────────────────────

DEDICACION_CANONICAL: dict[str, str] = {
    "tiempo completo o exclusiva": "Tiempo Completo o Exclusiva",
    "medio tiempo": "Medio Tiempo",
    "catedra": "Cátedra",
    "cátedra": "Cátedra",
    "sin información": "Sin información",
    "sin informacion": "Sin información",
}

CONTRATO_CANONICAL: dict[str, str] = {
    "término indefinido": "Término Indefinido",
    "termino indefinido": "Término Indefinido",
    "término fijo": "Término Fijo",
    "termino fijo": "Término Fijo",
    "horas (profesores de catedra)": "Horas (profesores de cátedra)",
    "horas (profesores de cátedra)": "Horas (profesores de cátedra)",
    "ocasional": "Ocasional",
    "ad honorem": "Ad honorem",
    "sin información": "Sin información",
    "sin informacion": "Sin información",
}


def create_dim_dedicacion_docente(conn: sqlite3.Connection) -> int:
    """Crea dim_dedicacion_docente con combinaciones de dedicación y contrato."""
    logger.info("[dim_dedicacion_docente] Extrayendo dedicaciones únicas...")

    table_name = "docentes_unified"
    if not table_exists(conn, table_name):
        logger.error("[dim_dedicacion_docente] Tabla %s no encontrada", table_name)
        return 0

    # Determinar columna de tipo_contrato (varía entre años)
    cols_info = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    col_names = {row[1] for row in cols_info}
    con_col_name = (
        "tipo_de_contrato_del_docente"
        if "tipo_de_contrato_del_docente" in col_names
        else "tipo_de_contrato"
    )

    rows = conn.execute(
        f"SELECT DISTINCT id_tiempo_de_dedicacion, tiempo_de_dedicacion_del_docente, "
        f'id_tipo_de_contrato, "{con_col_name}" FROM "{table_name}" '
        f"WHERE id_tiempo_de_dedicacion IS NOT NULL AND id_tipo_de_contrato IS NOT NULL"
    ).fetchall()

    combos: dict[tuple[int, int], tuple[str, str]] = {}
    for row in rows:
        id_d = safe_int(row[0])
        ded_raw = safe_str(row[1])
        id_c = safe_int(row[2])
        con_raw = safe_str(row[3])

        if id_d is None or id_c is None:
            continue

        ded_normalized = DEDICACION_CANONICAL.get(
            ded_raw.lower() if ded_raw else "", ded_raw or "Sin información"
        )
        con_normalized = CONTRATO_CANONICAL.get(
            con_raw.lower() if con_raw else "", con_raw or "Sin información"
        )

        key = (id_d, id_c)
        if key not in combos:
            combos[key] = (ded_normalized, con_normalized)

    if not combos:
        logger.error("[dim_dedicacion_docente] Sin datos de dedicación")
        return 0

    dim = pd.DataFrame(
        [
            {
                "id_tiempo_dedicacion": id_d,
                "tiempo_dedicacion": ded,
                "id_tipo_contrato": id_c,
                "tipo_contrato": con,
                "created_at": NOW,
                "updated_at": NOW,
            }
            for (id_d, id_c), (ded, con) in sorted(combos.items())
        ]
    )

    drop_if_exists(conn, "dim_dedicacion_docente")
    conn.execute("""
        CREATE TABLE dim_dedicacion_docente (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            id_tiempo_dedicacion    INTEGER NOT NULL,
            tiempo_dedicacion       TEXT    NOT NULL,
            id_tipo_contrato        INTEGER NOT NULL,
            tipo_contrato           TEXT    NOT NULL,
            created_at              TEXT    NOT NULL,
            updated_at              TEXT    NOT NULL
        )
    """)

    dim.to_sql("dim_dedicacion_docente", conn, if_exists="append", index=False)

    conn.execute("""
        CREATE UNIQUE INDEX uq_dim_dedicacion
        ON dim_dedicacion_docente (id_tiempo_dedicacion, id_tipo_contrato)
    """)

    conn.commit()

    row_count = conn.execute("SELECT COUNT(*) FROM dim_dedicacion_docente").fetchone()[
        0
    ]
    logger.info("[dim_dedicacion_docente] Tabla creada: %d combinaciones", row_count)
    return row_count


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────


def main():
    logger.info("=" * 60)
    logger.info("Creación de tablas de dimensiones — Star Schema SNIES")
    logger.info("Fuente: %s", SQLITE_UNIFIED_DB_PATH)
    logger.info("Destino: %s", SQLITE_FACTS_DB_PATH)
    logger.info("=" * 60)

    if not SQLITE_UNIFIED_DB_PATH.exists():
        logger.error(
            "Base de datos unificada no encontrada: %s", SQLITE_UNIFIED_DB_PATH
        )
        sys.exit(1)

    # Leer datos unificados desde seminario_unified.db
    src_conn = sqlite3.connect(SQLITE_UNIFIED_DB_PATH)
    src_conn.execute("PRAGMA journal_mode=WAL")

    # Crear/recrear la base de datos de hechos
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if SQLITE_FACTS_DB_PATH.exists():
        SQLITE_FACTS_DB_PATH.unlink()
        logger.info("Base de datos de hechos anterior eliminada")

    dst_conn = sqlite3.connect(SQLITE_FACTS_DB_PATH)
    dst_conn.execute("PRAGMA journal_mode=WAL")
    dst_conn.execute("PRAGMA synchronous=NORMAL")

    # Deshabilitar FK temporalmente para poder hacer DROP de tablas
    # que pudieran tener referencias de tablas de hechos previas
    dst_conn.execute("PRAGMA foreign_keys=OFF")

    # Limpiar dimensiones previas si existen
    dim_tables = [
        "dim_institucion",
        "dim_geografia",
        "dim_programa",
        "dim_tiempo",
        "dim_sexo",
        "dim_nivel_formacion_docente",
        "dim_dedicacion_docente",
    ]
    for t in dim_tables:
        drop_if_exists(dst_conn, t)
    dst_conn.commit()

    dst_conn.execute("PRAGMA foreign_keys=ON")

    # Monkey-patch: las funciones de creación leen de conn, así que
    # usamos ATTACH DATABASE para que lean las tablas unificadas.
    # Alternativa más limpia: copiar las tablas unificadas al destino primero.
    # Copiamos las tablas _unified al destino para que las funciones existentes
    # puedan leerlas directamente.
    _copy_unified_tables(src_conn, dst_conn)
    src_conn.close()

    # Crear cada dimensión (las funciones leen de dst_conn que ahora tiene las _unified)
    results: dict[str, int] = {}

    results["dim_institucion"] = create_dim_institucion(dst_conn)
    results["dim_geografia"] = create_dim_geografia(dst_conn)
    results["dim_programa"] = create_dim_programa(dst_conn)
    results["dim_tiempo"] = create_dim_tiempo(dst_conn)
    results["dim_sexo"] = create_dim_sexo(dst_conn)
    results["dim_nivel_formacion_docente"] = create_dim_nivel_formacion_docente(
        dst_conn
    )
    results["dim_dedicacion_docente"] = create_dim_dedicacion_docente(dst_conn)

    # Eliminar las copias temporales de tablas _unified de facts.db
    _drop_unified_tables(dst_conn)

    # Resumen final
    logger.info("=" * 60)
    logger.info("Resumen de dimensiones creadas")
    logger.info("=" * 60)

    total_rows = 0
    for dim_name, row_count in results.items():
        status = f"{row_count:>8} filas" if row_count > 0 else "  ¡ERROR!"
        logger.info("  %-35s %s", dim_name, status)
        total_rows += row_count

    # Verificar integridad con datos de muestra
    logger.info("-" * 60)
    logger.info("Verificación de muestras:")

    for dim_name in results:
        sample = dst_conn.execute(f'SELECT * FROM "{dim_name}" LIMIT 3').fetchall()
        col_names = [
            desc[0]
            for desc in dst_conn.execute(
                f'SELECT * FROM "{dim_name}" LIMIT 1'
            ).description
        ]
        logger.info("  [%s] Columnas: %s", dim_name, col_names)
        for row in sample:
            logger.debug("    %s", row)

    dst_conn.close()

    size_mb = SQLITE_FACTS_DB_PATH.stat().st_size / (1024 * 1024)
    logger.info("=" * 60)
    logger.info(
        "Dimensiones creadas exitosamente: %d tablas, %d registros totales",
        len(results),
        total_rows,
    )
    logger.info("Destino: %s (%.1f MB)", SQLITE_FACTS_DB_PATH, size_mb)
    logger.info("=" * 60)


def _copy_unified_tables(
    src_conn: sqlite3.Connection, dst_conn: sqlite3.Connection
) -> None:
    """Copia las tablas _unified de src a dst para que las funciones de dimensiones las lean."""
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


if __name__ == "__main__":
    main()
