import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import PG_SCHEMA_UNIFIED, PG_SCHEMA_FACTS
from utils.db import (
    get_column_names,
    get_engine,
    get_row_count as db_get_row_count,
    managed_connection,
)
from utils.logger import logger
from utils.schema_helpers import (
    safe_int,
    safe_str,
    unified_table_exists,
)

NOW = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

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

DDL_DIM_INSTITUCION = """
CREATE TABLE IF NOT EXISTS dim_institucion (
    id                      SERIAL PRIMARY KEY,
    codigo_ies              BIGINT  NOT NULL,
    codigo_ies_padre        BIGINT,
    nombre_ies              TEXT    NOT NULL,
    principal_o_seccional   TEXT,
    id_sector_ies           BIGINT,
    sector_ies              TEXT,
    id_caracter             BIGINT,
    caracter_ies            TEXT,
    created_at              TEXT    NOT NULL,
    updated_at              TEXT    NOT NULL
)
"""

DDL_DIM_GEOGRAFIA = """
CREATE TABLE IF NOT EXISTS dim_geografia (
    id                      SERIAL PRIMARY KEY,
    codigo_departamento     BIGINT  NOT NULL,
    nombre_departamento     TEXT    NOT NULL,
    codigo_municipio        BIGINT  NOT NULL,
    nombre_municipio        TEXT    NOT NULL,
    created_at              TEXT    NOT NULL,
    updated_at              TEXT    NOT NULL
)
"""

DDL_DIM_PROGRAMA = """
CREATE TABLE IF NOT EXISTS dim_programa (
    id                      SERIAL PRIMARY KEY,
    codigo_snies_programa   BIGINT  NOT NULL,
    nombre_programa         TEXT    NOT NULL,
    id_nivel_academico      BIGINT,
    nivel_academico         TEXT,
    id_nivel_formacion      BIGINT,
    nivel_formacion         TEXT,
    id_metodologia          BIGINT,
    metodologia             TEXT,
    id_area                 BIGINT,
    area_conocimiento       TEXT,
    id_nucleo               BIGINT,
    nucleo_basico           TEXT,
    created_at              TEXT    NOT NULL,
    updated_at              TEXT    NOT NULL
)
"""

DDL_DIM_TIEMPO = """
CREATE TABLE IF NOT EXISTS dim_tiempo (
    id              SERIAL PRIMARY KEY,
    ano             BIGINT  NOT NULL,
    semestre        BIGINT  NOT NULL CHECK (semestre IN (1, 2)),
    ano_semestre    TEXT    NOT NULL,
    created_at      TEXT    NOT NULL,
    updated_at      TEXT    NOT NULL
)
"""

DDL_DIM_SEXO = """
CREATE TABLE IF NOT EXISTS dim_sexo (
    id          SERIAL PRIMARY KEY,
    id_sexo     BIGINT  NOT NULL,
    sexo        TEXT    NOT NULL,
    created_at  TEXT    NOT NULL,
    updated_at  TEXT    NOT NULL
)
"""

DDL_DIM_NIVEL_FORMACION_DOCENTE = """
CREATE TABLE IF NOT EXISTS dim_nivel_formacion_docente (
    id                              SERIAL PRIMARY KEY,
    id_nivel_formacion_docente      BIGINT  NOT NULL,
    nivel_formacion_docente         TEXT    NOT NULL,
    created_at                      TEXT    NOT NULL,
    updated_at                      TEXT    NOT NULL
)
"""

DDL_DIM_DEDICACION_DOCENTE = """
CREATE TABLE IF NOT EXISTS dim_dedicacion_docente (
    id                      SERIAL PRIMARY KEY,
    id_tiempo_dedicacion    BIGINT  NOT NULL,
    tiempo_dedicacion       TEXT    NOT NULL,
    id_tipo_contrato        BIGINT  NOT NULL,
    tipo_contrato           TEXT    NOT NULL,
    created_at              TEXT    NOT NULL,
    updated_at              TEXT    NOT NULL
)
"""

INDEXES = {
    "dim_institucion": [
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_institucion_codigo ON dim_institucion (codigo_ies)",
        "CREATE INDEX IF NOT EXISTS idx_dim_institucion_sector ON dim_institucion (id_sector_ies)",
        "CREATE INDEX IF NOT EXISTS idx_dim_institucion_caracter ON dim_institucion (id_caracter)",
    ],
    "dim_geografia": [
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_geografia_municipio ON dim_geografia (codigo_municipio)",
        "CREATE INDEX IF NOT EXISTS idx_dim_geografia_depto ON dim_geografia (codigo_departamento)",
    ],
    "dim_programa": [
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_programa_snies ON dim_programa (codigo_snies_programa)",
        "CREATE INDEX IF NOT EXISTS idx_dim_programa_nivel ON dim_programa (id_nivel_academico)",
        "CREATE INDEX IF NOT EXISTS idx_dim_programa_area ON dim_programa (id_area)",
        "CREATE INDEX IF NOT EXISTS idx_dim_programa_metodologia ON dim_programa (id_metodologia)",
    ],
    "dim_tiempo": [
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_tiempo_periodo ON dim_tiempo (ano, semestre)",
    ],
    "dim_sexo": [
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_sexo ON dim_sexo (id_sexo)",
    ],
    "dim_nivel_formacion_docente": [
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_nivel_form_doc ON dim_nivel_formacion_docente (id_nivel_formacion_docente)",
    ],
    "dim_dedicacion_docente": [
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_dedicacion ON dim_dedicacion_docente (id_tiempo_dedicacion, id_tipo_contrato)",
    ],
}

SEXO_CANONICAL: dict[str, str] = {
    "hombre": "Masculino",
    "masculino": "Masculino",
    "mujer": "Femenino",
    "femenino": "Femenino",
    "no binario": "No binario",
    "trans": "Trans",
    "no informa": "No informa",
    "sin informacion": "Sin informacion",
}

NIVEL_FORMACION_CANONICAL: dict[str, str] = {
    "posdoctorado": "Posdoctorado",
    "doctorado": "Doctorado",
    "maestria": "Maestria",
    "especializacion universitaria": "Especializacion Universitaria",
    "especializacion tecnico profesional": "Especializacion Tecnico Profesional",
    "especializacion tecnologica": "Especializacion Tecnologica",
    "especializacion medico quirurgica": "Especializacion Medico Quirurgica",
    "universitaria": "Universitaria",
    "universitario": "Universitaria",
    "tecnologica": "Tecnologica",
    "tecnologico": "Tecnologica",
    "formacion tecnica profesional": "Formacion Tecnica Profesional",
    "docente sin titulo": "Docente sin titulo",
}

DEDICACION_CANONICAL: dict[str, str] = {
    "tiempo completo o exclusiva": "Tiempo Completo o Exclusiva",
    "medio tiempo": "Medio Tiempo",
    "catedra": "Catedra",
    "sin informacion": "Sin informacion",
}

CONTRATO_CANONICAL: dict[str, str] = {
    "termino indefinido": "Termino Indefinido",
    "termino fijo": "Termino Fijo",
    "horas (profesores de catedra)": "Horas (profesores de catedra)",
    "ocasional": "Ocasional",
    "ad honorem": "Ad honorem",
    "sin informacion": "Sin informacion",
}


def normalize_text(value: object) -> str | None:
    text = safe_str(value)
    if text is None:
        return None
    if text.isupper():
        return text.title()
    return text


def read_table(table_name: str) -> pd.DataFrame:
    engine = get_engine(schema=PG_SCHEMA_UNIFIED)
    return pd.read_sql_query(f'SELECT * FROM "{table_name}"', engine)


def get_col(df: pd.DataFrame, *candidates: str) -> pd.Series:
    for col in candidates:
        if col in df.columns:
            return df[col]
    return pd.Series([None] * len(df), name=candidates[0])


def _bulk_insert(cur, table_name: str, columns: list[str], df: pd.DataFrame) -> None:
    if df.empty:
        return
    values = [tuple(row) for row in df[columns].itertuples(index=False, name=None)]
    cols_str = ", ".join(columns)
    template = f"({', '.join(['%s'] * len(columns))})"
    execute_values(
        cur,
        f'INSERT INTO "{table_name}" ({cols_str}) VALUES %s',
        values,
        template=template,
        page_size=1000,
    )


def _ensure_table_and_truncate(cur, table_name: str, ddl: str) -> None:
    cur.execute(ddl)
    cur.execute(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE')


def _create_indexes(cur, table_name: str) -> None:
    for idx_sql in INDEXES.get(table_name, []):
        cur.execute(idx_sql)


def create_dim_institucion() -> int:
    logger.info("[dim_institucion] Extrayendo instituciones unicas...")

    frames: list[pd.DataFrame] = []

    for table_name in ALL_UNIFIED_TABLES:
        if not unified_table_exists(table_name):
            logger.warning(
                "[dim_institucion] Tabla %s no encontrada, saltando", table_name
            )
            continue

        df = read_table(table_name)

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
    all_inst["completeness"] = all_inst.notna().sum(axis=1)
    all_inst = all_inst.sort_values("completeness", ascending=False)
    dim = all_inst.drop_duplicates(subset=["codigo_ies"], keep="first").copy()
    dim = dim.drop(columns=["completeness"])
    dim = dim.sort_values("codigo_ies").reset_index(drop=True)
    dim["created_at"] = NOW
    dim["updated_at"] = NOW

    insert_cols = [
        "codigo_ies",
        "codigo_ies_padre",
        "nombre_ies",
        "principal_o_seccional",
        "id_sector_ies",
        "sector_ies",
        "id_caracter",
        "caracter_ies",
        "created_at",
        "updated_at",
    ]

    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            _ensure_table_and_truncate(cur, "dim_institucion", DDL_DIM_INSTITUCION)
            _bulk_insert(cur, "dim_institucion", insert_cols, dim)
            _create_indexes(cur, "dim_institucion")

    row_count = db_get_row_count(PG_SCHEMA_FACTS, "dim_institucion")
    logger.info("[dim_institucion] Tabla creada: %d instituciones unicas", row_count)
    return row_count


def create_dim_geografia() -> int:
    logger.info("[dim_geografia] Extrayendo ubicaciones geograficas unicas...")

    frames: list[pd.DataFrame] = []

    for table_name in ALL_UNIFIED_TABLES:
        if not unified_table_exists(table_name):
            continue

        df = read_table(table_name)

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
        logger.error("[dim_geografia] Sin datos geograficos")
        return 0

    all_geo = pd.concat(frames, ignore_index=True)
    all_geo["completeness"] = all_geo.notna().sum(axis=1)
    all_geo = all_geo.sort_values("completeness", ascending=False)
    dim = all_geo.drop_duplicates(subset=["codigo_municipio"], keep="first").copy()
    dim = dim.drop(columns=["completeness"])
    dim = dim.sort_values("codigo_municipio").reset_index(drop=True)
    dim["created_at"] = NOW
    dim["updated_at"] = NOW

    insert_cols = [
        "codigo_departamento",
        "nombre_departamento",
        "codigo_municipio",
        "nombre_municipio",
        "created_at",
        "updated_at",
    ]

    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            _ensure_table_and_truncate(cur, "dim_geografia", DDL_DIM_GEOGRAFIA)
            _bulk_insert(cur, "dim_geografia", insert_cols, dim)
            _create_indexes(cur, "dim_geografia")

    row_count = db_get_row_count(PG_SCHEMA_FACTS, "dim_geografia")
    logger.info("[dim_geografia] Tabla creada: %d municipios unicos", row_count)
    return row_count


def create_dim_programa() -> int:
    logger.info("[dim_programa] Extrayendo programas academicos unicos...")

    frames: list[pd.DataFrame] = []

    for table_name in STUDENT_TABLES:
        if not unified_table_exists(table_name):
            continue

        df = read_table(table_name)

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
    all_prog["completeness"] = all_prog.notna().sum(axis=1)
    all_prog = all_prog.sort_values("completeness", ascending=False)
    dim = all_prog.drop_duplicates(
        subset=["codigo_snies_programa"], keep="first"
    ).copy()
    dim = dim.drop(columns=["completeness"])
    dim = dim.sort_values("codigo_snies_programa").reset_index(drop=True)
    dim["created_at"] = NOW
    dim["updated_at"] = NOW

    insert_cols = [
        "codigo_snies_programa",
        "nombre_programa",
        "id_nivel_academico",
        "nivel_academico",
        "id_nivel_formacion",
        "nivel_formacion",
        "id_metodologia",
        "metodologia",
        "id_area",
        "area_conocimiento",
        "id_nucleo",
        "nucleo_basico",
        "created_at",
        "updated_at",
    ]

    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            _ensure_table_and_truncate(cur, "dim_programa", DDL_DIM_PROGRAMA)
            _bulk_insert(cur, "dim_programa", insert_cols, dim)
            _create_indexes(cur, "dim_programa")

    row_count = db_get_row_count(PG_SCHEMA_FACTS, "dim_programa")
    logger.info("[dim_programa] Tabla creada: %d programas unicos", row_count)
    return row_count


def create_dim_tiempo() -> int:
    logger.info("[dim_tiempo] Extrayendo periodos temporales unicos...")

    periods: set[tuple[int, int]] = set()

    for table_name in ALL_UNIFIED_TABLES:
        if not unified_table_exists(table_name):
            continue

        try:
            with managed_connection(schema=PG_SCHEMA_UNIFIED) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f'SELECT DISTINCT ano, semestre FROM "{table_name}" '
                        "WHERE ano IS NOT NULL AND semestre IS NOT NULL"
                    )
                    rows = cur.fetchall()
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

    insert_cols = ["ano", "semestre", "ano_semestre", "created_at", "updated_at"]

    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            _ensure_table_and_truncate(cur, "dim_tiempo", DDL_DIM_TIEMPO)
            _bulk_insert(cur, "dim_tiempo", insert_cols, dim)
            _create_indexes(cur, "dim_tiempo")

    row_count = db_get_row_count(PG_SCHEMA_FACTS, "dim_tiempo")
    logger.info("[dim_tiempo] Tabla creada: %d periodos unicos", row_count)
    return row_count


def create_dim_sexo() -> int:
    logger.info("[dim_sexo] Extrayendo valores de sexo unicos...")

    id_sexo_map: dict[int, str] = {}

    for table_name in ALL_UNIFIED_TABLES:
        if not unified_table_exists(table_name):
            continue

        col_names = set(get_column_names(PG_SCHEMA_UNIFIED, table_name))

        id_col_name = "id_sexo" if "id_sexo" in col_names else None
        sexo_col_name = None
        for candidate in ("sexo", "sexo_del_docente"):
            if candidate in col_names:
                sexo_col_name = candidate
                break

        if id_col_name is None or sexo_col_name is None:
            continue

        with managed_connection(schema=PG_SCHEMA_UNIFIED) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT DISTINCT "{id_col_name}", "{sexo_col_name}" '
                    f'FROM "{table_name}" '
                    f'WHERE "{id_col_name}" IS NOT NULL '
                    f'AND "{sexo_col_name}" IS NOT NULL'
                )
                rows = cur.fetchall()

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

    insert_cols = ["id_sexo", "sexo", "created_at", "updated_at"]

    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            _ensure_table_and_truncate(cur, "dim_sexo", DDL_DIM_SEXO)
            _bulk_insert(cur, "dim_sexo", insert_cols, dim)
            _create_indexes(cur, "dim_sexo")

    row_count = db_get_row_count(PG_SCHEMA_FACTS, "dim_sexo")
    logger.info("[dim_sexo] Tabla creada: %d valores de sexo", row_count)
    return row_count


def create_dim_nivel_formacion_docente() -> int:
    logger.info("[dim_nivel_formacion_docente] Extrayendo niveles de formacion...")

    table_name = "docentes_unified"
    if not unified_table_exists(table_name):
        logger.error("[dim_nivel_formacion_docente] Tabla %s no encontrada", table_name)
        return 0

    with managed_connection(schema=PG_SCHEMA_UNIFIED) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT id_maximo_nivel_de_formacion_del_docente, "
                f'maximo_nivel_de_formacion_del_docente FROM "{table_name}" '
                f"WHERE id_maximo_nivel_de_formacion_del_docente IS NOT NULL "
                f"AND maximo_nivel_de_formacion_del_docente IS NOT NULL"
            )
            rows = cur.fetchall()

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
        logger.error("[dim_nivel_formacion_docente] Sin datos de niveles de formacion")
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

    insert_cols = [
        "id_nivel_formacion_docente",
        "nivel_formacion_docente",
        "created_at",
        "updated_at",
    ]

    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            _ensure_table_and_truncate(
                cur, "dim_nivel_formacion_docente", DDL_DIM_NIVEL_FORMACION_DOCENTE
            )
            _bulk_insert(cur, "dim_nivel_formacion_docente", insert_cols, dim)
            _create_indexes(cur, "dim_nivel_formacion_docente")

    row_count = db_get_row_count(PG_SCHEMA_FACTS, "dim_nivel_formacion_docente")
    logger.info("[dim_nivel_formacion_docente] Tabla creada: %d niveles", row_count)
    return row_count


def create_dim_dedicacion_docente() -> int:
    logger.info("[dim_dedicacion_docente] Extrayendo dedicaciones unicas...")

    table_name = "docentes_unified"
    if not unified_table_exists(table_name):
        logger.error("[dim_dedicacion_docente] Tabla %s no encontrada", table_name)
        return 0

    col_names = set(get_column_names(PG_SCHEMA_UNIFIED, table_name))
    con_col_name = (
        "tipo_de_contrato_del_docente"
        if "tipo_de_contrato_del_docente" in col_names
        else "tipo_de_contrato"
    )

    with managed_connection(schema=PG_SCHEMA_UNIFIED) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT id_tiempo_de_dedicacion, "
                f"tiempo_de_dedicacion_del_docente, "
                f'id_tipo_de_contrato, "{con_col_name}" '
                f'FROM "{table_name}" '
                f"WHERE id_tiempo_de_dedicacion IS NOT NULL "
                f"AND id_tipo_de_contrato IS NOT NULL"
            )
            rows = cur.fetchall()

    combos: dict[tuple[int, int], tuple[str, str]] = {}
    for row in rows:
        id_d = safe_int(row[0])
        ded_raw = safe_str(row[1])
        id_c = safe_int(row[2])
        con_raw = safe_str(row[3])

        if id_d is None or id_c is None:
            continue

        ded_normalized = DEDICACION_CANONICAL.get(
            ded_raw.lower() if ded_raw else "", ded_raw or "Sin informacion"
        )
        con_normalized = CONTRATO_CANONICAL.get(
            con_raw.lower() if con_raw else "", con_raw or "Sin informacion"
        )

        key = (id_d, id_c)
        if key not in combos:
            combos[key] = (ded_normalized, con_normalized)

    if not combos:
        logger.error("[dim_dedicacion_docente] Sin datos de dedicacion")
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

    insert_cols = [
        "id_tiempo_dedicacion",
        "tiempo_dedicacion",
        "id_tipo_contrato",
        "tipo_contrato",
        "created_at",
        "updated_at",
    ]

    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            _ensure_table_and_truncate(
                cur, "dim_dedicacion_docente", DDL_DIM_DEDICACION_DOCENTE
            )
            _bulk_insert(cur, "dim_dedicacion_docente", insert_cols, dim)
            _create_indexes(cur, "dim_dedicacion_docente")

    row_count = db_get_row_count(PG_SCHEMA_FACTS, "dim_dedicacion_docente")
    logger.info("[dim_dedicacion_docente] Tabla creada: %d combinaciones", row_count)
    return row_count


def main():
    logger.info("=" * 60)
    logger.info("Creacion de tablas de dimensiones — Star Schema SNIES")
    logger.info("Fuente: PostgreSQL schema '%s'", PG_SCHEMA_UNIFIED)
    logger.info("Destino: PostgreSQL schema '%s'", PG_SCHEMA_FACTS)
    logger.info("=" * 60)

    results: dict[str, int] = {}

    results["dim_institucion"] = create_dim_institucion()
    results["dim_geografia"] = create_dim_geografia()
    results["dim_programa"] = create_dim_programa()
    results["dim_tiempo"] = create_dim_tiempo()
    results["dim_sexo"] = create_dim_sexo()
    results["dim_nivel_formacion_docente"] = create_dim_nivel_formacion_docente()
    results["dim_dedicacion_docente"] = create_dim_dedicacion_docente()

    logger.info("=" * 60)
    logger.info("Resumen de dimensiones creadas")
    logger.info("=" * 60)

    total_rows = 0
    for dim_name, row_count in results.items():
        status = f"{row_count:>8} filas" if row_count > 0 else "  ERROR!"
        logger.info("  %-35s %s", dim_name, status)
        total_rows += row_count

    logger.info("-" * 60)
    logger.info("Verificacion de muestras:")

    for dim_name in results:
        with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT * FROM "{dim_name}" LIMIT 3')
                sample = cur.fetchall()
                col_names = [desc[0] for desc in cur.description]
        logger.info("  [%s] Columnas: %s", dim_name, col_names)
        for row in sample:
            logger.debug("    %s", row)

    logger.info("=" * 60)
    logger.info(
        "Dimensiones creadas exitosamente: %d tablas, %d registros totales",
        len(results),
        total_rows,
    )
    logger.info("Destino: PostgreSQL schema '%s'", PG_SCHEMA_FACTS)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
