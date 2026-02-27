from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from psycopg2.extras import execute_values

from config.globals import PG_SCHEMA_UNIFIED, PG_SCHEMA_FACTS
from utils.db import get_column_names, managed_connection
from utils.logger import logger
from utils.schema_helpers import safe_int, unified_table_exists

NOW = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
BATCH_SIZE = 5000

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

MUNICIPIO_IES_COLS = ["codigo_del_municipio_ies", "codigo_del_municipio"]

DDL_FACT_ESTUDIANTES = """
CREATE TABLE IF NOT EXISTS fact_estudiantes (
    id                      SERIAL PRIMARY KEY,
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
)
"""

DDL_FACT_DOCENTES = """
CREATE TABLE IF NOT EXISTS fact_docentes (
    id                              SERIAL PRIMARY KEY,
    institucion_id                  INTEGER NOT NULL REFERENCES dim_institucion(id),
    geografia_ies_id                INTEGER NOT NULL REFERENCES dim_geografia(id),
    sexo_id                         INTEGER NOT NULL REFERENCES dim_sexo(id),
    nivel_formacion_docente_id      INTEGER NOT NULL REFERENCES dim_nivel_formacion_docente(id),
    dedicacion_docente_id           INTEGER NOT NULL REFERENCES dim_dedicacion_docente(id),
    tiempo_id                       INTEGER NOT NULL REFERENCES dim_tiempo(id),
    cantidad_docentes               INTEGER NOT NULL DEFAULT 0,
    created_at                      TEXT    NOT NULL
)
"""

DDL_FACT_ADMINISTRATIVOS = """
CREATE TABLE IF NOT EXISTS fact_administrativos (
    id                      SERIAL PRIMARY KEY,
    institucion_id          INTEGER NOT NULL REFERENCES dim_institucion(id),
    geografia_ies_id        INTEGER NOT NULL REFERENCES dim_geografia(id),
    tiempo_id               INTEGER NOT NULL REFERENCES dim_tiempo(id),
    auxiliar                INTEGER NOT NULL DEFAULT 0,
    tecnico                 INTEGER NOT NULL DEFAULT 0,
    profesional             INTEGER NOT NULL DEFAULT 0,
    directivo               INTEGER NOT NULL DEFAULT 0,
    total                   INTEGER NOT NULL DEFAULT 0,
    created_at              TEXT    NOT NULL
)
"""

FACT_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_fact_est_institucion   ON fact_estudiantes (institucion_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_est_programa      ON fact_estudiantes (programa_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_est_geo_ies       ON fact_estudiantes (geografia_ies_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_est_geo_prog      ON fact_estudiantes (geografia_programa_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_est_sexo          ON fact_estudiantes (sexo_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_est_tiempo        ON fact_estudiantes (tiempo_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_est_tipo_tiempo   ON fact_estudiantes (tipo_evento, tiempo_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_est_tipo_inst_tiempo ON fact_estudiantes (tipo_evento, institucion_id, tiempo_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_doc_institucion   ON fact_docentes (institucion_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_doc_geo_ies       ON fact_docentes (geografia_ies_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_doc_sexo          ON fact_docentes (sexo_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_doc_nivel         ON fact_docentes (nivel_formacion_docente_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_doc_dedicacion    ON fact_docentes (dedicacion_docente_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_doc_tiempo        ON fact_docentes (tiempo_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_doc_inst_tiempo   ON fact_docentes (institucion_id, tiempo_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_adm_institucion   ON fact_administrativos (institucion_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_adm_geo_ies       ON fact_administrativos (geografia_ies_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_adm_tiempo        ON fact_administrativos (tiempo_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_adm_inst_tiempo   ON fact_administrativos (institucion_id, tiempo_id)",
]

def _insert_batch(cur, insert_sql: str, batch: list[tuple]) -> None:
    for i in range(0, len(batch), BATCH_SIZE):
        chunk = batch[i : i + BATCH_SIZE]
        execute_values(cur, insert_sql, chunk, page_size=BATCH_SIZE)

def load_dim_institucion_mapping() -> dict[int, int]:
    logger.info("[dim_institucion] Construyendo mapping...")
    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT codigo_ies, id FROM dim_institucion")
            mapping = {row[0]: row[1] for row in cur.fetchall()}
    logger.info("[dim_institucion] %d entradas en mapping", len(mapping))
    return mapping

def load_dim_geografia_mapping() -> dict[int, int]:
    logger.info("[dim_geografia] Construyendo mapping...")
    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT codigo_municipio, id FROM dim_geografia")
            mapping = {row[0]: row[1] for row in cur.fetchall()}
    logger.info("[dim_geografia] %d entradas en mapping", len(mapping))
    return mapping

def load_dim_programa_mapping() -> dict[int, int]:
    logger.info("[dim_programa] Construyendo mapping...")
    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT codigo_snies_programa, id FROM dim_programa")
            mapping = {row[0]: row[1] for row in cur.fetchall()}
    logger.info("[dim_programa] %d entradas en mapping", len(mapping))
    return mapping

def load_dim_tiempo_mapping() -> dict[str, int]:
    logger.info("[dim_tiempo] Construyendo mapping...")
    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ano, semestre, id FROM dim_tiempo")
            mapping = {}
            for row in cur.fetchall():
                key = f"{int(row[0])}-{int(row[1])}"
                mapping[key] = row[2]
    logger.info("[dim_tiempo] %d entradas en mapping", len(mapping))
    return mapping

def load_dim_sexo_mapping() -> dict[int, int]:
    logger.info("[dim_sexo] Construyendo mapping...")
    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id_sexo, id FROM dim_sexo")
            mapping = {row[0]: row[1] for row in cur.fetchall()}
    logger.info("[dim_sexo] %d entradas en mapping", len(mapping))
    return mapping

def load_dim_nivel_formacion_docente_mapping() -> dict[int, int]:
    logger.info("[dim_nivel_formacion_docente] Construyendo mapping...")
    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id_nivel_formacion_docente, id FROM dim_nivel_formacion_docente"
            )
            mapping = {row[0]: row[1] for row in cur.fetchall()}
    logger.info("[dim_nivel_formacion_docente] %d entradas en mapping", len(mapping))
    return mapping

def load_dim_dedicacion_docente_mapping() -> dict[str, int]:
    logger.info("[dim_dedicacion_docente] Construyendo mapping...")
    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id_tiempo_dedicacion, id_tipo_contrato, id "
                "FROM dim_dedicacion_docente"
            )
            mapping = {}
            for row in cur.fetchall():
                key = f"{row[0]}-{row[1]}"
                mapping[key] = row[2]
    logger.info("[dim_dedicacion_docente] %d entradas en mapping", len(mapping))
    return mapping

def load_fact_estudiantes(
    inst_map: dict[int, int],
    geo_map: dict[int, int],
    prog_map: dict[int, int],
    tiempo_map: dict[str, int],
    sexo_map: dict[int, int],
) -> int:
    logger.info("=" * 50)
    logger.info("[fact_estudiantes] Iniciando carga...")

    total_inserted = 0
    total_skipped = 0

    for category, config in STUDENT_CATEGORIES.items():
        table_name = f"{category}_unified"
        tipo_evento = config["tipo_evento"]
        metric_cols = config["metric_cols"]

        logger.info("[fact_estudiantes][%s] Procesando...", category)

        if not unified_table_exists(table_name):
            logger.warning(
                "[fact_estudiantes][%s] Tabla %s no encontrada, saltando",
                category,
                table_name,
            )
            continue

        cols_info = set(get_column_names(PG_SCHEMA_UNIFIED, table_name))

        mun_ies_col = (
            "codigo_del_municipio_ies"
            if "codigo_del_municipio_ies" in cols_info
            else (
                "codigo_del_municipio" if "codigo_del_municipio" in cols_info else None
            )
        )

        if mun_ies_col is None:
            logger.warning(
                "[fact_estudiantes][%s] No se encontro columna de municipio IES, saltando",
                category,
            )
            continue

        mun_prog_col = get_column_name(cols_info, "codigo_del_municipio_programa")
        if mun_prog_col is None:
            logger.warning(
                "[fact_estudiantes][%s] No se encontro columna de municipio programa, usando municipio IES",
                category,
            )
            mun_prog_col = mun_ies_col

        available_metrics = [c for c in metric_cols if c in cols_info]
        if not available_metrics:
            logger.warning(
                "[fact_estudiantes][%s] No se encontro columna de metrica, saltando",
                category,
            )
            continue

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
                CAST(ano AS DOUBLE PRECISION)::INTEGER as ano,
                CAST(semestre AS DOUBLE PRECISION)::INTEGER as semestre,
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

        with managed_connection(schema=PG_SCHEMA_UNIFIED) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()

        logger.info(
            "[fact_estudiantes][%s] %d filas leidas de la fuente",
            category,
            len(rows),
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

            inst_id = inst_map.get(codigo_ies) if codigo_ies else None
            prog_id = prog_map.get(codigo_prog) if codigo_prog else None
            geo_ies_id = geo_map.get(codigo_mun_ies) if codigo_mun_ies else None
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

        if batch:
            with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
                with conn.cursor() as cur:
                    _insert_batch(
                        cur,
                        """
                        INSERT INTO fact_estudiantes
                            (tipo_evento, institucion_id, programa_id,
                             geografia_ies_id, geografia_programa_id,
                             sexo_id, tiempo_id, cantidad, created_at)
                        VALUES %s
                        """,
                        batch,
                    )

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
    inst_map: dict[int, int],
    geo_map: dict[int, int],
    sexo_map: dict[int, int],
    nivel_form_map: dict[int, int],
    dedicacion_map: dict[str, int],
    tiempo_map: dict[str, int],
) -> int:
    logger.info("=" * 50)
    logger.info("[fact_docentes] Iniciando carga...")

    table_name = "docentes_unified"
    if not unified_table_exists(table_name):
        logger.error("[fact_docentes] Tabla %s no encontrada", table_name)
        return 0

    cols_info = set(get_column_names(PG_SCHEMA_UNIFIED, table_name))

    mun_ies_col = (
        "codigo_del_municipio_ies"
        if "codigo_del_municipio_ies" in cols_info
        else "codigo_del_municipio"
    )

    metric_cols = [c for c in ["no_de_docentes", "docentes"] if c in cols_info]
    if not metric_cols:
        logger.error("[fact_docentes] No se encontro columna de metrica")
        return 0
    coalesce_expr = "COALESCE(" + ", ".join(f'"{c}"' for c in metric_cols) + ")"

    query = f"""
        SELECT
            codigo_de_la_institucion,
            {mun_ies_col} as codigo_municipio_ies,
            id_sexo,
            id_maximo_nivel_de_formacion_del_docente,
            id_tiempo_de_dedicacion,
            id_tipo_de_contrato,
            CAST(ano AS DOUBLE PRECISION)::INTEGER as ano,
            CAST(semestre AS DOUBLE PRECISION)::INTEGER as semestre,
            {coalesce_expr} as cantidad
        FROM "{table_name}"
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

    with managed_connection(schema=PG_SCHEMA_UNIFIED) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    logger.info("[fact_docentes] %d filas leidas de la fuente", len(rows))

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
        with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
            with conn.cursor() as cur:
                _insert_batch(
                    cur,
                    """
                    INSERT INTO fact_docentes
                        (institucion_id, geografia_ies_id, sexo_id,
                         nivel_formacion_docente_id, dedicacion_docente_id,
                         tiempo_id, cantidad_docentes, created_at)
                    VALUES %s
                    """,
                    batch,
                )

    inserted = len(batch)
    logger.info("[fact_docentes] %d filas insertadas, %d saltadas", inserted, skipped)
    return inserted

def load_fact_administrativos(
    inst_map: dict[int, int],
    geo_map: dict[int, int],
    tiempo_map: dict[str, int],
) -> int:
    logger.info("=" * 50)
    logger.info("[fact_administrativos] Iniciando carga...")

    table_name = "administrativos_unified"
    if not unified_table_exists(table_name):
        logger.error("[fact_administrativos] Tabla %s no encontrada", table_name)
        return 0

    cols_info = set(get_column_names(PG_SCHEMA_UNIFIED, table_name))

    mun_ies_col = (
        "codigo_del_municipio_ies"
        if "codigo_del_municipio_ies" in cols_info
        else "codigo_del_municipio"
    )

    query = f"""
        SELECT
            codigo_de_la_institucion,
            {mun_ies_col} as codigo_municipio_ies,
            CAST(ano AS DOUBLE PRECISION)::INTEGER as ano,
            CAST(semestre AS DOUBLE PRECISION)::INTEGER as semestre,
            auxiliar,
            tecnico,
            profesional,
            directivo,
            total
        FROM "{table_name}"
        WHERE codigo_de_la_institucion IS NOT NULL
          AND {mun_ies_col} IS NOT NULL
          AND ano IS NOT NULL
          AND semestre IS NOT NULL
          AND total IS NOT NULL
    """

    with managed_connection(schema=PG_SCHEMA_UNIFIED) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    logger.info("[fact_administrativos] %d filas leidas de la fuente", len(rows))

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
        with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
            with conn.cursor() as cur:
                _insert_batch(
                    cur,
                    """
                    INSERT INTO fact_administrativos
                        (institucion_id, geografia_ies_id, tiempo_id,
                         auxiliar, tecnico, profesional, directivo, total,
                         created_at)
                    VALUES %s
                    """,
                    batch,
                )

    inserted = len(batch)
    logger.info(
        "[fact_administrativos] %d filas insertadas, %d saltadas",
        inserted,
        skipped,
    )
    return inserted

def validate_star_schema():
    logger.info("=" * 50)
    logger.info("VALIDACION DEL STAR SCHEMA")
    logger.info("=" * 50)

    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
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
                cur.execute(f"SELECT COUNT(*) FROM {dim}")
                count = cur.fetchone()[0]
                logger.info("  %-35s %8d registros", dim, count)

            facts = ["fact_estudiantes", "fact_docentes", "fact_administrativos"]
            for fact in facts:
                cur.execute(f"SELECT COUNT(*) FROM {fact}")
                count = cur.fetchone()[0]
                logger.info("  %-35s %8d registros", fact, count)

            logger.info("")
            logger.info("  Desglose fact_estudiantes por tipo_evento:")
            cur.execute(
                """
                SELECT tipo_evento, COUNT(*), SUM(cantidad)
                FROM fact_estudiantes
                GROUP BY tipo_evento
                ORDER BY tipo_evento
                """
            )
            for r in cur.fetchall():
                logger.info(
                    "    %-20s %8d filas | SUM(cantidad) = %s",
                    r[0],
                    r[1],
                    f"{r[2]:,}" if r[2] else "0",
                )

            logger.info("")
            logger.info("  Validacion de integridad referencial:")

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
                (
                    "fact_docentes",
                    "dedicacion_docente_id",
                    "dim_dedicacion_docente",
                    "id",
                ),
                ("fact_docentes", "tiempo_id", "dim_tiempo", "id"),
                ("fact_administrativos", "institucion_id", "dim_institucion", "id"),
                ("fact_administrativos", "geografia_ies_id", "dim_geografia", "id"),
                ("fact_administrativos", "tiempo_id", "dim_tiempo", "id"),
            ]

            all_ok = True
            for fact, fk_col, dim, dim_pk in fk_checks:
                cur.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {fact} f
                    LEFT JOIN {dim} d ON f.{fk_col} = d.{dim_pk}
                    WHERE d.{dim_pk} IS NULL
                    """
                )
                orphans = cur.fetchone()[0]
                if orphans > 0:
                    logger.warning(
                        "    HUERFANOS: %s.%s -> %s: %d registros sin referencia",
                        fact,
                        fk_col,
                        dim,
                        orphans,
                    )
                    all_ok = False

            if all_ok:
                logger.info("    Todas las FKs son validas (0 huerfanos)")

            logger.info("")
            logger.info("  Top 5 IES por matriculados:")
            cur.execute(
                """
                SELECT di.nombre_ies, SUM(fe.cantidad) as total
                FROM fact_estudiantes fe
                JOIN dim_institucion di ON fe.institucion_id = di.id
                WHERE fe.tipo_evento = 'matriculados'
                GROUP BY di.nombre_ies
                ORDER BY total DESC
                LIMIT 5
                """
            )
            for r in cur.fetchall():
                logger.info(
                    "    %-50s %s",
                    r[0][:50] if r[0] else "DESCONOCIDA",
                    f"{r[1]:,}" if r[1] else "0",
                )

def main():
    logger.info("=" * 60)
    logger.info("CREACION DE TABLAS DE HECHOS — Star Schema SNIES")
    logger.info("Fuente: PostgreSQL schema '%s'", PG_SCHEMA_UNIFIED)
    logger.info("Destino: PostgreSQL schema '%s'", PG_SCHEMA_FACTS)
    logger.info("=" * 60)

    start_time = time.time()

    logger.info("Preparando tablas de hechos (CREATE IF NOT EXISTS + TRUNCATE)...")
    with managed_connection(schema=PG_SCHEMA_FACTS) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL_FACT_ESTUDIANTES)
            cur.execute(DDL_FACT_DOCENTES)
            cur.execute(DDL_FACT_ADMINISTRATIVOS)
            for idx_sql in FACT_INDEXES:
                cur.execute(idx_sql)
            for table in [
                "fact_estudiantes",
                "fact_docentes",
                "fact_administrativos",
            ]:
                cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
    logger.info("Estructura DDL de hechos verificada y tablas truncadas")

    logger.info("")
    logger.info("CONSTRUYENDO MAPPINGS DE DIMENSIONES...")
    logger.info("-" * 50)

    inst_map = load_dim_institucion_mapping()
    geo_map = load_dim_geografia_mapping()
    prog_map = load_dim_programa_mapping()
    tiempo_map = load_dim_tiempo_mapping()
    sexo_map = load_dim_sexo_mapping()
    nivel_form_map = load_dim_nivel_formacion_docente_mapping()
    dedicacion_map = load_dim_dedicacion_docente_mapping()

    logger.info("")
    logger.info("CARGANDO TABLAS DE HECHOS...")
    logger.info("-" * 50)

    n_est = load_fact_estudiantes(inst_map, geo_map, prog_map, tiempo_map, sexo_map)
    n_doc = load_fact_docentes(
        inst_map, geo_map, sexo_map, nivel_form_map, dedicacion_map, tiempo_map
    )
    n_adm = load_fact_administrativos(inst_map, geo_map, tiempo_map)

    validate_star_schema()

    elapsed = time.time() - start_time

    logger.info("")
    logger.info("=" * 60)
    logger.info("STAR SCHEMA CREADO EXITOSAMENTE")
    logger.info("  fact_estudiantes:     %8d filas", n_est)
    logger.info("  fact_docentes:        %8d filas", n_doc)
    logger.info("  fact_administrativos: %8d filas", n_adm)
    logger.info("  Tiempo total:         %.1f segundos", elapsed)
    logger.info("  Destino: PostgreSQL schema '%s'", PG_SCHEMA_FACTS)
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
