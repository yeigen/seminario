import csv
import hashlib
import io
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import (
    HASH_CHUNK_SIZE,
    RAW_SNIES_DIR,
    SNIES_CATEGORIES,
    PG_SCHEMA_RAW,
    INPUT_EXTENSION_SNIES,
)
from utils.db import (
    ensure_schemas,
    get_column_names,
    get_row_count,
    managed_connection,
)
from utils.logger import logger
from utils.text import normalize_column_name

_IMPORT_LOG_TABLE = "_import_log"

HEADER_SEARCH_ROWS = 15
MIN_HEADER_COLUMNS = 5
HEADER_KEYWORDS = ("código", "codigo", "ies", "institución", "institucion")

_BASE_COLUMNS = [
    "codigo_de_la_institucion",
    "ies_padre",
    "institucion_de_educacion_superior_ies",
    "principal_o_seccional",
    "id_sector_ies",
    "sector_ies",
    "id_caracter",
    "caracter_ies",
    "codigo_del_departamento_ies",
    "departamento_de_domicilio_de_la_ies",
]

_MUNICIPIO_IES_COLUMNS = [
    "codigo_del_municipio_ies",
    "municipio_de_domicilio_de_la_ies",
]

_PROGRAMA_COLUMNS = [
    "codigo_snies_del_programa",
    "programa_academico",
    "id_nivel_academico",
    "nivel_academico",
    "id_nivel_de_formacion",
    "nivel_de_formacion",
    "id_metodologia",
    "metodologia",
    "id_area",
    "area_de_conocimiento",
    "id_nucleo",
    "nucleo_basico_del_conocimiento_nbc",
    "codigo_del_departamento_programa",
    "departamento_de_oferta_del_programa",
    "codigo_del_municipio_programa",
    "municipio_de_oferta_del_programa",
]

_SEXO_PERIODO = ["id_sexo", "sexo", "ano", "semestre"]

_STANDARD_WITH_PROGRAMA = (
    _BASE_COLUMNS + _MUNICIPIO_IES_COLUMNS + _PROGRAMA_COLUMNS + _SEXO_PERIODO
)

FALLBACK_SCHEMAS: dict[str, list[str]] = {
    "matriculados": _STANDARD_WITH_PROGRAMA + ["matriculados"],
    "inscritos": _STANDARD_WITH_PROGRAMA + ["inscritos"],
    "admitidos": _STANDARD_WITH_PROGRAMA + ["admitidos"],
    "matriculados_primer_curso": _STANDARD_WITH_PROGRAMA
    + ["matriculados_primer_curso"],
    "graduados": (
        _BASE_COLUMNS
        + ["codigo_del_municipio", "municipio_de_domicilio_de_la_ies"]
        + _PROGRAMA_COLUMNS
        + _SEXO_PERIODO
        + ["graduados"]
    ),
    "docentes": (
        _BASE_COLUMNS
        + _MUNICIPIO_IES_COLUMNS
        + [
            "id_sexo",
            "sexo_del_docente",
            "id_maximo_nivel_de_formacion_del_docente",
            "maximo_nivel_de_formacion_del_docente",
            "id_tiempo_de_dedicacion",
            "tiempo_de_dedicacion_del_docente",
            "id_tipo_de_contrato",
            "tipo_de_contrato_del_docente",
            "ano",
            "semestre",
            "no_de_docentes",
        ]
    ),
    "administrativos": (
        _BASE_COLUMNS
        + _MUNICIPIO_IES_COLUMNS
        + [
            "ano",
            "semestre",
            "auxiliar",
            "tecnico",
            "profesional",
            "directivo",
            "total",
        ]
    ),
}

def _file_md5(path: Path) -> str:
    h = hashlib.md5(usedforsecurity=False)
    with open(path, "rb") as f:
        while chunk := f.read(HASH_CHUNK_SIZE):
            h.update(chunk)
    return h.hexdigest()

def _ensure_import_log_table() -> None:
    with managed_connection(schema=PG_SCHEMA_RAW) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE IF NOT EXISTS "{_IMPORT_LOG_TABLE}" ('
                "  id SERIAL PRIMARY KEY,"
                "  category TEXT NOT NULL,"
                "  file_name TEXT NOT NULL,"
                "  file_hash TEXT NOT NULL,"
                "  rows_imported INTEGER NOT NULL DEFAULT 0,"
                "  imported_at TIMESTAMP DEFAULT NOW(),"
                "  UNIQUE(category, file_name, file_hash)"
                ")"
            )

def _is_file_already_imported(category: str, file_name: str, file_hash: str) -> bool:
    with managed_connection(schema=PG_SCHEMA_RAW) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT EXISTS ("
                f'  SELECT 1 FROM "{_IMPORT_LOG_TABLE}"'
                f"  WHERE category = %s AND file_name = %s AND file_hash = %s"
                f")",
                (category, file_name, file_hash),
            )
            result = cur.fetchone()
            return bool(result and result[0])

def _register_imported_file(
    category: str, file_name: str, file_hash: str, rows: int
) -> None:
    with managed_connection(schema=PG_SCHEMA_RAW) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO "{_IMPORT_LOG_TABLE}" '
                f"(category, file_name, file_hash, rows_imported) "
                f"VALUES (%s, %s, %s, %s) "
                f"ON CONFLICT (category, file_name, file_hash) DO NOTHING",
                (category, file_name, file_hash, rows),
            )

def detect_header_row(path: Path, sheet_name: str | int = 0) -> int | None:
    df_raw = pd.read_excel(
        path,
        sheet_name=sheet_name,
        engine="openpyxl",
        header=None,
        nrows=HEADER_SEARCH_ROWS,
    )
    for idx, row in df_raw.iterrows():
        non_null = row.dropna()
        if len(non_null) < MIN_HEADER_COLUMNS:
            continue
        values_lower = " ".join(str(v).lower() for v in non_null.values)
        if any(kw in values_lower for kw in HEADER_KEYWORDS):
            return int(str(idx))
    return None

def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [normalize_column_name(str(c)) for c in df.columns]
    df = df.dropna(how="all").reset_index(drop=True)
    unnamed = [c for c in df.columns if c.startswith("unnamed")]
    if unnamed:
        df = df.drop(columns=unnamed)
    return df

def _find_best_sheet(path: Path) -> tuple[str | int, int]:
    xls = pd.ExcelFile(path, engine="openpyxl")
    for sheet_name in xls.sheet_names:
        header_row = detect_header_row(path, sheet_name=sheet_name)
        if header_row is not None:
            return sheet_name, header_row
    return xls.sheet_names[0], 0

def read_excel_with_header(path: Path) -> pd.DataFrame | None:
    xls = pd.ExcelFile(path, engine="openpyxl")

    for sheet_name in xls.sheet_names:
        header_row = detect_header_row(path, sheet_name=sheet_name)
        if header_row is not None:
            df = pd.read_excel(xls, sheet_name=sheet_name, header=header_row)
            df = _clean_dataframe(df)
            if not df.empty:
                logger.debug(
                    "    Usando hoja '%s' (header en fila %d)", sheet_name, header_row
                )
                return df

    for sheet_name in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name, header=0)
            df = _clean_dataframe(df)
            if not df.empty:
                logger.warning(
                    "No se detectó header con keywords en %s, usando hoja '%s' fila 0",
                    path.name,
                    sheet_name,
                )
                return df
        except Exception:
            continue

    logger.warning("No se pudo leer ningún dato válido de %s", path.name)
    return None

def create_empty_table(table_name: str, columns: list[str]):
    cols_sql = ", ".join(f'"{c}" TEXT' for c in columns)
    with managed_connection(schema=PG_SCHEMA_RAW) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE IF NOT EXISTS "{table_name}" '
                f"(id SERIAL PRIMARY KEY, {cols_sql})"
            )

def _bulk_insert_copy(conn, table_name: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    columns = list(df.columns)
    cols_quoted = ", ".join(f'"{c}"' for c in columns)
    buf = io.StringIO()
    df.to_csv(
        buf,
        index=False,
        header=False,
        sep="\t",
        na_rep="\\N",
        quoting=csv.QUOTE_NONE,
        escapechar="\\",
    )
    buf.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY \"{table_name}\" ({cols_quoted}) FROM STDIN WITH (FORMAT text, NULL '\\N')",
            buf,
        )

def collect_xlsx_files(category: str) -> list[Path]:
    category_dir = RAW_SNIES_DIR / category
    if not category_dir.exists():
        return []
    return sorted(category_dir.glob(f"*{INPUT_EXTENSION_SNIES}"))

def build_unified_schema(files: list[Path]) -> list[str]:
    all_columns: list[str] = []
    seen: set[str] = set()
    for path in files:
        sheet_name, header_row = _find_best_sheet(path)
        df = pd.read_excel(
            path, sheet_name=sheet_name, engine="openpyxl", header=header_row, nrows=0
        )
        for col in df.columns:
            normalized = normalize_column_name(str(col))
            if (
                normalized
                and not normalized.startswith("unnamed")
                and normalized not in seen
            ):
                all_columns.append(normalized)
                seen.add(normalized)
    return all_columns

def process_category(category: str):
    logger.info("[%s] Procesando categoría", category)
    files = collect_xlsx_files(category)

    if files:
        logger.info("[%s] Encontrados %d archivos Excel", category, len(files))
        schema = build_unified_schema(files)
        create_empty_table(category, schema)

        imported_count = 0
        skipped_count = 0

        for path in files:
            file_hash = _file_md5(path)

            if _is_file_already_imported(category, path.name, file_hash):
                logger.info(
                    "[%s] SKIP %s (ya importado, hash: %s...)",
                    category,
                    path.name,
                    file_hash[:8],
                )
                skipped_count += 1
                continue

            logger.info("[%s] Importando %s...", category, path.name)
            df = read_excel_with_header(path)
            if df is not None and not df.empty:
                existing_cols = set(get_column_names(PG_SCHEMA_RAW, category))
                with managed_connection(schema=PG_SCHEMA_RAW) as conn:
                    with conn.cursor() as cur:
                        new_cols = [c for c in df.columns if c not in existing_cols]
                        for col in new_cols:
                            cur.execute(
                                f'ALTER TABLE "{category}" ADD COLUMN "{col}" TEXT'
                            )
                    _bulk_insert_copy(conn, category, df)
                _register_imported_file(category, path.name, file_hash, len(df))
                imported_count += 1
                logger.info(
                    "[%s] %d filas importadas de %s", category, len(df), path.name
                )
            else:
                logger.warning("[%s] Sin datos válidos en %s", category, path.name)

        if skipped_count > 0:
            logger.info(
                "[%s] Resumen: %d importados, %d omitidos (ya existían)",
                category,
                imported_count,
                skipped_count,
            )
    else:
        logger.info(
            "[%s] Sin archivos locales, creando tabla con esquema base", category
        )
        fallback = FALLBACK_SCHEMAS.get(category, ["ano", "semestre"])
        create_empty_table(category, fallback)

    row_count = get_row_count(PG_SCHEMA_RAW, category)
    col_count = len(get_column_names(PG_SCHEMA_RAW, category))
    logger.info(
        "[%s] Tabla lista: %d filas, %d columnas", category, row_count, col_count
    )

def create_db():
    logger.info("=" * 60)
    logger.info("Cargando tablas en PostgreSQL (schema: %s)", PG_SCHEMA_RAW)
    logger.info("=" * 60)

    ensure_schemas()
    _ensure_import_log_table()

    with managed_connection(schema=PG_SCHEMA_RAW) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = ANY(%s)",
                (PG_SCHEMA_RAW, list(SNIES_CATEGORIES)),
            )
            existing_tables = {row[0] for row in cur.fetchall()}

    for table_name in SNIES_CATEGORIES:
        if table_name in existing_tables:
            count = get_row_count(PG_SCHEMA_RAW, table_name)
            logger.info(
                "Tabla '%s' existente (%d filas) — se preserva", table_name, count
            )
        else:
            logger.info("Tabla '%s' no existe — se creará", table_name)

    for category in SNIES_CATEGORIES:
        process_category(category)

    logger.info("=" * 60)
    logger.info("Resumen final")
    logger.info("=" * 60)
    for category in SNIES_CATEGORIES:
        row_count = get_row_count(PG_SCHEMA_RAW, category)
        col_count = len(get_column_names(PG_SCHEMA_RAW, category))
        status = f"{row_count:>8} filas" if row_count > 0 else "    vacía"
        logger.info("  %-30s %s | %d cols", category, status, col_count)

    logger.info("Carga completada en schema '%s'", PG_SCHEMA_RAW)

def main():
    create_db()

if __name__ == "__main__":
    main()
