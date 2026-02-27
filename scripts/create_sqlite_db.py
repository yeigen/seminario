import re
import sqlite3
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import (
    DATA_DIR,
    RAW_SNIES_DIR,
    SNIES_CATEGORIES,
    SQLITE_DB_PATH,
    INPUT_EXTENSION_SNIES,
)
from utils.logger import logger

HEADER_SEARCH_ROWS = 15
MIN_HEADER_COLUMNS = 5
HEADER_KEYWORDS = ("código", "codigo", "ies", "institución", "institucion")

FALLBACK_SCHEMAS: dict[str, list[str]] = {
    "matriculados": [
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
        "codigo_del_municipio_ies",
        "municipio_de_domicilio_de_la_ies",
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
        "id_sexo",
        "sexo",
        "ano",
        "semestre",
        "matriculados",
    ],
    "inscritos": [
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
        "codigo_del_municipio_ies",
        "municipio_de_domicilio_de_la_ies",
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
        "id_sexo",
        "sexo",
        "ano",
        "semestre",
        "inscritos",
    ],
    "graduados": [
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
        "codigo_del_municipio",
        "municipio_de_domicilio_de_la_ies",
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
        "id_sexo",
        "sexo",
        "ano",
        "semestre",
        "graduados",
    ],
    "docentes": [
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
        "codigo_del_municipio_ies",
        "municipio_de_domicilio_de_la_ies",
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
    ],
    "admitidos": [
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
        "codigo_del_municipio_ies",
        "municipio_de_domicilio_de_la_ies",
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
        "id_sexo",
        "sexo",
        "ano",
        "semestre",
        "admitidos",
    ],
    "administrativos": [
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
        "codigo_del_municipio_ies",
        "municipio_de_domicilio_de_la_ies",
        "ano",
        "semestre",
        "auxiliar",
        "tecnico",
        "profesional",
        "directivo",
        "total",
    ],
    "matriculados_primer_curso": [
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
        "codigo_del_municipio_ies",
        "municipio_de_domicilio_de_la_ies",
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
        "id_sexo",
        "sexo",
        "ano",
        "semestre",
        "matriculados_primer_curso",
    ],
}


def normalize_column_name(col: str) -> str:
    col = col.strip().lower()
    col = re.sub(r"[áà]", "a", col)
    col = re.sub(r"[éè]", "e", col)
    col = re.sub(r"[íì]", "i", col)
    col = re.sub(r"[óò]", "o", col)
    col = re.sub(r"[úù]", "u", col)
    col = re.sub(r"[ñ]", "n", col)
    col = re.sub(r"[\s\-\.]+", "_", col)
    col = re.sub(r"[^a-z0-9_]", "", col)
    return col


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


def read_excel_with_header(path: Path) -> pd.DataFrame | None:
    xls = pd.ExcelFile(path, engine="openpyxl")
    for sheet_name in xls.sheet_names:
        header_row = detect_header_row(path, sheet_name=sheet_name)
        if header_row is not None:
            df = pd.read_excel(
                path, sheet_name=sheet_name, engine="openpyxl", header=header_row
            )
            df = _clean_dataframe(df)
            if not df.empty:
                logger.debug(
                    "    Usando hoja '%s' (header en fila %d)", sheet_name, header_row
                )
                return df

    for sheet_name in xls.sheet_names:
        try:
            df = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl", header=0)
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


def create_empty_table(conn: sqlite3.Connection, table_name: str, columns: list[str]):
    cols_sql = ", ".join(f'"{c}" TEXT' for c in columns)
    conn.execute(
        f'CREATE TABLE IF NOT EXISTS "{table_name}" (id INTEGER PRIMARY KEY AUTOINCREMENT, {cols_sql})'
    )


def import_dataframe(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame):
    df.to_sql(table_name, conn, if_exists="append", index=False)


def collect_xlsx_files(category: str) -> list[Path]:
    category_dir = RAW_SNIES_DIR / category
    if not category_dir.exists():
        return []
    return sorted(category_dir.glob(f"*{INPUT_EXTENSION_SNIES}"))


def _find_best_sheet(path: Path) -> tuple[str | int, int]:
    xls = pd.ExcelFile(path, engine="openpyxl")
    for sheet_name in xls.sheet_names:
        header_row = detect_header_row(path, sheet_name=sheet_name)
        if header_row is not None:
            return sheet_name, header_row
    return xls.sheet_names[0], 0


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


def process_category(conn: sqlite3.Connection, category: str):
    logger.info("[%s] Procesando categoría", category)
    files = collect_xlsx_files(category)

    if files:
        logger.info("[%s] Encontrados %d archivos Excel", category, len(files))
        schema = build_unified_schema(files)
        create_empty_table(conn, category, schema)

        for path in files:
            logger.info("[%s] Importando %s...", category, path.name)
            df = read_excel_with_header(path)
            if df is not None and not df.empty:
                existing_cols = {
                    row[1]
                    for row in conn.execute(
                        f'PRAGMA table_info("{category}")'
                    ).fetchall()
                }
                for col in df.columns:
                    if col not in existing_cols:
                        conn.execute(
                            f'ALTER TABLE "{category}" ADD COLUMN "{col}" TEXT'
                        )
                        existing_cols.add(col)
                import_dataframe(conn, category, df)
                logger.info(
                    "[%s] %d filas importadas de %s", category, len(df), path.name
                )
            else:
                logger.warning("[%s] Sin datos válidos en %s", category, path.name)

        conn.commit()
    else:
        logger.info(
            "[%s] Sin archivos locales, creando tabla con esquema base", category
        )
        fallback = FALLBACK_SCHEMAS.get(category, ["ano", "semestre"])
        create_empty_table(conn, category, fallback)
        conn.commit()

    row_count = conn.execute(f'SELECT COUNT(*) FROM "{category}"').fetchone()[0]
    col_count = len(conn.execute(f'PRAGMA table_info("{category}")').fetchall())
    logger.info(
        "[%s] Tabla creada: %d filas, %d columnas", category, row_count, col_count
    )


def main():
    logger.info("=" * 60)
    logger.info("Creando base de datos SQLite para datos SNIES")
    logger.info("Destino: %s", SQLITE_DB_PATH)
    logger.info("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if SQLITE_DB_PATH.exists():
        SQLITE_DB_PATH.unlink()
        logger.info("Base de datos anterior eliminada")

    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    for category in SNIES_CATEGORIES:
        process_category(conn, category)

    logger.info("=" * 60)
    logger.info("Resumen final")
    logger.info("=" * 60)
    for category in SNIES_CATEGORIES:
        row_count = conn.execute(f'SELECT COUNT(*) FROM "{category}"').fetchone()[0]
        col_count = len(conn.execute(f'PRAGMA table_info("{category}")').fetchall())
        status = f"{row_count:>8} filas" if row_count > 0 else "    vacía"
        logger.info("  %-30s %s | %d cols", category, status, col_count)

    conn.close()
    size_mb = SQLITE_DB_PATH.stat().st_size / (1024 * 1024)
    logger.info("Base de datos creada: %s (%.1f MB)", SQLITE_DB_PATH, size_mb)


if __name__ == "__main__":
    main()
