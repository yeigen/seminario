from __future__ import annotations

import argparse
import csv
import io
import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import PG_SCHEMA_RAW, PROCESSED_DIR, RAW_ICFES_DIR
from utils.db import ensure_schemas, managed_connection
from utils.logger import logger
from utils.text import remove_accents

CHUNK_SIZE = 200_000

SABER11_INPUT = RAW_ICFES_DIR / "saber11_puntajes_2018_2024.csv"
SABERPRO_INPUT = RAW_ICFES_DIR / "saberpro_puntajes_2018_2024.csv"
PROCESSED_ICFES_DIR = PROCESSED_DIR / "icfes"

SABER11_TABLE = "icfes_saber11_puntajes_trimestral"
SABERPRO_TABLE = "icfes_saberpro_puntajes_trimestral"

_SPACE_RE = re.compile(r"\s+")
_UNDERSCORE_RE = re.compile(r"_+")


def normalize_value(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    text = remove_accents(text.lower())
    text = _SPACE_RE.sub("_", text)
    text = _UNDERSCORE_RE.sub("_", text).strip("_")
    return text or None


def normalize_text_columns(df: pd.DataFrame, columns: list[str]) -> None:
    for col in columns:
        if col in df.columns:
            df[col] = df[col].map(normalize_value)


def to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def parse_period_columns(df: pd.DataFrame) -> pd.DataFrame:
    periodo = df["periodo"].astype("string").str.strip()
    df["ano"] = pd.to_numeric(periodo.str[:4], errors="coerce").astype("Int64")
    componente = pd.to_numeric(periodo.str[-1], errors="coerce").astype("Int64")
    df["periodo_componente"] = componente
    df["trimestre"] = componente.where(componente.between(1, 4)).astype("Int64")
    return df


def aggregate_saber11(path: Path) -> pd.DataFrame:
    usecols = [
        "periodo",
        "cole_cod_depto_ubicacion",
        "cole_depto_ubicacion",
        "cole_cod_mcpio_ubicacion",
        "cole_mcpio_ubicacion",
        "cole_codigo_icfes",
        "cole_nombre_establecimiento",
        "cole_naturaleza",
        "punt_global",
    ]
    group_cols = [
        "ano",
        "trimestre",
        "periodo_componente",
        "departamento_codigo",
        "departamento",
        "municipio_codigo",
        "municipio",
        "colegio_icfes_codigo",
        "colegio",
        "colegio_naturaleza",
    ]
    partials: list[pd.DataFrame] = []

    for chunk in pd.read_csv(path, usecols=usecols, chunksize=CHUNK_SIZE, dtype="string"):
        chunk = parse_period_columns(chunk)
        chunk["departamento_codigo"] = to_number(chunk["cole_cod_depto_ubicacion"]).astype("Int64")
        chunk["municipio_codigo"] = to_number(chunk["cole_cod_mcpio_ubicacion"]).astype("Int64")
        chunk["colegio_icfes_codigo"] = to_number(chunk["cole_codigo_icfes"]).astype("Int64")
        chunk["puntaje_global"] = to_number(chunk["punt_global"])
        chunk = chunk.rename(
            columns={
                "cole_depto_ubicacion": "departamento",
                "cole_mcpio_ubicacion": "municipio",
                "cole_nombre_establecimiento": "colegio",
                "cole_naturaleza": "colegio_naturaleza",
            }
        )
        normalize_text_columns(
            chunk,
            ["departamento", "municipio", "colegio", "colegio_naturaleza"],
        )
        chunk = chunk.dropna(subset=["ano", "puntaje_global"])
        partial = (
            chunk.groupby(group_cols, dropna=False, observed=True)["puntaje_global"]
            .agg(suma_puntaje_global="sum", observaciones="count")
            .reset_index()
        )
        partials.append(partial)

    if not partials:
        return pd.DataFrame()

    df = pd.concat(partials, ignore_index=True)
    df = (
        df.groupby(group_cols, dropna=False, observed=True)
        .agg(suma_puntaje_global=("suma_puntaje_global", "sum"), observaciones=("observaciones", "sum"))
        .reset_index()
    )
    df["promedio_puntaje_global"] = df["suma_puntaje_global"] / df["observaciones"]
    return df.drop(columns=["suma_puntaje_global"]).sort_values(group_cols).reset_index(drop=True)


def aggregate_saberpro(path: Path) -> pd.DataFrame:
    score_cols = [
        "mod_razona_cuantitat_punt",
        "mod_comuni_escrita_punt",
        "mod_lectura_critica_punt",
        "mod_ingles_punt",
        "mod_competen_ciudada_punt",
    ]
    usecols = [
        "periodo",
        "estu_inst_departamento",
        "estu_inst_codmunicipio",
        "estu_inst_municipio",
        "inst_cod_institucion",
        "inst_nombre_institucion",
        "inst_caracter_academico",
        "inst_origen",
        *score_cols,
    ]
    group_cols = [
        "ano",
        "trimestre",
        "periodo_componente",
        "departamento",
        "municipio_codigo",
        "municipio",
        "institucion_codigo",
        "institucion",
        "tipo_institucion",
        "origen_institucion",
    ]
    partials: list[pd.DataFrame] = []

    for chunk in pd.read_csv(path, usecols=usecols, chunksize=CHUNK_SIZE, dtype="string"):
        chunk = parse_period_columns(chunk)
        chunk["municipio_codigo"] = to_number(chunk["estu_inst_codmunicipio"]).astype("Int64")
        chunk["institucion_codigo"] = to_number(chunk["inst_cod_institucion"]).astype("Int64")
        chunk = chunk.rename(
            columns={
                "estu_inst_departamento": "departamento",
                "estu_inst_municipio": "municipio",
                "inst_nombre_institucion": "institucion",
                "inst_caracter_academico": "tipo_institucion",
                "inst_origen": "origen_institucion",
            }
        )
        normalize_text_columns(
            chunk,
            ["departamento", "municipio", "institucion", "tipo_institucion", "origen_institucion"],
        )
        for col in score_cols:
            chunk[col] = to_number(chunk[col])
        chunk["promedio_modulos"] = chunk[score_cols].mean(axis=1, skipna=True)
        chunk = chunk.dropna(subset=["ano"])

        agg_map = {col: (col, "sum") for col in score_cols}
        agg_map.update({f"{col}_n": (col, "count") for col in score_cols})
        agg_map["suma_promedio_modulos"] = ("promedio_modulos", "sum")
        agg_map["observaciones"] = ("promedio_modulos", "count")

        partial = chunk.groupby(group_cols, dropna=False, observed=True).agg(**agg_map).reset_index()
        partials.append(partial)

    if not partials:
        return pd.DataFrame()

    df = pd.concat(partials, ignore_index=True)
    agg_map = {col: (col, "sum") for col in score_cols}
    agg_map.update({f"{col}_n": (f"{col}_n", "sum") for col in score_cols})
    agg_map["suma_promedio_modulos"] = ("suma_promedio_modulos", "sum")
    agg_map["observaciones"] = ("observaciones", "sum")
    df = df.groupby(group_cols, dropna=False, observed=True).agg(**agg_map).reset_index()

    rename_scores = {
        "mod_razona_cuantitat_punt": "promedio_razonamiento_cuantitativo",
        "mod_comuni_escrita_punt": "promedio_comunicacion_escrita",
        "mod_lectura_critica_punt": "promedio_lectura_critica",
        "mod_ingles_punt": "promedio_ingles",
        "mod_competen_ciudada_punt": "promedio_competencias_ciudadanas",
    }
    for source_col, target_col in rename_scores.items():
        df[target_col] = df[source_col] / df[f"{source_col}_n"].replace(0, pd.NA)
    df["promedio_modulos"] = df["suma_promedio_modulos"] / df["observaciones"]
    return (
        df.drop(columns=[*score_cols, *(f"{col}_n" for col in score_cols), "suma_promedio_modulos"])
        .sort_values(group_cols)
        .reset_index(drop=True)
    )


def _sql_type(col: str) -> str:
    if col in {"ano", "trimestre", "periodo_componente", "departamento_codigo"}:
        return "SMALLINT"
    if col.endswith("_codigo") or col == "colegio_icfes_codigo":
        return "INTEGER"
    if col == "observaciones":
        return "INTEGER"
    if col.startswith("promedio_"):
        return "NUMERIC(10,2)"
    return "TEXT"


def create_table(table_name: str, columns: list[str]) -> None:
    cols_sql = ", ".join(f'"{col}" {_sql_type(col)}' for col in columns)
    with managed_connection(schema=PG_SCHEMA_RAW) as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            cur.execute(f'CREATE TABLE "{table_name}" ({cols_sql})')


def copy_dataframe(table_name: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    columns = list(df.columns)
    buf = io.StringIO()
    df.to_csv(
        buf,
        index=False,
        header=False,
        sep="\t",
        na_rep="\\N",
        quoting=csv.QUOTE_NONE,
        escapechar="\\",
        float_format="%.2f",
    )
    buf = io.StringIO(buf.getvalue().replace("\\\\N", "\\N"))
    buf.seek(0)
    cols = ", ".join(f'"{col}"' for col in columns)
    with managed_connection(schema=PG_SCHEMA_RAW) as conn:
        with conn.cursor() as cur:
            copy_sql = (
                f'COPY "{table_name}" ({cols}) FROM STDIN '
                + r"WITH (FORMAT text, NULL '\N')"
            )
            cur.copy_expert(
                copy_sql,
                buf,
            )


def create_indexes(table_name: str) -> None:
    with managed_connection(schema=PG_SCHEMA_RAW) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_periodo" ON "{table_name}" (ano, trimestre)')
            cur.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_municipio" ON "{table_name}" (municipio_codigo)')
            code_col = "colegio_icfes_codigo" if table_name == SABER11_TABLE else "institucion_codigo"
            cur.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_entidad" ON "{table_name}" ({code_col})')


def save_outputs(df: pd.DataFrame, table_name: str) -> None:
    PROCESSED_ICFES_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PROCESSED_ICFES_DIR / f"{table_name}.csv"
    df.to_csv(out_path, index=False)
    logger.info("Archivo normalizado escrito: %s (%d filas)", out_path, len(df))

    create_table(table_name, list(df.columns))
    copy_dataframe(table_name, df)
    create_indexes(table_name)
    logger.info("Tabla raw.%s cargada: %d filas", table_name, len(df))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Normaliza y agrega puntajes ICFES por año/trimestre")
    parser.add_argument("--saber11", type=Path, default=SABER11_INPUT)
    parser.add_argument("--saberpro", type=Path, default=SABERPRO_INPUT)
    parser.add_argument("--skip-saber11", action="store_true")
    parser.add_argument("--skip-saberpro", action="store_true")
    args = parser.parse_args(argv)

    ensure_schemas()

    if not args.skip_saber11:
        if not args.saber11.exists():
            logger.warning("No existe archivo Saber 11: %s", args.saber11)
        else:
            logger.info("Normalizando Saber 11 desde %s", args.saber11)
            save_outputs(aggregate_saber11(args.saber11), SABER11_TABLE)

    if not args.skip_saberpro:
        if not args.saberpro.exists():
            logger.warning("No existe archivo Saber Pro: %s", args.saberpro)
        else:
            logger.info("Normalizando Saber Pro desde %s", args.saberpro)
            save_outputs(aggregate_saberpro(args.saberpro), SABERPRO_TABLE)


if __name__ == "__main__":
    main()
