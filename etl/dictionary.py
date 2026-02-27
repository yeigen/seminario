import json

import pandas as pd

from config.globals import (
    PROCESSED_DIR,
    DICTIONARIES_DIR,
    DATA_DICTIONARY_JSON_PATH,
    DATA_DICTIONARY_MD_PATH,
    PROJECT_TITLE,
    PG_SCHEMA_RAW,
)
from utils.db import list_tables, managed_connection, table_exists
from utils.logger import logger

def _profile_column_sql(
    schema: str, table: str, column: str, data_type: str, total_rows: int
) -> dict:
    profile: dict = {
        "dtype": data_type,
        "non_null_count": 0,
        "null_count": 0,
        "null_pct": 0.0,
    }

    if total_rows == 0:
        return profile

    try:
        with managed_connection(schema=schema) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT "
                    f'COUNT("{column}") AS non_null, '
                    f'COUNT(*) - COUNT("{column}") AS nulls '
                    f'FROM "{table}"'
                )
                row = cur.fetchone()
                non_null = row[0]
                nulls = row[1]
                profile["non_null_count"] = non_null
                profile["null_count"] = nulls
                profile["null_pct"] = round(nulls / total_rows * 100, 2)

                is_numeric = data_type in (
                    "integer",
                    "bigint",
                    "smallint",
                    "numeric",
                    "real",
                    "double precision",
                    "decimal",
                    "float",
                )

                if is_numeric and non_null > 0:
                    cur.execute(
                        f"SELECT "
                        f'MIN("{column}")::float, '
                        f'MAX("{column}")::float, '
                        f'AVG("{column}")::float, '
                        f'STDDEV("{column}")::float, '
                        f'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{column}")::float '
                        f'FROM "{table}" WHERE "{column}" IS NOT NULL'
                    )
                    stats = cur.fetchone()
                    profile.update(
                        {
                            "min": stats[0] if stats[0] is not None else 0,
                            "max": stats[1] if stats[1] is not None else 0,
                            "mean": round(stats[2], 4) if stats[2] is not None else 0,
                            "std": round(stats[3], 4) if stats[3] is not None else 0,
                            "median": stats[4],
                        }
                    )

                is_text = data_type in (
                    "text",
                    "character varying",
                    "character",
                    "varchar",
                    "char",
                )

                if is_text and non_null > 0:
                    cur.execute(
                        f'SELECT COUNT(DISTINCT "{column}") '
                        f'FROM "{table}" WHERE "{column}" IS NOT NULL'
                    )
                    profile["unique_count"] = cur.fetchone()[0]

                    cur.execute(
                        f'SELECT "{column}", COUNT(*) AS cnt '
                        f'FROM "{table}" WHERE "{column}" IS NOT NULL '
                        f'GROUP BY "{column}" ORDER BY cnt DESC LIMIT 5'
                    )
                    profile["top_values"] = {
                        str(r[0]): int(r[1]) for r in cur.fetchall()
                    }

                    cur.execute(
                        f"SELECT "
                        f'MIN(LENGTH("{column}")), '
                        f'MAX(LENGTH("{column}")) '
                        f'FROM "{table}" WHERE "{column}" IS NOT NULL'
                    )
                    lengths = cur.fetchone()
                    profile["min_length"] = lengths[0] if lengths[0] is not None else 0
                    profile["max_length"] = lengths[1] if lengths[1] is not None else 0

    except Exception as e:
        logger.warning(
            "Error perfilando columna '%s.%s.%s': %s", schema, table, column, e
        )

    return profile

def _generate_dictionary_for_table(schema: str, table: str) -> dict:
    columns = {}
    try:
        with managed_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name, data_type "
                    "FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s "
                    "ORDER BY ordinal_position",
                    (schema, table),
                )
                col_info = cur.fetchall()

        with managed_connection(schema=schema) as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM "{table}"')
                total_rows = cur.fetchone()[0]

        for col_name, data_type in col_info:
            columns[col_name] = _profile_column_sql(
                schema, table, col_name, data_type, total_rows
            )

        return {
            "dataset": table,
            "source_file": f"pg://{schema}.{table}",
            "total_rows": total_rows,
            "total_columns": len(col_info),
            "columns": columns,
        }
    except Exception as e:
        logger.warning("Error generando diccionario para '%s.%s': %s", schema, table, e)
        return {
            "dataset": table,
            "source_file": f"pg://{schema}.{table}",
            "total_rows": 0,
            "total_columns": 0,
            "columns": {},
        }

def profile_column(series: pd.Series) -> dict:
    total = len(series)
    nulls = int(series.isna().sum())
    dtype = str(series.dtype)
    profile: dict = {
        "dtype": dtype,
        "non_null_count": total - nulls,
        "null_count": nulls,
        "null_pct": round(nulls / total * 100, 2) if total > 0 else 0,
    }
    if pd.api.types.is_numeric_dtype(series):
        desc = series.describe()
        profile.update(
            {
                "min": float(desc.get("min", 0)),
                "max": float(desc.get("max", 0)),
                "mean": round(float(desc.get("mean", 0)), 4),
                "std": round(float(desc.get("std", 0)), 4),
                "median": float(series.median()) if not series.isna().all() else None,
            }
        )
    elif pd.api.types.is_string_dtype(series):
        non_null = series.dropna()
        profile["unique_count"] = int(non_null.nunique())
        if len(non_null) > 0:
            top_values = non_null.value_counts().head(5)
            profile["top_values"] = {str(k): int(v) for k, v in top_values.items()}
            profile["min_length"] = int(non_null.str.len().min())
            profile["max_length"] = int(non_null.str.len().max())
    return profile

def generate_dictionary_for_dataset(parquet_path) -> dict:
    df = pd.read_parquet(parquet_path)
    dataset_name = parquet_path.stem
    columns = {}
    for col in df.columns:
        columns[col] = profile_column(df[col])
    return {
        "dataset": dataset_name,
        "source_file": str(parquet_path),
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "columns": columns,
    }

def generate_all_dictionaries():
    logger.info("=== GENERANDO DICCIONARIO DE DATOS ===")
    DICTIONARIES_DIR.mkdir(parents=True, exist_ok=True)
    all_dicts = {}

    pg_tables = list_tables(PG_SCHEMA_RAW)
    if pg_tables:
        logger.info(
            "[SQL] Perfilando %d tablas desde PostgreSQL schema '%s'",
            len(pg_tables),
            PG_SCHEMA_RAW,
        )
        for table_name in sorted(pg_tables):
            key = f"{PG_SCHEMA_RAW}__{table_name}"
            logger.info("  Perfilando %s.%s ...", PG_SCHEMA_RAW, table_name)
            try:
                d = _generate_dictionary_for_table(PG_SCHEMA_RAW, table_name)
                all_dicts[key] = d
            except Exception as e:
                logger.warning("  [ERROR] %s.%s: %s", PG_SCHEMA_RAW, table_name, e)
    else:
        logger.info("[Parquet] PostgreSQL sin tablas, leyendo archivos procesados")
        for parquet_file in sorted(PROCESSED_DIR.rglob("*.parquet")):
            rel = parquet_file.relative_to(PROCESSED_DIR)
            key = str(rel).replace(".parquet", "").replace("/", "__")
            logger.info("  Perfilando %s ...", rel)
            try:
                d = generate_dictionary_for_dataset(parquet_file)
                all_dicts[key] = d
            except Exception as e:
                logger.warning("  [ERROR] %s: %s", parquet_file, e)

    DATA_DICTIONARY_JSON_PATH.write_text(
        json.dumps(all_dicts, indent=2, ensure_ascii=False, default=str)
    )

    _generate_markdown_dictionary(all_dicts)

    logger.info("=== DICCIONARIO COMPLETO: %d datasets ===", len(all_dicts))
    return all_dicts

def _generate_markdown_dictionary(all_dicts: dict):
    lines = [f"# Diccionario de Datos - {PROJECT_TITLE}\n"]

    for key, info in sorted(all_dicts.items()):
        lines.append(f"## {info['dataset']}")
        lines.append(f"- **Filas**: {info['total_rows']:,}")
        lines.append(f"- **Columnas**: {info['total_columns']}")
        lines.append(f"- **Fuente**: `{info['source_file']}`\n")
        lines.append("| Columna | Tipo | No Nulos | % Nulos | Detalle |")
        lines.append("|---------|------|----------|---------|---------|")
        for col_name, col_info in info["columns"].items():
            detail = ""
            if "mean" in col_info:
                detail = (
                    f"min={col_info['min']}, "
                    f"max={col_info['max']}, "
                    f"mean={col_info['mean']}"
                )
            elif "unique_count" in col_info:
                detail = f"unique={col_info['unique_count']}"
            lines.append(
                f"| {col_name} | {col_info['dtype']} | "
                f"{col_info['non_null_count']:,} | "
                f"{col_info['null_pct']}% | {detail} |"
            )
        lines.append("")

    DATA_DICTIONARY_MD_PATH.write_text("\n".join(lines))
