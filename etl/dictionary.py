import json

import pandas as pd

from config.globals import (
    PROCESSED_DIR,
    DICTIONARIES_DIR,
    DATA_DICTIONARY_JSON_PATH,
    DATA_DICTIONARY_MD_PATH,
    PROJECT_TITLE,
)

def profile_column(series: pd.Series) -> dict:
    total = len(series)
    nulls = int(series.isna().sum())
    dtype = str(series.dtype)
    profile = {
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
    print("=== GENERANDO DICCIONARIO DE DATOS ===\n")
    DICTIONARIES_DIR.mkdir(parents=True, exist_ok=True)
    all_dicts = {}

    for parquet_file in sorted(PROCESSED_DIR.rglob("*.parquet")):
        rel = parquet_file.relative_to(PROCESSED_DIR)
        key = str(rel).replace(".parquet", "").replace("/", "__")
        print(f"  Perfilando {rel}...")
        try:
            d = generate_dictionary_for_dataset(parquet_file)
            all_dicts[key] = d
        except Exception as e:
            print(f"  [ERROR] {parquet_file}: {e}")

    DATA_DICTIONARY_JSON_PATH.write_text(
        json.dumps(all_dicts, indent=2, ensure_ascii=False, default=str)
    )

    _generate_markdown_dictionary(all_dicts)

    print(f"\n=== DICCIONARIO COMPLETO: {len(all_dicts)} datasets ===")
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
                detail = f"min={col_info['min']}, max={col_info['max']}, mean={col_info['mean']}"
            elif "unique_count" in col_info:
                detail = f"unique={col_info['unique_count']}"
            lines.append(
                f"| {col_name} | {col_info['dtype']} | "
                f"{col_info['non_null_count']:,} | "
                f"{col_info['null_pct']}% | {detail} |"
            )
        lines.append("")

    DATA_DICTIONARY_MD_PATH.write_text("\n".join(lines))
