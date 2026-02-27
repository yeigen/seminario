import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

RAW_DIR = Path(__file__).parents[1] / "data" / "raw"
PROCESSED_DIR = Path(__file__).parents[1] / "data" / "processed"
LINEAGE_PATH = PROCESSED_DIR / "_lineage.json"

def load_lineage() -> list:
    if LINEAGE_PATH.exists():
        return json.loads(LINEAGE_PATH.read_text())
    return []


def save_lineage(lineage: list):
    LINEAGE_PATH.parent.mkdir(parents=True, exist_ok=True)
    LINEAGE_PATH.write_text(json.dumps(lineage, indent=2, ensure_ascii=False))


def record_lineage(
    source: str, dest: str, operation: str, rows_in: int, rows_out: int, cols: int
):
    lineage = load_lineage()
    lineage.append(
        {
            "source": source,
            "destination": dest,
            "operation": operation,
            "rows_input": rows_in,
            "rows_output": rows_out,
            "columns": cols,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )
    save_lineage(lineage)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(r"[áà]", "a", regex=True)
        .str.replace(r"[éè]", "e", regex=True)
        .str.replace(r"[íì]", "i", regex=True)
        .str.replace(r"[óò]", "o", regex=True)
        .str.replace(r"[úù]", "u", regex=True)
        .str.replace(r"[ñ]", "n", regex=True)
        .str.replace(r"[\s\-\.]+", "_", regex=True)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    return df


def clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.strip()
        df[col] = df[col].replace(r"^\s*$", pd.NA, regex=True)
    return df


def drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(how="all").reset_index(drop=True)


def standardize_year_column(df: pd.DataFrame, year_value: int) -> pd.DataFrame:
    year_candidates = [
        c for c in df.columns if "ano" in c or "year" in c or "periodo" in c
    ]
    if not year_candidates:
        df["anio"] = year_value
    return df


def clean_snies_file(path: Path, category: str, year: str) -> pd.DataFrame | None:
    try:
        df = pd.read_excel(path, engine="openpyxl")
    except Exception as e:
        print(f"  [ERROR] No se pudo leer {path}: {e}")
        return None

    rows_in = len(df)
    df = normalize_columns(df)
    df = clean_text_columns(df)
    df = drop_empty_rows(df)
    df = standardize_year_column(df, int(year))

    dest = PROCESSED_DIR / "snies" / category / f"{category}-{year}.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dest, index=False)

    record_lineage(
        str(path),
        str(dest),
        f"clean_snies_{category}",
        rows_in,
        len(df),
        len(df.columns),
    )
    return df


def clean_csv_file(path: Path, dest_name: str, sep: str = ",") -> pd.DataFrame | None:
    try:
        for encoding in ["utf-8", "latin-1", "cp1252"]:
            try:
                df = pd.read_csv(path, sep=sep, encoding=encoding, low_memory=False)
                break
            except UnicodeDecodeError:
                continue
        else:
            print(f"  [ERROR] No se pudo decodificar {path}")
            return None
    except Exception as e:
        print(f"  [ERROR] No se pudo leer {path}: {e}")
        return None

    rows_in = len(df)
    df = normalize_columns(df)
    df = clean_text_columns(df)
    df = drop_empty_rows(df)

    dest = PROCESSED_DIR / f"{dest_name}.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dest, index=False)

    record_lineage(
        str(path), str(dest), f"clean_{dest_name}", rows_in, len(df), len(df.columns)
    )
    return df


def transform_all():
    print("=== TRANSFORMACIÓN Y LIMPIEZA ===\n")
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    results = {}

    print("[1/3] Limpiando SNIES...")
    snies_dir = RAW_DIR / "snies"
    if snies_dir.exists():
        for category_dir in sorted(snies_dir.iterdir()):
            if not category_dir.is_dir():
                continue
            category = category_dir.name
            for xlsx_file in sorted(category_dir.glob("*.xlsx")):
                year = xlsx_file.stem.split("-")[-1]
                print(f"  Procesando {category}/{year}...")
                df = clean_snies_file(xlsx_file, category, year)
                if df is not None:
                    results[f"snies/{category}/{year}"] = {
                        "rows": len(df),
                        "cols": len(df.columns),
                        "columns": list(df.columns),
                    }

    print("\n[2/3] Limpiando Seguimiento PND...")
    pnd_path = RAW_DIR / "pnd" / "seguimiento_pnd.csv"
    if pnd_path.exists():
        df = clean_csv_file(pnd_path, "pnd/seguimiento_pnd")
        if df is not None:
            results["pnd/seguimiento_pnd"] = {
                "rows": len(df),
                "cols": len(df.columns),
                "columns": list(df.columns),
            }

    print("\n[3/3] Limpiando Saber 3-5-9...")
    saber_path = RAW_DIR / "icfes" / "saber_359.csv"
    if saber_path.exists():
        df = clean_csv_file(saber_path, "icfes/saber_359", sep=",")
        if df is not None:
            results["icfes/saber_359"] = {
                "rows": len(df),
                "cols": len(df.columns),
                "columns": list(df.columns),
            }

    print(f"\n=== TRANSFORMACIÓN COMPLETA: {len(results)} datasets ===")
    return results
