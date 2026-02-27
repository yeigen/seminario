import json
import sqlite3

import numpy as np
import pandas as pd

from config.globals import (
    PROCESSED_DIR,
    QUALITY_REPORTS_DIR,
    QUALITY_REPORT_PATH,
    QUALITY_NULL_THRESHOLD_PCT,
    QUALITY_MIN_COLUMNS,
    CSV_DATASETS,
    SQLITE_DB_PATH,
    SNIES_CATEGORIES,
    processed_parquet_path,
)


class NumpyEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (np.bool_, np.generic)):
            return o.item()
        if isinstance(o, np.ndarray):
            return o.tolist()
        return super().default(o)


class QualityCheck:
    def __init__(self, name: str, dataset: str):
        self.name = name
        self.dataset = dataset
        self.passed = False
        self.details = ""

    def to_dict(self) -> dict:
        return {
            "check": self.name,
            "dataset": self.dataset,
            "passed": bool(self.passed),
            "details": self.details,
        }


def check_not_empty(df: pd.DataFrame, dataset: str) -> QualityCheck:
    qc = QualityCheck("not_empty", dataset)
    qc.passed = len(df) > 0
    qc.details = f"{len(df)} filas"
    return qc


def check_no_duplicate_rows(df: pd.DataFrame, dataset: str) -> QualityCheck:
    qc = QualityCheck("no_duplicate_rows", dataset)
    dupes = int(df.duplicated().sum())
    qc.passed = dupes == 0
    qc.details = f"{dupes} filas duplicadas de {len(df)}"
    return qc


def check_null_threshold(
    df: pd.DataFrame, dataset: str, threshold: float = QUALITY_NULL_THRESHOLD_PCT
) -> QualityCheck:
    qc = QualityCheck("null_threshold", dataset)
    total = len(df)
    if total == 0:
        qc.passed = False
        qc.details = "Dataset vacío"
        return qc
    bad_cols = []
    for col in df.columns:
        null_pct = df[col].isna().sum() / total * 100
        if null_pct > threshold:
            bad_cols.append(f"{col}({null_pct:.1f}%)")
    qc.passed = len(bad_cols) == 0
    qc.details = f"Columnas con >{threshold}% nulos: {', '.join(bad_cols) if bad_cols else 'ninguna'}"
    return qc


def check_column_count(
    df: pd.DataFrame, dataset: str, min_cols: int = QUALITY_MIN_COLUMNS
) -> QualityCheck:
    qc = QualityCheck("min_columns", dataset)
    qc.passed = len(df.columns) >= min_cols
    qc.details = f"{len(df.columns)} columnas (mínimo: {min_cols})"
    return qc


def check_schema_consistency(
    dfs: dict[str, pd.DataFrame], category: str
) -> QualityCheck:
    qc = QualityCheck("schema_consistency", category)
    if len(dfs) < 2:
        qc.passed = True
        qc.details = "Solo un archivo, no se puede comparar"
        return qc
    col_sets = {name: set(df.columns) for name, df in dfs.items()}
    all_cols = set()
    for cols in col_sets.values():
        all_cols.update(cols)
    common = all_cols.copy()
    for cols in col_sets.values():
        common = common.intersection(cols)
    diff = all_cols - common
    qc.passed = len(diff) == 0
    qc.details = f"Columnas comunes: {len(common)}, columnas variables: {len(diff)}"
    if diff:
        qc.details += f" ({', '.join(sorted(diff)[:10])})"
    return qc


def _load_sqlite_table(table_name: str) -> pd.DataFrame | None:
    if not SQLITE_DB_PATH.exists():
        return None
    conn = sqlite3.connect(SQLITE_DB_PATH)
    try:
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        if table_name not in tables:
            return None
        return pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
    finally:
        conn.close()


def _run_checks_on_df(df: pd.DataFrame, dataset_name: str) -> list[dict]:
    results = []
    checks = [
        check_not_empty(df, dataset_name),
        check_no_duplicate_rows(df, dataset_name),
        check_null_threshold(df, dataset_name),
        check_column_count(df, dataset_name),
    ]
    for c in checks:
        status = "✓" if c.passed else "✗"
        print(f"  [{status}] {c.name}: {dataset_name} — {c.details}")
        results.append(c.to_dict())
    return results


def run_quality_checks():
    from datetime import datetime, timezone

    print("=== PRUEBAS DE CALIDAD ===\n")
    QUALITY_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    all_results = []

    if SQLITE_DB_PATH.exists():
        print(f"[SQLite] Leyendo desde {SQLITE_DB_PATH}\n")
        for category in SNIES_CATEGORIES:
            df = _load_sqlite_table(category)
            if df is None:
                print(f"  [SKIP] Tabla '{category}' no encontrada en SQLite")
                continue
            dataset_name = f"sqlite/{category}"
            all_results.extend(_run_checks_on_df(df, dataset_name))
    else:
        print("[Parquet] SQLite no encontrada, leyendo desde archivos procesados\n")
        snies_dir = PROCESSED_DIR / "snies"
        if snies_dir.exists():
            for category_dir in sorted(snies_dir.iterdir()):
                if not category_dir.is_dir():
                    continue
                category = category_dir.name
                category_dfs = {}
                for pq_file in sorted(category_dir.glob("*.parquet")):
                    dataset_name = f"snies/{category}/{pq_file.stem}"
                    df = pd.read_parquet(pq_file)
                    category_dfs[pq_file.stem] = df
                    all_results.extend(_run_checks_on_df(df, dataset_name))

                if len(category_dfs) >= 2:
                    sc = check_schema_consistency(category_dfs, f"snies/{category}")
                    status = "✓" if sc.passed else "✗"
                    print(f"  [{status}] {sc.name}: snies/{category} — {sc.details}")
                    all_results.append(sc.to_dict())

    for csv_dataset in CSV_DATASETS:
        pq_path = processed_parquet_path(csv_dataset)
        if pq_path.exists():
            df = pd.read_parquet(pq_path)
            all_results.extend(_run_checks_on_df(df, csv_dataset))

    report = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "total_checks": len(all_results),
        "passed": sum(1 for r in all_results if r["passed"]),
        "failed": sum(1 for r in all_results if not r["passed"]),
        "results": all_results,
    }

    QUALITY_REPORT_PATH.write_text(
        json.dumps(report, indent=2, ensure_ascii=False, cls=NumpyEncoder)
    )

    print(
        f"\n=== CALIDAD: {report['passed']}/{report['total_checks']} pruebas pasaron ==="
    )
    return report
