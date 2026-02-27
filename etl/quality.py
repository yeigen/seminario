import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

PROCESSED_DIR = Path(__file__).parents[1] / "data" / "processed"
REPORTS_DIR = Path(__file__).parents[1] / "data" / "processed" / "_quality_reports"


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
            "passed": self.passed,
            "details": self.details,
        }


def check_not_empty(df: pd.DataFrame, dataset: str) -> QualityCheck:
    qc = QualityCheck("not_empty", dataset)
    qc.passed = len(df) > 0
    qc.details = f"{len(df)} filas"
    return qc


def check_no_duplicate_rows(df: pd.DataFrame, dataset: str) -> QualityCheck:
    qc = QualityCheck("no_duplicate_rows", dataset)
    dupes = df.duplicated().sum()
    qc.passed = dupes == 0
    qc.details = f"{dupes} filas duplicadas de {len(df)}"
    return qc


def check_null_threshold(
    df: pd.DataFrame, dataset: str, threshold: float = 50.0
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
    df: pd.DataFrame, dataset: str, min_cols: int = 2
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


def run_quality_checks():
    print("=== PRUEBAS DE CALIDAD ===\n")
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    all_results = []

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

                checks = [
                    check_not_empty(df, dataset_name),
                    check_no_duplicate_rows(df, dataset_name),
                    check_null_threshold(df, dataset_name),
                    check_column_count(df, dataset_name),
                ]
                for c in checks:
                    status = "✓" if c.passed else "✗"
                    print(f"  [{status}] {c.name}: {dataset_name} — {c.details}")
                    all_results.append(c.to_dict())

            if len(category_dfs) >= 2:
                sc = check_schema_consistency(category_dfs, f"snies/{category}")
                status = "✓" if sc.passed else "✗"
                print(f"  [{status}] {sc.name}: snies/{category} — {sc.details}")
                all_results.append(sc.to_dict())

    for csv_dataset in ["pnd/seguimiento_pnd", "icfes/saber_359"]:
        pq_path = PROCESSED_DIR / f"{csv_dataset}.parquet"
        if pq_path.exists():
            df = pd.read_parquet(pq_path)
            checks = [
                check_not_empty(df, csv_dataset),
                check_no_duplicate_rows(df, csv_dataset),
                check_null_threshold(df, csv_dataset),
                check_column_count(df, csv_dataset),
            ]
            for c in checks:
                status = "✓" if c.passed else "✗"
                print(f"  [{status}] {c.name}: {csv_dataset} — {c.details}")
                all_results.append(c.to_dict())

    report = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "total_checks": len(all_results),
        "passed": sum(1 for r in all_results if r["passed"]),
        "failed": sum(1 for r in all_results if not r["passed"]),
        "results": all_results,
    }

    report_path = REPORTS_DIR / "quality_report.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))

    print(
        f"\n=== CALIDAD: {report['passed']}/{report['total_checks']} pruebas pasaron ==="
    )
    return report
