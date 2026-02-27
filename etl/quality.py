import json
from datetime import datetime, timezone

import pandas as pd

from config.globals import (
    PROCESSED_DIR,
    QUALITY_REPORTS_DIR,
    QUALITY_REPORT_PATH,
    QUALITY_NULL_THRESHOLD_PCT,
    QUALITY_MIN_COLUMNS,
    CSV_DATASETS,
    PG_SCHEMA_RAW,
    SNIES_CATEGORIES,
    processed_parquet_path,
)
from utils.db import get_engine, list_tables, managed_connection, table_exists
from utils.logger import logger

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

def _sql_check_not_empty(schema: str, table: str, dataset: str) -> QualityCheck:
    qc = QualityCheck("not_empty", dataset)
    try:
        with managed_connection(schema=schema) as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM "{table}"')
                count = cur.fetchone()[0]
        qc.passed = count > 0
        qc.details = f"{count} filas"
    except Exception as e:
        qc.passed = False
        qc.details = f"Error: {e}"
    return qc

def _sql_check_no_duplicate_rows(schema: str, table: str, dataset: str) -> QualityCheck:
    qc = QualityCheck("no_duplicate_rows", dataset)
    try:
        with managed_connection(schema=schema) as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM "{table}"')
                total = cur.fetchone()[0]
                cur.execute(
                    f'SELECT COUNT(*) FROM (SELECT DISTINCT * FROM "{table}") AS sub'
                )
                distinct_count = cur.fetchone()[0]
        dupes = total - distinct_count
        qc.passed = dupes == 0
        qc.details = f"{dupes} filas duplicadas de {total}"
    except Exception as e:
        qc.passed = False
        qc.details = f"Error: {e}"
    return qc

def _sql_check_null_threshold(
    schema: str,
    table: str,
    dataset: str,
    threshold: float = QUALITY_NULL_THRESHOLD_PCT,
) -> QualityCheck:
    qc = QualityCheck("null_threshold", dataset)
    try:
        with managed_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s "
                    "ORDER BY ordinal_position",
                    (schema, table),
                )
                columns = [row[0] for row in cur.fetchall()]

        if not columns:
            qc.passed = False
            qc.details = "Sin columnas"
            return qc

        null_expressions = ", ".join(
            f'COUNT(*) FILTER (WHERE "{col}" IS NULL) AS "{col}"' for col in columns
        )
        query = f'SELECT COUNT(*) AS total, {null_expressions} FROM "{table}"'

        with managed_connection(schema=schema) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()

        total = row[0]
        if total == 0:
            qc.passed = False
            qc.details = "Dataset vacío"
            return qc

        bad_cols = []
        for i, col in enumerate(columns):
            null_count = row[i + 1]
            null_pct = null_count / total * 100
            if null_pct > threshold:
                bad_cols.append(f"{col}({null_pct:.1f}%)")

        qc.passed = len(bad_cols) == 0
        qc.details = (
            f"Columnas con >{threshold}% nulos: "
            f"{', '.join(bad_cols) if bad_cols else 'ninguna'}"
        )
    except Exception as e:
        qc.passed = False
        qc.details = f"Error: {e}"
    return qc

def _sql_check_column_count(
    schema: str,
    table: str,
    dataset: str,
    min_cols: int = QUALITY_MIN_COLUMNS,
) -> QualityCheck:
    qc = QualityCheck("min_columns", dataset)
    try:
        with managed_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s",
                    (schema, table),
                )
                col_count = cur.fetchone()[0]
        qc.passed = col_count >= min_cols
        qc.details = f"{col_count} columnas (mínimo: {min_cols})"
    except Exception as e:
        qc.passed = False
        qc.details = f"Error: {e}"
    return qc

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
    qc.details = (
        f"Columnas con >{threshold}% nulos: "
        f"{', '.join(bad_cols) if bad_cols else 'ninguna'}"
    )
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

def _run_sql_checks(schema: str, table: str, dataset_name: str) -> list[dict]:
    results = []
    checks = [
        _sql_check_not_empty(schema, table, dataset_name),
        _sql_check_no_duplicate_rows(schema, table, dataset_name),
        _sql_check_null_threshold(schema, table, dataset_name),
        _sql_check_column_count(schema, table, dataset_name),
    ]
    for c in checks:
        status = "OK" if c.passed else "FAIL"
        logger.info("  [%s] %s: %s — %s", status, c.name, dataset_name, c.details)
        results.append(c.to_dict())
    return results

def _run_checks_on_df(df: pd.DataFrame, dataset_name: str) -> list[dict]:
    results = []
    checks = [
        check_not_empty(df, dataset_name),
        check_no_duplicate_rows(df, dataset_name),
        check_null_threshold(df, dataset_name),
        check_column_count(df, dataset_name),
    ]
    for c in checks:
        status = "OK" if c.passed else "FAIL"
        logger.info("  [%s] %s: %s — %s", status, c.name, dataset_name, c.details)
        results.append(c.to_dict())
    return results

def run_quality_checks():
    logger.info("=== PRUEBAS DE CALIDAD (PostgreSQL) ===")
    QUALITY_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    all_results = []

    pg_tables = list_tables(PG_SCHEMA_RAW)
    has_pg_tables = len(pg_tables) > 0

    if has_pg_tables:
        logger.info(
            "[PostgreSQL] Verificando schema '%s' (%d tablas) via SQL directo",
            PG_SCHEMA_RAW,
            len(pg_tables),
        )
        for category in SNIES_CATEGORIES:
            if not table_exists(PG_SCHEMA_RAW, category):
                logger.info(
                    "  [SKIP] Tabla '%s' no encontrada en '%s'",
                    category,
                    PG_SCHEMA_RAW,
                )
                continue
            dataset_name = f"pg/{PG_SCHEMA_RAW}/{category}"
            all_results.extend(_run_sql_checks(PG_SCHEMA_RAW, category, dataset_name))
    else:
        logger.info(
            "[Parquet] PostgreSQL sin tablas, leyendo desde archivos procesados"
        )
        snies_dir = PROCESSED_DIR / "snies"
        if snies_dir.exists():
            for category_dir in sorted(snies_dir.iterdir()):
                if not category_dir.is_dir():
                    continue
                category = category_dir.name
                category_dfs = {}
                for pq_file in sorted(category_dir.glob("*.parquet")):
                    dataset_name = f"snies/{category}/{pq_file.stem}"
                    try:
                        df = pd.read_parquet(pq_file)
                        category_dfs[pq_file.stem] = df
                        all_results.extend(_run_checks_on_df(df, dataset_name))
                    except Exception as e:
                        logger.warning("Error leyendo %s: %s", pq_file, e)

                if len(category_dfs) >= 2:
                    sc = check_schema_consistency(category_dfs, f"snies/{category}")
                    status = "OK" if sc.passed else "FAIL"
                    logger.info(
                        "  [%s] %s: snies/%s — %s",
                        status,
                        sc.name,
                        category,
                        sc.details,
                    )
                    all_results.append(sc.to_dict())

    for csv_dataset in CSV_DATASETS:
        table_name = csv_dataset.split("/")[-1]
        if has_pg_tables and table_exists(PG_SCHEMA_RAW, table_name):
            dataset_name = f"pg/{PG_SCHEMA_RAW}/{table_name}"
            all_results.extend(_run_sql_checks(PG_SCHEMA_RAW, table_name, dataset_name))
            continue
        pq_path = processed_parquet_path(csv_dataset)
        if pq_path.exists():
            try:
                df = pd.read_parquet(pq_path)
                all_results.extend(_run_checks_on_df(df, csv_dataset))
            except Exception as e:
                logger.warning("Error leyendo %s: %s", pq_path, e)

    report = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "total_checks": len(all_results),
        "passed": sum(1 for r in all_results if r["passed"]),
        "failed": sum(1 for r in all_results if not r["passed"]),
        "results": all_results,
    }

    QUALITY_REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False))

    logger.info(
        "=== CALIDAD: %d/%d pruebas pasaron ===",
        report["passed"],
        report["total_checks"],
    )
    return report
