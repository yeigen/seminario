"""Verificacion minima de reproducibilidad para Hito 4/5.

No ejecuta el ETL. Revisa que la base, los schemas principales y los
artefactos finales existan o reporta que falta.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

CONFIG_IMPORT_ERROR = None
try:
    from config.globals import DATA_DIR, PG_SCHEMAS
except Exception as exc:
    CONFIG_IMPORT_ERROR = str(exc)
    DATA_DIR = PROJECT_ROOT / "data"
    PG_SCHEMAS = ["raw", "unified", "facts"]


EXPECTED_RESULTS = [
    "resumen_ejecutivo_hito3.json",
    "resumen_final_hito4_hito5.json",
    "robustez_sensibilidad.csv",
    "robustez_resumen.json",
    "triangulacion_pnd_snies.csv",
    "triangulacion_icfes_snies.csv",
    "triangulacion_embudo_snies.csv",
    "triangulacion_resumen.json",
]


def _check_database() -> dict:
    result = {
        "ok": False,
        "schemas": {},
        "error": CONFIG_IMPORT_ERROR,
    }
    if CONFIG_IMPORT_ERROR:
        return result
    try:
        from sqlalchemy import inspect, text

        from analysis.queries import _get_engine

        engine = _get_engine()
        inspector = inspect(engine)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        for schema in PG_SCHEMAS:
            tables = inspector.get_table_names(schema=schema)
            result["schemas"][schema] = {
                "exists": len(tables) > 0,
                "tables": tables,
                "n_tables": len(tables),
            }
        result["ok"] = all(info["exists"] for info in result["schemas"].values())
    except Exception as exc:
        result["error"] = str(exc)
    return result


def _check_results() -> dict:
    results_dir = DATA_DIR / "results"
    files = {}
    for name in EXPECTED_RESULTS:
        path = results_dir / name
        files[name] = {
            "exists": path.exists(),
            "path": str(path),
            "size_bytes": path.stat().st_size if path.exists() else 0,
        }
    return {
        "ok": all(info["exists"] for info in files.values()),
        "results_dir": str(results_dir),
        "files": files,
    }


def run() -> dict:
    report = {
        "database": _check_database(),
        "results": _check_results(),
    }
    report["ok"] = bool(report["database"]["ok"] and report["results"]["ok"])

    out_path = DATA_DIR / "results" / "verify_reproducibility.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report


if __name__ == "__main__":
    result = run()
    raise SystemExit(0 if result["ok"] else 1)
