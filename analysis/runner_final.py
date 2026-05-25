"""Runner final para Hito 4/5.

Ejecuta el analisis base de Hito 3 y agrega los artefactos finales:
robustez, triangulacion y resumen integrado.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import DATA_DIR
from utils.logger import logger

RESULTS_DIR = DATA_DIR / "results"


def run() -> dict:
    start = time.time()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info("HITO 4/5 - RUNNER FINAL")
    logger.info("=" * 70)

    resultados: dict = {
        "hito3": None,
        "robustez": None,
        "triangulacion": None,
        "errores": [],
    }

    try:
        logger.info("[1/3] Ejecutando analisis base Hito 3...")
        from analysis.runner import run as run_hito3

        resultados["hito3"] = run_hito3()
    except Exception as exc:
        logger.exception("Fallo el analisis base Hito 3")
        resultados["errores"].append({"paso": "hito3", "error": str(exc)})

    try:
        logger.info("[2/3] Ejecutando robustez Hito 4...")
        from analysis.robustez import run_robustez_completa

        robustez = run_robustez_completa()
        resultados["robustez"] = {
            "csv_path": robustez.get("csv_path"),
            "json_path": robustez.get("json_path"),
            "plot_path": robustez.get("plot_path"),
            "resumen": robustez.get("resumen"),
        }
    except Exception as exc:
        logger.exception("Fallo la robustez")
        resultados["errores"].append({"paso": "robustez", "error": str(exc)})

    try:
        logger.info("[3/3] Ejecutando triangulacion Hito 4...")
        from analysis.triangulacion import run_triangulacion_completa

        triangulacion = run_triangulacion_completa()
        resultados["triangulacion"] = {
            "paths": triangulacion.get("paths"),
            "resumen": triangulacion.get("resumen"),
        }
    except Exception as exc:
        logger.exception("Fallo la triangulacion")
        resultados["errores"].append({"paso": "triangulacion", "error": str(exc)})

    elapsed = time.time() - start
    resumen_final = {
        "proyecto": "Seminario Ingenieria de Datos - Educacion Superior Colombia",
        "hitos": [4, 5],
        "fecha_ejecucion": time.strftime("%Y-%m-%d %H:%M:%S"),
        "duracion_s": round(elapsed, 2),
        "estado": "ok" if not resultados["errores"] else "con_errores",
        "artefactos_esperados": [
            "resumen_ejecutivo_hito3.json",
            "robustez_sensibilidad.csv",
            "robustez_resumen.json",
            "triangulacion_pnd_snies.csv",
            "triangulacion_icfes_snies.csv",
            "triangulacion_embudo_snies.csv",
        ],
        "errores": resultados["errores"],
        "robustez": resultados["robustez"],
        "triangulacion": resultados["triangulacion"],
        "nota_metodologica": (
            "Los resultados se interpretan como evidencia de contribucion, "
            "no atribucion causal completa. La robustez y triangulacion "
            "documentan sensibilidad de supuestos y compatibilidad entre fuentes."
        ),
    }

    path = RESULTS_DIR / "resumen_final_hito4_hito5.json"
    path.write_text(json.dumps(resumen_final, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Resumen final guardado en %s", path)
    logger.info("Runner final terminado en %.1f s", elapsed)

    resultados["resumen_final"] = resumen_final
    return resultados


if __name__ == "__main__":
    run()
