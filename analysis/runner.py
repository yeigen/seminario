"""
runner.py — Orquestador principal del análisis de Hito 3.

Ejecuta en orden:
  1. Carga de datos desde PostgreSQL (star schema)
  2. Análisis de tendencias
  3. ITS (Sector Oficial y Privada, tipos: matriculados, primer_curso, graduados)
  4. DiD agregado y en panel
  5. Event study (pre-tendencias)
  6. Bootstrap e intervalos de confianza
  7. Genera resumen ejecutivo en JSON

Uso:
    uv run python analysis/runner.py
    # o dentro del entorno virtual:
    python analysis/runner.py
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
PLOTS_DIR = RESULTS_DIR / "plots"

# Punto de quiebre: t=10 corresponde a 2022-S2 si la serie comienza en 2018-S1
# (se calcula dinámicamente desde los datos en cada módulo)
T0_ANO = 2022
T0_SEM = 2
TIPOS_EVENTO = ["matriculados", "primer_curso", "graduados"]


def run():
    start = time.time()
    logger.info("=" * 60)
    logger.info("HITO 3 — Análisis de metodología e incertidumbre")
    logger.info("=" * 60)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # PASO 1: Cargar datos
    # ------------------------------------------------------------------
    logger.info("\n[Paso 1] Cargando datos desde PostgreSQL...")
    from analysis.queries import (
        get_embudo_estudiantil,
        get_matricula_por_departamento,
        get_matricula_por_sector,
        get_panel_ies,
    )

    datos = {}
    for tipo in TIPOS_EVENTO:
        datos[tipo] = get_matricula_por_sector(tipo_evento=tipo)
        logger.info(f"  {tipo}: {len(datos[tipo])} filas")

    df_panel = get_panel_ies()
    df_embudo = get_embudo_estudiantil()
    df_depto = get_matricula_por_departamento()

    logger.info(f"  Panel IES: {df_panel['codigo_ies'].nunique()} IES, {len(df_panel)} filas")
    logger.info(f"  Embudo: {len(df_embudo)} filas")
    logger.info(f"  Departamental: {len(df_depto)} filas")

    # Calcular t0
    df_ref = datos["matriculados"]
    t0_mask = (df_ref["ano"] == T0_ANO) & (df_ref["semestre"] == T0_SEM)
    t0 = int(df_ref.loc[t0_mask, "t"].drop_duplicates().iloc[0])
    logger.info(f"  Punto de quiebre: {T0_ANO}-S{T0_SEM} → t0 = {t0}")

    # ------------------------------------------------------------------
    # PASO 2: Análisis de tendencias
    # ------------------------------------------------------------------
    logger.info("\n[Paso 2] Análisis de tendencias...")
    from analysis.tendencias import run_tendencias

    resultados_tendencias = {}
    for tipo in TIPOS_EVENTO:
        resultados_tendencias[tipo] = run_tendencias(datos[tipo], tipo_evento=tipo)

    # ------------------------------------------------------------------
    # PASO 3: ITS por sector y tipo de evento
    # ------------------------------------------------------------------
    logger.info("\n[Paso 3] Series de Tiempo Interrumpidas (ITS)...")
    from analysis.its import run_its

    # Placebos: t0 en pandemia (2020-S2 ≈ t=5) y año previo (2021-S2 ≈ t=7)
    placebos = [t0 - 4, t0 - 2]  # aproximados; se ajustan si no existen

    resultados_its = {}
    for tipo in ["matriculados", "primer_curso"]:
        for sector in ["Oficial", "Privada"]:
            key = f"{sector}_{tipo}"
            try:
                resultados_its[key] = run_its(
                    datos[tipo],
                    sector=sector,
                    tipo_evento=tipo,
                    placebos_t0=placebos,
                )
            except Exception as e:
                logger.warning(f"  ITS {key}: {e}")

    # ------------------------------------------------------------------
    # PASO 4: DiD
    # ------------------------------------------------------------------
    logger.info("\n[Paso 4] Diferencias en Diferencias (DiD)...")
    from analysis.did import run_did_agregado, run_did_panel, run_event_study

    resultados_did = {}
    for tipo in ["matriculados", "primer_curso"]:
        try:
            resultados_did[f"agregado_{tipo}"] = run_did_agregado(datos[tipo], tipo_evento=tipo)
        except Exception as e:
            logger.warning(f"  DiD agregado {tipo}: {e}")

        try:
            resultados_did[f"panel_{tipo}"] = run_did_panel(df_panel, tipo_evento=tipo)
        except Exception as e:
            logger.warning(f"  DiD panel {tipo}: {e}")

        try:
            resultados_did[f"event_study_{tipo}"] = run_event_study(datos[tipo], tipo_evento=tipo)
        except Exception as e:
            logger.warning(f"  Event study {tipo}: {e}")

    # ------------------------------------------------------------------
    # PASO 5: Bootstrap
    # ------------------------------------------------------------------
    logger.info("\n[Paso 5] Bootstrap (N=1000 re-muestras)...")
    from analysis.bootstrap import run_bootstrap_completo

    resultados_bootstrap = {}
    for tipo in ["matriculados", "primer_curso"]:
        try:
            resultados_bootstrap[tipo] = run_bootstrap_completo(
                datos[tipo], t0=t0, tipo_evento=tipo, n_boot=1000
            )
        except Exception as e:
            logger.warning(f"  Bootstrap {tipo}: {e}")

    # ------------------------------------------------------------------
    # PASO 6: Resumen ejecutivo
    # ------------------------------------------------------------------
    logger.info("\n[Paso 6] Generando resumen ejecutivo...")
    resumen = _generar_resumen_ejecutivo(resultados_its, resultados_did, resultados_bootstrap)

    resumen_path = RESULTS_DIR / "resumen_ejecutivo_hito3.json"
    with open(resumen_path, "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)

    elapsed = time.time() - start
    logger.info(f"\n{'='*60}")
    logger.info(f"ANÁLISIS COMPLETO EN {elapsed:.1f} s")
    logger.info(f"Resultados en: {RESULTS_DIR}")
    logger.info(f"{'='*60}")

    return {
        "tendencias": resultados_tendencias,
        "its": resultados_its,
        "did": resultados_did,
        "bootstrap": resultados_bootstrap,
        "resumen": resumen,
    }


def _generar_resumen_ejecutivo(its: dict, did: dict, boot: dict) -> dict:
    """Consolida los hallazgos principales en un JSON legible."""
    hallazgos = []

    # ITS — cambio de nivel Oficial
    key_its = "Oficial_matriculados"
    if key_its in its:
        coefs = its[key_its].get("coeficientes", {})
        a2 = coefs.get("alpha_2_cambio_nivel", {})
        a3 = coefs.get("alpha_3_cambio_tendencia", {})
        hallazgos.append({
            "analisis": "ITS — Cambio de nivel (Oficial, matriculados)",
            "estimado": a2.get("estimado"),
            "ic_95": [a2.get("ic_95_lower"), a2.get("ic_95_upper")],
            "p_value": a2.get("p_value"),
            "significativo": a2.get("p_value", 1) < 0.05 if a2.get("p_value") is not None else None,
            "interpretacion": "Cambio inmediato en matriculados de IES Oficiales al inicio del gobierno Petro",
        })
        hallazgos.append({
            "analisis": "ITS — Cambio de tendencia (Oficial, matriculados)",
            "estimado": a3.get("estimado"),
            "ic_95": [a3.get("ic_95_lower"), a3.get("ic_95_upper")],
            "p_value": a3.get("p_value"),
            "significativo": a3.get("p_value", 1) < 0.05 if a3.get("p_value") is not None else None,
            "interpretacion": "Cambio en la tasa de crecimiento semestral post-2022 vs. pre-2022",
        })

    # DiD agregado
    key_did = "agregado_matriculados"
    if key_did in did:
        res = did[key_did].get("resultados", {})
        est = res.get("estimador_did", {})
        hallazgos.append({
            "analisis": "DiD — Diferencial Oficial vs. Privada (agregado, matriculados)",
            "estimado": est.get("beta_3"),
            "ic_95": [est.get("ic_95_lower"), est.get("ic_95_upper")],
            "p_value": est.get("p_value"),
            "significativo": est.get("significativo"),
            "interpretacion": "Diferencia en el cambio de matrícula entre sector oficial y privado post-2022",
        })

    # Bootstrap IC
    if "matriculados" in boot:
        br = boot["matriculados"].get("resultado", {})
        b3 = br.get("did_bootstrap", {}).get("beta_3_did", {})
        a2_b = br.get("its_bootstrap", {}).get("alpha_2_cambio_nivel", {})
        hallazgos.append({
            "analisis": "Bootstrap IC95% — α₂ ITS (Oficial, matriculados)",
            "ic_95_bootstrap": [a2_b.get("ic_95_lower"), a2_b.get("ic_95_upper")],
            "n_resamples": br.get("n_bootstrap"),
        })
        hallazgos.append({
            "analisis": "Bootstrap IC95% — β₃ DiD (matriculados)",
            "ic_95_bootstrap": [b3.get("ic_95_lower"), b3.get("ic_95_upper")],
            "n_resamples": br.get("n_bootstrap"),
        })

    return {
        "proyecto": "Seminario Ingeniería de Datos — Educación Superior Colombia 2022-2026",
        "hito": 3,
        "fecha_ejecucion": time.strftime("%Y-%m-%d %H:%M:%S"),
        "periodo_datos": "2018-S1 a 2024-S2",
        "punto_quiebre": f"{T0_ANO}-S{T0_SEM}",
        "nota_metodologica": (
            "Los estimadores miden asociación (contribución), no causalidad pura. "
            "La atribución completa requiere supuestos adicionales que se detallan "
            "en docs/metodologia_hito3.md."
        ),
        "hallazgos_principales": hallazgos,
    }


if __name__ == "__main__":
    run()
