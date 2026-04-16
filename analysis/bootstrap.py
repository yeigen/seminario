"""
bootstrap.py — Intervalos de confianza bootstrap para los estimadores ITS y DiD.

Método: bootstrap por bloques (block bootstrap) con bloque de 2 semestres
para preservar la estructura de autocorrelación de la serie temporal.

Produce:
  - IC 95% por percentil para α₂, α₃ (ITS) y β₃ (DiD)
  - Distribución de los estimadores bootstrap
  - Análisis de escenarios (optimista / base / adverso)
  - Exporta a data/results/bootstrap_*.json y data/results/plots/bootstrap_*.html
"""

from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import statsmodels.api as sm
from plotly.subplots import make_subplots

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import DATA_DIR

RESULTS_DIR = DATA_DIR / "results"
PLOTS_DIR = RESULTS_DIR / "plots"

T0_ANO = 2022
T0_SEM = 2
N_BOOTSTRAP = 1000
BLOCK_SIZE = 2  # semestres por bloque
RANDOM_SEED = 42


def _block_resample(df: pd.DataFrame, block_size: int, rng: np.random.Generator) -> pd.DataFrame:
    """
    Remuestrea el DataFrame por bloques de tamaño `block_size` filas (preserva
    la estructura temporal dentro de cada bloque).
    """
    n = len(df)
    n_blocks = max(1, n // block_size)
    starts = rng.integers(0, n, size=n_blocks)
    indices = []
    for s in starts:
        block_idx = list(range(s, min(s + block_size, n)))
        indices.extend(block_idx)
    indices = indices[:n]
    return df.iloc[indices].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Bootstrap para ITS
# ---------------------------------------------------------------------------

def bootstrap_its(
    df_serie: pd.DataFrame,
    t0: int,
    n_boot: int = N_BOOTSTRAP,
    block_size: int = BLOCK_SIZE,
) -> dict:
    """
    Bootstrap por bloques del modelo ITS.

    Args:
        df_serie: Serie temporal de UN sector (ya filtrada), con columnas t, total
        t0: índice del punto de quiebre
        n_boot: número de re-muestras
        block_size: tamaño del bloque en número de periodos

    Returns:
        dict con distribuciones bootstrap e IC 95%
    """
    rng = np.random.default_rng(RANDOM_SEED)
    alpha2_boot = []
    alpha3_boot = []

    df_sorted = df_serie.sort_values("t").reset_index(drop=True).copy()
    df_sorted["D"] = (df_sorted["t"] >= t0).astype(int)
    df_sorted["t_post"] = (df_sorted["t"] - t0) * df_sorted["D"]

    for _ in range(n_boot):
        df_b = _block_resample(df_sorted, block_size, rng)
        # Re-asignar índice t secuencial en la re-muestra para que las regresiones tengan sentido
        df_b = df_b.sort_values("t").reset_index(drop=True)
        df_b["t_b"] = range(1, len(df_b) + 1)
        df_b["D_b"] = (df_b["t"] >= t0).astype(int)
        df_b["t_post_b"] = (df_b["t"] - t0) * df_b["D_b"]

        X_b = sm.add_constant(df_b[["t_b", "D_b", "t_post_b"]])
        y_b = df_b["total"]
        if len(X_b) < 4 or y_b.std() == 0:
            continue
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                coefs = sm.OLS(y_b, X_b).fit().params
            alpha2_boot.append(coefs["D_b"])
            alpha3_boot.append(coefs["t_post_b"])
        except Exception:
            continue

    a2 = np.array(alpha2_boot)
    a3 = np.array(alpha3_boot)

    result = {
        "n_exitosas": len(a2),
        "alpha_2_cambio_nivel": {
            "media_boot": round(float(np.mean(a2)), 2),
            "ic_95_lower": round(float(np.percentile(a2, 2.5)), 2),
            "ic_95_upper": round(float(np.percentile(a2, 97.5)), 2),
            "sesgo": round(float(np.mean(a2) - np.mean(a2)), 2),
        },
        "alpha_3_cambio_tendencia": {
            "media_boot": round(float(np.mean(a3)), 2),
            "ic_95_lower": round(float(np.percentile(a3, 2.5)), 2),
            "ic_95_upper": round(float(np.percentile(a3, 97.5)), 2),
        },
        "_distribuciones": {
            "alpha_2": a2.tolist(),
            "alpha_3": a3.tolist(),
        }
    }
    return result


# ---------------------------------------------------------------------------
# Bootstrap para DiD
# ---------------------------------------------------------------------------

def bootstrap_did(
    df_sector: pd.DataFrame,
    n_boot: int = N_BOOTSTRAP,
    block_size: int = BLOCK_SIZE,
) -> dict:
    """
    Bootstrap por bloques del estimador DiD agregado.

    Returns:
        dict con distribución bootstrap e IC 95% de β₃
    """
    rng = np.random.default_rng(RANDOM_SEED)
    beta3_boot = []

    df = df_sector.copy()
    df["POST"] = ((df["ano"] > T0_ANO) | ((df["ano"] == T0_ANO) & (df["semestre"] >= T0_SEM))).astype(int)
    df["OFICIAL"] = (df["sector_ies"] == "Oficial").astype(int)
    df["POST_OFICIAL"] = df["POST"] * df["OFICIAL"]
    df = df.sort_values(["sector_ies", "t"]).reset_index(drop=True)

    for _ in range(n_boot):
        # Re-muestrear por bloques dentro de cada sector para preservar estructura
        partes = []
        for sector in df["sector_ies"].unique():
            sub = df[df["sector_ies"] == sector].sort_values("t").reset_index(drop=True)
            partes.append(_block_resample(sub, block_size, rng))
        df_b = pd.concat(partes, ignore_index=True)
        df_b["POST"] = ((df_b["ano"] > T0_ANO) | ((df_b["ano"] == T0_ANO) & (df_b["semestre"] >= T0_SEM))).astype(int)
        df_b["OFICIAL"] = (df_b["sector_ies"] == "Oficial").astype(int)
        df_b["POST_OFICIAL"] = df_b["POST"] * df_b["OFICIAL"]

        X_b = sm.add_constant(df_b[["POST", "OFICIAL", "POST_OFICIAL"]])
        y_b = df_b["total"]
        if len(X_b) < 4:
            continue
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                coefs = sm.OLS(y_b, X_b).fit().params
            beta3_boot.append(coefs["POST_OFICIAL"])
        except Exception:
            continue

    b3 = np.array(beta3_boot)
    result = {
        "n_exitosas": len(b3),
        "beta_3_did": {
            "media_boot": round(float(np.mean(b3)), 2),
            "ic_95_lower": round(float(np.percentile(b3, 2.5)), 2),
            "ic_95_upper": round(float(np.percentile(b3, 97.5)), 2),
        },
        "_distribuciones": {"beta_3": b3.tolist()},
    }
    return result


# ---------------------------------------------------------------------------
# Análisis de escenarios
# ---------------------------------------------------------------------------

def analisis_escenarios(df_sector: pd.DataFrame, sector: str = "Oficial") -> dict:
    """
    Proyecta tres escenarios para la matrícula post-2022 en un sector:
    - Base: tendencia OLS pre-2022 proyectada al futuro
    - Optimista: Base + 1 desvío estándar de los residuos pre-2022
    - Adverso: Base − 1 desvío estándar

    Retorna dict con valores proyectados 2023-2024.
    """
    df = df_sector[df_sector["sector_ies"] == sector].sort_values("t").copy()
    df_pre = df[(df["ano"] < T0_ANO) | ((df["ano"] == T0_ANO) & (df["semestre"] < T0_SEM))].copy()

    X_pre = sm.add_constant(df_pre[["t"]])
    y_pre = df_pre["total"]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        mod_pre = sm.OLS(y_pre, X_pre).fit()

    sigma = mod_pre.resid.std()

    df_post = df[(df["ano"] >= T0_ANO) & ~((df["ano"] == T0_ANO) & (df["semestre"] < T0_SEM))].copy()
    X_post = sm.add_constant(df_post[["t"]], has_constant="add")
    df_post["escenario_base"] = mod_pre.predict(X_post)
    df_post["escenario_optimista"] = df_post["escenario_base"] + sigma
    df_post["escenario_adverso"] = df_post["escenario_base"] - sigma
    df_post["observado"] = df_post["total"]
    df_post["vs_base"] = df_post["observado"] - df_post["escenario_base"]

    return {
        "sector": sector,
        "sigma_residuos_pre": round(float(sigma), 0),
        "escenarios": df_post[
            ["periodo", "observado", "escenario_base", "escenario_optimista", "escenario_adverso", "vs_base"]
        ].to_dict(orient="records"),
    }


# ---------------------------------------------------------------------------
# Visualización y runner principal
# ---------------------------------------------------------------------------

def grafico_bootstrap(dist: list[float], estimador_nombre: str, ic_lower: float, ic_upper: float) -> go.Figure:
    """Histograma de la distribución bootstrap con IC 95% marcado."""
    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=dist, nbinsx=50, name="Bootstrap",
        marker_color="#aec7e8", opacity=0.85,
    ))
    for val, label, color in [
        (ic_lower, "IC 2.5%", "red"),
        (ic_upper, "IC 97.5%", "red"),
        (np.mean(dist), "Media", "navy"),
    ]:
        fig.add_vline(x=val, line_dash="dash", line_color=color,
                      annotation_text=f"{label}: {val:,.0f}")

    fig.update_layout(
        title=f"Distribución Bootstrap — {estimador_nombre} (N={len(dist):,})",
        xaxis_title="Valor del estimador",
        yaxis_title="Frecuencia",
        template="plotly_white",
        height=380,
    )
    return fig


def run_bootstrap_completo(
    df_sector: pd.DataFrame,
    t0: int,
    tipo_evento: str = "matriculados",
    n_boot: int = N_BOOTSTRAP,
) -> dict:
    """
    Ejecuta bootstrap para ITS (sector Oficial) y DiD, genera figuras y guarda resultados.
    """
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\n[Bootstrap] N={n_boot} re-muestras | tipo_evento={tipo_evento}")

    # ITS bootstrap (sector Oficial)
    df_oficial = df_sector[df_sector["sector_ies"] == "Oficial"].sort_values("t").copy()
    boot_its = bootstrap_its(df_oficial, t0, n_boot=n_boot)

    # DiD bootstrap
    boot_did = bootstrap_did(df_sector, n_boot=n_boot)

    # Escenarios
    esc_oficial = analisis_escenarios(df_sector, "Oficial")
    esc_privada = analisis_escenarios(df_sector, "Privada")

    # Figuras
    fig_a2 = grafico_bootstrap(
        boot_its["_distribuciones"]["alpha_2"],
        f"α₂ ITS Oficial ({tipo_evento})",
        boot_its["alpha_2_cambio_nivel"]["ic_95_lower"],
        boot_its["alpha_2_cambio_nivel"]["ic_95_upper"],
    )
    fig_b3 = grafico_bootstrap(
        boot_did["_distribuciones"]["beta_3"],
        f"β₃ DiD ({tipo_evento})",
        boot_did["beta_3_did"]["ic_95_lower"],
        boot_did["beta_3_did"]["ic_95_upper"],
    )

    fig_a2.write_html(str(PLOTS_DIR / f"bootstrap_its_alpha2_{tipo_evento}.html"))
    fig_b3.write_html(str(PLOTS_DIR / f"bootstrap_did_beta3_{tipo_evento}.html"))

    # Limpiar distribuciones para JSON (no serializar listas de 1000 floats en el resumen)
    boot_its_clean = {k: v for k, v in boot_its.items() if k != "_distribuciones"}
    boot_did_clean = {k: v for k, v in boot_did.items() if k != "_distribuciones"}

    resultado_final = {
        "tipo_evento": tipo_evento,
        "n_bootstrap": n_boot,
        "its_bootstrap": boot_its_clean,
        "did_bootstrap": boot_did_clean,
        "escenarios_oficial": esc_oficial,
        "escenarios_privada": esc_privada,
    }

    json_path = RESULTS_DIR / f"bootstrap_{tipo_evento}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(resultado_final, f, ensure_ascii=False, indent=2)

    print(f"  ITS α₂ IC95% = [{boot_its['alpha_2_cambio_nivel']['ic_95_lower']:,.0f}, "
          f"{boot_its['alpha_2_cambio_nivel']['ic_95_upper']:,.0f}]")
    print(f"  DiD β₃ IC95% = [{boot_did['beta_3_did']['ic_95_lower']:,.0f}, "
          f"{boot_did['beta_3_did']['ic_95_upper']:,.0f}]")

    return {
        "resultado": resultado_final,
        "boot_its_dist": boot_its["_distribuciones"],
        "boot_did_dist": boot_did["_distribuciones"],
        "fig_alpha2": fig_a2,
        "fig_beta3": fig_b3,
    }
