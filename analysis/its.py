"""
its.py — Análisis de Series de Tiempo Interrumpidas (Interrupted Time Series / ITS).

Modelo de regressed segmentada:
    Y_t = α₀ + α₁·t + α₂·D_t + α₃·(t - T₀)·D_t + ε_t

donde:
    t    = índice de tiempo secuencial (1 = 2018-S1)
    D_t  = 1 si t >= T₀ (inicio gobierno Petro, 2022-S2)
    α₀   = nivel inicial
    α₁   = tendencia pre-intervención (por semestre)
    α₂   = cambio inmediato de nivel en T₀
    α₃   = cambio de tendencia post-intervención

Produce:
  - Coeficientes del modelo con IC 95% (Newey-West HAC)
  - Prueba de Chow para quiebre estructural
  - Proyección del contrafactual
  - Pruebas placebo (T₀ alternativo)
  - Exporta a data/results/its_*.json y data/results/plots/its_*.html
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
from scipy import stats
from statsmodels.stats.diagnostic import het_breuschpagan

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import DATA_DIR

RESULTS_DIR = DATA_DIR / "results"
PLOTS_DIR = RESULTS_DIR / "plots"

T0_ANO = 2022
T0_SEM = 2


def _build_its_features(df: pd.DataFrame, t0: int) -> pd.DataFrame:
    """
    Construye las variables del modelo ITS para un punto de quiebre t0.

    Columnas añadidas:
        D      : indicador post-intervención
        t_post : (t - t0) * D  — tendencia post-intervención
    """
    df = df.copy()
    df["D"] = (df["t"] >= t0).astype(int)
    df["t_post"] = (df["t"] - t0) * df["D"]
    return df


def _run_ols_hac(df: pd.DataFrame, y_col: str = "total") -> sm.regression.linear_model.RegressionResultsWrapper:
    """
    Ajusta OLS con errores HAC (Newey-West, maxlags=2) sobre las variables ITS.
    Retorna el objeto de resultados de statsmodels.
    """
    X = sm.add_constant(df[["t", "D", "t_post"]])
    y = df[y_col]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        modelo = sm.OLS(y, X).fit(cov_type="HAC", cov_kwds={"maxlags": 2})
    return modelo


def _proyectar_contrafactual(df: pd.DataFrame, t0: int, modelo) -> pd.DataFrame:
    """
    Proyecta el contrafactual: qué habría ocurrido si D = 0 para todo t >= t0.
    """
    df_cf = df.copy()
    df_cf["D"] = 0
    df_cf["t_post"] = 0
    X_cf = sm.add_constant(df_cf[["t", "D", "t_post"]], has_constant="add")
    df["contrafactual"] = modelo.predict(X_cf)
    df["efecto_estimado"] = df["total"] - df["contrafactual"]
    return df


def prueba_chow(df: pd.DataFrame, t0: int, y_col: str = "total") -> dict:
    """
    Prueba de Chow: quiebre estructural en t0.
    Compara el RSS del modelo restringido (sin quiebre) vs. no restringido (con quiebre).
    """
    n = len(df)
    df_pre = df[df["t"] < t0].copy()
    df_post = df[df["t"] >= t0].copy()

    if len(df_pre) < 3 or len(df_post) < 3:
        return {"chow_F": np.nan, "chow_p": np.nan, "nota": "Muestra insuficiente para Chow"}

    def rss(sub):
        X = sm.add_constant(sub[["t"]])
        return sm.OLS(sub[y_col], X).fit().ssr

    rss_restricted = rss(df)
    rss_pre = rss(df_pre)
    rss_post = rss(df_post)

    k = 2  # número de parámetros (intercepto + tendencia)
    F = ((rss_restricted - (rss_pre + rss_post)) / k) / ((rss_pre + rss_post) / (n - 2 * k))
    p_value = 1 - stats.f.cdf(F, k, n - 2 * k)

    return {
        "chow_F": round(float(F), 4),
        "chow_p": round(float(p_value), 4),
        "conclusion": "Quiebre estructural significativo (p<0.05)" if p_value < 0.05 else "Sin evidencia de quiebre estructural",
    }


def run_its(
    df_sector: pd.DataFrame,
    sector: str = "Oficial",
    tipo_evento: str = "matriculados",
    placebos_t0: list[int] | None = None,
) -> dict:
    """
    Ejecuta el análisis ITS completo para un sector dado.

    Args:
        df_sector: DataFrame de get_matricula_por_sector() con columna 't'
        sector: 'Oficial' o 'Privada'
        tipo_evento: nombre del tipo de evento para etiquetas
        placebos_t0: lista de índices t alternativos para pruebas placebo

    Returns:
        dict con modelo, coeficientes, contrafactual, prueba Chow, placebos y figura
    """
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    df = df_sector[df_sector["sector_ies"] == sector].sort_values("t").copy()
    if df.empty:
        raise ValueError(f"No hay datos para sector '{sector}'")

    # Identificar t0
    t0_mask = (df["ano"] == T0_ANO) & (df["semestre"] == T0_SEM)
    if not t0_mask.any():
        raise ValueError(f"Punto de quiebre {T0_ANO}-S{T0_SEM} no encontrado")
    t0 = int(df.loc[t0_mask, "t"].iloc[0])

    # Construir features y ajustar modelo
    df_its = _build_its_features(df, t0)
    modelo = _run_ols_hac(df_its)

    # Extraer coeficientes
    coeficientes = {
        "alpha_0_intercepto": {
            "estimado": round(float(modelo.params["const"]), 2),
            "ic_95_lower": round(float(modelo.conf_int().loc["const", 0]), 2),
            "ic_95_upper": round(float(modelo.conf_int().loc["const", 1]), 2),
            "p_value": round(float(modelo.pvalues["const"]), 4),
        },
        "alpha_1_tendencia_pre": {
            "estimado": round(float(modelo.params["t"]), 2),
            "ic_95_lower": round(float(modelo.conf_int().loc["t", 0]), 2),
            "ic_95_upper": round(float(modelo.conf_int().loc["t", 1]), 2),
            "p_value": round(float(modelo.pvalues["t"]), 4),
            "interpretacion": "Cambio en matrícula por semestre, tendencia pre-2022",
        },
        "alpha_2_cambio_nivel": {
            "estimado": round(float(modelo.params["D"]), 2),
            "ic_95_lower": round(float(modelo.conf_int().loc["D", 0]), 2),
            "ic_95_upper": round(float(modelo.conf_int().loc["D", 1]), 2),
            "p_value": round(float(modelo.pvalues["D"]), 4),
            "interpretacion": "Cambio inmediato de nivel en el punto de quiebre (2022-S2)",
        },
        "alpha_3_cambio_tendencia": {
            "estimado": round(float(modelo.params["t_post"]), 2),
            "ic_95_lower": round(float(modelo.conf_int().loc["t_post", 0]), 2),
            "ic_95_upper": round(float(modelo.conf_int().loc["t_post", 1]), 2),
            "p_value": round(float(modelo.pvalues["t_post"]), 4),
            "interpretacion": "Cambio en la tendencia (pendiente) post-2022 respecto a pre-2022",
        },
    }

    bondad_ajuste = {
        "R2": round(float(modelo.rsquared), 4),
        "R2_ajustado": round(float(modelo.rsquared_adj), 4),
        "AIC": round(float(modelo.aic), 2),
        "BIC": round(float(modelo.bic), 2),
        "n_observaciones": int(modelo.nobs),
        "durbin_watson": round(float(sm.stats.durbin_watson(modelo.resid)), 4),
    }

    # Proyección contrafactual
    df_its = _proyectar_contrafactual(df_its, t0, modelo)

    # Prueba de Chow
    chow = prueba_chow(df_its, t0)

    # Pruebas placebo
    placebos = {}
    if placebos_t0:
        for t_placebo in placebos_t0:
            df_pl = _build_its_features(df.copy(), t_placebo)
            mod_pl = _run_ols_hac(df_pl)
            placebos[f"t0={t_placebo}"] = {
                "alpha_2": round(float(mod_pl.params["D"]), 2),
                "alpha_2_p": round(float(mod_pl.pvalues["D"]), 4),
                "alpha_3": round(float(mod_pl.params["t_post"]), 2),
                "alpha_3_p": round(float(mod_pl.pvalues["t_post"]), 4),
            }

    # Figura
    fig = _grafico_its(df_its, sector, tipo_evento, t0)
    fig.write_html(str(PLOTS_DIR / f"its_{sector.lower()}_{tipo_evento}.html"))

    # Guardar resultados
    resultados = {
        "sector": sector,
        "tipo_evento": tipo_evento,
        "t0": t0,
        "t0_label": f"{T0_ANO}-S{T0_SEM}",
        "coeficientes": coeficientes,
        "bondad_ajuste": bondad_ajuste,
        "prueba_chow": chow,
        "placebos": placebos,
    }
    json_path = RESULTS_DIR / f"its_{sector.lower()}_{tipo_evento}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)

    df_its.to_csv(RESULTS_DIR / f"its_datos_{sector.lower()}_{tipo_evento}.csv", index=False)

    print(f"\n[ITS] Sector: {sector} | Tipo evento: {tipo_evento}")
    print(f"  α₂ (cambio nivel)    = {coeficientes['alpha_2_cambio_nivel']['estimado']:>10,.0f}  "
          f"(p={coeficientes['alpha_2_cambio_nivel']['p_value']:.3f})")
    print(f"  α₃ (cambio tendencia)= {coeficientes['alpha_3_cambio_tendencia']['estimado']:>10,.0f}  "
          f"(p={coeficientes['alpha_3_cambio_tendencia']['p_value']:.3f})")
    print(f"  Chow F={chow.get('chow_F')}, p={chow.get('chow_p')}  → {chow.get('conclusion')}")

    return {
        "modelo": modelo,
        "coeficientes": coeficientes,
        "bondad_ajuste": bondad_ajuste,
        "chow": chow,
        "placebos": placebos,
        "datos": df_its,
        "fig": fig,
    }


def _grafico_its(df: pd.DataFrame, sector: str, tipo_evento: str, t0: int) -> go.Figure:
    """Visualización ITS: observado, ajustado y contrafactual."""
    t0_label = f"{T0_ANO}-S{T0_SEM}"
    periodos = df["periodo"].tolist()

    fig = go.Figure()

    # Observado
    fig.add_trace(go.Scatter(
        x=df["periodo"], y=df["total"],
        mode="lines+markers", name="Observado",
        line=dict(color="#1f77b4", width=2),
        marker=dict(size=7),
    ))

    # Contrafactual (sólo post T0)
    df_post = df[df["t"] >= t0]
    fig.add_trace(go.Scatter(
        x=df_post["periodo"], y=df_post["contrafactual"],
        mode="lines", name="Contrafactual (tendencia pre-2022)",
        line=dict(color="gray", width=2, dash="dash"),
    ))

    # Brecha (efecto estimado)
    for _, row in df_post.iterrows():
        fig.add_shape(
            type="line",
            x0=row["periodo"], x1=row["periodo"],
            y0=row["contrafactual"], y1=row["total"],
            line=dict(color="rgba(255,0,0,0.4)", width=2),
        )

    # Línea de intervención
    if t0_label in periodos:
        fig.add_vline(
            x=periodos.index(t0_label),
            line_dash="dash", line_color="red",
            annotation_text="Inicio Gobierno Petro",
            annotation_position="top left",
        )

    fig.update_layout(
        title=f"ITS — {sector} | {tipo_evento.replace('_', ' ').title()} (2018–2024)",
        xaxis_title="Semestre",
        yaxis_title="Estudiantes",
        template="plotly_white",
        hovermode="x unified",
        height=480,
    )
    return fig
