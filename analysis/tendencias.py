"""
tendencias.py — Análisis exploratorio de tendencias pre/post 2022.

Produce:
  - Gráficos de series temporales (matrícula, primer curso, graduados)
  - Tasas de variación interanual y entre períodos de gobierno
  - Estadísticas descriptivas por sector
  - Exporta a data/results/tendencias_*.csv y data/results/plots/tendencias_*.html
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import DATA_DIR

RESULTS_DIR = DATA_DIR / "results"
PLOTS_DIR = RESULTS_DIR / "plots"

# Punto de quiebre: semestre ordinal donde inicia el gobierno Petro
# 2018-S1 = t=1, ..., 2022-S2 = t=10
T0_ANO = 2022
T0_SEM = 2

COLORES = {"Oficial": "#1f77b4", "Privada": "#ff7f0e"}


def _t0_index(df: pd.DataFrame) -> int:
    """Retorna el valor de t correspondiente al punto de quiebre."""
    mask = (df["ano"] == T0_ANO) & (df["semestre"] == T0_SEM)
    row = df[mask]
    if row.empty:
        raise ValueError(f"Punto de quiebre {T0_ANO}-S{T0_SEM} no encontrado en los datos")
    return int(row["t"].iloc[0])


def calcular_variaciones(df_sector: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula variaciones porcentuales interanuales y entre periodos.
    df_sector debe tener columnas: ano, semestre, sector_ies, total, t
    """
    records = []
    for sector in df_sector["sector_ies"].unique():
        sub = df_sector[df_sector["sector_ies"] == sector].sort_values("t").copy()
        sub["var_pct_anual"] = sub["total"].pct_change(periods=2) * 100  # 2 semestres = 1 año
        sub["var_pct_sem"] = sub["total"].pct_change(periods=1) * 100

        pre = sub[sub["ano"] < T0_ANO]["total"].mean()
        post = sub[(sub["ano"] > T0_ANO) | ((sub["ano"] == T0_ANO) & (sub["semestre"] >= T0_SEM))]["total"].mean()
        cambio_relativo = (post - pre) / pre * 100 if pre > 0 else np.nan

        records.append({
            "sector_ies": sector,
            "media_pre_2022": round(pre, 0),
            "media_post_2022": round(post, 0),
            "cambio_pct_pre_post": round(cambio_relativo, 2),
        })

    resumen = pd.DataFrame(records)
    variaciones = df_sector.copy()
    for sector in df_sector["sector_ies"].unique():
        mask = df_sector["sector_ies"] == sector
        variaciones.loc[mask, "var_pct_anual"] = (
            df_sector[mask].sort_values("t")["total"].pct_change(periods=2).values * 100
        )
    return variaciones, resumen


def grafico_serie_temporal(
    df_sector: pd.DataFrame,
    titulo: str,
    tipo_evento: str,
) -> go.Figure:
    """
    Genera un gráfico de series temporales con línea vertical en T0.
    """
    fig = go.Figure()

    for sector, color in COLORES.items():
        sub = df_sector[df_sector["sector_ies"] == sector].sort_values("t")
        if sub.empty:
            continue
        fig.add_trace(
            go.Scatter(
                x=sub["periodo"],
                y=sub["total"],
                mode="lines+markers",
                name=sector,
                line=dict(color=color, width=2),
                marker=dict(size=7),
                hovertemplate="<b>%{x}</b><br>%{y:,.0f} estudiantes<extra>" + sector + "</extra>",
            )
        )

    # Línea de intervención
    periodos = df_sector["periodo"].unique().tolist()
    periodos.sort()
    t0_label = f"{T0_ANO}-S{T0_SEM}"
    if t0_label in periodos:
        fig.add_vline(
            x=periodos.index(t0_label),
            line_dash="dash",
            line_color="red",
            annotation_text="Inicio Gobierno Petro<br>(2022-S2)",
            annotation_position="top right",
        )

    fig.update_layout(
        title=dict(text=titulo, font=dict(size=16)),
        xaxis_title="Semestre",
        yaxis_title="Estudiantes",
        legend_title="Sector IES",
        hovermode="x unified",
        template="plotly_white",
        height=450,
    )
    return fig


def grafico_variacion_anual(df_variaciones: pd.DataFrame, titulo: str) -> go.Figure:
    """Gráfico de barras con variación % anual por sector."""
    fig = go.Figure()
    for sector, color in COLORES.items():
        sub = df_variaciones[
            (df_variaciones["sector_ies"] == sector) & df_variaciones["var_pct_anual"].notna()
        ].sort_values("t")
        if sub.empty:
            continue
        fig.add_trace(
            go.Bar(
                x=sub["periodo"],
                y=sub["var_pct_anual"].round(2),
                name=sector,
                marker_color=color,
                hovertemplate="<b>%{x}</b><br>Var. anual: %{y:.1f}%<extra>" + sector + "</extra>",
            )
        )

    fig.add_hline(y=0, line_color="black", line_width=1)
    t0_label = f"{T0_ANO}-S{T0_SEM}"
    periodos = df_variaciones["periodo"].unique().tolist()
    periodos.sort()
    if t0_label in periodos:
        fig.add_vline(
            x=periodos.index(t0_label),
            line_dash="dash",
            line_color="red",
        )

    fig.update_layout(
        title=dict(text=titulo, font=dict(size=16)),
        xaxis_title="Semestre",
        yaxis_title="Variación % anual",
        barmode="group",
        template="plotly_white",
        height=400,
    )
    return fig


def run_tendencias(df_sector: pd.DataFrame, tipo_evento: str = "matriculados") -> dict:
    """
    Ejecuta el análisis completo de tendencias para un tipo de evento.

    Args:
        df_sector: DataFrame de get_matricula_por_sector()
        tipo_evento: 'matriculados', 'primer_curso', 'graduados', etc.

    Returns:
        dict con DataFrames de resultados y figuras Plotly
    """
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    df_var, df_resumen = calcular_variaciones(df_sector)

    fig_serie = grafico_serie_temporal(
        df_sector,
        titulo=f"Matrícula por sector IES — {tipo_evento.replace('_', ' ').title()} (2018–2024)",
        tipo_evento=tipo_evento,
    )
    fig_var = grafico_variacion_anual(
        df_var,
        titulo=f"Variación % anual — {tipo_evento.replace('_', ' ').title()} por sector",
    )

    # Guardar
    df_var.to_csv(RESULTS_DIR / f"tendencias_{tipo_evento}.csv", index=False)
    df_resumen.to_csv(RESULTS_DIR / f"resumen_pre_post_{tipo_evento}.csv", index=False)
    fig_serie.write_html(str(PLOTS_DIR / f"serie_{tipo_evento}.html"))
    fig_var.write_html(str(PLOTS_DIR / f"variacion_{tipo_evento}.html"))

    print(f"[tendencias] {tipo_evento}: guardado en {RESULTS_DIR}")
    print(df_resumen.to_string(index=False))

    return {
        "variaciones": df_var,
        "resumen": df_resumen,
        "fig_serie": fig_serie,
        "fig_variacion": fig_var,
    }
