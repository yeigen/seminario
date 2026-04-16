"""
did.py — Diferencias en Diferencias (DiD) para evaluar el efecto diferencial
del gobierno Petro en matrícula de IES Oficiales vs. Privadas.

Modelo TWFE simplificado (datos agregados por sector):
    Y_{st} = α + β₁·POST_t + β₂·OFICIAL_s + β₃·(POST_t × OFICIAL_s) + ε_{st}

Y modelo con efectos fijos de IES (panel de IES individuales):
    ln(Y_{it}+1) = μ_i + γ_t + β·(POST_t × OFICIAL_i) + ε_{it}

El estimador de interés es β₃ / β (diferencial post-tratamiento).

Produce:
  - Coeficientes DiD con IC 95%
  - Gráfico de pre-tendencias (event study)
  - Prueba de tendencias paralelas
  - Exporta a data/results/did_*.json y data/results/plots/did_*.html
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
import statsmodels.formula.api as smf

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import DATA_DIR

RESULTS_DIR = DATA_DIR / "results"
PLOTS_DIR = RESULTS_DIR / "plots"

T0_ANO = 2022
T0_SEM = 2


# ---------------------------------------------------------------------------
# DiD AGREGADO (nivel sector)
# ---------------------------------------------------------------------------

def run_did_agregado(df_sector: pd.DataFrame, tipo_evento: str = "matriculados") -> dict:
    """
    DiD sobre datos agregados por sector y semestre.

    Retorna dict con coeficientes, IC, interpretación y figura.
    """
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    df = df_sector.copy()
    df["POST"] = ((df["ano"] > T0_ANO) | ((df["ano"] == T0_ANO) & (df["semestre"] >= T0_SEM))).astype(int)
    df["OFICIAL"] = (df["sector_ies"] == "Oficial").astype(int)
    df["POST_OFICIAL"] = df["POST"] * df["OFICIAL"]

    X = sm.add_constant(df[["POST", "OFICIAL", "POST_OFICIAL"]])
    y = df["total"]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        modelo = sm.OLS(y, X).fit(cov_type="HC1")

    ci = modelo.conf_int()
    did_est = float(modelo.params["POST_OFICIAL"])
    did_lower = float(ci.loc["POST_OFICIAL", 0])
    did_upper = float(ci.loc["POST_OFICIAL", 1])
    did_p = float(modelo.pvalues["POST_OFICIAL"])

    # Medias pre/post por sector para cálculo manual DiD
    medias = df.groupby(["sector_ies", "POST"])["total"].mean().reset_index()
    try:
        of_pre = medias.loc[(medias["sector_ies"] == "Oficial") & (medias["POST"] == 0), "total"].values[0]
        of_post = medias.loc[(medias["sector_ies"] == "Oficial") & (medias["POST"] == 1), "total"].values[0]
        pr_pre = medias.loc[(medias["sector_ies"] == "Privada") & (medias["POST"] == 0), "total"].values[0]
        pr_post = medias.loc[(medias["sector_ies"] == "Privada") & (medias["POST"] == 1), "total"].values[0]
        did_manual = (of_post - of_pre) - (pr_post - pr_pre)
    except (IndexError, KeyError):
        did_manual = np.nan
        of_pre = of_post = pr_pre = pr_post = np.nan

    resultados = {
        "tipo_evento": tipo_evento,
        "metodo": "DiD agregado (sector × semestre)",
        "t0_label": f"{T0_ANO}-S{T0_SEM}",
        "estimador_did": {
            "beta_3": round(did_est, 2),
            "ic_95_lower": round(did_lower, 2),
            "ic_95_upper": round(did_upper, 2),
            "p_value": round(did_p, 4),
            "significativo": did_p < 0.05,
        },
        "medias": {
            "oficial_pre": round(float(of_pre), 0) if not np.isnan(of_pre) else None,
            "oficial_post": round(float(of_post), 0) if not np.isnan(of_post) else None,
            "privada_pre": round(float(pr_pre), 0) if not np.isnan(pr_pre) else None,
            "privada_post": round(float(pr_post), 0) if not np.isnan(pr_post) else None,
            "did_manual": round(float(did_manual), 0) if not np.isnan(did_manual) else None,
        },
        "bondad_ajuste": {
            "R2": round(float(modelo.rsquared), 4),
            "AIC": round(float(modelo.aic), 2),
            "n_obs": int(modelo.nobs),
        },
    }

    json_path = RESULTS_DIR / f"did_agregado_{tipo_evento}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)

    fig = _grafico_did_medias(medias, tipo_evento)
    fig.write_html(str(PLOTS_DIR / f"did_medias_{tipo_evento}.html"))

    print(f"\n[DiD Agregado] Tipo evento: {tipo_evento}")
    print(f"  β₃ (DiD) = {did_est:,.0f}  IC95%=[{did_lower:,.0f}, {did_upper:,.0f}]  p={did_p:.4f}")
    print(f"  DiD manual: ΔOficial={of_post-of_pre:,.0f} − ΔPrivada={pr_post-pr_pre:,.0f} = {did_manual:,.0f}")

    return {"resultados": resultados, "modelo": modelo, "fig": fig}


# ---------------------------------------------------------------------------
# DiD EN PANEL DE IES (efectos fijos de institución)
# ---------------------------------------------------------------------------

def run_did_panel(df_panel: pd.DataFrame, tipo_evento: str = "matriculados") -> dict:
    """
    DiD en panel de IES individuales con efectos fijos de institución y tiempo.

    Requiere df de get_panel_ies() con columna 'matriculados'.

    La variable dependiente es ln(matriculados + 1).
    """
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    df = df_panel.copy()
    df["POST"] = ((df["ano"] > T0_ANO) | ((df["ano"] == T0_ANO) & (df["semestre"] >= T0_SEM))).astype(int)
    df["OFICIAL"] = (df["sector_ies"] == "Oficial").astype(int)
    df["POST_OFICIAL"] = df["POST"] * df["OFICIAL"]
    df["ln_matriculados"] = np.log1p(df["matriculados"])
    df["periodo_str"] = df["ano"].astype(str) + "_S" + df["semestre"].astype(str)

    # Filtrar IES con al menos 4 periodos de datos (panel razonablemente balanceado)
    conteo = df.groupby("codigo_ies")["t"].count()
    ies_validas = conteo[conteo >= 4].index
    df = df[df["codigo_ies"].isin(ies_validas)].copy()

    if df.empty or len(df) < 10:
        return {"error": "Insuficientes observaciones para DiD en panel"}

    # TWFE con efectos fijos de IES y periodo
    try:
        formula = "ln_matriculados ~ POST_OFICIAL + C(codigo_ies) + C(periodo_str)"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            modelo = smf.ols(formula, data=df).fit(cov_type="HC1")

        beta = float(modelo.params["POST_OFICIAL"])
        ci = modelo.conf_int()
        beta_lower = float(ci.loc["POST_OFICIAL", 0])
        beta_upper = float(ci.loc["POST_OFICIAL", 1])
        beta_p = float(modelo.pvalues["POST_OFICIAL"])

        # Interpretar en niveles: efecto ≈ (exp(β)-1)*100 %
        efecto_pct = (np.exp(beta) - 1) * 100

        resultados_panel = {
            "tipo_evento": tipo_evento,
            "metodo": "DiD panel TWFE (EF institución + EF tiempo)",
            "n_ies": int(df["codigo_ies"].nunique()),
            "n_obs": int(len(df)),
            "estimador_did": {
                "beta": round(beta, 6),
                "efecto_pct_aprox": round(efecto_pct, 2),
                "ic_95_lower_beta": round(beta_lower, 6),
                "ic_95_upper_beta": round(beta_upper, 6),
                "p_value": round(beta_p, 4),
                "significativo": beta_p < 0.05,
                "interpretacion": (
                    f"Las IES Oficiales tuvieron un cambio diferencial de "
                    f"aproximadamente {efecto_pct:.1f}% en matrícula post-2022 "
                    f"respecto a las privadas, controlando por efectos fijos."
                ),
            },
        }

    except Exception as e:
        resultados_panel = {"error": str(e), "tipo_evento": tipo_evento}
        return {"resultados": resultados_panel}

    json_path = RESULTS_DIR / f"did_panel_{tipo_evento}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(resultados_panel, f, ensure_ascii=False, indent=2)

    print(f"\n[DiD Panel TWFE] Tipo evento: {tipo_evento}")
    print(f"  β (ln) = {beta:.4f}  ≈ {efecto_pct:.1f}%  IC95%=[{beta_lower:.4f}, {beta_upper:.4f}]  p={beta_p:.4f}")
    print(f"  N IES = {df['codigo_ies'].nunique()}, N obs = {len(df)}")

    return {"resultados": resultados_panel, "modelo": modelo}


# ---------------------------------------------------------------------------
# EVENT STUDY (pre-tendencias)
# ---------------------------------------------------------------------------

def run_event_study(df_sector: pd.DataFrame, tipo_evento: str = "matriculados") -> dict:
    """
    Event study: estima el efecto diferencial (Oficial − Privada) para cada
    semestre respecto al periodo base (2022-S1, periodo inmediatamente anterior).

    Los coeficientes pre-2022 deben ser ~0 si se cumple el supuesto de
    tendencias paralelas.
    """
    df = df_sector.copy()

    # Identificar el periodo base (t0 - 1)
    periodos = df[["ano", "semestre", "t"]].drop_duplicates().sort_values("t")
    t0_mask = (periodos["ano"] == T0_ANO) & (periodos["semestre"] == T0_SEM)
    t0 = int(periodos.loc[t0_mask, "t"].iloc[0])
    t_base = t0 - 1  # periodo de referencia

    df["OFICIAL"] = (df["sector_ies"] == "Oficial").astype(int)

    results = []
    for t_val in sorted(df["t"].unique()):
        if t_val == t_base:
            continue
        df_t = df[df["t"].isin([t_base, t_val])].copy()
        df_t["POST_T"] = (df_t["t"] == t_val).astype(int)
        df_t["POST_T_OFICIAL"] = df_t["POST_T"] * df_t["OFICIAL"]

        X = sm.add_constant(df_t[["POST_T", "OFICIAL", "POST_T_OFICIAL"]])
        y = df_t["total"]

        if len(X) < 4:
            continue

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                mod = sm.OLS(y, X).fit(cov_type="HC1")
            ci = mod.conf_int()
            periodo_row = periodos[periodos["t"] == t_val].iloc[0]
            results.append({
                "t": t_val,
                "ano": int(periodo_row["ano"]),
                "semestre": int(periodo_row["semestre"]),
                "periodo": f"{int(periodo_row['ano'])}-S{int(periodo_row['semestre'])}",
                "coef": float(mod.params["POST_T_OFICIAL"]),
                "ic_lower": float(ci.loc["POST_T_OFICIAL", 0]),
                "ic_upper": float(ci.loc["POST_T_OFICIAL", 1]),
                "p_value": float(mod.pvalues["POST_T_OFICIAL"]),
                "pre_tratamiento": int(t_val < t0),
            })
        except Exception:
            continue

    df_es = pd.DataFrame(results)

    # Test de pre-tendencias: ¿los coeficientes pre-T0 son conjuntamente = 0?
    pre_coefs = df_es[df_es["pre_tratamiento"] == 1]["coef"]
    if len(pre_coefs) >= 2:
        t_stat, p_joint = _test_pretendencias(pre_coefs)
    else:
        t_stat, p_joint = np.nan, np.nan

    fig = _grafico_event_study(df_es, t0, tipo_evento)
    fig.write_html(str(PLOTS_DIR / f"event_study_{tipo_evento}.html"))
    df_es.to_csv(RESULTS_DIR / f"event_study_{tipo_evento}.csv", index=False)

    resultado = {
        "tipo_evento": tipo_evento,
        "periodo_base": f"{T0_ANO}-S{T0_SEM - 1}",
        "test_pretendencias_p": round(float(p_joint), 4) if not np.isnan(p_joint) else None,
        "conclusion_pretendencias": (
            "No se rechaza el supuesto de tendencias paralelas (p >= 0.05)"
            if (not np.isnan(p_joint) and p_joint >= 0.05)
            else "Posible violación de tendencias paralelas (p < 0.05)"
        ),
    }

    print(f"\n[Event Study] {tipo_evento}")
    print(f"  Test pre-tendencias: p = {p_joint:.4f}  → {resultado['conclusion_pretendencias']}")

    return {"datos": df_es, "resultado": resultado, "fig": fig}


def _test_pretendencias(pre_coefs: pd.Series) -> tuple[float, float]:
    """Test t de la media de coeficientes pre-tratamiento vs. 0."""
    from scipy import stats as sp_stats
    if len(pre_coefs) < 2:
        return np.nan, np.nan
    t_stat, p = sp_stats.ttest_1samp(pre_coefs.dropna(), 0)
    return float(t_stat), float(p)


def _grafico_did_medias(medias: pd.DataFrame, tipo_evento: str) -> go.Figure:
    """Gráfico clásico DiD con 4 puntos (2×2)."""
    fig = go.Figure()
    colores = {"Oficial": "#1f77b4", "Privada": "#ff7f0e"}
    etiquetas = {0: "Pre-2022", 1: "Post-2022"}

    for sector, color in colores.items():
        sub = medias[medias["sector_ies"] == sector].sort_values("POST")
        if sub.empty:
            continue
        fig.add_trace(go.Scatter(
            x=[etiquetas[p] for p in sub["POST"]],
            y=sub["total"],
            mode="lines+markers",
            name=sector,
            line=dict(color=color, width=2),
            marker=dict(size=12),
            hovertemplate="%{x}: %{y:,.0f}<extra>" + sector + "</extra>",
        ))

    fig.update_layout(
        title=f"DiD — Medias por sector y periodo | {tipo_evento.replace('_', ' ').title()}",
        xaxis_title="Periodo",
        yaxis_title="Matrícula media",
        template="plotly_white",
        height=400,
    )
    return fig


def _grafico_event_study(df_es: pd.DataFrame, t0: int, tipo_evento: str) -> go.Figure:
    """Gráfico de event study con coeficientes e IC por periodo."""
    if df_es.empty:
        return go.Figure()

    colores = df_es["pre_tratamiento"].map({1: "#aec7e8", 0: "#1f77b4"})

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_es["periodo"],
        y=df_es["coef"],
        mode="markers+lines",
        name="Coeficiente DiD",
        marker=dict(color=colores.tolist(), size=9),
        line=dict(color="gray", width=1),
        error_y=dict(
            type="data",
            symmetric=False,
            array=(df_es["ic_upper"] - df_es["coef"]).tolist(),
            arrayminus=(df_es["coef"] - df_es["ic_lower"]).tolist(),
            color="gray",
        ),
    ))

    fig.add_hline(y=0, line_dash="dash", line_color="black", line_width=1)

    t0_label = f"{T0_ANO}-S{T0_SEM}"
    periodos_list = df_es["periodo"].tolist()
    if t0_label in periodos_list:
        fig.add_vline(
            x=periodos_list.index(t0_label),
            line_dash="dash", line_color="red",
            annotation_text="T₀",
        )

    fig.update_layout(
        title=f"Event Study — Diferencial Oficial vs. Privada | {tipo_evento.replace('_', ' ').title()}",
        xaxis_title="Semestre",
        yaxis_title="Coeficiente DiD (Oficial − Privada)",
        template="plotly_white",
        height=450,
    )
    return fig
