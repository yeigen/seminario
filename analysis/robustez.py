"""Robustez y sensibilidad para la entrega final Hito 4/5.

El modulo genera una matriz reproducible de estimadores bajo variaciones del
punto de quiebre, la forma funcional y la muestra. Usa las mismas fuentes del
analisis de Hito 3 y guarda artefactos en data/results/.
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

T0_CANDIDATOS = [(2021, 2), (2022, 1), (2022, 2), (2023, 1)]
FORMAS_FUNCIONALES = ["niveles", "log", "diff"]
MUESTRAS_PANEL = ["todas", "solo_universidades", "excluir_menores_500"]


def _periodo_label(ano: int, semestre: int) -> str:
    return f"{int(ano)}-S{int(semestre)}"


def _get_t0(df: pd.DataFrame, ano: int, semestre: int) -> int | None:
    mask = (df["ano"] == ano) & (df["semestre"] == semestre)
    if not mask.any():
        return None
    return int(df.loc[mask, "t"].drop_duplicates().iloc[0])


def _with_outcome(df: pd.DataFrame, forma: str, value_col: str = "total") -> pd.DataFrame:
    out = df.sort_values(["sector_ies", "t"]).copy()
    if forma == "niveles":
        out["y"] = out[value_col].astype(float)
    elif forma == "log":
        out["y"] = np.log1p(out[value_col].astype(float))
    elif forma == "diff":
        out["y"] = out.groupby("sector_ies")[value_col].diff()
    else:
        raise ValueError(f"Forma funcional no soportada: {forma}")
    return out.dropna(subset=["y"]).copy()


def _row_base(
    estimador: str,
    tipo_evento: str,
    t0_label: str,
    forma: str,
    muestra: str,
    n_obs: int,
) -> dict:
    return {
        "estimador": estimador,
        "tipo_evento": tipo_evento,
        "t0": t0_label,
        "forma_funcional": forma,
        "muestra": muestra,
        "n_obs": int(n_obs),
        "coeficiente": None,
        "ic_95_lower": None,
        "ic_95_upper": None,
        "p_value": None,
        "significativo": None,
        "signo": None,
        "estado": "ok",
        "nota": "",
    }


def _complete_row(row: dict, coef: float, lower: float, upper: float, p_value: float) -> dict:
    row["coeficiente"] = round(float(coef), 6)
    row["ic_95_lower"] = round(float(lower), 6)
    row["ic_95_upper"] = round(float(upper), 6)
    row["p_value"] = round(float(p_value), 6)
    row["significativo"] = bool(p_value < 0.05)
    row["signo"] = "positivo" if coef > 0 else "negativo" if coef < 0 else "cero"
    return row


def _error_row(row: dict, exc: Exception | str) -> dict:
    row["estado"] = "error"
    row["nota"] = str(exc)
    return row


def estimar_its(
    df_sector: pd.DataFrame,
    tipo_evento: str,
    ano_t0: int,
    semestre_t0: int,
    forma: str,
    sector: str = "Oficial",
) -> list[dict]:
    """Estima ITS para alpha_2 y alpha_3 bajo una especificacion."""
    t0_label = _periodo_label(ano_t0, semestre_t0)
    rows = []
    t0 = _get_t0(df_sector, ano_t0, semestre_t0)
    base_row = _row_base("ITS_alpha2_nivel", tipo_evento, t0_label, forma, sector, 0)
    trend_row = _row_base("ITS_alpha3_tendencia", tipo_evento, t0_label, forma, sector, 0)
    if t0 is None:
        return [_error_row(base_row, "t0 no encontrado"), _error_row(trend_row, "t0 no encontrado")]

    try:
        df = _with_outcome(df_sector[df_sector["sector_ies"] == sector], forma)
        df["D"] = (df["t"] >= t0).astype(int)
        df["t_post"] = (df["t"] - t0) * df["D"]
        if len(df) < 6:
            raise ValueError("muestra insuficiente para ITS")

        X = sm.add_constant(df[["t", "D", "t_post"]])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            modelo = sm.OLS(df["y"], X).fit(cov_type="HAC", cov_kwds={"maxlags": 2})
        ci = modelo.conf_int()

        base_row["n_obs"] = int(modelo.nobs)
        trend_row["n_obs"] = int(modelo.nobs)
        rows.append(_complete_row(base_row, modelo.params["D"], ci.loc["D", 0], ci.loc["D", 1], modelo.pvalues["D"]))
        rows.append(
            _complete_row(
                trend_row,
                modelo.params["t_post"],
                ci.loc["t_post", 0],
                ci.loc["t_post", 1],
                modelo.pvalues["t_post"],
            )
        )
    except Exception as exc:
        rows.append(_error_row(base_row, exc))
        rows.append(_error_row(trend_row, exc))
    return rows


def estimar_did_agregado(
    df_sector: pd.DataFrame,
    tipo_evento: str,
    ano_t0: int,
    semestre_t0: int,
    forma: str,
) -> dict:
    """Estima DiD agregado bajo una especificacion."""
    t0_label = _periodo_label(ano_t0, semestre_t0)
    row = _row_base("DiD_agregado_beta3", tipo_evento, t0_label, forma, "sectores", 0)
    try:
        df = _with_outcome(df_sector, forma)
        df["POST"] = ((df["ano"] > ano_t0) | ((df["ano"] == ano_t0) & (df["semestre"] >= semestre_t0))).astype(int)
        df["OFICIAL"] = (df["sector_ies"] == "Oficial").astype(int)
        df["POST_OFICIAL"] = df["POST"] * df["OFICIAL"]
        if df["POST"].nunique() < 2 or df["OFICIAL"].nunique() < 2:
            raise ValueError("sin variacion suficiente para DiD")

        X = sm.add_constant(df[["POST", "OFICIAL", "POST_OFICIAL"]])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            modelo = sm.OLS(df["y"], X).fit(cov_type="HC1")
        ci = modelo.conf_int()
        row["n_obs"] = int(modelo.nobs)
        return _complete_row(
            row,
            modelo.params["POST_OFICIAL"],
            ci.loc["POST_OFICIAL", 0],
            ci.loc["POST_OFICIAL", 1],
            modelo.pvalues["POST_OFICIAL"],
        )
    except Exception as exc:
        return _error_row(row, exc)


def _filtrar_panel(df_panel: pd.DataFrame, muestra: str) -> pd.DataFrame:
    df = df_panel.copy()
    if muestra == "solo_universidades":
        df = df[df["caracter_ies"].astype(str).str.contains("universidad", case=False, na=False)]
    elif muestra == "excluir_menores_500":
        promedio = df.groupby("codigo_ies")["matriculados"].mean()
        validas = promedio[promedio >= 500].index
        df = df[df["codigo_ies"].isin(validas)]
    elif muestra != "todas":
        raise ValueError(f"Muestra no soportada: {muestra}")
    return df.copy()


def estimar_did_panel(
    df_panel: pd.DataFrame,
    ano_t0: int,
    semestre_t0: int,
    muestra: str,
) -> dict:
    """Estima DiD panel TWFE en log(matriculados + 1)."""
    t0_label = _periodo_label(ano_t0, semestre_t0)
    row = _row_base("DiD_panel_TWFE_beta", "matriculados", t0_label, "log", muestra, 0)
    try:
        df = _filtrar_panel(df_panel, muestra)
        df["POST"] = ((df["ano"] > ano_t0) | ((df["ano"] == ano_t0) & (df["semestre"] >= semestre_t0))).astype(int)
        df["OFICIAL"] = (df["sector_ies"] == "Oficial").astype(int)
        df["POST_OFICIAL"] = df["POST"] * df["OFICIAL"]
        df["ln_matriculados"] = np.log1p(df["matriculados"].astype(float))
        df["periodo_str"] = df["ano"].astype(str) + "_S" + df["semestre"].astype(str)

        conteo = df.groupby("codigo_ies")["t"].count()
        df = df[df["codigo_ies"].isin(conteo[conteo >= 4].index)].copy()
        if len(df) < 20 or df["POST"].nunique() < 2 or df["OFICIAL"].nunique() < 2:
            raise ValueError("muestra insuficiente para TWFE")

        formula = "ln_matriculados ~ POST_OFICIAL + C(codigo_ies) + C(periodo_str)"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            modelo = smf.ols(formula, data=df).fit(cov_type="HC1")
        ci = modelo.conf_int()
        row["n_obs"] = int(modelo.nobs)
        row["n_ies"] = int(df["codigo_ies"].nunique())
        return _complete_row(
            row,
            modelo.params["POST_OFICIAL"],
            ci.loc["POST_OFICIAL", 0],
            ci.loc["POST_OFICIAL", 1],
            modelo.pvalues["POST_OFICIAL"],
        )
    except Exception as exc:
        return _error_row(row, exc)


def _resumen_estabilidad(df: pd.DataFrame) -> dict:
    ok = df[df["estado"] == "ok"].copy()
    resumen = {
        "total_corridas": int(len(df)),
        "corridas_exitosas": int(len(ok)),
        "corridas_error": int((df["estado"] != "ok").sum()),
        "por_estimador": {},
    }
    for estimador, sub in ok.groupby("estimador"):
        signos = sorted(str(s) for s in sub["signo"].dropna().unique())
        resumen["por_estimador"][estimador] = {
            "n": int(len(sub)),
            "signos_observados": signos,
            "estable_en_signo": len(signos) <= 1,
            "significativos": int(sub["significativo"].sum()),
        }
    return resumen


def _grafico_forest(df: pd.DataFrame) -> go.Figure:
    ok = df[(df["estado"] == "ok") & df["coeficiente"].notna()].copy()
    if ok.empty:
        return go.Figure()
    ok["label"] = (
        ok["estimador"]
        + " | "
        + ok["tipo_evento"]
        + " | "
        + ok["t0"]
        + " | "
        + ok["forma_funcional"]
        + " | "
        + ok["muestra"]
    )
    ok = ok.sort_values(["estimador", "tipo_evento", "t0", "forma_funcional", "muestra"])
    fig = go.Figure(
        go.Scatter(
            x=ok["coeficiente"],
            y=ok["label"],
            mode="markers",
            error_x=dict(
                type="data",
                symmetric=False,
                array=(ok["ic_95_upper"] - ok["coeficiente"]).clip(lower=0),
                arrayminus=(ok["coeficiente"] - ok["ic_95_lower"]).clip(lower=0),
            ),
            marker=dict(
                color=np.where(ok["significativo"], "#1f77b4", "#9aa0a6"),
                size=8,
            ),
            hovertemplate="%{y}<br>coef=%{x:.4f}<extra></extra>",
        )
    )
    fig.add_vline(x=0, line_dash="dash", line_color="black")
    fig.update_layout(
        title="Robustez de estimadores Hito 4/5",
        xaxis_title="Coeficiente estimado",
        yaxis_title="Especificacion",
        template="plotly_white",
        height=max(500, 18 * len(ok)),
        margin=dict(l=260, r=40, t=70, b=40),
    )
    return fig


def run_robustez_completa() -> dict:
    """Ejecuta la matriz de robustez y guarda CSV, JSON y grafico."""
    from analysis.queries import get_matricula_por_sector, get_panel_ies

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []
    datos = {
        "matriculados": get_matricula_por_sector("matriculados"),
        "primer_curso": get_matricula_por_sector("primer_curso"),
    }
    df_panel = get_panel_ies()

    for tipo_evento, df_sector in datos.items():
        for ano_t0, semestre_t0 in T0_CANDIDATOS:
            for forma in FORMAS_FUNCIONALES:
                rows.extend(estimar_its(df_sector, tipo_evento, ano_t0, semestre_t0, forma))
                rows.append(estimar_did_agregado(df_sector, tipo_evento, ano_t0, semestre_t0, forma))

    for ano_t0, semestre_t0 in T0_CANDIDATOS:
        for muestra in MUESTRAS_PANEL:
            rows.append(estimar_did_panel(df_panel, ano_t0, semestre_t0, muestra))

    df = pd.DataFrame(rows)
    csv_path = RESULTS_DIR / "robustez_sensibilidad.csv"
    df.to_csv(csv_path, index=False)

    resumen = _resumen_estabilidad(df)
    resumen_path = RESULTS_DIR / "robustez_resumen.json"
    resumen_path.write_text(json.dumps(resumen, ensure_ascii=False, indent=2), encoding="utf-8")

    fig = _grafico_forest(df)
    fig_path = PLOTS_DIR / "robustez_forest_plot.html"
    fig.write_html(str(fig_path))

    return {
        "tabla": df,
        "resumen": resumen,
        "csv_path": str(csv_path),
        "json_path": str(resumen_path),
        "plot_path": str(fig_path),
    }


if __name__ == "__main__":
    run_robustez_completa()
