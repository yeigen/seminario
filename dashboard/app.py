"""
dashboard/app.py — Dashboard Streamlit para Hito 3.

Muestra:
  - Serie temporal de matrícula por sector (Oficial vs Privada)
  - Resultados ITS: observado vs contrafactual
  - Resultados DiD: estimador, medias pre/post, event study
  - Análisis de escenarios
  - Resumen ejecutivo de hallazgos

Uso:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

# ── rutas ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.globals import DATA_DIR

RESULTS_DIR = DATA_DIR / "results"
PLOTS_DIR = RESULTS_DIR / "plots"

# ── SVG icons ────────────────────────────────────────────────────────────────

SVG_GRADUATION = '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 10v6M2 10l10-5 10 5-10 5z"/><path d="M6 12v5c0 2 4 3 6 3s6-1 6-3v-5"/></svg>'
SVG_TREND_UP = '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/></svg>'
SVG_TREND_DOWN = '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 17 13.5 8.5 8.5 13.5 2 7"/><polyline points="16 17 22 17 22 11"/></svg>'
SVG_CHART = '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>'
SVG_SCALE = '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 3l5 5-5 5"/><path d="M21 8H9"/><path d="M8 21l-5-5 5-5"/><path d="M3 16h12"/></svg>'
SVG_REFRESH = '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0114.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0020.49 15"/></svg>'
SVG_CLIPBOARD = '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 4h2a2 2 0 012 2v14a2 2 0 01-2 2H6a2 2 0 01-2-2V6a2 2 0 012-2h2"/><rect x="8" y="2" width="8" height="4" rx="1" ry="1"/></svg>'
SVG_SHUFFLE = '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 3 21 3 21 8"/><line x1="4" y1="20" x2="21" y2="3"/><polyline points="21 16 21 21 16 21"/><line x1="15" y1="15" x2="21" y2="21"/><line x1="4" y1="4" x2="9" y2="9"/></svg>'
SVG_CHECK = '<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#22c55e" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M9 12l2 2 4-4"/></svg>'
SVG_CROSS = '<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#ef4444" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>'
SVG_QUESTION = '<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#a3a3a3" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 015.83 1c0 2-3 3-3 3"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>'


def _svg_icon(svg: str, label: str = "") -> str:
    """Wrap an SVG inline with optional label text."""
    if label:
        return f"{svg} {label}"
    return svg


st.set_page_config(
    page_title="Hito 3 — Educación Superior Colombia",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── helpers ──────────────────────────────────────────────────────────────────


def _load_json(path: Path) -> dict | None:
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return None


def _load_csv(path: Path) -> pd.DataFrame | None:
    if path.exists():
        return pd.read_csv(path)
    return None


def _badge(sig: bool | None) -> str:
    if sig is True:
        return f"{_svg_icon(SVG_CHECK)} Significativo"
    if sig is False:
        return f"{_svg_icon(SVG_CROSS)} No significativo"
    return f"{_svg_icon(SVG_QUESTION)} No disponible"


def _badge_plain(sig: bool | None) -> str:
    """Text-only badge for st.metric delta (no HTML)."""
    if sig is True:
        return "Significativo"
    if sig is False:
        return "No significativo"
    return "No disponible"


# ── sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown(f"{SVG_GRADUATION} **Educación Superior**", unsafe_allow_html=True)
    st.markdown("**Colombia 2018–2024**")
    st.caption("Hito 3 — Metodología y primeros resultados")
    st.divider()
    tipo_evento = st.selectbox(
        "Tipo de evento",
        ["matriculados", "primer_curso", "graduados"],
        format_func=lambda x: {
            "matriculados": "Matriculados",
            "primer_curso": "Primera matrícula",
            "graduados": "Graduados",
        }[x],
    )
    sector_sel = st.selectbox("Sector IES", ["Oficial", "Privada"])
    st.divider()
    st.markdown("**Punto de quiebre:** 2022-S2  \n(inicio gobierno Petro)")
    st.markdown("**Fuente:** SNIES 2018–2024")
    st.divider()
    st.info(
        "Dashboard en modo lectura: los resultados se cargan desde data/results/. "
        "Para regenerarlos usa analysis/runner_final.py fuera del dashboard."
    )


def _fmt(v: object) -> str:
    """Formatea un número como entero con separador de miles, o 'N/A' si es None."""
    if v is None or (isinstance(v, float) and v != v):  # None o NaN
        return "N/A"
    try:
        return f"{v:,.0f}"
    except (TypeError, ValueError):
        return str(v)


# ── tabs ─────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
    [
        "Tendencias",
        "Análisis temporal",
        "Comparación sectorial",
        "Incertidumbre",
        "Robustez",
        "Triangulación",
        "Resumen ejecutivo",
    ]
)

# ────────────────────────────────────────────────────────────────────────────
# TAB 1: TENDENCIAS
# ────────────────────────────────────────────────────────────────────────────

with tab1:
    st.markdown(
        f"{SVG_TREND_UP} **Tendencias de matrícula por sector (2018–2024)**",
        unsafe_allow_html=True,
    )

    df_tend = _load_csv(RESULTS_DIR / f"tendencias_{tipo_evento}.csv")
    df_resumen = _load_csv(RESULTS_DIR / f"resumen_pre_post_{tipo_evento}.csv")

    if df_tend is None:
        st.info(
            "Ejecuta el análisis desde el botón en la barra lateral para generar los resultados."
        )
    else:
        # Serie temporal
        fig = go.Figure()
        for sector, color in [("Oficial", "#1f77b4"), ("Privada", "#ff7f0e")]:
            sub = df_tend[df_tend["sector_ies"] == sector].sort_values("t")
            fig.add_trace(
                go.Scatter(
                    x=sub["periodo"],
                    y=sub["total"],
                    mode="lines+markers",
                    name=sector,
                    line=dict(color=color, width=2.5),
                    marker=dict(size=8),
                )
            )
        periodos = sorted(df_tend["periodo"].unique().tolist())
        if "2022-S2" in periodos:
            fig.add_vline(
                x=periodos.index("2022-S2"),
                line_dash="dash",
                line_color="red",
                annotation_text="Inicio Gobierno Petro (2022-S2)",
                annotation_position="top right",
            )
        fig.update_layout(
            title=f"{tipo_evento.replace('_', ' ').title()} por sector IES — Colombia 2018–2024",
            xaxis_title="Semestre",
            yaxis_title="Estudiantes",
            hovermode="x unified",
            template="plotly_white",
            height=430,
        )
        st.plotly_chart(fig, use_container_width=True)

        if df_resumen is not None:
            st.subheader("Cambio pre/post 2022 por sector")
            cols = st.columns(len(df_resumen))
            for i, row in df_resumen.iterrows():
                with cols[i]:
                    delta = row.get("cambio_pct_pre_post", 0)
                    st.metric(
                        label=row["sector_ies"],
                        value=f"{row['media_post_2022']:,.0f}",
                        delta=f"{delta:+.1f}% vs. pre-2022",
                        delta_color="normal",
                    )
            st.caption("Media semestral de estudiantes por periodo de gobierno.")

        if "var_pct_anual" in df_tend.columns:
            st.subheader("Variación porcentual anual")
            fig_var = go.Figure()
            for sector, color in [("Oficial", "#1f77b4"), ("Privada", "#ff7f0e")]:
                sub = df_tend[
                    (df_tend["sector_ies"] == sector) & df_tend["var_pct_anual"].notna()
                ]
                fig_var.add_trace(
                    go.Bar(
                        x=sub["periodo"],
                        y=sub["var_pct_anual"].round(2),
                        name=sector,
                        marker_color=color,
                    )
                )
            fig_var.add_hline(y=0, line_color="black", line_width=1)
            fig_var.update_layout(barmode="group", template="plotly_white", height=380)
            st.plotly_chart(fig_var, use_container_width=True)


# ────────────────────────────────────────────────────────────────────────────
# TAB 2: ANÁLISIS TEMPORAL (ITS)
# ────────────────────────────────────────────────────────────────────────────

with tab2:
    st.markdown(
        f"{SVG_CHART} **Análisis temporal (antes y después del cambio de gobierno)**",
        unsafe_allow_html=True,
    )
    st.markdown(
        "Se compara la tendencia observada contra lo que habría ocurrido "
        "si la tendencia anterior se hubiera mantenido (escenario sin cambio de política)."
    )

    its_json = _load_json(RESULTS_DIR / f"its_{sector_sel.lower()}_{tipo_evento}.json")
    df_its = _load_csv(
        RESULTS_DIR / f"its_datos_{sector_sel.lower()}_{tipo_evento}.csv"
    )

    if its_json is None:
        st.info("Sin resultados aún. Ejecuta el análisis.")
    else:
        coefs = its_json.get("coeficientes", {})
        bondad = its_json.get("bondad_ajuste", {})
        chow = its_json.get("prueba_chow", {})

        # Métricas principales
        a2 = coefs.get("alpha_2_cambio_nivel", {})
        a3 = coefs.get("alpha_3_cambio_tendencia", {})

        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric(
                "Cambio inmediato",
                f"{a2.get('estimado', 'N/A'):,.0f}",
                delta=_badge_plain(
                    a2.get("p_value", 1) < 0.05
                    if a2.get("p_value") is not None
                    else None
                ),
            )
        with c2:
            st.metric(
                "Cambio de tendencia por semestre",
                f"{a3.get('estimado', 'N/A'):,.0f}",
                delta=_badge_plain(
                    a3.get("p_value", 1) < 0.05
                    if a3.get("p_value") is not None
                    else None
                ),
            )
        with c3:
            st.metric("Ajuste del modelo", f"{bondad.get('R2', 'N/A')}")

        conclusion_chow = chow.get("conclusion", "")
        if conclusion_chow:
            st.caption(f"Prueba de cambio estructural: {conclusion_chow}")

        # Gráfico observado vs contrafactual
        if df_its is not None and "contrafactual" in df_its.columns:
            fig_its = go.Figure()
            fig_its.add_trace(
                go.Scatter(
                    x=df_its["periodo"],
                    y=df_its["total"],
                    mode="lines+markers",
                    name="Observado",
                    line=dict(color="#1f77b4", width=2.5),
                    marker=dict(size=8),
                )
            )
            df_post = df_its[df_its["D"] == 1]
            fig_its.add_trace(
                go.Scatter(
                    x=df_post["periodo"],
                    y=df_post["contrafactual"],
                    mode="lines",
                    name="Sin cambio de política (proyección)",
                    line=dict(color="gray", dash="dash", width=2),
                )
            )
            periodos_list = sorted(df_its["periodo"].unique().tolist())
            if "2022-S2" in periodos_list:
                fig_its.add_vline(
                    x=periodos_list.index("2022-S2"),
                    line_dash="dash",
                    line_color="red",
                    annotation_text="2022-S2",
                )
            fig_its.update_layout(
                title=f"Observado vs. proyección sin política — {sector_sel} | {tipo_evento.replace('_', ' ').title()}",
                xaxis_title="Semestre",
                yaxis_title="Estudiantes",
                template="plotly_white",
                height=430,
                hovermode="x unified",
            )
            st.plotly_chart(fig_its, use_container_width=True)

            # Efecto estimado
            if "efecto_estimado" in df_its.columns:
                st.subheader("Diferencia entre lo observado y la proyección")
                df_ef = df_its[df_its["D"] == 1][["periodo", "efecto_estimado"]].copy()
                df_ef["efecto_estimado"] = df_ef["efecto_estimado"].round(0)
                fig_ef = go.Figure(
                    go.Bar(
                        x=df_ef["periodo"],
                        y=df_ef["efecto_estimado"],
                        marker_color=[
                            "green" if v >= 0 else "red"
                            for v in df_ef["efecto_estimado"]
                        ],
                    )
                )
                fig_ef.add_hline(y=0, line_color="black")
                fig_ef.update_layout(
                    template="plotly_white",
                    height=320,
                    yaxis_title="Estudiantes (diferencia)",
                )
                st.plotly_chart(fig_ef, use_container_width=True)

        # Placebos
        placebos = its_json.get("placebos", {})
        if placebos:
            st.subheader("Pruebas con puntos de quiebre alternativos")
            df_pl = pd.DataFrame(
                [{"Punto alternativo": k, **v} for k, v in placebos.items()]
            )
            st.dataframe(df_pl, use_container_width=True)


# ────────────────────────────────────────────────────────────────────────────
# TAB 3: COMPARACIÓN SECTORIAL (DiD)
# ────────────────────────────────────────────────────────────────────────────

with tab3:
    st.markdown(
        f"{SVG_SCALE} **Comparación sectorial (Oficial vs. Privada)**",
        unsafe_allow_html=True,
    )
    st.markdown(
        "Se compara cómo cambió la matrícula en el sector **Oficial** respecto al sector "
        "**Privado** después de 2022. Si la política afecta solo al sector oficial, "
        "la diferencia entre ambos debería cambiar tras la intervención."
    )

    did_json = _load_json(RESULTS_DIR / f"did_agregado_{tipo_evento}.json")
    did_panel_json = _load_json(RESULTS_DIR / f"did_panel_{tipo_evento}.json")
    df_es = _load_csv(RESULTS_DIR / f"event_study_{tipo_evento}.csv")

    if did_json is None:
        st.info("Sin resultados de comparación. Ejecuta el análisis.")
    else:
        est = did_json.get("estimador_did", {})
        medias = did_json.get("medias", {})

        # Estimador principal
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Efecto diferencial estimado", _fmt(est.get("beta_3")))
        with c2:
            st.metric(
                "Intervalo de confianza 95%",
                f"[{_fmt(est.get('ic_95_lower'))}, {_fmt(est.get('ic_95_upper'))}]",
            )
        with c3:
            st.metric(
                "Significancia",
                _badge_plain(est.get("significativo")),
            )

        # Tabla 2×2
        st.subheader("Medias por sector y periodo")
        tabla_2x2 = pd.DataFrame(
            {
                "Sector": ["Oficial", "Privada", "Diferencia"],
                "Pre-2022": [
                    _fmt(medias.get("oficial_pre")),
                    _fmt(medias.get("privada_pre")),
                    _fmt(
                        (medias.get("oficial_pre") or 0)
                        - (medias.get("privada_pre") or 0)
                    ),
                ],
                "Post-2022": [
                    _fmt(medias.get("oficial_post")),
                    _fmt(medias.get("privada_post")),
                    _fmt(
                        (medias.get("oficial_post") or 0)
                        - (medias.get("privada_post") or 0)
                    ),
                ],
                "Cambio (Post - Pre)": [
                    _fmt(
                        (medias.get("oficial_post") or 0)
                        - (medias.get("oficial_pre") or 0)
                    ),
                    _fmt(
                        (medias.get("privada_post") or 0)
                        - (medias.get("privada_pre") or 0)
                    ),
                    _fmt(medias.get("did_manual")),
                ],
            }
        )
        st.dataframe(tabla_2x2, use_container_width=True)

        # Gráfico DiD
        fig_did = go.Figure()
        for sector, color, pre_key, post_key in [
            ("Oficial", "#1f77b4", "oficial_pre", "oficial_post"),
            ("Privada", "#ff7f0e", "privada_pre", "privada_post"),
        ]:
            pre = medias.get(pre_key)
            post = medias.get(post_key)
            if pre is not None and post is not None:
                fig_did.add_trace(
                    go.Scatter(
                        x=["Pre-2022", "Post-2022"],
                        y=[pre, post],
                        mode="lines+markers",
                        name=sector,
                        line=dict(color=color, width=2.5),
                        marker=dict(size=12),
                    )
                )
        fig_did.update_layout(
            title=f"Medias por periodo y sector | {tipo_evento.replace('_', ' ').title()}",
            xaxis_title="Periodo",
            yaxis_title="Matrícula media por semestre",
            template="plotly_white",
            height=380,
        )
        st.plotly_chart(fig_did, use_container_width=True)

        # DiD Panel
        if did_panel_json:
            est_p = did_panel_json.get("estimador_did", {})
            st.subheader("Análisis con controles por institución y tiempo")
            efecto_pct = est_p.get("efecto_pct_aprox", "N/A")
            interp = est_p.get("interpretacion", "")
            st.info(
                f"Efecto estimado: **{efecto_pct}%**  \n"
                f"Instituciones analizadas: {did_panel_json.get('n_ies', '?')}, "
                f"Observaciones: {did_panel_json.get('n_obs', '?')}  \n"
                f"{interp}"
            )

    # Event Study
    if df_es is not None and not df_es.empty:
        st.subheader("Evolución del efecto por semestre")
        fig_es = go.Figure()
        colores_es = df_es["pre_tratamiento"].map({1: "#aec7e8", 0: "#1f77b4"})
        fig_es.add_trace(
            go.Scatter(
                x=df_es["periodo"],
                y=df_es["coef"],
                mode="markers+lines",
                marker=dict(color=colores_es.tolist(), size=9),
                error_y=dict(
                    type="data",
                    symmetric=False,
                    array=(df_es["ic_upper"] - df_es["coef"]).tolist(),
                    arrayminus=(df_es["coef"] - df_es["ic_lower"]).tolist(),
                ),
                name="Diferencia Oficial - Privada por semestre",
            )
        )
        fig_es.add_hline(y=0, line_dash="dash", line_color="black")
        periodos_es = df_es["periodo"].tolist()
        if "2022-S2" in periodos_es:
            fig_es.add_vline(
                x=periodos_es.index("2022-S2"),
                line_dash="dash",
                line_color="red",
                annotation_text="2022-S2",
            )
        fig_es.update_layout(
            title="Diferencia entre sectores por semestre",
            xaxis_title="Semestre",
            yaxis_title="Diferencia (Oficial - Privada)",
            template="plotly_white",
            height=420,
        )
        st.plotly_chart(fig_es, use_container_width=True)
        st.caption(
            "Los puntos azul claro (pre-2022) cerca de cero indican que ambos sectores "
            "tenían una tendencia similar antes del cambio de gobierno."
        )


# ────────────────────────────────────────────────────────────────────────────
# TAB 4: INCERTIDUMBRE
# ────────────────────────────────────────────────────────────────────────────

with tab4:
    st.markdown(
        f"{SVG_SHUFFLE} **Análisis de incertidumbre (1,000 simulaciones)**",
        unsafe_allow_html=True,
    )
    st.markdown(
        "Se realizaron 1,000 simulaciones para estimar qué tan confiables "
        "son los resultados. Los intervalos muestran el rango probable de valores."
    )

    boot_json = _load_json(RESULTS_DIR / f"bootstrap_{tipo_evento}.json")

    if boot_json is None:
        st.info("Sin resultados de simulación. Ejecuta el análisis.")
    else:
        its_b = boot_json.get("its_bootstrap", {})
        did_b = boot_json.get("did_bootstrap", {})
        a2_b = its_b.get("alpha_2_cambio_nivel", {})
        b3_b = did_b.get("beta_3_did", {})

        st.subheader("Rangos de confianza (95%)")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Cambio inmediato en la tendencia**")
            st.metric("Valor central", f"{a2_b.get('media_boot', 'N/A'):,.0f}")
            st.metric("Límite inferior", f"{a2_b.get('ic_95_lower', 'N/A'):,.0f}")
            st.metric("Límite superior", f"{a2_b.get('ic_95_upper', 'N/A'):,.0f}")
        with col2:
            st.markdown("**Efecto diferencial entre sectores**")
            st.metric("Valor central", f"{b3_b.get('media_boot', 'N/A'):,.0f}")
            st.metric("Límite inferior", f"{b3_b.get('ic_95_lower', 'N/A'):,.0f}")
            st.metric("Límite superior", f"{b3_b.get('ic_95_upper', 'N/A'):,.0f}")

        # Escenarios
        esc_of = boot_json.get("escenarios_oficial", {})
        if esc_of and "escenarios" in esc_of:
            st.subheader("Proyecciones — Sector Oficial")
            df_esc = pd.DataFrame(esc_of["escenarios"])
            fig_esc = go.Figure()
            fig_esc.add_trace(
                go.Scatter(
                    x=df_esc["periodo"],
                    y=df_esc["observado"],
                    name="Observado",
                    mode="lines+markers",
                    line=dict(color="#1f77b4", width=2.5),
                )
            )
            fig_esc.add_trace(
                go.Scatter(
                    x=df_esc["periodo"],
                    y=df_esc["escenario_base"],
                    name="Sin cambio de política",
                    mode="lines",
                    line=dict(color="gray", dash="dash"),
                )
            )
            fig_esc.add_trace(
                go.Scatter(
                    x=df_esc["periodo"],
                    y=df_esc["escenario_optimista"],
                    name="Escenario favorable",
                    mode="lines",
                    line=dict(color="green", dash="dot"),
                )
            )
            fig_esc.add_trace(
                go.Scatter(
                    x=df_esc["periodo"],
                    y=df_esc["escenario_adverso"],
                    name="Escenario desfavorable",
                    mode="lines",
                    line=dict(color="red", dash="dot"),
                )
            )
            fig_esc.update_layout(
                title="Observado vs. proyecciones (base / favorable / desfavorable)",
                xaxis_title="Semestre",
                yaxis_title="Matriculados",
                template="plotly_white",
                height=420,
                hovermode="x unified",
            )
            st.plotly_chart(fig_esc, use_container_width=True)


# ────────────────────────────────────────────────────────────────────────────
# TAB 5: ROBUSTEZ
# ────────────────────────────────────────────────────────────────────────────

with tab5:
    st.markdown(
        f"{SVG_REFRESH} **Robustez y sensibilidad — Hito 4**",
        unsafe_allow_html=True,
    )

    robustez_json = _load_json(RESULTS_DIR / "robustez_resumen.json")
    df_robustez = _load_csv(RESULTS_DIR / "robustez_sensibilidad.csv")

    if robustez_json is None or df_robustez is None:
        st.info("Sin resultados de robustez. Ejecuta el análisis final Hito 4/5.")
    else:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Corridas totales", robustez_json.get("total_corridas", "N/A"))
        with c2:
            st.metric("Corridas exitosas", robustez_json.get("corridas_exitosas", "N/A"))
        with c3:
            st.metric("Corridas con error", robustez_json.get("corridas_error", "N/A"))

        st.subheader("Estabilidad por estimador")
        resumen_estimadores = []
        for estimador, info in robustez_json.get("por_estimador", {}).items():
            resumen_estimadores.append(
                {
                    "Estimador": estimador,
                    "Corridas": info.get("n"),
                    "Signos observados": ", ".join(info.get("signos_observados", [])),
                    "Estable en signo": "Sí" if info.get("estable_en_signo") else "No",
                    "Significativos": info.get("significativos"),
                }
            )
        if resumen_estimadores:
            st.dataframe(pd.DataFrame(resumen_estimadores), use_container_width=True)

        st.subheader("Matriz de sensibilidad")
        cols = [
            c
            for c in [
                "estimador",
                "tipo_evento",
                "t0",
                "forma_funcional",
                "muestra",
                "coeficiente",
                "ic_95_lower",
                "ic_95_upper",
                "p_value",
                "significativo",
                "signo",
            ]
            if c in df_robustez.columns
        ]
        st.dataframe(df_robustez[cols], use_container_width=True, height=360)

        if {"coeficiente", "ic_95_lower", "ic_95_upper", "estimador"}.issubset(
            df_robustez.columns
        ):
            plot_df = df_robustez[df_robustez["estado"].eq("ok")].copy()
            plot_df = plot_df.head(40)
            plot_df["label"] = (
                plot_df["estimador"].astype(str)
                + " | "
                + plot_df["tipo_evento"].astype(str)
                + " | "
                + plot_df["t0"].astype(str)
                + " | "
                + plot_df["forma_funcional"].astype(str)
            )
            fig_rob = go.Figure(
                go.Scatter(
                    x=plot_df["coeficiente"],
                    y=plot_df["label"],
                    mode="markers",
                    error_x=dict(
                        type="data",
                        symmetric=False,
                        array=(plot_df["ic_95_upper"] - plot_df["coeficiente"]).clip(lower=0),
                        arrayminus=(plot_df["coeficiente"] - plot_df["ic_95_lower"]).clip(lower=0),
                    ),
                )
            )
            fig_rob.add_vline(x=0, line_dash="dash", line_color="gray")
            fig_rob.update_layout(
                title="Primeras 40 corridas de sensibilidad",
                xaxis_title="Coeficiente estimado",
                yaxis_title="Especificación",
                template="plotly_white",
                height=700,
            )
            st.plotly_chart(fig_rob, use_container_width=True)


# ────────────────────────────────────────────────────────────────────────────
# TAB 6: TRIANGULACIÓN
# ────────────────────────────────────────────────────────────────────────────

with tab6:
    st.markdown(
        f"{SVG_SCALE} **Triangulación PND/SNIES/ICFES — Hito 4**",
        unsafe_allow_html=True,
    )

    triang_json = _load_json(RESULTS_DIR / "triangulacion_resumen.json")
    df_pnd = _load_csv(RESULTS_DIR / "triangulacion_pnd_snies.csv")
    df_icfes = _load_csv(RESULTS_DIR / "triangulacion_icfes_snies.csv")
    df_embudo = _load_csv(RESULTS_DIR / "triangulacion_embudo_snies.csv")

    if triang_json is None:
        st.info("Sin resultados de triangulación. Ejecuta el análisis final Hito 4/5.")
    else:
        st.info(triang_json.get("interpretacion", ""))

        pnd_info = triang_json.get("pnd_snies", {})
        icfes_info = triang_json.get("icfes", {})
        embudo_info = triang_json.get("embudo_snies", {})

        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("PND disponible", "Sí" if pnd_info.get("fuente_pnd_disponible") else "No")
        with c2:
            st.metric("Filas PND ID 91", pnd_info.get("filas_pnd_id91", "N/A"))
        with c3:
            st.metric("Filas ICFES", icfes_info.get("filas", "N/A"))

        if pnd_info.get("nota"):
            st.caption(f"PND/SINERGIA: {pnd_info.get('nota')}")
        if icfes_info.get("nota"):
            st.caption(f"ICFES: {icfes_info.get('nota')}")

        if df_pnd is not None and not df_pnd.empty:
            st.subheader("PND/SINERGIA vs SNIES")
            st.dataframe(df_pnd, use_container_width=True)
            if "ano" in df_pnd.columns and "snies_primer_curso_oficial" in df_pnd.columns:
                fig_pnd = go.Figure()
                fig_pnd.add_trace(
                    go.Scatter(
                        x=df_pnd["ano"],
                        y=df_pnd["snies_primer_curso_oficial"],
                        mode="lines+markers",
                        name="SNIES primer curso oficial",
                    )
                )
                if "pnd_indicador_id91" in df_pnd.columns and df_pnd["pnd_indicador_id91"].notna().any():
                    fig_pnd.add_trace(
                        go.Scatter(
                            x=df_pnd["ano"],
                            y=df_pnd["pnd_indicador_id91"],
                            mode="lines+markers",
                            name="PND/SINERGIA ID 91",
                            yaxis="y2",
                        )
                    )
                    fig_pnd.update_layout(
                        yaxis2=dict(title="PND/SINERGIA", overlaying="y", side="right")
                    )
                fig_pnd.update_layout(
                    template="plotly_white",
                    height=380,
                    xaxis_title="Año",
                    yaxis_title="SNIES primer curso oficial",
                )
                st.plotly_chart(fig_pnd, use_container_width=True)

        if df_embudo is not None and not df_embudo.empty:
            st.subheader("Embudo SNIES")
            st.caption(
                f"Periodo: {embudo_info.get('periodo_min')} a {embudo_info.get('periodo_max')} | "
                f"Sectores: {', '.join(embudo_info.get('sectores', []))}"
            )
            st.dataframe(df_embudo, use_container_width=True, height=320)

        if df_icfes is not None and not df_icfes.empty:
            st.subheader("Proxy ICFES de contexto")
            st.dataframe(df_icfes, use_container_width=True)


# ────────────────────────────────────────────────────────────────────────────
# TAB 7: RESUMEN EJECUTIVO
# ────────────────────────────────────────────────────────────────────────────

with tab7:
    st.markdown(
        f"{SVG_CLIPBOARD} **Resumen ejecutivo — Hito 4/5**", unsafe_allow_html=True
    )

    resumen = _load_json(RESULTS_DIR / "resumen_ejecutivo_hito3.json")
    resumen_final = _load_json(RESULTS_DIR / "resumen_final_hito4_hito5.json")

    if resumen is None and resumen_final is None:
        st.info("Sin resumen ejecutivo. Ejecuta el análisis desde la barra lateral.")
    else:
        if resumen_final:
            st.markdown(f"**Fecha de ejecución final:** {resumen_final.get('fecha_ejecucion', 'N/A')}")
            st.markdown(f"**Estado:** {resumen_final.get('estado', 'N/A')}")
            st.markdown(f"**Duración:** {resumen_final.get('duracion_s', 'N/A')} s")
        if resumen:
            st.markdown(f"**Periodo de datos:** {resumen.get('periodo_datos', 'N/A')}")
            st.markdown(f"**Punto de quiebre:** {resumen.get('punto_quiebre', 'N/A')}")

        nota = (resumen_final or {}).get("nota_metodologica") or (resumen or {}).get("nota_metodologica", "")
        if nota:
            st.info(f"**Nota:** {nota}")

        st.subheader("Hallazgos principales")
        hallazgos = (resumen or {}).get("hallazgos_principales", [])
        for h in hallazgos:
            with st.expander(h.get("analisis", "Hallazgo")):
                est = h.get("estimado")
                ic = h.get("ic_95") or h.get("ic_95_bootstrap")
                sig = h.get("significativo")
                interp = h.get("interpretacion", "")

                if est is not None:
                    st.metric(
                        "Valor estimado",
                        f"{est:,.0f}" if isinstance(est, (int, float)) else str(est),
                    )
                if ic:
                    st.write(
                        f"**Intervalo de confianza 95%:** [{ic[0]:,.0f}, {ic[1]:,.0f}]"
                        if all(isinstance(v, (int, float)) for v in ic if v is not None)
                        else f"Intervalo: {ic}"
                    )
                if sig is not None:
                    st.write(f"**Resultado:** {_badge_plain(sig)}")
                if interp:
                    st.write(f"*{interp}*")

        st.divider()
        st.subheader("Interpretación")
        st.markdown("""
Los resultados son **descriptivos**, no prueban causalidad de forma definitiva.

| Tipo | Ejemplo |
|---|---|
| **Dato medido** | La matrícula en IES Oficiales creció X% entre 2022-S2 y 2024-S2 |
| **Hallazgo condicionado** | La comparación entre sectores sugiere un diferencial de Y estudiantes asociado a la política |
| **Limitación** | No se puede descartar que la recuperación post-pandemia u otros factores expliquen parte del cambio |

Es posible argumentar que el crecimiento post-2022 es una continuación de la recuperación post-COVID.
También es posible señalar que el diferencial positivo Oficial - Privada es consistente con una política
focalizada en el sector público.

**La evidencia no permite inclinar definitivamente la balanza entre estas lecturas.**
        """)

    # Footer
    st.divider()
    st.caption(
        "Seminario Ingeniería de Datos e IA — UAO | "
        "Hito 4/5: Robustez, triangulación y cierre final | "
        "Datos: SNIES 2018–2024 (MEN/Colombia)"
    )
