"""
dashboard/app.py — Dashboard Streamlit para Hito 3.

Muestra:
  - Serie temporal de matrícula por sector (Oficial vs Privada)
  - Resultados ITS: coeficientes, contrafactual, IC bootstrap
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
        return "🟢 Significativo (p<0.05)"
    if sig is False:
        return "🔴 No significativo (p≥0.05)"
    return "❓ No disponible"


# ── sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🎓 Educación Superior\nColombia 2018–2024")
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
    st.markdown("**Fuente:** SNIES 2018–2024  \nvía star schema PostgreSQL")
    st.divider()
    if st.button("🔄 Re-ejecutar análisis", type="primary", use_container_width=True):
        with st.spinner("Ejecutando pipeline de análisis… (puede tomar ~2 min)"):
            try:
                from analysis.runner import run

                run()
                st.success("Análisis completado. Recarga la página.")
            except Exception as e:
                st.error(f"Error: {e}")


def _fmt(v: object) -> str:
    """Formatea un número como entero con separador de miles, o 'N/A' si es None."""
    if v is None or (isinstance(v, float) and v != v):  # None o NaN
        return "N/A"
    try:
        return f"{v:,.0f}"
    except (TypeError, ValueError):
        return str(v)


# ── tabs ─────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    [
        "📈 Tendencias",
        "📉 ITS",
        "⚖️ DiD",
        "🔁 Bootstrap",
        "📋 Resumen ejecutivo",
    ]
)

# ────────────────────────────────────────────────────────────────────────────
# TAB 1: TENDENCIAS
# ────────────────────────────────────────────────────────────────────────────

with tab1:
    st.header("Tendencias de matrícula por sector (2018–2024)")

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
            st.subheader("Variación % anual")
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
# TAB 2: ITS
# ────────────────────────────────────────────────────────────────────────────

with tab2:
    st.header("Series de Tiempo Interrumpidas (ITS)")
    st.markdown(
        "Modelo: $Y_t = \\alpha_0 + \\alpha_1 t + \\alpha_2 D_t + \\alpha_3 (t-T_0)D_t + \\varepsilon_t$  \n"
        "Errores HAC (Newey-West, maxlags=2)."
    )

    its_json = _load_json(RESULTS_DIR / f"its_{sector_sel.lower()}_{tipo_evento}.json")
    df_its = _load_csv(
        RESULTS_DIR / f"its_datos_{sector_sel.lower()}_{tipo_evento}.csv"
    )

    if its_json is None:
        st.info("Sin resultados ITS aún. Ejecuta el análisis.")
    else:
        coefs = its_json.get("coeficientes", {})
        bondad = its_json.get("bondad_ajuste", {})
        chow = its_json.get("prueba_chow", {})

        # Métricas principales
        c1, c2, c3, c4 = st.columns(4)
        a2 = coefs.get("alpha_2_cambio_nivel", {})
        a3 = coefs.get("alpha_3_cambio_tendencia", {})
        with c1:
            st.metric(
                "α₂ — Cambio de nivel",
                f"{a2.get('estimado', 'N/A'):,.0f}",
                delta=_badge(
                    a2.get("p_value", 1) < 0.05
                    if a2.get("p_value") is not None
                    else None
                ),
            )
        with c2:
            st.metric(
                "IC 95% α₂",
                f"[{a2.get('ic_95_lower', 'N/A'):,.0f}, {a2.get('ic_95_upper', 'N/A'):,.0f}]",
            )
        with c3:
            st.metric(
                "α₃ — Cambio tendencia",
                f"{a3.get('estimado', 'N/A'):,.0f}",
                delta=_badge(
                    a3.get("p_value", 1) < 0.05
                    if a3.get("p_value") is not None
                    else None
                ),
            )
        with c4:
            st.metric("R²", f"{bondad.get('R2', 'N/A')}")

        st.caption(
            f"Chow F={chow.get('chow_F')} (p={chow.get('chow_p')}) — {chow.get('conclusion', '')}"
        )

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
                    name="Contrafactual (sin política)",
                    line=dict(color="gray", dash="dash", width=2),
                )
            )
            periodos_list = sorted(df_its["periodo"].unique().tolist())
            if "2022-S2" in periodos_list:
                fig_its.add_vline(
                    x=periodos_list.index("2022-S2"),
                    line_dash="dash",
                    line_color="red",
                    annotation_text="T₀ = 2022-S2",
                )
            fig_its.update_layout(
                title=f"ITS — {sector_sel} | {tipo_evento.replace('_', ' ').title()}",
                xaxis_title="Semestre",
                yaxis_title="Estudiantes",
                template="plotly_white",
                height=430,
                hovermode="x unified",
            )
            st.plotly_chart(fig_its, use_container_width=True)

            # Efecto estimado
            if "efecto_estimado" in df_its.columns:
                st.subheader("Efecto estimado (Observado − Contrafactual)")
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
                    yaxis_title="Estudiantes (Obs − Contrafactual)",
                )
                st.plotly_chart(fig_ef, use_container_width=True)

        # Placebos
        placebos = its_json.get("placebos", {})
        if placebos:
            st.subheader("Pruebas placebo (T₀ alternativo)")
            df_pl = pd.DataFrame([{"T₀ placebo": k, **v} for k, v in placebos.items()])
            st.dataframe(df_pl, use_container_width=True)


# ────────────────────────────────────────────────────────────────────────────
# TAB 3: DiD
# ────────────────────────────────────────────────────────────────────────────

with tab3:
    st.header("Diferencias en Diferencias (DiD)")
    st.markdown(
        "Modelo: $Y_{st} = \\alpha + \\beta_1 \\text{POST}_t + \\beta_2 \\text{OFICIAL}_s "
        "+ \\beta_3 (\\text{POST}_t \\times \\text{OFICIAL}_s) + \\varepsilon_{st}$  \n"
        "El estimador de interés es **β₃** (diferencial de cambio, Oficial − Privada, post-2022)."
    )

    did_json = _load_json(RESULTS_DIR / f"did_agregado_{tipo_evento}.json")
    did_panel_json = _load_json(RESULTS_DIR / f"did_panel_{tipo_evento}.json")
    df_es = _load_csv(RESULTS_DIR / f"event_study_{tipo_evento}.csv")

    if did_json is None:
        st.info("Sin resultados DiD. Ejecuta el análisis.")
    else:
        est = did_json.get("estimador_did", {})
        medias = did_json.get("medias", {})

        # Estimador principal
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("β₃ (DiD estimador)", _fmt(est.get("beta_3")))
        with c2:
            st.metric(
                "IC 95% β₃",
                f"[{_fmt(est.get('ic_95_lower'))}, {_fmt(est.get('ic_95_upper'))}]",
            )
        with c3:
            st.metric(
                "p-value",
                f"{est.get('p_value', 'N/A')}",
                delta=_badge(est.get("significativo")),
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
                "Δ (Post − Pre)": [
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
            title=f"DiD — Medias por periodo y sector | {tipo_evento.replace('_', ' ').title()}",
            xaxis_title="Periodo",
            yaxis_title="Matrícula media por semestre",
            template="plotly_white",
            height=380,
        )
        st.plotly_chart(fig_did, use_container_width=True)

        # DiD Panel
        if did_panel_json:
            est_p = did_panel_json.get("estimador_did", {})
            st.subheader("DiD Panel (efectos fijos de IES y tiempo)")
            st.info(
                f"β = {est_p.get('beta', 'N/A')} "
                f"(≈ **{est_p.get('efecto_pct_aprox', 'N/A')}%**), "
                f"p = {est_p.get('p_value', 'N/A')}  \n"
                f"N IES = {did_panel_json.get('n_ies', '?')}, "
                f"N obs = {did_panel_json.get('n_obs', '?')}  \n"
                f"{est_p.get('interpretacion', '')}"
            )

    # Event Study
    if df_es is not None and not df_es.empty:
        st.subheader("Event Study — Pre-tendencias")
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
                name="Coef. DiD por periodo",
            )
        )
        fig_es.add_hline(y=0, line_dash="dash", line_color="black")
        periodos_es = df_es["periodo"].tolist()
        if "2022-S2" in periodos_es:
            fig_es.add_vline(
                x=periodos_es.index("2022-S2"),
                line_dash="dash",
                line_color="red",
                annotation_text="T₀",
            )
        fig_es.update_layout(
            title="Event Study — Coeficientes diferenciales por semestre",
            xaxis_title="Semestre",
            yaxis_title="Coeficiente (Oficial − Privada) relativo al periodo base",
            template="plotly_white",
            height=420,
        )
        st.plotly_chart(fig_es, use_container_width=True)
        st.caption(
            "Los coeficientes pre-2022 (azul claro) deben ser ~0 si se cumple el supuesto de tendencias paralelas."
        )


# ────────────────────────────────────────────────────────────────────────────
# TAB 4: BOOTSTRAP
# ────────────────────────────────────────────────────────────────────────────

with tab4:
    st.header("Bootstrap e incertidumbre (N=1,000 re-muestras)")
    st.markdown(
        "Método: **block bootstrap** (bloque = 2 semestres) para preservar "
        "la autocorrelación temporal. IC 95% por método de percentiles."
    )

    boot_json = _load_json(RESULTS_DIR / f"bootstrap_{tipo_evento}.json")

    if boot_json is None:
        st.info("Sin resultados bootstrap. Ejecuta el análisis.")
    else:
        its_b = boot_json.get("its_bootstrap", {})
        did_b = boot_json.get("did_bootstrap", {})
        a2_b = its_b.get("alpha_2_cambio_nivel", {})
        b3_b = did_b.get("beta_3_did", {})

        st.subheader("Intervalos de confianza bootstrap 95%")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**α₂ — Cambio de nivel ITS (Oficial)**")
            st.metric("Media bootstrap", f"{a2_b.get('media_boot', 'N/A'):,.0f}")
            st.metric("IC 95% inferior", f"{a2_b.get('ic_95_lower', 'N/A'):,.0f}")
            st.metric("IC 95% superior", f"{a2_b.get('ic_95_upper', 'N/A'):,.0f}")
        with col2:
            st.markdown("**β₃ — Estimador DiD**")
            st.metric("Media bootstrap", f"{b3_b.get('media_boot', 'N/A'):,.0f}")
            st.metric("IC 95% inferior", f"{b3_b.get('ic_95_lower', 'N/A'):,.0f}")
            st.metric("IC 95% superior", f"{b3_b.get('ic_95_upper', 'N/A'):,.0f}")

        # Escenarios
        esc_of = boot_json.get("escenarios_oficial", {})
        if esc_of and "escenarios" in esc_of:
            st.subheader("Análisis de escenarios — Sector Oficial")
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
                    name="Escenario base (contrafactual)",
                    mode="lines",
                    line=dict(color="gray", dash="dash"),
                )
            )
            fig_esc.add_trace(
                go.Scatter(
                    x=df_esc["periodo"],
                    y=df_esc["escenario_optimista"],
                    name="Optimista (+1σ)",
                    mode="lines",
                    line=dict(color="green", dash="dot"),
                )
            )
            fig_esc.add_trace(
                go.Scatter(
                    x=df_esc["periodo"],
                    y=df_esc["escenario_adverso"],
                    name="Adverso (−1σ)",
                    mode="lines",
                    line=dict(color="red", dash="dot"),
                )
            )
            fig_esc.update_layout(
                title="Escenarios: observado vs. contrafactual (base / optimista / adverso)",
                xaxis_title="Semestre",
                yaxis_title="Matriculados",
                template="plotly_white",
                height=420,
                hovermode="x unified",
            )
            st.plotly_chart(fig_esc, use_container_width=True)
            st.caption(
                f"σ residuos pre-2022 = {esc_of.get('sigma_residuos_pre', '?'):,}"
            )


# ────────────────────────────────────────────────────────────────────────────
# TAB 5: RESUMEN EJECUTIVO
# ────────────────────────────────────────────────────────────────────────────

with tab5:
    st.header("Resumen ejecutivo — Hito 3")

    resumen = _load_json(RESULTS_DIR / "resumen_ejecutivo_hito3.json")

    if resumen is None:
        st.info("Sin resumen ejecutivo. Ejecuta el análisis desde la barra lateral.")
    else:
        st.markdown(f"**Fecha de ejecución:** {resumen.get('fecha_ejecucion', 'N/A')}")
        st.markdown(f"**Periodo de datos:** {resumen.get('periodo_datos', 'N/A')}")
        st.markdown(f"**Punto de quiebre:** {resumen.get('punto_quiebre', 'N/A')}")

        st.info(f"📌 **Nota metodológica:** {resumen.get('nota_metodologica', '')}")

        st.subheader("Hallazgos principales")
        hallazgos = resumen.get("hallazgos_principales", [])
        for h in hallazgos:
            with st.expander(h.get("analisis", "Hallazgo")):
                est = h.get("estimado")
                ic = h.get("ic_95") or h.get("ic_95_bootstrap")
                p = h.get("p_value")
                sig = h.get("significativo")
                interp = h.get("interpretacion", "")

                if est is not None:
                    st.metric(
                        "Estimador",
                        f"{est:,.0f}" if isinstance(est, (int, float)) else str(est),
                    )
                if ic:
                    st.write(
                        f"**IC 95%:** [{ic[0]:,.0f}, {ic[1]:,.0f}]"
                        if all(isinstance(v, (int, float)) for v in ic if v is not None)
                        else f"IC 95%: {ic}"
                    )
                if p is not None:
                    st.write(f"**p-value:** {p}  →  {_badge(sig)}")
                if interp:
                    st.write(f"*{interp}*")

        st.divider()
        st.subheader("Interpretación técnicamente neutral")
        st.markdown("""
Los resultados de este hito son **descriptivos y asociativos**, no causales en sentido estricto.

| Tipo de afirmación | Ejemplo en este análisis |
|---|---|
| **Hecho medido** | La matrícula en IES Oficiales creció X% entre 2022-S2 y 2024-S2 |
| **Inferencia condicionada** | Bajo el supuesto de tendencias paralelas, el DiD sugiere un diferencial de Y estudiantes atribuible a la política |
| **Límite explícito** | No es posible descartar que la recuperación post-pandemia u otros factores expliquen parte del cambio |

**Un crítico** podría argumentar que el crecimiento post-2022 es continuación de la tendencia de recuperación post-COVID,
no de la política de gratuidad.

**Un defensor** podría señalar que el diferencial positivo Oficial − Privada es consistente con una política focalizada
en el sector público que no debería afectar al privado de la misma forma.

La evidencia no permite inclinar definitivamente la balanza entre estas lecturas con los datos disponibles.
        """)

    # Footer
    st.divider()
    st.caption(
        "Seminario Ingeniería de Datos e IA — UAO | "
        "Hito 3: Metodología aprobada + Primeros resultados | "
        "Datos: SNIES 2018–2024 (MEN/Colombia)"
    )
