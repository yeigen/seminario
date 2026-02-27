"""
Dashboard de Seguimiento — Educacion Superior Colombia (SNIES)
Seminario de Ingenieria de Datos e IA — UAO

Prototipo de tablero interactivo que consulta el star schema
(PostgreSQL, schema 'facts') y presenta indicadores clave.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ---------------------------------------------------------------------------
# Configuracion de pagina (debe ser lo primero)
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="SNIES — Educacion Superior Colombia",
    page_icon=":mortar_board:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Conexion a PostgreSQL
# ---------------------------------------------------------------------------
@st.cache_resource
def get_engine():
    user = os.getenv("POSTGRES_USER", "yeigen")
    password = os.getenv("POSTGRES_PASSWORD", "LavidaEsbella16*#")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5433")
    db = os.getenv("POSTGRES_DB", "seminario")
    url = f"postgresql://{user}:{quote_plus(password)}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True, pool_size=3)


def run_query(query: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params or {})


def table_exists(schema: str, table: str) -> bool:
    try:
        df = run_query(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = :schema AND table_name = :table",
            {"schema": schema, "table": table},
        )
        return len(df) > 0
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------
Q_KPI_ESTUDIANTES = """
SELECT
    tipo_evento,
    SUM(cantidad) AS total
FROM facts.fact_estudiantes
GROUP BY tipo_evento
ORDER BY tipo_evento
"""

Q_TENDENCIA_ESTUDIANTES = """
SELECT
    dt.ano,
    dt.semestre,
    dt.ano_semestre,
    fe.tipo_evento,
    SUM(fe.cantidad) AS total
FROM facts.fact_estudiantes fe
JOIN facts.dim_tiempo dt ON fe.tiempo_id = dt.id
GROUP BY dt.ano, dt.semestre, dt.ano_semestre, fe.tipo_evento
ORDER BY dt.ano, dt.semestre, fe.tipo_evento
"""

Q_SECTOR = """
SELECT
    di.sector_ies AS sector,
    fe.tipo_evento,
    SUM(fe.cantidad) AS total
FROM facts.fact_estudiantes fe
JOIN facts.dim_institucion di ON fe.institucion_id = di.id
GROUP BY di.sector_ies, fe.tipo_evento
ORDER BY di.sector_ies, fe.tipo_evento
"""

Q_TOP_DEPARTAMENTOS = """
SELECT
    dg.nombre_departamento AS departamento,
    fe.tipo_evento,
    SUM(fe.cantidad) AS total
FROM facts.fact_estudiantes fe
JOIN facts.dim_geografia dg ON fe.geografia_ies_id = dg.id
GROUP BY dg.nombre_departamento, fe.tipo_evento
ORDER BY total DESC
"""

Q_AREAS_CONOCIMIENTO = """
SELECT
    dp.area_conocimiento,
    fe.tipo_evento,
    SUM(fe.cantidad) AS total
FROM facts.fact_estudiantes fe
JOIN facts.dim_programa dp ON fe.programa_id = dp.id
WHERE dp.area_conocimiento IS NOT NULL
GROUP BY dp.area_conocimiento, fe.tipo_evento
ORDER BY total DESC
"""

Q_KPI_DOCENTES = """
SELECT SUM(cantidad_docentes) AS total FROM facts.fact_docentes
"""

Q_DOCENTES_NIVEL = """
SELECT
    dnf.nivel_formacion_docente AS nivel,
    SUM(fd.cantidad_docentes) AS total
FROM facts.fact_docentes fd
JOIN facts.dim_nivel_formacion_docente dnf
    ON fd.nivel_formacion_docente_id = dnf.id
GROUP BY dnf.nivel_formacion_docente
ORDER BY total DESC
"""

Q_DOCENTES_TENDENCIA = """
SELECT
    dt.ano,
    dt.semestre,
    dt.ano_semestre,
    SUM(fd.cantidad_docentes) AS total
FROM facts.fact_docentes fd
JOIN facts.dim_tiempo dt ON fd.tiempo_id = dt.id
GROUP BY dt.ano, dt.semestre, dt.ano_semestre
ORDER BY dt.ano, dt.semestre
"""

Q_KPI_ADMIN = """
SELECT
    SUM(auxiliar) AS auxiliar,
    SUM(tecnico) AS tecnico,
    SUM(profesional) AS profesional,
    SUM(directivo) AS directivo,
    SUM(total) AS total
FROM facts.fact_administrativos
"""

Q_INSTITUCIONES = """
SELECT COUNT(*) AS total FROM facts.dim_institucion
"""

Q_PROGRAMAS = """
SELECT COUNT(*) AS total FROM facts.dim_programa
"""

Q_SEXO_ESTUDIANTES = """
SELECT
    ds.sexo,
    fe.tipo_evento,
    SUM(fe.cantidad) AS total
FROM facts.fact_estudiantes fe
JOIN facts.dim_sexo ds ON fe.sexo_id = ds.id
GROUP BY ds.sexo, fe.tipo_evento
ORDER BY ds.sexo
"""

Q_METODOLOGIA = """
SELECT
    dp.metodologia,
    fe.tipo_evento,
    SUM(fe.cantidad) AS total
FROM facts.fact_estudiantes fe
JOIN facts.dim_programa dp ON fe.programa_id = dp.id
WHERE dp.metodologia IS NOT NULL
GROUP BY dp.metodologia, fe.tipo_evento
ORDER BY total DESC
"""

# ---------------------------------------------------------------------------
# Colores
# ---------------------------------------------------------------------------
COLORS = {
    "inscritos": "#636EFA",
    "admitidos": "#EF553B",
    "matriculados": "#00CC96",
    "primer_curso": "#AB63FA",
    "graduados": "#FFA15A",
}

TIPO_LABELS = {
    "inscritos": "Inscritos",
    "admitidos": "Admitidos",
    "matriculados": "Matriculados",
    "primer_curso": "Primer Curso",
    "graduados": "Graduados",
}


def fmt_number(n: int | float) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))


# ---------------------------------------------------------------------------
# Verificacion de conexion
# ---------------------------------------------------------------------------
def check_connection() -> bool:
    try:
        run_query("SELECT 1")
        return True
    except Exception as e:
        st.error(f"No se pudo conectar a PostgreSQL: {e}")
        return False


def check_schema() -> bool:
    required = ["dim_tiempo", "dim_institucion", "fact_estudiantes"]
    for t in required:
        if not table_exists("facts", t):
            return False
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # Sidebar
    st.sidebar.title("SNIES Dashboard")
    st.sidebar.markdown(
        "**Seminario de Ingenieria de Datos e IA**\n\n"
        "Universidad Autonoma de Occidente"
    )
    st.sidebar.markdown("---")

    if not check_connection():
        st.stop()

    if not check_schema():
        st.warning(
            "Las tablas del star schema no existen aun. "
            "Ejecuta el pipeline ETL primero:\n\n"
            "```\ndocker compose up pipeline\n```"
        )
        st.stop()

    # Titulo principal
    st.title("Educacion Superior en Colombia — Panel de Seguimiento")
    st.caption("Fuente: SNIES (Sistema Nacional de Informacion de la Educacion Superior) | 2018-2024")

    # ── KPIs principales ─────────────────────────────────────
    st.markdown("### Indicadores Generales")

    df_kpi_est = run_query(Q_KPI_ESTUDIANTES)
    df_inst = run_query(Q_INSTITUCIONES)
    df_prog = run_query(Q_PROGRAMAS)
    df_kpi_doc = run_query(Q_KPI_DOCENTES)

    kpi_cols = st.columns(6)

    total_matriculados = df_kpi_est.loc[
        df_kpi_est["tipo_evento"] == "matriculados", "total"
    ]
    total_matriculados = int(total_matriculados.iloc[0]) if len(total_matriculados) > 0 else 0

    total_graduados = df_kpi_est.loc[
        df_kpi_est["tipo_evento"] == "graduados", "total"
    ]
    total_graduados = int(total_graduados.iloc[0]) if len(total_graduados) > 0 else 0

    total_inscritos = df_kpi_est.loc[
        df_kpi_est["tipo_evento"] == "inscritos", "total"
    ]
    total_inscritos = int(total_inscritos.iloc[0]) if len(total_inscritos) > 0 else 0

    total_docentes = int(df_kpi_doc["total"].iloc[0]) if len(df_kpi_doc) > 0 else 0
    total_instituciones = int(df_inst["total"].iloc[0]) if len(df_inst) > 0 else 0
    total_programas = int(df_prog["total"].iloc[0]) if len(df_prog) > 0 else 0

    kpi_cols[0].metric("Matriculados", fmt_number(total_matriculados))
    kpi_cols[1].metric("Graduados", fmt_number(total_graduados))
    kpi_cols[2].metric("Inscritos", fmt_number(total_inscritos))
    kpi_cols[3].metric("Docentes", fmt_number(total_docentes))
    kpi_cols[4].metric("Instituciones", fmt_number(total_instituciones))
    kpi_cols[5].metric("Programas", fmt_number(total_programas))

    st.markdown("---")

    # ── Filtro de tipo de evento ─────────────────────────────
    st.sidebar.markdown("### Filtros")
    tipos_disponibles = sorted(df_kpi_est["tipo_evento"].unique())
    tipos_seleccionados = st.sidebar.multiselect(
        "Tipo de evento",
        options=tipos_disponibles,
        default=tipos_disponibles,
        format_func=lambda x: TIPO_LABELS.get(x, x),
    )

    if not tipos_seleccionados:
        tipos_seleccionados = tipos_disponibles

    # ── Tendencia temporal ───────────────────────────────────
    st.markdown("### Tendencia Temporal de Estudiantes")

    df_tendencia = run_query(Q_TENDENCIA_ESTUDIANTES)
    df_tendencia = df_tendencia[df_tendencia["tipo_evento"].isin(tipos_seleccionados)]

    fig_tendencia = px.line(
        df_tendencia,
        x="ano_semestre",
        y="total",
        color="tipo_evento",
        color_discrete_map=COLORS,
        labels={
            "ano_semestre": "Periodo",
            "total": "Cantidad",
            "tipo_evento": "Tipo",
        },
        markers=True,
    )
    fig_tendencia.update_layout(
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig_tendencia, use_container_width=True)

    # ── Sector y Sexo ────────────────────────────────────────
    col_sector, col_sexo = st.columns(2)

    with col_sector:
        st.markdown("### Distribucion por Sector (Oficial vs Privada)")
        df_sector = run_query(Q_SECTOR)
        df_sector = df_sector[df_sector["tipo_evento"].isin(tipos_seleccionados)]
        df_sector_agg = df_sector.groupby("sector", as_index=False)["total"].sum()
        if not df_sector_agg.empty:
            fig_sector = px.pie(
                df_sector_agg,
                names="sector",
                values="total",
                color_discrete_sequence=["#636EFA", "#EF553B"],
                hole=0.4,
            )
            fig_sector.update_traces(textinfo="percent+label")
            st.plotly_chart(fig_sector, use_container_width=True)

    with col_sexo:
        st.markdown("### Distribucion por Sexo")
        df_sexo = run_query(Q_SEXO_ESTUDIANTES)
        df_sexo = df_sexo[df_sexo["tipo_evento"].isin(tipos_seleccionados)]
        df_sexo_agg = df_sexo.groupby("sexo", as_index=False)["total"].sum()
        if not df_sexo_agg.empty:
            fig_sexo = px.pie(
                df_sexo_agg,
                names="sexo",
                values="total",
                color_discrete_sequence=["#00CC96", "#AB63FA", "#FFA15A"],
                hole=0.4,
            )
            fig_sexo.update_traces(textinfo="percent+label")
            st.plotly_chart(fig_sexo, use_container_width=True)

    # ── Top departamentos ────────────────────────────────────
    st.markdown("### Top 15 Departamentos por Matriculados")

    df_deptos = run_query(Q_TOP_DEPARTAMENTOS)
    df_deptos = df_deptos[df_deptos["tipo_evento"] == "matriculados"]
    df_deptos_top = df_deptos.head(15)
    if not df_deptos_top.empty:
        fig_deptos = px.bar(
            df_deptos_top,
            x="total",
            y="departamento",
            orientation="h",
            color_discrete_sequence=["#636EFA"],
            labels={"total": "Matriculados", "departamento": "Departamento"},
        )
        fig_deptos.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_deptos, use_container_width=True)

    # ── Areas de conocimiento y Metodologia ──────────────────
    col_areas, col_metodo = st.columns(2)

    with col_areas:
        st.markdown("### Matriculados por Area de Conocimiento")
        df_areas = run_query(Q_AREAS_CONOCIMIENTO)
        df_areas = df_areas[df_areas["tipo_evento"] == "matriculados"]
        if not df_areas.empty:
            fig_areas = px.bar(
                df_areas,
                x="total",
                y="area_conocimiento",
                orientation="h",
                color_discrete_sequence=["#00CC96"],
                labels={
                    "total": "Matriculados",
                    "area_conocimiento": "Area",
                },
            )
            fig_areas.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_areas, use_container_width=True)

    with col_metodo:
        st.markdown("### Matriculados por Metodologia")
        df_metodo = run_query(Q_METODOLOGIA)
        df_metodo = df_metodo[df_metodo["tipo_evento"] == "matriculados"]
        if not df_metodo.empty:
            fig_metodo = px.pie(
                df_metodo,
                names="metodologia",
                values="total",
                color_discrete_sequence=px.colors.qualitative.Set2,
                hole=0.4,
            )
            fig_metodo.update_traces(textinfo="percent+label")
            st.plotly_chart(fig_metodo, use_container_width=True)

    st.markdown("---")

    # ── Docentes ─────────────────────────────────────────────
    st.markdown("### Docentes")

    col_doc_nivel, col_doc_tend = st.columns(2)

    with col_doc_nivel:
        st.markdown("#### Nivel de Formacion")
        df_doc_nivel = run_query(Q_DOCENTES_NIVEL)
        if not df_doc_nivel.empty:
            fig_doc_nivel = px.bar(
                df_doc_nivel,
                x="total",
                y="nivel",
                orientation="h",
                color_discrete_sequence=["#AB63FA"],
                labels={"total": "Docentes", "nivel": "Nivel de Formacion"},
            )
            fig_doc_nivel.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_doc_nivel, use_container_width=True)

    with col_doc_tend:
        st.markdown("#### Tendencia Temporal")
        df_doc_tend = run_query(Q_DOCENTES_TENDENCIA)
        if not df_doc_tend.empty:
            fig_doc_tend = px.line(
                df_doc_tend,
                x="ano_semestre",
                y="total",
                markers=True,
                color_discrete_sequence=["#AB63FA"],
                labels={"ano_semestre": "Periodo", "total": "Docentes"},
            )
            st.plotly_chart(fig_doc_tend, use_container_width=True)

    # ── Administrativos ──────────────────────────────────────
    st.markdown("### Personal Administrativo")
    df_admin = run_query(Q_KPI_ADMIN)
    if not df_admin.empty and df_admin["total"].iloc[0]:
        admin_cols = st.columns(5)
        admin_cols[0].metric("Auxiliar", fmt_number(int(df_admin["auxiliar"].iloc[0])))
        admin_cols[1].metric("Tecnico", fmt_number(int(df_admin["tecnico"].iloc[0])))
        admin_cols[2].metric("Profesional", fmt_number(int(df_admin["profesional"].iloc[0])))
        admin_cols[3].metric("Directivo", fmt_number(int(df_admin["directivo"].iloc[0])))
        admin_cols[4].metric("Total", fmt_number(int(df_admin["total"].iloc[0])))

        # Desglose como barras
        admin_data = pd.DataFrame({
            "Cargo": ["Auxiliar", "Tecnico", "Profesional", "Directivo"],
            "Cantidad": [
                int(df_admin["auxiliar"].iloc[0]),
                int(df_admin["tecnico"].iloc[0]),
                int(df_admin["profesional"].iloc[0]),
                int(df_admin["directivo"].iloc[0]),
            ],
        })
        fig_admin = px.bar(
            admin_data,
            x="Cargo",
            y="Cantidad",
            color="Cargo",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        st.plotly_chart(fig_admin, use_container_width=True)

    # ── Footer ───────────────────────────────────────────────
    st.markdown("---")
    st.caption(
        "Seminario de Ingenieria de Datos e IA — UAO | "
        "Datos: SNIES, MEN Colombia | "
        "Pipeline ETL con PostgreSQL + Airflow"
    )


if __name__ == "__main__":
    main()
