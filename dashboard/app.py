"""
dashboard/app.py — Dashboard Streamlit para educación superior en Colombia.

Muestra:
  - Serie temporal de matrícula por sector (Oficial vs Privada)
  - Resultados ITS: observado vs contrafactual
  - Resultados DiD: estimador, medias pre/post, event study
  - Análisis de escenarios

Uso:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import base64
import html
import json
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st
import streamlit.components.v1 as components
from plotly.subplots import make_subplots

# ── rutas ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.globals import DATA_DIR
from utils.db import get_engine, table_exists

RESULTS_DIR = DATA_DIR / "results"
PLOTS_DIR = RESULTS_DIR / "plots"
TEAM_IMAGES_DIR = ROOT / "imagenes"
MAP_IMAGE_PATH = TEAM_IMAGES_DIR / "mapa-colombia.jpg"
PLOTLY_TEMPLATE = "seminario_dracula"
COLOR_PURPLE = "#bd93f9"
COLOR_CYAN = "#8be9fd"
COLOR_GREEN = "#50fa7b"
COLOR_PINK = "#ff79c6"
COLOR_YELLOW = "#f1fa8c"
COLOR_ORANGE = "#ffb86c"
COLOR_MUTED = "#c9bdd9"
COLOR_GRID_ZERO = "rgba(248,248,242,0.35)"
DRACULA_COLORSCALE = [
    [0.0, "#241832"],
    [0.35, "#5b3f82"],
    [0.7, COLOR_PURPLE],
    [1.0, COLOR_CYAN],
]

pio.templates[PLOTLY_TEMPLATE] = go.layout.Template(
    layout=go.Layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(23,18,31,0.72)",
        font=dict(color="#f8f8f2", family="Roboto, Arial, sans-serif"),
        colorway=[COLOR_PURPLE, COLOR_CYAN, COLOR_GREEN, COLOR_PINK, COLOR_YELLOW, COLOR_ORANGE],
        xaxis=dict(gridcolor="rgba(248,248,242,0.10)", zerolinecolor="rgba(248,248,242,0.20)"),
        yaxis=dict(gridcolor="rgba(248,248,242,0.10)", zerolinecolor="rgba(248,248,242,0.20)"),
    )
)


def _inject_theme_css() -> None:
    map_background = ""
    if MAP_IMAGE_PATH.exists():
        encoded_map = base64.b64encode(MAP_IMAGE_PATH.read_bytes()).decode("ascii")
        map_background = f"url('data:image/jpeg;base64,{encoded_map}')"

    css = """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700;800&display=swap');

        :root {
            --seminario-bg: #17121f;
            --seminario-bg-2: #21172d;
            --seminario-panel: rgba(33, 23, 45, 0.78);
            --seminario-panel-strong: rgba(46, 31, 66, 0.88);
            --seminario-border: rgba(189, 147, 249, 0.24);
            --seminario-border-strong: rgba(189, 147, 249, 0.46);
            --seminario-text: #f8f8f2;
            --seminario-muted: #c9bdd9;
            --seminario-purple: #bd93f9;
            --seminario-cyan: #8be9fd;
            --seminario-pink: #ff79c6;
            --seminario-green: #50fa7b;
            --mouse-x: 50%;
            --mouse-y: 50%;
            --parallax-x: 0px;
            --parallax-y: 0px;
        }

        html, body, [class*="css"] {
            font-family: "Roboto", Arial, sans-serif !important;
        }

        .stApp {
            background:
                radial-gradient(circle at 18% 12%, rgba(189, 147, 249, 0.22), transparent 34%),
                radial-gradient(circle at 86% 8%, rgba(139, 233, 253, 0.12), transparent 28%),
                linear-gradient(135deg, #17121f 0%, #1c1328 48%, #120f1a 100%);
            color: var(--seminario-text);
            isolation: isolate;
        }

        .stApp::before {
            content: "";
            position: fixed;
            inset: -6vh -6vw;
            z-index: 0;
            pointer-events: none;
            background-image:
                linear-gradient(135deg, rgba(23, 18, 31, 0.82), rgba(23, 18, 31, 0.92)),
                __MAP_BACKGROUND__;
            background-size: cover;
            /* el mouse desplaza la posición del fondo (parallax via JS) */
            background-position: calc(50% + var(--parallax-x)) calc(50% + var(--parallax-y));
            background-repeat: no-repeat;
            opacity: 0.34;
            filter: saturate(0.92) contrast(1.05) blur(0.4px);
            transform-origin: center;
            will-change: transform, background-position;
            transition: background-position 140ms ease-out;
            /* zoom/paneo autónomo (Ken Burns) sobre transform: no choca con el parallax */
            animation: seminarioKenBurns 40s ease-in-out infinite alternate;
        }

        .stApp::after {
            content: "";
            position: fixed;
            inset: 0;
            z-index: 0;
            pointer-events: none;
            /* el glow sigue el cursor (via JS) */
            background:
                radial-gradient(circle at var(--mouse-x) var(--mouse-y), rgba(189, 147, 249, 0.15), transparent 18rem),
                radial-gradient(circle at calc(100% - var(--mouse-x)) calc(100% - var(--mouse-y)), rgba(139, 233, 253, 0.09), transparent 22rem);
            mix-blend-mode: screen;
            opacity: 0.85;
            transition: background 120ms ease-out;
        }

        @keyframes seminarioKenBurns {
            0%   { transform: scale(1.06) translate3d(-1.2%, -0.8%, 0); }
            50%  { transform: scale(1.12) translate3d(1.0%, 0.6%, 0); }
            100% { transform: scale(1.08) translate3d(0.8%, -1.0%, 0); }
        }

        @media (prefers-reduced-motion: reduce) {
            .stApp::before,
            .stApp::after,
            [data-testid="stPlotlyChart"],
            [data-testid="stDataFrame"],
            .seminario-metric-card,
            .seminario-side-group,
            [data-testid="stMetric"] {
                animation: none !important;
            }
        }

        [data-testid="stAppViewContainer"],
        [data-testid="stSidebar"] {
            position: relative;
            z-index: 1;
        }

        [data-testid="stSidebar"] {
            background: rgba(23, 18, 31, 0.58) !important;
            backdrop-filter: blur(18px) saturate(145%);
            -webkit-backdrop-filter: blur(18px) saturate(145%);
            border-right: 1px solid var(--seminario-border);
            box-shadow: 18px 0 50px rgba(0, 0, 0, 0.22);
        }

        [data-testid="stSidebar"] > div {
            background: transparent !important;
        }

        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"],
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] p {
            color: var(--seminario-text) !important;
        }

        [data-testid="stSidebar"] [data-testid="stAlert"] {
            background: rgba(189, 147, 249, 0.10);
            border: 1px solid var(--seminario-border);
            border-radius: 16px;
        }

        [data-testid="stSidebar"] [data-testid="stImage"] img {
            aspect-ratio: 1 / 1;
            object-fit: cover;
            border-radius: 999px;
            border: 1px solid var(--seminario-border-strong);
            box-shadow: 0 10px 24px rgba(0, 0, 0, 0.26);
        }

        .main .block-container {
            padding-top: 2rem;
            max-width: 1380px;
        }

        h1, h2, h3, h4, h5, h6 {
            font-family: "Roboto", Arial, sans-serif !important;
            letter-spacing: -0.02em;
            color: var(--seminario-text) !important;
        }

        p, li, label, span, div[data-testid="stMarkdownContainer"] {
            color: var(--seminario-text);
        }

        div[data-testid="stTabs"] [role="tablist"] {
            gap: 0.45rem;
            background: rgba(10, 8, 16, 0.34);
            border: 1px solid var(--seminario-border);
            border-radius: 18px;
            padding: 0.45rem;
            box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05);
        }

        div[data-testid="stTabs"] button[role="tab"] {
            background: rgba(255, 255, 255, 0.045);
            border: 1px solid rgba(189, 147, 249, 0.14);
            border-radius: 14px;
            color: var(--seminario-muted) !important;
            min-height: 42px;
            padding: 0.35rem 0.8rem;
            transition: all 160ms ease;
            cursor: pointer;
        }

        div[data-testid="stTabs"] button[role="tab"] p {
            color: inherit !important;
            font-weight: 600;
        }

        div[data-testid="stTabs"] button[role="tab"]:hover {
            background: rgba(189, 147, 249, 0.14);
            border-color: var(--seminario-border-strong);
            color: var(--seminario-text) !important;
        }

        div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
            background: linear-gradient(135deg, rgba(189, 147, 249, 0.34), rgba(255, 121, 198, 0.18));
            border-color: var(--seminario-border-strong);
            color: #ffffff !important;
            box-shadow: 0 12px 32px rgba(189, 147, 249, 0.18);
        }

        [data-testid="stMetric"] {
            background: linear-gradient(145deg, rgba(36, 24, 52, 0.90), rgba(27, 20, 38, 0.82));
            border: 1px solid var(--seminario-border);
            border-radius: 20px;
            padding: 1rem 1.1rem;
            box-shadow: 0 16px 38px rgba(0, 0, 0, 0.22), inset 0 1px 0 rgba(255, 255, 255, 0.05);
        }

        [data-testid="stMetric"] label,
        [data-testid="stMetric"] [data-testid="stMetricLabel"] p {
            color: var(--seminario-muted) !important;
            font-weight: 600;
        }

        [data-testid="stMetricValue"] {
            color: var(--seminario-purple) !important;
            font-weight: 800;
            letter-spacing: -0.035em;
            text-shadow: 0 0 24px rgba(189, 147, 249, 0.24);
            line-height: 1.05;
            overflow-wrap: anywhere;
        }

        [data-testid="stMetricDelta"] {
            color: var(--seminario-cyan) !important;
        }

        [data-testid="stSelectbox"],
        [data-testid="stSlider"],
        [data-testid="stDataFrame"],
        [data-testid="stPlotlyChart"],
        [data-testid="stAlert"],
        .stExpander {
            border-radius: 18px;
        }

        [data-testid="stPlotlyChart"],
        [data-testid="stDataFrame"] {
            background: var(--seminario-panel);
            border: 1px solid var(--seminario-border);
            border-radius: 22px;
            padding: 0.5rem 0.55rem 0.35rem;
            box-shadow: 0 18px 44px rgba(0, 0, 0, 0.20);
            transition: border-color 180ms ease, box-shadow 180ms ease;
            width: 100% !important;
            max-width: 100% !important;
            min-width: 0;
            box-sizing: border-box;
            overflow: hidden;
            /* solo opacidad: no altera la geometría que mide Plotly */
            animation: seminarioChartIn 560ms ease-out both;
        }

        @keyframes seminarioChartIn {
            from { opacity: 0; }
            to   { opacity: 1; }
        }

        [data-testid="stPlotlyChart"] > div,
        [data-testid="stPlotlyChart"] .js-plotly-plot,
        [data-testid="stPlotlyChart"] .plot-container,
        [data-testid="stPlotlyChart"] .svg-container,
        [data-testid="stPlotlyChart"] .main-svg {
            width: 100% !important;
            max-width: 100% !important;
        }

        [data-testid="stPlotlyChart"]:hover {
            border-color: var(--seminario-border-strong);
            box-shadow: 0 22px 54px rgba(0, 0, 0, 0.28), 0 0 36px rgba(189, 147, 249, 0.08);
        }

        @keyframes seminarioFadeIn {
            from { opacity: 0; transform: translateY(6px); }
            to   { opacity: 1; transform: translateY(0); }
        }

        .seminario-metric-card,
        .seminario-side-group,
        [data-testid="stMetric"] {
            animation: seminarioFadeIn 360ms ease-out both;
            transition: border-color 200ms ease, box-shadow 200ms ease;
        }

        .seminario-metric-grid > .seminario-metric-card:nth-child(1) { animation-delay:  20ms; }
        .seminario-metric-grid > .seminario-metric-card:nth-child(2) { animation-delay:  80ms; }
        .seminario-metric-grid > .seminario-metric-card:nth-child(3) { animation-delay: 140ms; }
        .seminario-metric-grid > .seminario-metric-card:nth-child(4) { animation-delay: 200ms; }
        .seminario-metric-grid > .seminario-metric-card:nth-child(5) { animation-delay: 260ms; }
        .seminario-metric-grid > .seminario-metric-card:nth-child(6) { animation-delay: 320ms; }

        .seminario-metric-card:hover,
        .seminario-side-group:hover,
        [data-testid="stMetric"]:hover {
            border-color: var(--seminario-border-strong);
            box-shadow: 0 22px 50px rgba(0, 0, 0, 0.28), 0 0 28px rgba(189, 147, 249, 0.10);
        }

        h2, h3 {
            margin-top: 1.4rem !important;
            margin-bottom: 0.55rem !important;
        }

        [data-testid="stCaption"] {
            color: var(--seminario-muted) !important;
            opacity: 0.85;
        }

        [data-testid="stDataFrame"] [role="gridcell"],
        [data-testid="stDataFrame"] [role="columnheader"] {
            cursor: pointer;
        }

        [data-testid="stDataFrame"] div,
        [data-testid="stDataFrame"] span {
            color: var(--seminario-text);
        }

        [data-testid="stDataFrame"] canvas {
            border-radius: 16px;
        }

        /* Tablas de presentación (HTML) — no cortan números, hacen scroll */
        .seminario-table-wrap {
            overflow: auto;
            border: 1px solid var(--seminario-border);
            border-radius: 18px;
            background: var(--seminario-panel);
            box-shadow: 0 14px 34px rgba(0, 0, 0, 0.18);
            margin: 0.35rem 0 1.1rem;
            animation: seminarioChartIn 520ms ease-out both;
        }

        .seminario-table-wrap table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }

        .seminario-table-wrap thead th {
            position: sticky;
            top: 0;
            z-index: 1;
            text-align: right;
            padding: 0.6rem 0.95rem;
            white-space: nowrap;
            background: #2b1d3d;
            color: var(--seminario-purple) !important;
            font-weight: 800;
        }

        .seminario-table-wrap tbody td {
            text-align: right;
            padding: 0.5rem 0.95rem;
            white-space: nowrap;
            color: var(--seminario-text);
        }

        /* primera columna (etiquetas) alineada a la izquierda */
        .seminario-table-wrap thead th:first-child,
        .seminario-table-wrap tbody td:first-child {
            text-align: left;
            position: sticky;
            left: 0;
            background: #241832;
        }

        .seminario-table-wrap thead th:first-child {
            background: #2b1d3d;
        }

        [data-testid="stAlert"] {
            background: rgba(139, 233, 253, 0.08);
            border: 1px solid rgba(139, 233, 253, 0.22);
        }

        div[data-baseweb="select"] > div,
        div[data-baseweb="input"] > div {
            background: rgba(33, 23, 45, 0.92) !important;
            border-color: var(--seminario-border) !important;
            border-radius: 14px !important;
            cursor: pointer;
        }

        div[data-baseweb="select"],
        div[data-baseweb="select"] *,
        [data-testid="stSlider"] *,
        [data-testid="stExpander"] summary,
        button,
        a,
        input[type="range"] {
            cursor: pointer !important;
        }

        hr {
            border-color: rgba(189, 147, 249, 0.18) !important;
        }

        .seminario-metric-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 0.9rem;
            margin: 0.5rem 0 1rem;
        }

        .seminario-metric-card {
            min-width: 0;
            background: linear-gradient(145deg, rgba(36, 24, 52, 0.90), rgba(27, 20, 38, 0.82));
            border: 1px solid var(--seminario-border);
            border-radius: 20px;
            padding: 1rem 1.05rem;
            box-shadow: 0 16px 38px rgba(0, 0, 0, 0.22), inset 0 1px 0 rgba(255, 255, 255, 0.05);
        }

        .seminario-metric-label {
            color: var(--seminario-muted);
            font-size: clamp(0.72rem, 1.4vw, 0.86rem);
            font-weight: 700;
            line-height: 1.2;
            margin-bottom: 0.45rem;
        }

        .seminario-metric-value {
            color: var(--seminario-purple);
            font-size: clamp(1.05rem, 2.6vw, 1.8rem);
            font-weight: 800;
            letter-spacing: -0.035em;
            line-height: 1.1;
            overflow-wrap: anywhere;
            text-shadow: 0 0 24px rgba(189, 147, 249, 0.24);
        }

        .seminario-side-panel {
            display: grid;
            gap: 0.75rem;
        }

        .seminario-side-group {
            background: linear-gradient(145deg, rgba(36, 24, 52, 0.88), rgba(27, 20, 38, 0.78));
            border: 1px solid var(--seminario-border);
            border-radius: 18px;
            padding: 0.9rem;
            box-shadow: 0 12px 30px rgba(0, 0, 0, 0.18);
        }

        .seminario-side-title {
            color: var(--seminario-muted);
            font-size: 0.78rem;
            font-weight: 800;
            margin-bottom: 0.55rem;
            text-transform: uppercase;
            letter-spacing: 0.045em;
        }

        .seminario-side-row {
            display: flex;
            align-items: baseline;
            justify-content: space-between;
            gap: 0.8rem;
            padding: 0.45rem 0;
            border-top: 1px solid rgba(189, 147, 249, 0.12);
        }

        .seminario-side-row:first-of-type {
            border-top: 0;
        }

        .seminario-side-label {
            color: var(--seminario-text);
            font-weight: 700;
            font-size: 0.9rem;
            min-width: 0;
        }

        .seminario-side-value {
            color: var(--seminario-purple);
            font-weight: 800;
            font-size: clamp(1rem, 1.8vw, 1.35rem);
            white-space: nowrap;
        }

        @media (max-width: 640px) {
            .seminario-metric-grid {
                grid-template-columns: repeat(auto-fit, minmax(135px, 1fr));
                gap: 0.7rem;
            }

            .seminario-metric-card {
                padding: 0.85rem;
                border-radius: 16px;
            }
        }
        </style>
    """.replace("__MAP_BACKGROUND__", map_background or "none")
    st.markdown(css, unsafe_allow_html=True)


def _inject_parallax_js() -> None:
    """Mueve el fondo y el glow siguiendo el cursor.

    Usa components.html (un iframe) para poder ejecutar JS en el documento
    padre. Eso hace que el navegador imprima warnings inofensivos del tipo
    'Unrecognized feature: ...'; no afectan el funcionamiento.
    """
    components.html(
        """
        <script>
        (() => {
            const root = window.parent.document.documentElement;
            let raf = null;
            let targetX = 0;
            let targetY = 0;

            function apply() {
                raf = null;
                root.style.setProperty('--mouse-x', `${50 + targetX * 50}%`);
                root.style.setProperty('--mouse-y', `${50 + targetY * 50}%`);
                root.style.setProperty('--parallax-x', `${targetX * -20}px`);
                root.style.setProperty('--parallax-y', `${targetY * -16}px`);
            }

            function onMove(event) {
                const w = window.parent.innerWidth || 1;
                const h = window.parent.innerHeight || 1;
                targetX = (event.clientX / w - 0.5) * 2;
                targetY = (event.clientY / h - 0.5) * 2;
                if (!raf) raf = window.parent.requestAnimationFrame(apply);
            }

            if (window.parent.__seminarioParallaxHandler) {
                window.parent.removeEventListener('mousemove', window.parent.__seminarioParallaxHandler);
            }
            window.parent.__seminarioParallaxHandler = onMove;
            window.parent.addEventListener('mousemove', onMove, { passive: true });
        })();
        </script>
        """,
        height=0,
        width=0,
    )


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
    page_title="Educación Superior Colombia",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="expanded",
)

_inject_theme_css()
_inject_parallax_js()

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


def _display_label(value: object) -> str:
    if value is None or (isinstance(value, float) and value != value):
        return "N/A"
    return str(value).replace("_", " ").title()


def _read_sql(query: str, params: tuple = ()) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql_query(query, conn, params=params)


@st.cache_data(ttl=300)
def _icfes_tables_ready() -> bool:
    required = [
        "dim_icfes_periodo",
        "dim_icfes_geografia",
        "fact_icfes_saber11",
        "fact_icfes_saberpro",
    ]
    try:
        return all(table_exists("facts", table) for table in required)
    except Exception:
        return False


@st.cache_data(ttl=300)
def _load_icfes_national() -> pd.DataFrame:
    if not _icfes_tables_ready():
        return pd.DataFrame()
    try:
        return _read_sql(
            """
            SELECT
                'Saber 11' AS fuente,
                dp.ano,
                dp.periodo_componente,
                dp.trimestre,
                dp.ano_periodo,
                SUM(f.observaciones)::BIGINT AS observaciones,
                ROUND(
                    SUM(f.promedio_puntaje_global * f.observaciones)
                    / NULLIF(SUM(f.observaciones), 0),
                    2
                ) AS puntaje_promedio
            FROM facts.fact_icfes_saber11 f
            JOIN facts.dim_icfes_periodo dp ON dp.id = f.periodo_id
            GROUP BY dp.ano, dp.periodo_componente, dp.trimestre, dp.ano_periodo
            UNION ALL
            SELECT
                'Saber Pro' AS fuente,
                dp.ano,
                dp.periodo_componente,
                dp.trimestre,
                dp.ano_periodo,
                SUM(f.observaciones)::BIGINT AS observaciones,
                ROUND(
                    SUM(f.promedio_modulos * f.observaciones)
                    / NULLIF(SUM(f.observaciones), 0),
                    2
                ) AS puntaje_promedio
            FROM facts.fact_icfes_saberpro f
            JOIN facts.dim_icfes_periodo dp ON dp.id = f.periodo_id
            GROUP BY dp.ano, dp.periodo_componente, dp.trimestre, dp.ano_periodo
            ORDER BY fuente, ano, periodo_componente
            """
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def _load_icfes_departments(fuente: str, ano: int) -> pd.DataFrame:
    if not _icfes_tables_ready():
        return pd.DataFrame()
    if fuente == "Saber 11":
        score_expr = "f.promedio_puntaje_global"
        table = "facts.fact_icfes_saber11"
    else:
        score_expr = "f.promedio_modulos"
        table = "facts.fact_icfes_saberpro"
    try:
        return _read_sql(
            f"""
            SELECT
                dg.nombre_departamento AS departamento,
                SUM(f.observaciones)::BIGINT AS observaciones,
                ROUND(
                    SUM({score_expr} * f.observaciones)
                    / NULLIF(SUM(f.observaciones), 0),
                    2
                ) AS puntaje_promedio
            FROM {table} f
            JOIN facts.dim_icfes_periodo dp ON dp.id = f.periodo_id
            JOIN facts.dim_icfes_geografia dg ON dg.id = f.geografia_id
            WHERE dp.ano = %s
              AND dg.nombre_departamento <> 'desconocido'
            GROUP BY dg.nombre_departamento
            HAVING SUM(f.observaciones) > 0
            ORDER BY puntaje_promedio DESC
            """,
            (ano,),
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def _load_icfes_snies_cross(fuente: str, evento: str) -> pd.DataFrame:
    if not _icfes_tables_ready():
        return pd.DataFrame()
    if fuente == "Saber 11":
        score_expr = "f.promedio_puntaje_global"
        table = "facts.fact_icfes_saber11"
    else:
        score_expr = "f.promedio_modulos"
        table = "facts.fact_icfes_saberpro"
    try:
        return _read_sql(
            f"""
            WITH icfes AS (
                SELECT
                    dp.ano,
                    dg.nombre_departamento AS departamento,
                    SUM(f.observaciones)::BIGINT AS observaciones_icfes,
                    SUM({score_expr} * f.observaciones)
                        / NULLIF(SUM(f.observaciones), 0) AS puntaje_promedio
                FROM {table} f
                JOIN facts.dim_icfes_periodo dp ON dp.id = f.periodo_id
                JOIN facts.dim_icfes_geografia dg ON dg.id = f.geografia_id
                WHERE dg.nombre_departamento <> 'desconocido'
                GROUP BY dp.ano, dg.nombre_departamento
            ), snies AS (
                SELECT
                    dt.ano,
                    dg.nombre_departamento AS departamento,
                    SUM(fe.cantidad)::BIGINT AS total_snies
                FROM facts.fact_estudiantes fe
                JOIN facts.dim_tiempo dt ON dt.id = fe.tiempo_id
                JOIN facts.dim_geografia dg ON dg.id = fe.geografia_ies_id
                WHERE fe.tipo_evento = %s
                GROUP BY dt.ano, dg.nombre_departamento
            )
            SELECT
                i.ano,
                i.departamento,
                ROUND(i.puntaje_promedio, 2) AS puntaje_promedio,
                i.observaciones_icfes,
                s.total_snies
            FROM icfes i
            JOIN snies s ON s.ano = i.ano AND s.departamento = i.departamento
            WHERE s.total_snies > 0
            ORDER BY i.ano, i.departamento
            """,
            (evento,),
        )
    except Exception:
        return pd.DataFrame()


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
    with st.expander("📖 Glosario de siglas y conceptos"):
        st.markdown(
            "- **SNIES:** sistema oficial de información de educación superior.\n"
            "- **ICFES:** pruebas estandarizadas (Saber 11, Saber Pro).\n"
            "- **PND:** Plan Nacional de Desarrollo (seguimiento vía SINERGIA).\n"
            "- **ITS:** compara lo observado vs una proyección sin política.\n"
            "- **DiD:** compara Oficial vs Privada (control) antes/después de 2022.\n"
            "- **IC 95%:** rango probable del efecto; si no incluye 0, es significativo.\n"
            "- **Significativo:** el efecto es distinto de cero más allá del azar.\n"
            "- **R²:** qué tanto explica el modelo (0 a 1).\n\n"
            "**Datos:** 2019 no está en SNIES; primera matrícula y graduados "
            "existen desde 2020.\n\n"
            "_Detalle completo en `GLOSARIO.md`._"
        )
    st.divider()
    st.markdown("**Equipo**")
    team_members = [
        ("Belmos", TEAM_IMAGES_DIR / "belmos.png"),
        ("Jacobo", TEAM_IMAGES_DIR / "jacobo.png"),
        ("Martin", TEAM_IMAGES_DIR / "martin.png"),
        ("Nicolas", TEAM_IMAGES_DIR / "nicolas.png"),
        ("Orozco", TEAM_IMAGES_DIR / "orozco.png"),
        ("Gabriel", TEAM_IMAGES_DIR / "yo.png"),
    ]
    for row_start in range(0, len(team_members), 3):
        cols = st.columns(3)
        for col, (name, image_path) in zip(cols, team_members[row_start : row_start + 3]):
            with col:
                if image_path.exists():
                    st.image(image_path, width="stretch")
                st.caption(name)


def _fmt(v: object) -> str:
    """Formatea un número como entero con separador de miles, o 'N/A' si es None."""
    if v is None or (isinstance(v, float) and v != v):  # None o NaN
        return "N/A"
    try:
        return f"{v:,.0f}"
    except (TypeError, ValueError):
        return str(v)


def _metric_grid(items: list[tuple[str, object]]) -> None:
    cards = []
    for label, value in items:
        cards.append(
            '<div class="seminario-metric-card">'
            f'<div class="seminario-metric-label">{html.escape(str(label))}</div>'
            f'<div class="seminario-metric-value">{html.escape(str(value))}</div>'
            '</div>'
        )
    st.markdown(
        f'<div class="seminario-metric-grid">{"".join(cards)}</div>',
        unsafe_allow_html=True,
    )


def _side_metric_groups(groups: list[tuple[str, list[tuple[str, object]]]]) -> None:
    html_groups = []
    for title, rows in groups:
        row_html = "".join(
            '<div class="seminario-side-row">'
            f'<span class="seminario-side-label">{html.escape(str(label))}</span>'
            f'<span class="seminario-side-value">{html.escape(str(value))}</span>'
            '</div>'
            for label, value in rows
        )
        html_groups.append(
            '<div class="seminario-side-group">'
            f'<div class="seminario-side-title">{html.escape(title)}</div>'
            f'{row_html}'
            '</div>'
        )
    st.markdown(
        f'<div class="seminario-side-panel">{"".join(html_groups)}</div>',
        unsafe_allow_html=True,
    )


def _normalize_sector_name(value: object) -> str:
    """Unifica el sector: consolida 'Privado'/'privado' en 'Privada' y
    capitaliza (la fuente mezcla mayúsculas/minúsculas entre tablas)."""
    text = str(value).strip().lower()
    if text in ("privado", "privada"):
        return "Privada"
    if text == "oficial":
        return "Oficial"
    return str(value).strip().title()


def _plotly_chart(fig: go.Figure) -> None:
    fig.update_layout(
        autosize=True,
        margin=dict(l=48, r=24, t=70, b=78),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.18,
            xanchor="left",
            x=0,
            itemwidth=30,
        ),
    )
    st.plotly_chart(
        fig,
        width="stretch",
        config={"responsive": True, "displayModeBar": False},
    )


_DECIMAL_HINTS = (
    "pct", "coef", "tasa", "ratio", "puntaje", "promedio", "proxy",
    "indicador", "valor", "media", "cambio", "anual", "variaci", "%", "/",
)


def _style_table(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    numeric_cols = df.select_dtypes(include="number").columns
    formatter = {}
    for col in numeric_cols:
        key = col.lower()
        if any(hint in key for hint in _DECIMAL_HINTS):
            formatter[col] = "{:,.2f}"
        else:
            formatter[col] = "{:,.0f}"
    return (
        df.style.format(formatter, na_rep="")
        .set_properties(
            **{
                "background-color": "#21172d",
                "color": "#f8f8f2",
                "border-color": "rgba(189,147,249,0.18)",
            }
        )
        .set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "#2b1d3d"),
                        ("color", "#bd93f9"),
                        ("font-weight", "800"),
                        ("border-color", "rgba(189,147,249,0.28)"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [
                        ("border-color", "rgba(189,147,249,0.14)"),
                    ],
                },
                {
                    "selector": "tbody tr:nth-child(even) td",
                    "props": [("background-color", "#241832")],
                },
                {
                    "selector": "tbody tr:hover td",
                    "props": [("background-color", "rgba(189,147,249,0.16)")],
                },
            ]
        )
    )


_TECHNICAL_COLS = {
    "signo",
    "p_value",
    "p_valor",
    "forma_funcional",
    "muestra",
    "estado",
    "nota",
    "n_ies",
    "n_obs",
    "t",
    "t_post",
    "d",
    "pnd_meta_id91",
}


def _dataframe(df: pd.DataFrame, height: int | None = None) -> None:
    drop_cols = [c for c in df.columns if c.lower() in _TECHNICAL_COLS]
    clean = df.drop(columns=drop_cols) if drop_cols else df
    # Render como HTML: cada columna se ajusta a su contenido y, si la tabla
    # excede el ancho, el contenedor hace scroll en vez de cortar los números.
    table_html = _style_table(clean).hide(axis="index").to_html()
    style = f"max-height:{height}px;" if height is not None else ""
    st.markdown(
        f'<div class="seminario-table-wrap" style="{style}">{table_html}</div>',
        unsafe_allow_html=True,
    )


def _presentation_table(df: pd.DataFrame, rename: dict[str, str], height: int | None = None) -> None:
    cols = [col for col in rename if col in df.columns]
    if not cols:
        _dataframe(df, height=height)
        return
    table = df[cols].rename(columns=rename)
    _dataframe(table, height=height)


# ── tabs ─────────────────────────────────────────────────────────────────────

(
    tab_general,
    tab_snies,
    tab_icfes,
    tab_temporal,
    tab_sectorial,
    tab_robustez,
    tab_triang,
) = st.tabs(
    [
        "General",
        "SNIES",
        "ICFES",
        "Temporal",
        "Sectorial",
        "Robustez",
        "Triangulación",
    ]
)

# ────────────────────────────────────────────────────────────────────────────
# TAB GENERAL
# ────────────────────────────────────────────────────────────────────────────

with tab_general:
    st.markdown(
        f"{SVG_CHART} **Vista general**",
        unsafe_allow_html=True,
    )

    df_overview = _load_csv(RESULTS_DIR / f"tendencias_{tipo_evento}.csv")
    df_overview_resumen = _load_csv(RESULTS_DIR / f"resumen_pre_post_{tipo_evento}.csv")

    if df_overview is None or df_overview.empty:
        st.info("Sin datos disponibles.")
    else:
        df_overview = df_overview.copy()
        df_overview["sector_display"] = df_overview["sector_ies"].map(_normalize_sector_name)
        latest_period = df_overview.sort_values("t")["periodo"].iloc[-1]
        latest = df_overview[df_overview["periodo"] == latest_period]
        total_latest = latest["total"].sum()
        oficial_latest = latest.loc[latest["sector_display"].eq("Oficial"), "total"].sum()
        privada_latest = latest.loc[latest["sector_display"].eq("Privada"), "total"].sum()
        oficial_share = (oficial_latest / total_latest * 100) if total_latest else 0

        kpis = [
            ("Periodo reciente", latest_period),
            ("Total", _fmt(total_latest)),
            ("Oficial", _fmt(oficial_latest)),
            ("Privada", _fmt(privada_latest)),
            ("Participación oficial", f"{oficial_share:.1f}%"),
        ]
        _metric_grid(kpis)

        left, right = st.columns([2.6, 1], gap="large")
        with left:
            fig_overview = go.Figure()
            for sector, color in [("Oficial", COLOR_PURPLE), ("Privada", COLOR_CYAN)]:
                sub = (
                    df_overview[df_overview["sector_display"] == sector]
                    .groupby(["periodo", "t"], as_index=False)["total"]
                    .sum()
                    .sort_values("t")
                )
                fig_overview.add_trace(
                    go.Scatter(
                        x=sub["periodo"],
                        y=sub["total"],
                        mode="lines+markers",
                        name=sector,
                        line=dict(color=color, width=2.8),
                        marker=dict(size=8),
                    )
                )
            periodos_ov = sorted(df_overview["periodo"].unique().tolist())
            if "2022-S2" in periodos_ov:
                fig_overview.add_vline(
                    x=periodos_ov.index("2022-S2"),
                    line_dash="dash",
                    line_color=COLOR_PINK,
                    annotation_text="Cambio de gobierno (2022-S2)",
                    annotation_position="top left",
                )
            fig_overview.update_layout(
                title=f"Evolución de {tipo_evento.replace('_', ' ')}",
                xaxis_title="Periodo",
                yaxis_title="Total",
                template=PLOTLY_TEMPLATE,
                height=440,
                hovermode="x unified",
            )
            _plotly_chart(fig_overview)

        with right:
            side_groups: list[tuple[str, list[tuple[str, object]]]] = []
            if df_overview_resumen is not None and not df_overview_resumen.empty:
                resumen = df_overview_resumen.copy()
                resumen["sector_display"] = resumen["sector_ies"].map(_normalize_sector_name)
                resumen = resumen.groupby("sector_display", as_index=False).agg(
                    media_post_2022=("media_post_2022", "sum"),
                    cambio_pct_pre_post=("cambio_pct_pre_post", "mean"),
                )
                side_groups.append(
                    (
                        "Cambio pre/post",
                        [
                            (
                                row["sector_display"],
                                f"{row.get('cambio_pct_pre_post', 0):+.1f}%",
                            )
                            for _, row in resumen.iterrows()
                        ],
                    )
                )
            if "var_pct_anual" in df_overview.columns:
                last_var = (
                    df_overview[df_overview["var_pct_anual"].notna()]
                    .sort_values("t")
                    .groupby("sector_display")
                    .tail(1)
                )
                if not last_var.empty:
                    side_groups.append(
                        (
                            "Variación reciente",
                            [
                                (row["sector_display"], f"{row['var_pct_anual']:.1f}%")
                                for _, row in last_var.iterrows()
                            ],
                        )
                    )
            if side_groups:
                _side_metric_groups(side_groups)

        mix = (
            df_overview.pivot_table(
                index="periodo",
                columns="sector_display",
                values="total",
                aggfunc="sum",
                fill_value=0,
            )
            .reindex(sorted(df_overview["periodo"].unique()), fill_value=0)
            .reset_index()
        )
        for col in ["Oficial", "Privada"]:
            if col not in mix.columns:
                mix[col] = 0
        mix["total"] = mix["Oficial"] + mix["Privada"]
        mix["oficial_pct"] = (mix["Oficial"] / mix["total"].replace(0, pd.NA) * 100).fillna(0)
        mix["privada_pct"] = (mix["Privada"] / mix["total"].replace(0, pd.NA) * 100).fillna(0)

        fig_mix = go.Figure()
        fig_mix.add_trace(
            go.Scatter(
                x=mix["periodo"],
                y=mix["oficial_pct"],
                mode="lines",
                stackgroup="one",
                groupnorm="percent",
                name="Oficial",
                line=dict(color=COLOR_PURPLE, width=0),
                fillcolor="rgba(189,147,249,0.58)",
                hovertemplate="Oficial: %{y:.1f}%<extra></extra>",
            )
        )
        fig_mix.add_trace(
            go.Scatter(
                x=mix["periodo"],
                y=mix["privada_pct"],
                mode="lines",
                stackgroup="one",
                name="Privada",
                line=dict(color=COLOR_CYAN, width=0),
                fillcolor="rgba(139,233,253,0.44)",
                hovertemplate="Privada: %{y:.1f}%<extra></extra>",
            )
        )
        fig_mix.update_layout(
            title="Composición por sector",
            xaxis_title="Periodo",
            yaxis_title="Participación",
            template=PLOTLY_TEMPLATE,
            height=340,
            hovermode="x unified",
            yaxis=dict(ticksuffix="%", range=[0, 100]),
        )
        _plotly_chart(fig_mix)


# ────────────────────────────────────────────────────────────────────────────
# TAB SNIES
# ────────────────────────────────────────────────────────────────────────────

with tab_snies:
    st.markdown(
        f"{SVG_TREND_UP} **SNIES — detalle de la fuente**",
        unsafe_allow_html=True,
    )
    st.caption(
        "La evolución general está en la pestaña «General». Aquí se detalla el cambio "
        "antes y después de 2022 y la variación año a año."
    )

    df_tend = _load_csv(RESULTS_DIR / f"tendencias_{tipo_evento}.csv")
    df_resumen = _load_csv(RESULTS_DIR / f"resumen_pre_post_{tipo_evento}.csv")

    if df_tend is None:
        st.info("Sin datos disponibles.")
    else:
        if df_resumen is not None and not df_resumen.empty:
            st.subheader("Cambio pre/post 2022 por sector")
            # Consolida el rótulo sucio "Privado" dentro de "Privada" y
            # recalcula el cambio % sobre las medias agregadas.
            resumen_clean = df_resumen.copy()
            resumen_clean["sector_ies"] = resumen_clean["sector_ies"].map(_normalize_sector_name)
            resumen_clean = resumen_clean.groupby("sector_ies", as_index=False).agg(
                media_pre_2022=("media_pre_2022", "sum"),
                media_post_2022=("media_post_2022", "sum"),
            )
            resumen_clean["cambio_pct_pre_post"] = (
                (resumen_clean["media_post_2022"] - resumen_clean["media_pre_2022"])
                / resumen_clean["media_pre_2022"].replace(0, pd.NA)
                * 100
            ).fillna(0)
            cols = st.columns(len(resumen_clean))
            for i, (_, row) in enumerate(resumen_clean.iterrows()):
                with cols[i]:
                    st.metric(
                        label=row["sector_ies"],
                        value=f"{row['media_post_2022']:,.0f}",
                        delta=f"{row['cambio_pct_pre_post']:+.1f}% vs. pre-2022",
                        delta_color="normal",
                    )
            st.caption("Media semestral de estudiantes por periodo de gobierno.")

        if "var_pct_anual" in df_tend.columns:
            st.subheader("Variación porcentual anual")
            fig_var = go.Figure()
            for sector, color in [("Oficial", COLOR_PURPLE), ("Privada", COLOR_CYAN)]:
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
            fig_var.add_hline(y=0, line_color=COLOR_GRID_ZERO, line_width=1)
            fig_var.update_layout(
                barmode="group",
                template=PLOTLY_TEMPLATE,
                height=380,
                title="Crecimiento anual por sector",
                xaxis_title="Periodo",
                yaxis_title="Variación anual",
                yaxis=dict(ticksuffix="%"),
            )
            _plotly_chart(fig_var)

        st.subheader("Datos por semestre y sector")
        # Normaliza el sector y consolida el rótulo sucio "Privado" dentro de
        # "Privada" para no mostrar filas duplicadas; recalcula la variación
        # anual sobre la serie consolidada (mismo semestre del año anterior).
        df_tend_show = df_tend.copy()
        df_tend_show["sector_ies"] = df_tend_show["sector_ies"].map(_normalize_sector_name)
        df_tend_show = (
            df_tend_show.groupby(["periodo", "t", "sector_ies"], as_index=False)["total"]
            .sum()
            .sort_values(["sector_ies", "t"])
        )
        df_tend_show["var_pct_anual"] = (
            df_tend_show.groupby("sector_ies")["total"].pct_change(periods=2) * 100
        )
        _presentation_table(
            df_tend_show.sort_values(["t", "sector_ies"]),
            {
                "periodo": "Periodo",
                "sector_ies": "Sector",
                "total": "Estudiantes",
                "var_pct_anual": "Variación anual (%)",
            },
            height=360,
        )
        st.caption(
            "**Variación anual** = cambio frente al mismo semestre del año anterior. "
            "Aparece vacía en el primer año disponible porque no hay un periodo previo "
            "con el cual comparar. Además, **2019 no está en la fuente SNIES** (la serie "
            "pasa de 2018 a 2020), por lo que la primera variación calculada cubre dos años."
        )


# ────────────────────────────────────────────────────────────────────────────
# TAB 2: ANÁLISIS TEMPORAL (ITS)
# ────────────────────────────────────────────────────────────────────────────

with tab_temporal:
    st.markdown(
        f"{SVG_CHART} **Análisis temporal (antes y después del cambio de gobierno)**",
        unsafe_allow_html=True,
    )
    st.markdown(
        "Se compara la tendencia observada contra lo que habría ocurrido "
        "si la tendencia anterior se hubiera mantenido (escenario sin cambio de política)."
    )
    with st.expander("¿Qué es ITS (Interrupted Time Series)?"):
        st.markdown(
            "**ITS — Series de Tiempo Interrumpidas.** Se ajusta la tendencia de los datos "
            "**antes** de un evento (aquí, el cambio de gobierno en 2022-S2) y se **proyecta** "
            "como si nada hubiera pasado. La diferencia entre lo **observado** y esa **proyección** "
            "es el efecto atribuible al evento.\n\n"
            "- **Cambio inmediato (nivel):** salto justo después del evento.\n"
            "- **Cambio de tendencia (pendiente):** cómo cambia el ritmo de crecimiento por semestre.\n\n"
            "Es un método de un solo grupo: no necesita un grupo de comparación, "
            "pero asume que la tendencia previa habría continuado igual."
        )

    its_json = _load_json(RESULTS_DIR / f"its_{sector_sel.lower()}_{tipo_evento}.json")
    df_its = _load_csv(
        RESULTS_DIR / f"its_datos_{sector_sel.lower()}_{tipo_evento}.csv"
    )

    if its_json is None:
        st.info("Sin datos disponibles.")
    else:
        coefs = its_json.get("coeficientes", {})
        bondad = its_json.get("bondad_ajuste", {})
        chow = its_json.get("prueba_chow", {})

        # Métricas principales
        a2 = coefs.get("alpha_2_cambio_nivel", {})
        a3 = coefs.get("alpha_3_cambio_tendencia", {})

        _metric_grid(
            [
                ("Cambio inmediato", _fmt(a2.get("estimado"))),
                ("Cambio de tendencia / semestre", _fmt(a3.get("estimado"))),
                ("Ajuste del modelo (R²)", bondad.get("R2", "N/A")),
                (
                    "Significancia (nivel)",
                    _badge_plain(
                        a2.get("p_value", 1) < 0.05
                        if a2.get("p_value") is not None
                        else None
                    ),
                ),
            ]
        )

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
                    line=dict(color=COLOR_PURPLE, width=2.5),
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
                    line=dict(color=COLOR_MUTED, dash="dash", width=2),
                )
            )
            periodos_list = sorted(df_its["periodo"].unique().tolist())
            if "2022-S2" in periodos_list:
                fig_its.add_vline(
                    x=periodos_list.index("2022-S2"),
                    line_dash="dash",
                    line_color=COLOR_PINK,
                    annotation_text="2022-S2",
                )
            fig_its.update_layout(
                title=f"Observado vs. proyección sin política — {sector_sel} | {tipo_evento.replace('_', ' ').title()}",
                xaxis_title="Semestre",
                yaxis_title="Estudiantes",
                template=PLOTLY_TEMPLATE,
                height=430,
                hovermode="x unified",
            )
            _plotly_chart(fig_its)

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
                            COLOR_GREEN if v >= 0 else COLOR_PINK
                            for v in df_ef["efecto_estimado"]
                        ],
                    )
                )
                fig_ef.add_hline(y=0, line_color=COLOR_GRID_ZERO)
                fig_ef.update_layout(
                    template=PLOTLY_TEMPLATE,
                    height=320,
                    yaxis_title="Estudiantes (diferencia)",
                )
                _plotly_chart(fig_ef)

        # Placebos
        placebos = its_json.get("placebos", {})
        if placebos:
            st.subheader("Pruebas con puntos de quiebre alternativos")
            rows = []
            for k, v in placebos.items():
                a2_p = v.get("alpha_2_p")
                a3_p = v.get("alpha_3_p")
                rows.append(
                    {
                        "Punto de corte": str(k).replace("t0=", "t = "),
                        "Cambio inmediato": v.get("alpha_2"),
                        "Cambio de tendencia": v.get("alpha_3"),
                        "Significativo (nivel)": _badge_plain(a2_p < 0.05 if a2_p is not None else None),
                        "Significativo (tendencia)": _badge_plain(a3_p < 0.05 if a3_p is not None else None),
                    }
                )
            df_pl = pd.DataFrame(rows)
            _dataframe(df_pl)
            st.caption(
                "Repite el análisis cambiando el corte temporal. Si los efectos siguen siendo significativos, "
                "el resultado original se considera robusto."
            )


# ────────────────────────────────────────────────────────────────────────────
# TAB 3: COMPARACIÓN SECTORIAL (DiD)
# ────────────────────────────────────────────────────────────────────────────

with tab_sectorial:
    st.markdown(
        f"{SVG_SCALE} **Comparación sectorial (Oficial vs. Privada)**",
        unsafe_allow_html=True,
    )
    st.markdown(
        "Se compara cómo cambió la matrícula en el sector **Oficial** respecto al sector "
        "**Privado** después de 2022. Si la política afecta solo al sector oficial, "
        "la diferencia entre ambos debería cambiar tras la intervención."
    )
    with st.expander("¿Qué es DiD (Diferencias en Diferencias)?"):
        st.markdown(
            "**DiD — Diferencias en Diferencias.** Compara dos grupos (aquí **Oficial** vs "
            "**Privada**) antes y después del evento. La idea: el sector privado actúa como "
            "**grupo de control** para descontar todo lo que habría pasado de igual forma en "
            "ambos (economía, demografía).\n\n"
            "El efecto = *(cambio en Oficial)* − *(cambio en Privada)*. Así se aísla lo "
            "atribuible a la política que afecta principalmente al sector oficial.\n\n"
            "Supone **tendencias paralelas**: antes del evento ambos sectores se movían "
            "de forma similar (esto se chequea en el *event study* de más abajo)."
        )

    did_json = _load_json(RESULTS_DIR / f"did_agregado_{tipo_evento}.json")
    did_panel_json = _load_json(RESULTS_DIR / f"did_panel_{tipo_evento}.json")
    df_es = _load_csv(RESULTS_DIR / f"event_study_{tipo_evento}.csv")

    if did_json is None:
        st.info("Sin datos disponibles.")
    else:
        est = did_json.get("estimador_did", {})
        medias = did_json.get("medias", {})

        _metric_grid(
            [
                ("Efecto diferencial estimado", _fmt(est.get("beta_3"))),
                ("IC 95% inferior", _fmt(est.get("ic_95_lower"))),
                ("IC 95% superior", _fmt(est.get("ic_95_upper"))),
                ("Significancia", _badge_plain(est.get("significativo"))),
            ]
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
        _dataframe(tabla_2x2)

        # Gráfico DiD
        fig_did = go.Figure()
        for sector, color, pre_key, post_key in [
            ("Oficial", COLOR_PURPLE, "oficial_pre", "oficial_post"),
            ("Privada", COLOR_CYAN, "privada_pre", "privada_post"),
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
            template=PLOTLY_TEMPLATE,
            height=380,
        )
        _plotly_chart(fig_did)

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
        colores_es = df_es["pre_tratamiento"].map({1: COLOR_MUTED, 0: COLOR_PURPLE})
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
        fig_es.add_hline(y=0, line_dash="dash", line_color=COLOR_GRID_ZERO)
        periodos_es = df_es["periodo"].tolist()
        if "2022-S2" in periodos_es:
            fig_es.add_vline(
                x=periodos_es.index("2022-S2"),
                line_dash="dash",
                line_color=COLOR_PINK,
                annotation_text="2022-S2",
            )
        fig_es.update_layout(
            title="Diferencia entre sectores por semestre",
            xaxis_title="Semestre",
            yaxis_title="Diferencia (Oficial - Privada)",
            template=PLOTLY_TEMPLATE,
            height=420,
        )
        _plotly_chart(fig_es)
        st.caption(
            "Los puntos azul claro (pre-2022) cerca de cero indican que ambos sectores "
            "tenían una tendencia similar antes del cambio de gobierno."
        )


# ────────────────────────────────────────────────────────────────────────────
# TAB 4: INCERTIDUMBRE
# ────────────────────────────────────────────────────────────────────────────

with tab_temporal:
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
        st.info("Sin datos disponibles.")
    else:
        its_b = boot_json.get("its_bootstrap", {})
        did_b = boot_json.get("did_bootstrap", {})
        a2_b = its_b.get("alpha_2_cambio_nivel", {})
        b3_b = did_b.get("beta_3_did", {})

        st.subheader("Rangos de confianza (95%)")
        st.markdown("**Cambio inmediato en la tendencia**")
        _metric_grid(
            [
                ("Valor central", _fmt(a2_b.get("media_boot"))),
                ("Límite inferior", _fmt(a2_b.get("ic_95_lower"))),
                ("Límite superior", _fmt(a2_b.get("ic_95_upper"))),
            ]
        )
        st.markdown("**Efecto diferencial entre sectores**")
        _metric_grid(
            [
                ("Valor central", _fmt(b3_b.get("media_boot"))),
                ("Límite inferior", _fmt(b3_b.get("ic_95_lower"))),
                ("Límite superior", _fmt(b3_b.get("ic_95_upper"))),
            ]
        )

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
                    line=dict(color=COLOR_PURPLE, width=2.5),
                )
            )
            fig_esc.add_trace(
                go.Scatter(
                    x=df_esc["periodo"],
                    y=df_esc["escenario_base"],
                    name="Sin cambio de política",
                    mode="lines",
                    line=dict(color=COLOR_MUTED, dash="dash"),
                )
            )
            fig_esc.add_trace(
                go.Scatter(
                    x=df_esc["periodo"],
                    y=df_esc["escenario_optimista"],
                    name="Escenario favorable",
                    mode="lines",
                    line=dict(color=COLOR_GREEN, dash="dot"),
                )
            )
            fig_esc.add_trace(
                go.Scatter(
                    x=df_esc["periodo"],
                    y=df_esc["escenario_adverso"],
                    name="Escenario desfavorable",
                    mode="lines",
                    line=dict(color=COLOR_PINK, dash="dot"),
                )
            )
            fig_esc.update_layout(
                title="Observado vs. proyecciones (base / favorable / desfavorable)",
                xaxis_title="Semestre",
                yaxis_title="Matriculados",
                template=PLOTLY_TEMPLATE,
                height=420,
                hovermode="x unified",
            )
            _plotly_chart(fig_esc)


# ────────────────────────────────────────────────────────────────────────────
# TAB 5: ROBUSTEZ
# ────────────────────────────────────────────────────────────────────────────

with tab_robustez:
    st.markdown(
        f"{SVG_REFRESH} **Robustez y sensibilidad — Hito 4**",
        unsafe_allow_html=True,
    )

    robustez_json = _load_json(RESULTS_DIR / "robustez_resumen.json")
    df_robustez = _load_csv(RESULTS_DIR / "robustez_sensibilidad.csv")

    if robustez_json is None or df_robustez is None:
        st.info("Sin datos disponibles.")
    else:
        st.caption(
            "Se repitió cada análisis cambiando supuestos (corte temporal, forma del modelo, "
            "muestra). Si el resultado se mantiene, es robusto."
        )

        ok = df_robustez[df_robustez["estado"].eq("ok")].copy()
        estimador_labels = {
            "ITS_alpha2_nivel": "Cambio inmediato (ITS)",
            "ITS_alpha3_tendencia": "Cambio de tendencia (ITS)",
            "DiD_agregado_beta3": "Efecto diferencial (DiD)",
            "DiD_panel_TWFE_beta": "Efecto con controles (DiD panel)",
        }
        ok["metodo"] = ok["estimador"].map(estimador_labels).fillna(ok["estimador"])

        n_total = len(ok)
        n_sig = int(ok["significativo"].sum()) if "significativo" in ok.columns else 0
        pct_sig = (n_sig / n_total * 100) if n_total else 0
        _metric_grid(
            [
                ("Especificaciones probadas", f"{n_total:,.0f}"),
                ("Resultados significativos", f"{n_sig:,.0f}"),
                ("% significativas", f"{pct_sig:.0f}%"),
                ("Métodos evaluados", f"{ok['metodo'].nunique():,.0f}"),
            ]
        )

        # Resumen compacto: cuántas corridas son significativas por método
        if "significativo" in ok.columns:
            st.subheader("¿Cuántas pruebas confirman cada efecto?")
            resumen_metodo = (
                ok.assign(
                    Significativas=ok["significativo"].astype(bool),
                    No=~ok["significativo"].astype(bool),
                )
                .groupby("metodo", as_index=False)[["Significativas", "No"]]
                .sum()
                .sort_values("Significativas")
            )
            fig_rob = go.Figure()
            fig_rob.add_trace(
                go.Bar(
                    y=resumen_metodo["metodo"],
                    x=resumen_metodo["Significativas"],
                    name="Significativas",
                    orientation="h",
                    marker_color=COLOR_GREEN,
                )
            )
            fig_rob.add_trace(
                go.Bar(
                    y=resumen_metodo["metodo"],
                    x=resumen_metodo["No"],
                    name="No significativas",
                    orientation="h",
                    marker_color=COLOR_MUTED,
                )
            )
            fig_rob.update_layout(
                barmode="stack",
                template=PLOTLY_TEMPLATE,
                height=320,
                xaxis_title="Número de pruebas",
                yaxis_title="",
            )
            _plotly_chart(fig_rob)
            st.caption(
                "Cada método se probó varias veces con supuestos distintos. "
                "Más barras verdes = resultado más robusto."
            )

        with st.expander("Ver detalle de todas las pruebas"):
            df_show = ok.copy()
            df_show["significativo"] = df_show["significativo"].map(
                {True: "Sí", False: "No"}
            ).fillna("—")
            _presentation_table(
                df_show,
                {
                    "metodo": "Análisis",
                    "tipo_evento": "Indicador",
                    "t0": "Corte",
                    "coeficiente": "Efecto",
                    "ic_95_lower": "Límite inferior",
                    "ic_95_upper": "Límite superior",
                    "significativo": "Concluyente",
                },
                height=360,
            )


# ────────────────────────────────────────────────────────────────────────────
# TAB 6: TRIANGULACIÓN
# ────────────────────────────────────────────────────────────────────────────

with tab_triang:
    st.markdown(
        f"{SVG_SCALE} **Triangulación PND/SNIES/ICFES — Hito 4**",
        unsafe_allow_html=True,
    )

    triang_json = _load_json(RESULTS_DIR / "triangulacion_resumen.json")
    df_pnd = _load_csv(RESULTS_DIR / "triangulacion_pnd_snies.csv")
    df_icfes = _load_csv(RESULTS_DIR / "triangulacion_icfes_snies.csv")
    df_embudo = _load_csv(RESULTS_DIR / "triangulacion_embudo_snies.csv")

    if triang_json is None:
        st.info("Sin datos disponibles.")
    else:
        st.info(triang_json.get("interpretacion", ""))

        pnd_info = triang_json.get("pnd_snies", {})
        icfes_info = triang_json.get("icfes", {})
        embudo_info = triang_json.get("embudo_snies", {})

        _metric_grid(
            [
                ("PND disponible", "Sí" if pnd_info.get("fuente_pnd_disponible") else "No"),
                ("Filas PND ID 91", pnd_info.get("filas_pnd_id91", "N/A")),
                ("Filas ICFES", icfes_info.get("filas", "N/A")),
            ]
        )

        if df_pnd is not None and not df_pnd.empty:
            st.subheader("PND/SINERGIA vs SNIES")
            _presentation_table(
                df_pnd,
                {
                    "ano": "Año",
                    "snies_primer_curso_oficial": "SNIES — primera matrícula (oficial)",
                    "pnd_indicador_id91": "PND — indicador",
                },
            )
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
                    template=PLOTLY_TEMPLATE,
                    height=380,
                    xaxis_title="Año",
                    yaxis_title="SNIES primer curso oficial",
                )
                _plotly_chart(fig_pnd)

        if df_embudo is not None and not df_embudo.empty:
            st.subheader("Embudo SNIES")
            st.caption(
                f"Periodo: {embudo_info.get('periodo_min')} a {embudo_info.get('periodo_max')} | "
                "Sectores: Oficial, Privada"
            )
            # Consolida "privado" dentro de "privada", reagrega los conteos y
            # recalcula las tasas sobre los totales consolidados.
            emb = df_embudo.copy()
            emb["sector_ies"] = emb["sector_ies"].map(_normalize_sector_name)
            count_cols = ["inscritos", "admitidos", "matriculados", "primer_curso", "graduados"]
            present = [c for c in count_cols if c in emb.columns]
            emb = (
                emb.groupby(["periodo", "sector_ies"], as_index=False)[present].sum(min_count=1)
            )
            if {"admitidos", "inscritos"}.issubset(emb.columns):
                emb["tasa_admision"] = emb["admitidos"] / emb["inscritos"].replace(0, pd.NA)
            if {"matriculados", "admitidos"}.issubset(emb.columns):
                emb["tasa_matricula_sobre_admitidos"] = (
                    emb["matriculados"] / emb["admitidos"].replace(0, pd.NA)
                )
            if {"primer_curso", "matriculados"}.issubset(emb.columns):
                emb["primer_curso_sobre_matriculados"] = (
                    emb["primer_curso"] / emb["matriculados"].replace(0, pd.NA)
                )
            _presentation_table(
                emb,
                {
                    "periodo": "Periodo",
                    "sector_ies": "Sector",
                    "inscritos": "Inscritos",
                    "admitidos": "Admitidos",
                    "matriculados": "Matriculados",
                    "primer_curso": "Primera matrícula",
                    "graduados": "Graduados",
                    "tasa_admision": "Tasa de admisión",
                    "tasa_matricula_sobre_admitidos": "Matrícula / admitidos",
                    "primer_curso_sobre_matriculados": "1ra matrícula / matriculados",
                },
                height=320,
            )
            st.caption(
                "Las columnas de **2018** aparecen vacías en «Primera matrícula» y «Graduados» "
                "porque esos registros en SNIES inician en 2020. Las tres últimas columnas son "
                "**proporciones** (ej. 0.21 = 21 %); valores >1 indican más matriculados acumulados "
                "que admitidos del semestre."
            )

        if df_icfes is not None and not df_icfes.empty:
            st.subheader("Proxy ICFES de contexto")
            _presentation_table(
                df_icfes,
                {
                    "ano": "Año",
                    "icfes_valor_proxy": "Proxy ICFES",
                },
            )


# ────────────────────────────────────────────────────────────────────────────
# TAB 7: RESULTADOS ICFES
# ────────────────────────────────────────────────────────────────────────────

with tab_icfes:
    st.markdown(
        f"{SVG_CHART} **Resultados ICFES — Saber 11 y Saber Pro**",
        unsafe_allow_html=True,
    )
    df_icfes_nacional = _load_icfes_national()
    if df_icfes_nacional.empty:
        st.info("Sin datos disponibles.")
    else:
        fuente_icfes = st.selectbox(
            "Fuente ICFES",
            ["Saber 11", "Saber Pro"],
            key="fuente_icfes",
        )
        df_fuente = df_icfes_nacional[df_icfes_nacional["fuente"] == fuente_icfes].copy()
        df_fuente = df_fuente.sort_values(["ano", "periodo_componente"])

        promedio_total = (
            (df_fuente["puntaje_promedio"] * df_fuente["observaciones"]).sum()
            / df_fuente["observaciones"].sum()
        )
        _metric_grid(
            [
                ("Periodos", f"{df_fuente['ano_periodo'].nunique():,.0f}"),
                ("Observaciones", _fmt(df_fuente["observaciones"].sum())),
                ("Promedio ponderado", f"{promedio_total:,.2f}"),
            ]
        )

        fig_icfes = make_subplots(specs=[[{"secondary_y": True}]])
        fig_icfes.add_trace(
            go.Bar(
                x=df_fuente["ano_periodo"],
                y=df_fuente["observaciones"],
                name="Observaciones",
                marker_color=COLOR_MUTED,
                opacity=0.75,
            ),
            secondary_y=True,
        )
        fig_icfes.add_trace(
            go.Scatter(
                x=df_fuente["ano_periodo"],
                y=df_fuente["puntaje_promedio"],
                mode="lines+markers",
                name="Puntaje promedio",
                line=dict(color=COLOR_PURPLE, width=3),
                marker=dict(size=8),
            ),
            secondary_y=False,
        )
        fig_icfes.update_layout(
            title=f"Evolución nacional {fuente_icfes}",
            template=PLOTLY_TEMPLATE,
            height=430,
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        fig_icfes.update_xaxes(title_text="Año-periodo")
        fig_icfes.update_yaxes(title_text="Puntaje promedio", secondary_y=False)
        fig_icfes.update_yaxes(title_text="Observaciones", secondary_y=True)
        _plotly_chart(fig_icfes)

        st.subheader("Ranking territorial")
        years = sorted(df_fuente["ano"].dropna().astype(int).unique().tolist())
        selected_year = st.selectbox("Año", years, index=len(years) - 1, key="icfes_year")
        df_dept = _load_icfes_departments(fuente_icfes, selected_year)
        if df_dept.empty:
            st.info("No hay datos territoriales para la selección actual.")
        else:
            top_n = st.slider("Departamentos a mostrar", 5, 25, 12, key="icfes_top_n")
            df_top = df_dept.head(top_n).copy()
            df_top["departamento_label"] = df_top["departamento"].map(_display_label)
            fig_dept = go.Figure(
                go.Bar(
                    x=df_top["puntaje_promedio"],
                    y=df_top["departamento_label"],
                    orientation="h",
                    marker_color=COLOR_CYAN,
                    customdata=df_top[["observaciones"]],
                    hovertemplate=(
                        "Departamento: %{y}<br>"
                        "Puntaje: %{x:.2f}<br>"
                        "Observaciones: %{customdata[0]:,.0f}<extra></extra>"
                    ),
                )
            )
            fig_dept.update_layout(
                title=f"Top {top_n} departamentos por puntaje promedio — {fuente_icfes} {selected_year}",
                xaxis_title="Puntaje promedio",
                yaxis_title="Departamento",
                yaxis=dict(autorange="reversed"),
                template=PLOTLY_TEMPLATE,
                height=max(380, top_n * 30),
            )
            _plotly_chart(fig_dept)

        st.subheader("Cruce territorial con SNIES")
        evento_cruce = st.selectbox(
            "Indicador SNIES para cruzar",
            ["matriculados", "primer_curso", "graduados"],
            format_func=lambda x: {
                "matriculados": "Matriculados",
                "primer_curso": "Primera matrícula",
                "graduados": "Graduados",
            }[x],
            key="icfes_snies_evento",
        )
        df_cross = _load_icfes_snies_cross(fuente_icfes, evento_cruce)
        if df_cross.empty:
            st.info("No hubo emparejamientos por departamento/año entre ICFES y SNIES.")
        else:
            cross_years = sorted(df_cross["ano"].dropna().astype(int).unique().tolist())
            cross_year = st.selectbox(
                "Año del cruce",
                cross_years,
                index=len(cross_years) - 1,
                key="icfes_cross_year",
            )
            df_cross_year = df_cross[df_cross["ano"] == cross_year].copy()
            df_cross_year["departamento_label"] = df_cross_year["departamento"].map(_display_label)
            fig_cross = go.Figure(
                go.Scatter(
                    x=df_cross_year["puntaje_promedio"],
                    y=df_cross_year["total_snies"],
                    mode="markers",
                    text=df_cross_year["departamento_label"],
                    marker=dict(
                        size=12,
                        color=COLOR_CYAN,
                        line=dict(color="rgba(248,248,242,0.55)", width=1),
                    ),
                    name="Departamentos",
                    hovertemplate=(
                        "%{text}<br>"
                        "Puntaje ICFES: %{x:.2f}<br>"
                        "SNIES: %{y:,.0f}<extra></extra>"
                    ),
                )
            )
            # Línea de tendencia (ajuste lineal simple)
            x = df_cross_year["puntaje_promedio"]
            y = df_cross_year["total_snies"]
            if len(x) >= 2 and x.nunique() >= 2:
                xm, ym = x.mean(), y.mean()
                denom = ((x - xm) ** 2).sum()
                if denom:
                    slope = ((x - xm) * (y - ym)).sum() / denom
                    intercept = ym - slope * xm
                    x_line = [x.min(), x.max()]
                    fig_cross.add_trace(
                        go.Scatter(
                            x=x_line,
                            y=[slope * xv + intercept for xv in x_line],
                            mode="lines",
                            name="Tendencia",
                            line=dict(color=COLOR_PINK, dash="dash", width=2),
                            hoverinfo="skip",
                        )
                    )
            fig_cross.update_layout(
                title=f"{fuente_icfes} vs {evento_cruce.replace('_', ' ')} SNIES — {cross_year}",
                xaxis_title="Puntaje ICFES promedio",
                yaxis_title=f"SNIES {evento_cruce.replace('_', ' ')}",
                template=PLOTLY_TEMPLATE,
                height=460,
            )
            _plotly_chart(fig_cross)

            corr = df_cross_year[["puntaje_promedio", "total_snies"]].corr().iloc[0, 1]
            st.caption("Cada punto es un departamento. La línea muestra la tendencia general.")
            st.metric("Correlación descriptiva", f"{corr:.3f}" if pd.notna(corr) else "N/A")
            _dataframe(
                df_cross_year[
                    [
                        "departamento_label",
                        "puntaje_promedio",
                        "observaciones_icfes",
                        "total_snies",
                    ]
                ].rename(
                    columns={
                        "departamento_label": "Departamento",
                        "puntaje_promedio": "Puntaje ICFES",
                        "observaciones_icfes": "Observaciones ICFES",
                        "total_snies": "Total SNIES",
                    }
                ),
                height=300,
            )
