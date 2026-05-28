"""Triangulacion PND/SNIES/ICFES para la entrega final Hito 4/5."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import inspect, text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import DATA_DIR, PG_SCHEMA_RAW, raw_csv_path, DATASET_PND, DATASET_ICFES_SABER

RESULTS_DIR = DATA_DIR / "results"
PLOTS_DIR = RESULTS_DIR / "plots"


def _normalizar_periodo_snies(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["periodo"] = out["ano"].astype(str) + "-S" + out["semestre"].astype(str)
    return out


def _read_optional_csv(dataset_key: str) -> pd.DataFrame | None:
    path = raw_csv_path(dataset_key)
    if not path.exists():
        return None
    for encoding in ["utf-8", "latin-1", "cp1252"]:
        try:
            return pd.read_csv(path, encoding=encoding)
        except Exception:
            continue
    return None


def _find_raw_table(engine, candidates: list[str]) -> str | None:
    inspector = inspect(engine)
    try:
        tables = inspector.get_table_names(schema=PG_SCHEMA_RAW)
    except Exception:
        return None
    lowered = {t.lower(): t for t in tables}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    for table in tables:
        t_low = table.lower()
        if any(candidate.lower() in t_low for candidate in candidates):
            return table
    return None


def _read_optional_raw_table(engine, candidates: list[str]) -> pd.DataFrame | None:
    table = _find_raw_table(engine, candidates)
    if not table:
        return None
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(f'SELECT * FROM {PG_SCHEMA_RAW}."{table}"'), conn)
    except Exception:
        return None


def _guess_year_column(df: pd.DataFrame) -> str | None:
    for col in df.columns:
        c = str(col).lower()
        if c in {"ano", "anio", "año", "vigencia", "year"} or "ano" in c or "vigencia" in c:
            return col
    return None


def _guess_value_column(df: pd.DataFrame) -> str | None:
    preferred = ["avance", "valor", "resultado", "meta", "numerador", "cantidad"]
    for token in preferred:
        for col in df.columns:
            if token in str(col).lower():
                return col
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    return numeric_cols[-1] if numeric_cols else None


def _filter_pnd_id91(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    if "IdIndicador" in df.columns:
        ids = pd.to_numeric(df["IdIndicador"], errors="coerce")
        exact = df[ids.eq(91)].copy()
        if not exact.empty:
            return exact
    mask = pd.Series(False, index=df.index)
    for col in df.columns:
        text = df[col].astype(str).str.lower()
        mask = mask | text.str.contains("estudiantes nuevos", na=False)
    return df[mask].copy()


def _pnd_wide_to_annual(pnd_91: pd.DataFrame) -> pd.DataFrame:
    """Convierte columnas AvanceYYYY/MetaYYYY de SINERGIA a serie anual."""
    if pnd_91.empty:
        return pd.DataFrame(columns=["ano", "pnd_indicador_id91", "pnd_meta_id91"])

    row = pnd_91.iloc[0]
    records = []
    for col in pnd_91.columns:
        col_text = str(col)
        if not col_text.startswith("Avance") or not col_text[-4:].isdigit():
            continue
        year = int(col_text[-4:])
        value = pd.to_numeric(pd.Series([row[col]]), errors="coerce").iloc[0]
        meta_col = f"Meta{year}"
        meta = (
            pd.to_numeric(pd.Series([row[meta_col]]), errors="coerce").iloc[0]
            if meta_col in pnd_91.columns
            else pd.NA
        )
        if pd.notna(value):
            records.append(
                {
                    "ano": year,
                    "pnd_indicador_id91": value,
                    "pnd_meta_id91": meta,
                }
            )

    return pd.DataFrame(records)


def triangulacion_pnd_snies(df_primer_curso: pd.DataFrame, pnd_df: pd.DataFrame | None) -> tuple[pd.DataFrame, dict]:
    snies = df_primer_curso[df_primer_curso["sector_ies"] == "Oficial"].copy()
    snies_anual = snies.groupby("ano", as_index=False)["total"].sum()
    snies_anual = snies_anual.rename(columns={"total": "snies_primer_curso_oficial"})

    resumen = {
        "fuente_pnd_disponible": bool(pnd_df is not None and not pnd_df.empty),
        "filas_pnd_id91": 0,
        "correlacion_pnd_snies": None,
        "nota": "",
    }
    if pnd_df is None or pnd_df.empty:
        snies_anual["pnd_indicador_id91"] = pd.NA
        resumen["nota"] = "No se encontro fuente PND/SINERGIA local; se deja serie SNIES lista para comparar."
        return snies_anual, resumen

    pnd_91 = _filter_pnd_id91(pnd_df)
    resumen["filas_pnd_id91"] = int(len(pnd_91))
    if pnd_91.empty:
        snies_anual["pnd_indicador_id91"] = pd.NA
        resumen["nota"] = "La fuente PND existe, pero no se encontro una fila claramente asociada a ID 91."
        return snies_anual, resumen

    pnd_wide = _pnd_wide_to_annual(pnd_91)
    if not pnd_wide.empty:
        merged = snies_anual.merge(pnd_wide, on="ano", how="left")
        valid = merged[["snies_primer_curso_oficial", "pnd_indicador_id91"]].dropna()
        if len(valid) >= 2:
            resumen["correlacion_pnd_snies"] = round(float(valid.corr().iloc[0, 1]), 4)
        resumen["nota"] = (
            "Indicador PND/SINERGIA ID 91 convertido desde columnas AvanceYYYY. "
            "Comparar tendencias, no niveles: PND reporta avance administrativo y SNIES conteos agregados."
        )
        return merged, resumen

    year_col = _guess_year_column(pnd_91)
    value_col = _guess_value_column(pnd_91)
    if year_col is None or value_col is None:
        snies_anual["pnd_indicador_id91"] = pd.NA
        resumen["nota"] = "La fuente PND existe, pero no se pudieron inferir columnas de ano/valor."
        return snies_anual, resumen

    pnd_tmp = pnd_91[[year_col, value_col]].copy()
    pnd_tmp.columns = ["ano", "pnd_indicador_id91"]
    pnd_tmp["ano"] = pd.to_numeric(pnd_tmp["ano"], errors="coerce")
    pnd_tmp["pnd_indicador_id91"] = pd.to_numeric(pnd_tmp["pnd_indicador_id91"], errors="coerce")
    pnd_tmp = pnd_tmp.dropna(subset=["ano"]).groupby("ano", as_index=False)["pnd_indicador_id91"].mean()
    pnd_tmp["ano"] = pnd_tmp["ano"].astype(int)

    merged = snies_anual.merge(pnd_tmp, on="ano", how="left")
    valid = merged[["snies_primer_curso_oficial", "pnd_indicador_id91"]].dropna()
    if len(valid) >= 2:
        resumen["correlacion_pnd_snies"] = round(float(valid.corr().iloc[0, 1]), 4)
    resumen["nota"] = "Comparacion anual PND/SINERGIA vs SNIES; revisar definiciones antes de interpretar niveles."
    return merged, resumen


def triangulacion_icfes(icfes_df: pd.DataFrame | None) -> tuple[pd.DataFrame, dict]:
    resumen = {
        "fuente_icfes_disponible": bool(icfes_df is not None and not icfes_df.empty),
        "filas": 0,
        "columnas": [],
        "nota": "",
    }
    if icfes_df is None or icfes_df.empty:
        resumen["nota"] = "No se encontro fuente ICFES local; queda documentada como triangulacion pendiente."
        return pd.DataFrame(), resumen

    resumen["filas"] = int(len(icfes_df))
    resumen["columnas"] = [str(c) for c in icfes_df.columns[:25]]
    year_col = _guess_year_column(icfes_df)
    value_col = _guess_value_column(icfes_df)
    if year_col is None or value_col is None:
        resumen["nota"] = "Fuente ICFES disponible, pero no se pudieron inferir columnas de ano/valor."
        return pd.DataFrame(), resumen

    out = icfes_df[[year_col, value_col]].copy()
    out.columns = ["ano", "icfes_valor_proxy"]
    out["ano"] = pd.to_numeric(out["ano"], errors="coerce")
    out["icfes_valor_proxy"] = pd.to_numeric(out["icfes_valor_proxy"], errors="coerce")
    out = out.dropna(subset=["ano"]).groupby("ano", as_index=False)["icfes_valor_proxy"].mean()
    out["ano"] = out["ano"].astype(int)
    resumen["nota"] = "Proxy ICFES agregado automaticamente; usar solo como contexto si la variable inferida es adecuada."
    return out, resumen


def triangulacion_embudo_snies(df_embudo: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    df = _normalizar_periodo_snies(df_embudo)
    pivot = (
        df.pivot_table(
            index=["ano", "semestre", "periodo", "sector_ies"],
            columns="tipo_evento",
            values="total",
            aggfunc="sum",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    for col in ["inscritos", "admitidos", "matriculados", "primer_curso", "graduados"]:
        if col not in pivot.columns:
            pivot[col] = pd.NA
    pivot["tasa_admision"] = pivot["admitidos"] / pivot["inscritos"]
    pivot["tasa_matricula_sobre_admitidos"] = pivot["matriculados"] / pivot["admitidos"]
    pivot["primer_curso_sobre_matriculados"] = pivot["primer_curso"] / pivot["matriculados"]
    resumen = {
        "filas": int(len(pivot)),
        "sectores": sorted(str(s) for s in pivot["sector_ies"].dropna().unique()),
        "periodo_min": str(pivot["periodo"].min()) if not pivot.empty else None,
        "periodo_max": str(pivot["periodo"].max()) if not pivot.empty else None,
    }
    return pivot, resumen


def _plot_pnd_snies(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        return fig
    fig.add_trace(
        go.Scatter(
            x=df["ano"],
            y=df["snies_primer_curso_oficial"],
            mode="lines+markers",
            name="SNIES primer curso oficial",
        )
    )
    if "pnd_indicador_id91" in df.columns and df["pnd_indicador_id91"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=df["ano"],
                y=df["pnd_indicador_id91"],
                mode="lines+markers",
                name="PND/SINERGIA ID 91",
                yaxis="y2",
            )
        )
        fig.update_layout(yaxis2=dict(title="PND/SINERGIA", overlaying="y", side="right"))
    fig.update_layout(
        title="Triangulacion PND/SINERGIA vs SNIES",
        xaxis_title="Ano",
        yaxis_title="SNIES primer curso oficial",
        template="plotly_white",
        height=420,
    )
    return fig


def run_triangulacion_completa() -> dict:
    """Ejecuta triangulacion y guarda artefactos."""
    from analysis.queries import _get_engine, get_embudo_estudiantil, get_matricula_por_sector

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    engine = _get_engine()
    df_primer_curso = get_matricula_por_sector("primer_curso")
    df_embudo = get_embudo_estudiantil()

    pnd_df = _read_optional_raw_table(engine, ["seguimiento_pnd", "pnd"])
    if pnd_df is None:
        pnd_df = _read_optional_csv(DATASET_PND)

    icfes_df = _read_optional_raw_table(engine, ["saber_359", "icfes"])
    if icfes_df is None:
        icfes_df = _read_optional_csv(DATASET_ICFES_SABER)

    pnd_snies, resumen_pnd = triangulacion_pnd_snies(df_primer_curso, pnd_df)
    icfes_proxy, resumen_icfes = triangulacion_icfes(icfes_df)
    embudo, resumen_embudo = triangulacion_embudo_snies(df_embudo)

    pnd_path = RESULTS_DIR / "triangulacion_pnd_snies.csv"
    icfes_path = RESULTS_DIR / "triangulacion_icfes_snies.csv"
    embudo_path = RESULTS_DIR / "triangulacion_embudo_snies.csv"
    resumen_path = RESULTS_DIR / "triangulacion_resumen.json"
    plot_path = PLOTS_DIR / "triangulacion_pnd_snies.html"

    pnd_snies.to_csv(pnd_path, index=False)
    icfes_proxy.to_csv(icfes_path, index=False)
    embudo.to_csv(embudo_path, index=False)

    resumen = {
        "pnd_snies": resumen_pnd,
        "icfes": resumen_icfes,
        "embudo_snies": resumen_embudo,
        "interpretacion": (
            "La triangulacion compara compatibilidad de tendencias entre fuentes. "
            "No debe leerse como atribucion causal independiente."
        ),
    }
    resumen_path.write_text(json.dumps(resumen, ensure_ascii=False, indent=2), encoding="utf-8")
    _plot_pnd_snies(pnd_snies).write_html(str(plot_path))

    return {
        "pnd_snies": pnd_snies,
        "icfes_proxy": icfes_proxy,
        "embudo": embudo,
        "resumen": resumen,
        "paths": {
            "pnd_snies": str(pnd_path),
            "icfes": str(icfes_path),
            "embudo": str(embudo_path),
            "resumen": str(resumen_path),
            "plot": str(plot_path),
        },
    }


if __name__ == "__main__":
    run_triangulacion_completa()
