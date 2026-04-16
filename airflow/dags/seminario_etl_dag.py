from __future__ import annotations

import logging
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


logger = logging.getLogger("airflow.task")

ALERT_EMAIL: str = os.getenv("AIRFLOW_ALERT_EMAIL", "admin@seminario.local")
SMTP_FROM: str = os.getenv("AIRFLOW_SMTP_FROM", "airflow@seminario.local")

SEMINARIO_ROOT = Path(os.getenv("SEMINARIO_PROJECT_ROOT", "/opt/airflow/seminario"))
if str(SEMINARIO_ROOT) not in sys.path:
    sys.path.insert(0, str(SEMINARIO_ROOT))


def _on_failure_callback(context: dict[str, Any]) -> None:
    ti = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = ti.task_id if ti else "unknown"
    execution_date = context.get("execution_date", "unknown")
    exception = context.get("exception", "No exception info")
    log_url = ti.log_url if ti else "N/A"

    logger.error(
        "TASK FAILURE ALERT\n"
        "  DAG:            %s\n"
        "  Task:           %s\n"
        "  Execution Date: %s\n"
        "  Exception:      %s\n"
        "  Log URL:        %s\n"
        "  Try Number:     %s",
        dag_id,
        task_id,
        execution_date,
        exception,
        log_url,
        ti.try_number if ti else "N/A",
    )

    try:
        from airflow.utils.email import send_email

        subject = f"[FALLO] Seminario ETL - Tarea '{task_id}'"
        html_body = (
            f"<h2>Fallo en Pipeline ETL</h2>"
            f"<table>"
            f"<tr><td><b>DAG</b></td><td>{dag_id}</td></tr>"
            f"<tr><td><b>Tarea</b></td><td>{task_id}</td></tr>"
            f"<tr><td><b>Fecha</b></td><td>{execution_date}</td></tr>"
            f"<tr><td><b>Intento</b></td><td>{ti.try_number if ti else 'N/A'}</td></tr>"
            f"<tr><td><b>Excepcion</b></td><td><pre>{exception}</pre></td></tr>"
            f"<tr><td><b>Log</b></td><td><a href='{log_url}'>Ver log</a></td></tr>"
            f"</table>"
        )
        send_email(to=ALERT_EMAIL, subject=subject, html_content=html_body)
        logger.info("Email de alerta enviado a %s", ALERT_EMAIL)
    except Exception:
        logger.warning(
            "No se pudo enviar email de alerta (SMTP no configurado o error)."
        )


def _on_success_callback(context: dict[str, Any]) -> None:
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "unknown"
    duration = ti.duration if ti else 0

    logger.info(
        "Tarea '%s' completada exitosamente en %.1f segundos.",
        task_id,
        duration or 0,
    )


def _on_dag_success_callback(context: dict[str, Any]) -> None:
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    run_id = context.get("run_id", "unknown")

    logger.info(
        "PIPELINE COMPLETADO\n  DAG: %s\n  Run ID: %s",
        dag_id,
        run_id,
    )

    try:
        from airflow.utils.email import send_email

        subject = "[EXITO] Seminario ETL - Pipeline completado"
        html_body = (
            f"<h2>Pipeline ETL Completado</h2>"
            f"<p>DAG <b>{dag_id}</b> ejecutado correctamente.</p>"
            f"<p>Run ID: <code>{run_id}</code></p>"
        )
        send_email(to=ALERT_EMAIL, subject=subject, html_content=html_body)
        logger.info("Email de exito enviado a %s", ALERT_EMAIL)
    except Exception:
        logger.warning("No se pudo enviar email de exito (SMTP no configurado).")


def _sla_miss_callback(
    dag: Any,
    task_list: str,
    blocking_task_list: str,
    slas: list,
    blocking_tis: list,
) -> None:
    logger.warning(
        "SLA MISS DETECTED\n"
        "  DAG:               %s\n"
        "  Tasks afectadas:   %s\n"
        "  Tasks bloqueantes: %s",
        dag.dag_id if dag else "unknown",
        task_list,
        blocking_task_list,
    )

    try:
        from airflow.utils.email import send_email

        subject = "[SLA] Seminario ETL - SLA superado"
        html_body = (
            f"<h2>SLA Superado</h2>"
            f"<p><b>DAG:</b> {dag.dag_id if dag else 'unknown'}</p>"
            f"<p><b>Tareas afectadas:</b> {task_list}</p>"
            f"<p><b>Tareas bloqueantes:</b> {blocking_task_list}</p>"
        )
        send_email(to=ALERT_EMAIL, subject=subject, html_content=html_body)
    except Exception:
        logger.warning("No se pudo enviar email de SLA miss (SMTP no configurado).")


def _safe_execute(step_name: str, func, *args, **kwargs) -> Any:
    logger.info("INICIO: %s", step_name)
    start_time = time.monotonic()

    try:
        result = func(*args, **kwargs)
        elapsed = time.monotonic() - start_time
        logger.info("FIN OK: %s (%.2f s)", step_name, elapsed)
        return result
    except Exception as exc:
        elapsed = time.monotonic() - start_time
        logger.error(
            "ERROR: %s (%.2f s) - %s\n%s",
            step_name,
            elapsed,
            type(exc).__name__,
            traceback.format_exc(),
        )
        raise


def _force_reprocess(kwargs: dict[str, Any]) -> bool:
    """Verifica si el DAG run tiene force_reprocess=true en la conf."""
    dag_run = kwargs.get("dag_run")
    if dag_run and dag_run.conf and dag_run.conf.get("force_reprocess", False):
        return True
    return False


def _has_data(schema: str, table: str) -> bool:
    """Verifica si una tabla existe y tiene al menos una fila."""
    from utils.db import table_exists, get_row_count

    return table_exists(schema, table) and get_row_count(schema, table) > 0


# ── Tablas clave por etapa (lo que cada etapa produce) ──────────────────────
_RAW_TABLES = ["matriculados", "inscritos", "admitidos", "graduados", "docentes"]
_UNIFIED_TABLES = [
    "matriculados_unified",
    "inscritos_unified",
    "admitidos_unified",
    "graduados_unified",
]
_DIM_TABLES = ["dim_institucion", "dim_programa", "dim_tiempo", "dim_sexo"]
_FACT_TABLES = ["fact_matriculados", "fact_inscritos", "fact_admitidos"]


def step_validate_google_auth(**kwargs: Any) -> None:
    def _validate() -> None:
        token_path = SEMINARIO_ROOT / "token.json"
        if not token_path.exists():
            raise FileNotFoundError(
                f"Token no encontrado en {token_path}. "
                "Verifica que el volumen este montado en docker-compose.yaml."
            )

        from utils.google_auth import get_google_credentials

        logger.info("Validando token desde %s", token_path)
        creds = get_google_credentials()
        logger.info("Token valido. Scopes: %s", creds.scopes)

    _safe_execute("Validar Google Auth", _validate)


def step_ingest(**kwargs: Any) -> None:
    if not _force_reprocess(kwargs) and all(_has_data("raw", t) for t in _RAW_TABLES):
        logger.info("SKIP: Ingesta — tablas raw ya tienen datos")
        return

    def _ingest() -> None:
        from config.globals import MAX_SNIES_FILE_SIZE_MB
        from config.sources import PRIORITY_FILES
        from etl.ingest import ingest_all

        logger.info(
            "Iniciando ingesta (%d archivos, max SNIES: %.1f MB)",
            len(PRIORITY_FILES),
            MAX_SNIES_FILE_SIZE_MB,
        )
        ingest_all(PRIORITY_FILES, max_snies_size_mb=MAX_SNIES_FILE_SIZE_MB)
        logger.info("Ingesta completada.")

    _safe_execute("Ingesta desde Google Drive", _ingest)


def step_create_db(**kwargs: Any) -> None:
    if not _force_reprocess(kwargs) and all(_has_data("raw", t) for t in _RAW_TABLES):
        logger.info("SKIP: Crear DB — tablas raw ya tienen datos")
        return

    def _create_db() -> None:
        from scripts.create_db import main as create_db

        create_db()

    _safe_execute("Crear DB (raw schema)", _create_db)


def step_transform(**kwargs: Any) -> None:
    if not _force_reprocess(kwargs) and all(
        _has_data("unified", t) for t in _UNIFIED_TABLES
    ):
        logger.info("SKIP: Transformar — tablas unified ya tienen datos")
        return

    def _transform() -> None:
        from etl.transform import transform_all

        transform_all()

    _safe_execute("Transformar datos", _transform)


def step_normalize(**kwargs: Any) -> None:
    if not _force_reprocess(kwargs) and all(
        _has_data("unified", t) for t in _UNIFIED_TABLES
    ):
        logger.info("SKIP: Normalizar — tablas unified ya tienen datos")
        return

    def _normalize() -> None:
        from scripts.normalize_data import main as normalize_data

        normalize_data()

    _safe_execute("Normalizar datos", _normalize)


def step_create_indexes_raw(**kwargs: Any) -> None:
    def _create_indexes() -> None:
        from scripts.create_indexes import create_indexes

        create_indexes(target="raw")

    _safe_execute("Crear indices (raw)", _create_indexes)


def step_create_indexes_unified(**kwargs: Any) -> None:
    def _create_indexes() -> None:
        from scripts.create_indexes import create_indexes

        create_indexes(target="unified")

    _safe_execute("Crear indices (unified)", _create_indexes)


def step_unify_by_year(**kwargs: Any) -> None:
    if not _force_reprocess(kwargs) and all(
        _has_data("unified", t) for t in _UNIFIED_TABLES
    ):
        logger.info("SKIP: Unificar por año — tablas unified ya tienen datos")
        return

    def _unify() -> None:
        from scripts.unify_by_year import main as unify_all

        unify_all()

    _safe_execute("Unificar por ano", _unify)


def step_create_dimensions(**kwargs: Any) -> None:
    if not _force_reprocess(kwargs) and all(_has_data("facts", t) for t in _DIM_TABLES):
        logger.info("SKIP: Crear dimensiones — tablas dim ya tienen datos")
        return

    def _create_dims() -> None:
        from scripts.create_dimensions import main as create_all_dimensions

        create_all_dimensions()

    _safe_execute("Crear dimensiones", _create_dims)


def step_create_facts(**kwargs: Any) -> None:
    if not _force_reprocess(kwargs) and all(
        _has_data("facts", t) for t in _FACT_TABLES
    ):
        logger.info("SKIP: Crear hechos — tablas fact ya tienen datos")
        return

    def _create_facts() -> None:
        from scripts.create_facts import main as create_all_facts

        create_all_facts()

    _safe_execute("Crear hechos", _create_facts)


def step_upload(**kwargs: Any) -> None:
    def _upload() -> None:
        from etl.upload import upload_databases

        upload_databases()

    _safe_execute("Upload a Drive", _upload)


def step_dictionaries(**kwargs: Any) -> None:
    def _dictionaries() -> None:
        from etl.dictionary import generate_all_dictionaries

        generate_all_dictionaries()

    _safe_execute("Generar diccionarios", _dictionaries)


def step_quality(**kwargs: Any) -> None:
    def _quality() -> None:
        from etl.quality import run_quality_checks

        run_quality_checks()

    _safe_execute("Checks de calidad", _quality)


def step_check_db_ready(**kwargs: Any) -> bool:
    """Short-circuit: salta ingestion+staging+star_schema si el pipeline ya completó
    hasta las tablas de facts (schema 'facts').

    Checkpoints por etapa (de más avanzado a menos):
      - facts.fact_* con datos       → salta TODO (solo corre delivery)
      - facts.dim_* con datos        → salta ingestion+staging+star_schema hasta dims
      - unified.*_unified con datos  → salta ingestion+staging
      - raw.*_2018+ con datos        → salta ingestion

    Para forzar re-proceso completo, trigger con conf: {"force_reprocess": true}
    Para indicar desde qué etapa reanudar: {"resume_from": "star_schema"}
      Valores: "ingestion" | "staging" | "star_schema" | "delivery" (default: auto)
    """
    from utils.db import table_exists, get_row_count

    dag_run = kwargs.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    if conf.get("force_reprocess", False):
        logger.info("force_reprocess=true → pipeline completo")
        return True

    # ── Detectar etapa más avanzada completada ──────────────────────────────
    def _has_data(schema: str, table: str) -> bool:
        return table_exists(schema, table) and get_row_count(schema, table) > 0

    # Etapa 4: facts ya construidos → solo delivery
    FACT_TABLES = ["fact_matriculados", "fact_inscritos", "fact_admitidos"]
    if all(_has_data("facts", t) for t in FACT_TABLES):
        logger.info(
            "Checkpoint: facts completos → saltando ingestion+staging+star_schema"
        )
        return False  # short-circuit

    # Etapas 1-3 aún incompletas → correr pipeline
    DIM_TABLES = ["dim_institucion", "dim_programa", "dim_tiempo"]
    if all(_has_data("facts", t) for t in DIM_TABLES):
        logger.info(
            "Checkpoint: dimensiones listas pero facts vacíos → "
            "pipeline necesario desde star_schema.create_facts"
        )
        # No podemos saltar parcialmente con ShortCircuit; correr desde el inicio
        # es seguro porque create_dimensions hace TRUNCATE+INSERT idempotente
        return True

    UNIFIED_TABLES = ["matriculados_unified", "inscritos_unified", "admitidos_unified"]
    if all(_has_data("unified", t) for t in UNIFIED_TABLES):
        logger.info(
            "Checkpoint: unified listo, faltan dimensiones/facts → pipeline desde star_schema"
        )
        return True

    RAW_TABLES = ["matriculados_2018", "inscritos_2018", "admitidos_2018"]
    if all(_has_data("raw", t) for t in RAW_TABLES):
        logger.info(
            "Checkpoint: raw listo, falta unify+dims+facts → pipeline desde staging.transform"
        )
        return True

    logger.info("Sin datos previos → pipeline completo")
    return True


default_args: dict[str, Any] = {
    "owner": "seminario",
    "depends_on_past": False,
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "on_failure_callback": _on_failure_callback,
    "on_success_callback": _on_success_callback,
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="seminario_etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL completo - Seminario Ingenieria de Datos (PostgreSQL)",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(hours=4),
    tags=["seminario", "etl", "pipeline", "postgresql"],
    on_success_callback=_on_dag_success_callback,
    on_failure_callback=_on_failure_callback,
    sla_miss_callback=_sla_miss_callback,
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:
    t_check_db_ready = ShortCircuitOperator(
        task_id="check_db_ready",
        python_callable=step_check_db_ready,
        ignore_downstream_trigger_rules=False,  # solo salta las tareas en cadena directa
        execution_timeout=timedelta(minutes=5),
        retries=1,
    )

    with TaskGroup(group_id="ingestion") as tg_ingestion:
        t_validate_auth = PythonOperator(
            task_id="validate_google_auth",
            python_callable=step_validate_google_auth,
            retries=3,
            retry_delay=timedelta(minutes=1),
            execution_timeout=timedelta(minutes=5),
        )

        t_ingest = PythonOperator(
            task_id="ingest",
            python_callable=step_ingest,
            retries=3,
            retry_delay=timedelta(minutes=3),
            execution_timeout=timedelta(minutes=45),
            sla=timedelta(minutes=30),
        )

        t_validate_auth >> t_ingest

    with TaskGroup(group_id="staging") as tg_staging:
        t_create_db = PythonOperator(
            task_id="create_db",
            python_callable=step_create_db,
            retries=2,
            retry_delay=timedelta(minutes=2),
            execution_timeout=timedelta(minutes=30),
        )

        t_transform = PythonOperator(
            task_id="transform",
            python_callable=step_transform,
            retries=2,
            retry_delay=timedelta(minutes=2),
            execution_timeout=timedelta(minutes=30),
        )

        t_normalize = PythonOperator(
            task_id="normalize_data",
            python_callable=step_normalize,
            retries=2,
            retry_delay=timedelta(minutes=3),
            execution_timeout=timedelta(minutes=45),
        )

        t_create_indexes_raw = PythonOperator(
            task_id="create_indexes_raw",
            python_callable=step_create_indexes_raw,
            retries=2,
            retry_delay=timedelta(minutes=1),
            execution_timeout=timedelta(minutes=15),
        )

        t_create_db >> t_transform >> t_normalize >> t_create_indexes_raw

    with TaskGroup(group_id="star_schema") as tg_star_schema:
        t_unify = PythonOperator(
            task_id="unify_by_year",
            python_callable=step_unify_by_year,
            retries=2,
            retry_delay=timedelta(minutes=2),
            execution_timeout=timedelta(minutes=30),
        )

        t_create_indexes_unified = PythonOperator(
            task_id="create_indexes_unified",
            python_callable=step_create_indexes_unified,
            retries=2,
            retry_delay=timedelta(minutes=1),
            execution_timeout=timedelta(minutes=15),
        )

        t_dimensions = PythonOperator(
            task_id="create_dimensions",
            python_callable=step_create_dimensions,
            retries=2,
            retry_delay=timedelta(minutes=2),
            execution_timeout=timedelta(minutes=30),
            sla=timedelta(minutes=30),
        )

        t_facts = PythonOperator(
            task_id="create_facts",
            python_callable=step_create_facts,
            retries=2,
            retry_delay=timedelta(minutes=2),
            execution_timeout=timedelta(minutes=30),
            sla=timedelta(minutes=30),
        )

        t_unify >> t_create_indexes_unified >> t_dimensions >> t_facts

    with TaskGroup(group_id="delivery") as tg_delivery:
        t_quality = PythonOperator(
            task_id="quality_checks",
            python_callable=step_quality,
            retries=1,
            retry_delay=timedelta(minutes=1),
            execution_timeout=timedelta(minutes=15),
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        t_dictionaries = PythonOperator(
            task_id="dictionaries",
            python_callable=step_dictionaries,
            retries=1,
            retry_delay=timedelta(minutes=1),
            execution_timeout=timedelta(minutes=15),
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        t_upload = PythonOperator(
            task_id="upload_to_drive",
            python_callable=step_upload,
            retries=3,
            retry_delay=timedelta(minutes=3),
            execution_timeout=timedelta(minutes=30),
        )

        [t_quality, t_dictionaries] >> t_upload

    t_check_db_ready >> tg_ingestion
    tg_ingestion >> tg_staging >> tg_star_schema >> tg_delivery
