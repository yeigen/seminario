"""
DAG de Airflow para el Pipeline ETL del Seminario de Ingeniería de Datos.

Este DAG ejecuta las siguientes etapas:
1. Ingest - Descarga datos desde Google Drive
2. Transform - Limpia y normaliza datos, convierte a Parquet
3. Dictionary - Genera diccionario de datos
4. Quality - Ejecuta verificaciones de calidad

Uso:
    Place el archivo en $AIRFLOW_HOME/dags/
    o configura airflow.cfg:dags_folder para incluir esta ruta.
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from config.globals import MAX_SNIES_FILE_SIZE_MB
from config.sources import PRIORITY_FILES
from etl.ingest import ingest_all
from etl.transform import transform_all
from etl.dictionary import generate_all_dictionaries
from etl.quality import run_quality_checks


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "catchup": False,
}


with DAG(
    dag_id="seminario_etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL completo para el Seminario de Ingeniería de Datos",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "seminario", "google-drive"],
) as dag:

    def task_ingest(**context):
        execution_date = context.get("execution_date")
        print("=" * 60)
        print(f"  INGESTIÓN - Fecha de ejecución: {execution_date}")
        print("=" * 60)
        ingest_all(PRIORITY_FILES, max_snies_size_mb=MAX_SNIES_FILE_SIZE_MB)
        return "Ingest completed successfully"

    def task_transform(**context):
        execution_date = context.get("execution_date")
        print("=" * 60)
        print(f"  TRANSFORMACIÓN - Fecha de ejecución: {execution_date}")
        print("=" * 60)
        transform_all()
        return "Transform completed successfully"

    def task_generate_dictionary(**context):
        execution_date = context.get("execution_date")
        print("=" * 60)
        print(f"  GENERANDO DICCIONARIO - Fecha de ejecución: {execution_date}")
        print("=" * 60)
        generate_all_dictionaries()
        return "Dictionary generation completed successfully"

    def task_quality_checks(**context):
        execution_date = context.get("execution_date")
        print("=" * 60)
        print(f"  VERIFICACIONES DE CALIDAD - Fecha de ejecución: {execution_date}")
        print("=" * 60)
        run_quality_checks()
        return "Quality checks completed successfully"

    with TaskGroup("etl_tasks") as etl_group:
        ingest_task = PythonOperator(
            task_id="ingest_data",
            python_callable=task_ingest,
            provide_context=True,
        )

        transform_task = PythonOperator(
            task_id="transform_data",
            python_callable=task_transform,
            provide_context=True,
        )

        dictionary_task = PythonOperator(
            task_id="generate_dictionary",
            python_callable=task_generate_dictionary,
            provide_context=True,
        )

        quality_task = PythonOperator(
            task_id="quality_checks",
            python_callable=task_quality_checks,
            provide_context=True,
        )

        ingest_task >> transform_task >> dictionary_task >> quality_task
