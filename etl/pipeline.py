import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from config.globals import MAX_SNIES_FILE_SIZE_MB, PG_SCHEMA_RAW
from config.sources import PRIORITY_FILES
from etl.ingest import ingest_all
from etl.transform import transform_all
from etl.upload import upload_databases
from etl.dictionary import generate_all_dictionaries
from etl.quality import run_quality_checks
from scripts.create_db import main as create_db
from scripts.create_indexes import create_indexes
from scripts.normalize_data import main as normalize_data
from scripts.unify_by_year import main as unify_all
from scripts.create_dimensions import main as create_all_dimensions
from scripts.create_facts import main as create_all_facts

from utils.db import list_tables
from utils.logger import logger

TOTAL_STEPS = 11


@dataclass
class StepResult:
    name: str
    status: str = "PENDING"
    duration_s: float = 0.0
    error: str = ""


@dataclass
class PipelineReport:
    steps: list[StepResult] = field(default_factory=list)
    total_duration_s: float = 0.0

    @property
    def succeeded(self) -> list[StepResult]:
        return [s for s in self.steps if s.status == "OK"]

    @property
    def failed(self) -> list[StepResult]:
        return [s for s in self.steps if s.status == "FAIL"]

    @property
    def skipped(self) -> list[StepResult]:
        return [s for s in self.steps if s.status == "SKIP"]


def _run_step(
    step_num: int,
    name: str,
    func,
    report: PipelineReport,
    abort_on_failure: bool = True,
) -> bool:
    logger.info("=== PASO %d/%d: %s ===", step_num, TOTAL_STEPS, name.upper())
    result = StepResult(name=name)
    start = time.time()
    try:
        func()
        result.duration_s = time.time() - start
        result.status = "OK"
        logger.info("  Paso %d completado en %.1f s", step_num, result.duration_s)
    except Exception as exc:
        result.duration_s = time.time() - start
        result.status = "FAIL"
        result.error = str(exc)
        logger.error(
            "  Paso %d FALLO tras %.1f s: %s",
            step_num,
            result.duration_s,
            exc,
        )
        if abort_on_failure:
            report.steps.append(result)
            raise
    finally:
        report.steps.append(result)
    logger.info("")
    return result.status == "OK"


def _skip_step(step_num: int, name: str, reason: str, report: PipelineReport):
    logger.info(
        "=== PASO %d/%d: %s OMITIDO (%s) ===",
        step_num,
        TOTAL_STEPS,
        name.upper(),
        reason,
    )
    report.steps.append(StepResult(name=name, status="SKIP"))
    logger.info("")


def _log_summary(report: PipelineReport):
    logger.info("=" * 60)
    logger.info("  RESUMEN DEL PIPELINE")
    logger.info("=" * 60)

    for step in report.steps:
        if step.status == "OK":
            icon = "OK"
            detail = f"{step.duration_s:.1f} s"
        elif step.status == "FAIL":
            icon = "FAIL"
            detail = f"{step.duration_s:.1f} s — {step.error}"
        else:
            icon = "SKIP"
            detail = "omitido"
        logger.info("  [%s] %s (%s)", icon, step.name, detail)

    logger.info("-" * 60)
    logger.info(
        "  Exitosos: %d | Fallidos: %d | Omitidos: %d",
        len(report.succeeded),
        len(report.failed),
        len(report.skipped),
    )
    logger.info("  Duracion total: %.1f s", report.total_duration_s)
    logger.info("=" * 60)


def run_pipeline(skip_ingest: bool = False, skip_upload: bool = False):
    logger.info("=" * 60)
    logger.info("  PIPELINE ETL - SEMINARIO INGENIERIA DE DATOS")
    logger.info("=" * 60)
    logger.info("")

    report = PipelineReport()
    pipeline_start = time.time()

    try:
        if not skip_ingest:
            _run_step(
                1,
                "Ingesta de datos",
                lambda: ingest_all(
                    PRIORITY_FILES,
                    max_snies_size_mb=MAX_SNIES_FILE_SIZE_MB,
                ),
                report,
            )
        else:
            _skip_step(1, "Ingesta de datos", "--skip-ingest", report)

        def _create_db_step():
            create_db()
            tables = list_tables(PG_SCHEMA_RAW)
            logger.info(
                "  Schema '%s': %d tablas creadas",
                PG_SCHEMA_RAW,
                len(tables),
            )

        _run_step(2, "Creando base de datos PostgreSQL", _create_db_step, report)
        _run_step(3, "Transformacion y limpieza", transform_all, report)
        _run_step(4, "Normalizacion de datos", normalize_data, report)
        _run_step(
            5,
            "Creacion de indices (raw)",
            lambda: create_indexes(target="raw"),
            report,
        )
        _run_step(6, "Unificacion por año", unify_all, report)
        _run_step(7, "Creacion de dimensiones", create_all_dimensions, report)
        _run_step(8, "Creacion de tablas de hechos", create_all_facts, report)

        _run_step(
            9,
            "Verificacion de calidad",
            run_quality_checks,
            report,
            abort_on_failure=False,
        )
        _run_step(
            10,
            "Diccionarios de datos",
            generate_all_dictionaries,
            report,
            abort_on_failure=False,
        )

        if not skip_upload:
            _run_step(
                11,
                "Export + subida a Google Drive",
                upload_databases,
                report,
            )
        else:
            _skip_step(11, "Export + subida a Google Drive", "--skip-upload", report)

    except Exception:
        logger.error("Pipeline abortado por fallo critico.")

    report.total_duration_s = time.time() - pipeline_start
    _log_summary(report)

    if report.failed:
        logger.warning(
            "Pipeline finalizado con %d paso(s) fallido(s).",
            len(report.failed),
        )


if __name__ == "__main__":
    skip_ingest = "--skip-ingest" in sys.argv
    skip_upload = "--skip-upload" in sys.argv
    run_pipeline(skip_ingest=skip_ingest, skip_upload=skip_upload)
