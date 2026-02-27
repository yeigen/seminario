import subprocess
import sys
from pathlib import Path

from googleapiclient.http import MediaFileUpload

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import (
    SCOPES_READWRITE,
    DRIVE_UPLOAD_FOLDER_ID,
    PG_EXPORT_DIR,
    PG_EXPORT_FILES,
    PG_SCHEMAS,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)
from utils.google_auth import build_drive_service
from utils.logger import logger


def export_pg_schemas() -> list[Path]:
    PG_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    exported: list[Path] = []
    env = {
        "PGPASSWORD": POSTGRES_PASSWORD,
        "PATH": "/usr/bin:/usr/local/bin",
    }

    for schema, export_path in zip(PG_SCHEMAS, PG_EXPORT_FILES):
        logger.info("Exportando schema '%s' → %s", schema, export_path)
        try:
            with open(export_path, "wb") as dump_file:
                proc = subprocess.run(
                    [
                        "pg_dump",
                        "-h",
                        POSTGRES_HOST,
                        "-p",
                        str(POSTGRES_PORT),
                        "-U",
                        POSTGRES_USER,
                        "-d",
                        POSTGRES_DB,
                        "-n",
                        schema,
                        "--no-owner",
                        "--no-privileges",
                    ],
                    env=env,
                    stdout=dump_file,
                    stderr=subprocess.PIPE,
                    text=False,
                    timeout=300,
                )
            if proc.returncode != 0:
                logger.error(
                    "pg_dump falló para schema '%s': %s",
                    schema,
                    proc.stderr.decode(errors="replace"),
                )
                export_path.unlink(missing_ok=True)
                continue

            size_mb = export_path.stat().st_size / (1024 * 1024)
            logger.info("  OK: %s (%.1f MB)", export_path.name, size_mb)
            exported.append(export_path)

        except FileNotFoundError:
            logger.error(
                "pg_dump no encontrado. Instala postgresql-client o verifica PATH."
            )
            break
        except subprocess.TimeoutExpired:
            logger.error("pg_dump timeout para schema '%s'", schema)
            export_path.unlink(missing_ok=True)
        except Exception as e:
            logger.error("Error exportando schema '%s': %s", schema, e)
            export_path.unlink(missing_ok=True)

    return exported


def _build_drive_service():
    return build_drive_service(scopes=SCOPES_READWRITE)


def _find_existing_file(service, filename: str, folder_id: str) -> str | None:
    query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
    results = (
        service.files().list(q=query, fields="files(id, name)", pageSize=5).execute()
    )
    files = results.get("files", [])
    return files[0]["id"] if files else None


def _upload_file(service, local_path: Path, folder_id: str) -> dict:
    filename = local_path.name
    media = MediaFileUpload(str(local_path), mimetype="application/sql", resumable=True)
    existing_id = _find_existing_file(service, filename, folder_id)

    if existing_id:
        logger.info("  [UPDATE] %s (id=%s)", filename, existing_id)
        return (
            service.files()
            .update(
                fileId=existing_id,
                body={"name": filename},
                media_body=media,
                fields="id, name, size, modifiedTime",
            )
            .execute()
        )

    logger.info("  [CREATE] %s", filename)
    return (
        service.files()
        .create(
            body={"name": filename, "parents": [folder_id]},
            media_body=media,
            fields="id, name, size, modifiedTime",
        )
        .execute()
    )


def upload_databases() -> list[dict]:
    logger.info("=" * 60)
    logger.info("Export + Upload de bases de datos a Google Drive")
    logger.info("Carpeta destino: %s", DRIVE_UPLOAD_FOLDER_ID)
    logger.info("=" * 60)

    logger.info("[1/2] Exportando schemas PostgreSQL...")
    exported_files = export_pg_schemas()

    if not exported_files:
        logger.error("No se generaron archivos de exportación")
        return []

    logger.info("[2/2] Subiendo %d archivos a Google Drive...", len(exported_files))
    for p in exported_files:
        logger.info("  %s (%.1f MB)", p.name, p.stat().st_size / (1024 * 1024))

    service = _build_drive_service()

    results: list[dict] = []
    for local_path in exported_files:
        try:
            result = _upload_file(service, local_path, DRIVE_UPLOAD_FOLDER_ID)
            size_mb = int(result.get("size", 0)) / (1024 * 1024)
            logger.info(
                "  OK: %s (%.1f MB, id=%s)",
                result["name"],
                size_mb,
                result["id"],
            )
            results.append(result)
        except Exception as e:
            logger.error("  ERROR subiendo %s: %s", local_path.name, e)

    logger.info("=" * 60)
    logger.info("Upload completado: %d/%d archivos", len(results), len(exported_files))
    logger.info("=" * 60)

    return results


if __name__ == "__main__":
    upload_databases()
