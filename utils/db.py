import threading
import time
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.sql import SQL, Identifier
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from config.globals import (
    DATABASE_URL,
    PG_SCHEMAS,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from utils.logger import logger

_engine_cache: dict[str, Engine] = {}
_engine_lock = threading.Lock()

_MAX_RETRIES: int = 3
_RETRY_BASE_DELAY_S: float = 1.0

_TCP_KEEPALIVE_OPTS = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}

def get_connection(
    schema: str | None = None,
    autocommit: bool = False,
) -> PgConnection:
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=int(POSTGRES_PORT),
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=10,
        **_TCP_KEEPALIVE_OPTS,
    )
    if autocommit:
        conn.autocommit = True
    if schema:
        with conn.cursor() as cur:
            cur.execute("SET search_path TO %s, public", (schema,))
    return conn

def _get_connection_with_retry(
    schema: str | None = None,
    autocommit: bool = False,
    max_retries: int = _MAX_RETRIES,
) -> PgConnection:
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            return get_connection(schema=schema, autocommit=autocommit)
        except psycopg2.OperationalError as exc:
            last_error = exc
            if attempt < max_retries:
                delay = _RETRY_BASE_DELAY_S * (2 ** (attempt - 1))
                logger.warning(
                    "Connection failed (attempt %d/%d): %s — retrying in %.1fs",
                    attempt,
                    max_retries,
                    exc,
                    delay,
                )
                time.sleep(delay)
            else:
                logger.error(
                    "Connection failed after %d attempts: %s",
                    max_retries,
                    exc,
                )
    raise last_error

@contextmanager
def managed_connection(
    schema: str | None = None,
    autocommit: bool = False,
) -> Generator[PgConnection, None, None]:
    conn = _get_connection_with_retry(schema=schema, autocommit=autocommit)
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()

def get_engine(schema: str | None = None) -> Engine:
    cache_key = schema or "__default__"

    with _engine_lock:
        if cache_key in _engine_cache:
            return _engine_cache[cache_key]

    connect_args: dict = {**_TCP_KEEPALIVE_OPTS}
    if schema:
        connect_args["options"] = f"-c search_path={schema},public"

    engine = create_engine(
        DATABASE_URL,
        connect_args=connect_args,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_timeout=30,
    )

    with _engine_lock:
        if cache_key in _engine_cache:
            engine.dispose()
            return _engine_cache[cache_key]
        _engine_cache[cache_key] = engine

    return engine

def ensure_schemas() -> None:
    with managed_connection(autocommit=True) as conn:
        with conn.cursor() as cur:
            for schema in PG_SCHEMAS:
                cur.execute(
                    SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(schema))
                )
                logger.info("Schema '%s' verified/created", schema)

def list_tables(schema: str) -> list[str]:
    with managed_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
                "ORDER BY table_name",
                (schema,),
            )
            return [row[0] for row in cur.fetchall()]

def get_columns(schema: str, table: str) -> list[dict]:
    with managed_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type, ordinal_position "
                "FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position",
                (schema, table),
            )
            return [
                {
                    "column_name": row[0],
                    "data_type": row[1],
                    "ordinal_position": row[2],
                }
                for row in cur.fetchall()
            ]

def get_column_names(schema: str, table: str) -> list[str]:
    return [col["column_name"] for col in get_columns(schema, table)]

def get_row_count(schema: str, table: str) -> int:
    with managed_connection(schema=schema) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL("SELECT COUNT(*) FROM {}").format(Identifier(table)))
            return cur.fetchone()[0]

def table_exists(schema: str, table: str) -> bool:
    with managed_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.tables "
                "  WHERE table_schema = %s AND table_name = %s"
                ")",
                (schema, table),
            )
            return cur.fetchone()[0]

def dispose_engines() -> None:
    with _engine_lock:
        for key, engine in _engine_cache.items():
            try:
                engine.dispose()
            except Exception as exc:
                logger.warning("Failed to dispose engine '%s': %s", key, exc)
        _engine_cache.clear()
    logger.debug("All SQLAlchemy engines disposed")
