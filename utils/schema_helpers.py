import pandas as pd

from config.globals import PG_SCHEMA_UNIFIED
from utils.db import table_exists as db_table_exists


def safe_int(value: object) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip().replace("\ufeff", "")
    if not text or text.lower() in ("nan", "none", ""):
        return None
    try:
        return int(float(text))
    except (ValueError, TypeError, OverflowError):
        return None


def safe_str(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() in ("nan", "none"):
        return None
    return text


def unified_table_exists(table_name: str) -> bool:
    return db_table_exists(PG_SCHEMA_UNIFIED, table_name)


def drop_if_exists(cur, table_name: str) -> None:
    cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
