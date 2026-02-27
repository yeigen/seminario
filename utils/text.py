import re

import pandas as pd

_ACCENT_MAP = str.maketrans(
    "áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜñÑ",
    "aeiouAEIOUaeiouAEIOUaeiouAEIOUnN",
)

_MULTI_SPACE_RE = re.compile(r"\s{2,}")

_COL_ACCENT_TABLE = str.maketrans(
    "áàäâéèëêíìïîóòöôúùüûñ",
    "aaaaeeeeiiiioooouuuun",
)


def remove_accents(text: str) -> str:
    return text.translate(_ACCENT_MAP)


def normalize_text(text: str) -> str | None:
    text = text.strip()
    if not text:
        return None
    text = text.lower()
    text = remove_accents(text)
    text = _MULTI_SPACE_RE.sub(" ", text)
    return text


def normalize_column_name(col: str) -> str:
    col = col.strip().lower()
    col = col.translate(_COL_ACCENT_TABLE)
    col = re.sub(r"[\s\-\.]+", "_", col)
    col = re.sub(r"[^a-z0-9_]", "", col)
    return col


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = pd.Index([normalize_column_name(str(c)) for c in df.columns])
    return df


_PG_NORMALIZE_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION pg_normalize_text(input_text TEXT)
RETURNS TEXT AS $$
BEGIN
    IF input_text IS NULL THEN
        RETURN NULL;
    END IF;
    input_text := TRIM(input_text);
    IF input_text = '' THEN
        RETURN NULL;
    END IF;
    input_text := LOWER(input_text);
    input_text := TRANSLATE(
        input_text,
        'áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜñÑ',
        'aeiouAEIOUaeiouAEIOUaeiouAEIOUnN'
    );
    input_text := REGEXP_REPLACE(input_text, '\\s{2,}', ' ', 'g');
    RETURN input_text;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
"""


def create_pg_normalize_function(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_PG_NORMALIZE_FUNCTION_SQL)
    conn.commit()
