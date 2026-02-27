from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request as URLRequest
from urllib.request import urlopen

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from config.globals import (
    CLIENT_ID,
    CLIENT_SECRET,
    DRIVE_API_SERVICE,
    DRIVE_API_VERSION,
    GOOGLE_TOKEN_URI,
    SCOPES,
    SCOPES_READWRITE,
    TOKEN_PATH,
    build_oauth_client_config,
)
from utils.logger import logger

_REFRESH_MAX_RETRIES: int = 3
_REFRESH_BASE_DELAY_S: float = 2.0
_REFRESH_TIMEOUT_S: int = 30

def _refresh_token_via_http(
    refresh_token: str,
    client_id: str,
    client_secret: str,
    token_uri: str = GOOGLE_TOKEN_URI,
) -> dict:
    payload = urlencode(
        {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        }
    ).encode("utf-8")

    last_error: Exception | None = None

    for attempt in range(_REFRESH_MAX_RETRIES):
        try:
            req = URLRequest(
                token_uri,
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            )
            with urlopen(req, timeout=_REFRESH_TIMEOUT_S) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            if "error" in data:
                raise RuntimeError(
                    f"Google OAuth error: {data['error']} — "
                    f"{data.get('error_description', 'no details')}"
                )

            logger.info(
                "Token refreshed via HTTP (attempt %d/%d), expires in %ds",
                attempt + 1,
                _REFRESH_MAX_RETRIES,
                data.get("expires_in", 0),
            )
            return data

        except (URLError, OSError, TimeoutError) as exc:
            last_error = exc
            delay = _REFRESH_BASE_DELAY_S * (2**attempt)
            logger.warning(
                "HTTP refresh attempt %d/%d failed: %s — retrying in %.1fs",
                attempt + 1,
                _REFRESH_MAX_RETRIES,
                exc,
                delay,
            )
            time.sleep(delay)

    raise RuntimeError(
        f"Token refresh failed after {_REFRESH_MAX_RETRIES} attempts. "
        f"Last error: {last_error}"
    )

def _build_credentials_from_refresh(
    token_data: dict,
    original_creds_info: dict,
    scopes: list[str],
) -> Credentials:
    expires_in = token_data.get("expires_in", 3600)
    expiry = datetime.fromtimestamp(time.time() + expires_in, tz=timezone.utc)

    new_refresh = token_data.get(
        "refresh_token",
        original_creds_info.get("refresh_token"),
    )

    return Credentials(
        token=token_data["access_token"],
        refresh_token=new_refresh,
        token_uri=original_creds_info.get("token_uri", GOOGLE_TOKEN_URI),
        client_id=original_creds_info.get("client_id", CLIENT_ID),
        client_secret=original_creds_info.get("client_secret", CLIENT_SECRET),
        scopes=scopes,
        expiry=expiry,
    )

def _save_token(creds: Credentials) -> None:
    try:
        TOKEN_PATH.write_text(creds.to_json())
        logger.info("Token saved to %s", TOKEN_PATH)
    except OSError as exc:
        logger.debug("Could not persist token (read-only FS?): %s", exc)

def _load_token(scopes: list[str]) -> tuple[Credentials | None, dict]:
    if not TOKEN_PATH.exists():
        return None, {}

    creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), scopes)

    try:
        creds_info = json.loads(TOKEN_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        creds_info = {}

    return creds, creds_info

def _try_google_auth_refresh(creds: Credentials) -> bool:
    try:
        logger.info("Token expired, refreshing via google-auth...")
        creds.refresh(Request())
        return True
    except Exception as exc:
        logger.warning(
            "google-auth refresh failed: %s — falling back to HTTP refresh",
            exc,
        )
        return False

def _try_http_refresh(
    creds: Credentials | None,
    creds_info: dict,
    scopes: list[str],
) -> Credentials | None:
    refresh_token = (creds.refresh_token if creds else None) or creds_info.get(
        "refresh_token"
    )
    token_client_id = creds_info.get("client_id") or CLIENT_ID
    token_client_secret = creds_info.get("client_secret") or CLIENT_SECRET

    if not (refresh_token and token_client_id and token_client_secret):
        logger.warning(
            "HTTP refresh skipped: missing refresh_token=%s, "
            "client_id=%s, client_secret=%s",
            bool(refresh_token),
            bool(token_client_id),
            bool(token_client_secret),
        )
        return None

    try:
        logger.info("Attempting manual HTTP token refresh...")
        token_data = _refresh_token_via_http(
            refresh_token=refresh_token,
            client_id=token_client_id,
            client_secret=token_client_secret,
            token_uri=creds_info.get("token_uri", GOOGLE_TOKEN_URI),
        )
        return _build_credentials_from_refresh(token_data, creds_info, scopes)
    except RuntimeError as exc:
        logger.error("HTTP token refresh failed: %s", exc)
        return None

def get_google_credentials(
    scopes: list[str] | None = None,
) -> Credentials:
    if scopes is None:
        scopes = SCOPES

    in_container = bool(os.getenv("SEMINARIO_PROJECT_ROOT"))

    creds, creds_info = _load_token(scopes)

    if creds and creds.valid:
        logger.debug("Token valid (expires: %s)", creds.expiry)
        return creds

    if creds and creds.expired and creds.refresh_token:
        if _try_google_auth_refresh(creds):
            _save_token(creds)
            return creds

    refreshed = _try_http_refresh(creds, creds_info, scopes)
    if refreshed:
        _save_token(refreshed)
        return refreshed

    if in_container:
        refresh_token = (creds.refresh_token if creds else None) or creds_info.get(
            "refresh_token"
        )
        raise RuntimeError(
            "Google Drive token invalid or expired inside container.\n"
            "The container cannot reach oauth2.googleapis.com to refresh.\n\n"
            "Fix:\n"
            "  1. On your local machine run: python reauth.py\n"
            "  2. Restart containers: docker compose -f airflow/docker-compose.yaml restart\n\n"
            "Diagnostics:\n"
            f"  - Token path: {TOKEN_PATH}\n"
            f"  - Token exists: {TOKEN_PATH.exists()}\n"
            f"  - Refresh token present: {bool(refresh_token)}\n"
            f"  - Client ID present: {bool(creds_info.get('client_id') or CLIENT_ID)}\n"
        )

    logger.info("Starting interactive OAuth2 flow (local browser)...")
    client_config = build_oauth_client_config()
    flow = InstalledAppFlow.from_client_config(client_config, scopes)
    interactive_creds: Credentials = flow.run_local_server(port=0)
    _save_token(interactive_creds)
    return interactive_creds

def build_drive_service(scopes: list[str] | None = None):
    creds = get_google_credentials(scopes=scopes)
    return build(DRIVE_API_SERVICE, DRIVE_API_VERSION, credentials=creds)
