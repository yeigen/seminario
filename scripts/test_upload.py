import sys
from pathlib import Path

import google.auth
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.globals import (
    SCOPES_READWRITE,
    TOKEN_PATH,
    DRIVE_UPLOAD_FOLDER_ID,
)

SCOPES = SCOPES_READWRITE


def get_credentials():
    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(TOKEN_PATH, "w") as token:
                token.write(creds.to_json())
        else:
            print("No hay token válido. Ejecutá reauth.py primero.")
            sys.exit(1)
    return creds


def upload_file(file_path: Path, folder_id: str = None):
    creds = get_credentials()
    service = build("drive", "v3", credentials=creds)

    file_metadata = {
        "name": file_path.name,
    }
    if folder_id:
        file_metadata["parents"] = [folder_id]

    media = MediaFileUpload(file_path, resumable=True)

    file = (
        service.files()
        .create(
            body=file_metadata,
            media_body=media,
            fields="id, name, webViewLink",
        )
        .execute()
    )

    print(f"✓ Subido: {file['name']} (ID: {file['id']})")
    print(f"  Link: {file.get('webViewLink', 'N/A')}")
    return file


if __name__ == "__main__":
    test_file = Path(__file__).resolve().parents[1] / "files" / "file-to-drive.txt"
    print(f"Subiendo {test_file} a Drive...")
    upload_file(test_file, DRIVE_UPLOAD_FOLDER_ID)
