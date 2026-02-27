import os
from pathlib import Path
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
TOKEN_PATH = Path(__file__).parent / "token.json"
FOLDER_ID = os.getenv("FOLDER_ID", "1tcRwkHfaMpu2TUoNgU_GU8SwUGz9WBGK")

def get_credentials() -> Credentials:
    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if creds and creds.valid:
        return creds
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        TOKEN_PATH.write_text(creds.to_json())
    return creds


def list_folder(service, folder_id, indent=0):
    query = f"'{folder_id}' in parents and trashed = false"
    results = (
        service.files()
        .list(
            q=query,
            pageSize=100,
            fields="files(id, name, mimeType, size, modifiedTime)",
            orderBy="name",
        )
        .execute()
    )
    files = results.get("files", [])
    for f in files:
        prefix = "  " * indent
        size = f.get("size", "N/A")
        print(
            f"{prefix}- {f['name']} | type={f['mimeType']} | size={size} | id={f['id']}"
        )
        if f["mimeType"] == "application/vnd.google-apps.folder":
            list_folder(service, f["id"], indent + 1)


def main():
    creds = get_credentials()
    service = build("drive", "v3", credentials=creds)
    print(f"Listing files in folder: {FOLDER_ID}\n")
    list_folder(service, FOLDER_ID)


if __name__ == "__main__":
    main()
