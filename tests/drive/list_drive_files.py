from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from config.globals import (
    SCOPES_METADATA,
    TOKEN_PATH,
    FOLDER_ID,
    DRIVE_API_SERVICE,
    DRIVE_API_VERSION,
    DRIVE_LIST_PAGE_SIZE,
    GOOGLE_FOLDER_MIME_TYPE,
)


def get_credentials() -> Credentials:
    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES_METADATA)
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
            pageSize=DRIVE_LIST_PAGE_SIZE,
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
        if f["mimeType"] == GOOGLE_FOLDER_MIME_TYPE:
            list_folder(service, f["id"], indent + 1)


def main():
    creds = get_credentials()
    service = build(DRIVE_API_SERVICE, DRIVE_API_VERSION, credentials=creds)
    print(f"Listing files in folder: {FOLDER_ID}\n")
    list_folder(service, FOLDER_ID)


if __name__ == "__main__":
    main()
