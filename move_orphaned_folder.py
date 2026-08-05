"""One-off maintenance script: reparent folders that landed outside the
correct "Screenshot Output" folder back into it.

Run once via the oneoff-move-folder workflow, then delete both this file
and that workflow.
"""
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/drive"]

NEW_PARENT = "1Ly3VOtIZDex4djWlU0lh3aP1n4vuE_ao"  # Screenshot Output

TARGETS = [
    "1-hv-SjEs4vPOyUP7LDqVP3tyX4eSn4Qp",  # Elizabeth Elmer - Empowering Learners Through Language Coaching
    "1FIi62P22_ynh6otG46lto32wpaWtoWnx",  # ... filtered
]


def main():
    creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)

    for file_id in TARGETS:
        meta = drive.files().get(
            fileId=file_id, fields="id,name,parents", supportsAllDrives=True
        ).execute()
        old_parents = ",".join(meta.get("parents", []))
        print(f"{meta['name']} ({file_id}): current parents = {old_parents or '(none)'}", flush=True)

        updated = drive.files().update(
            fileId=file_id,
            addParents=NEW_PARENT,
            removeParents=old_parents,
            fields="id,parents",
            supportsAllDrives=True,
        ).execute()
        print(f"  -> new parents = {updated.get('parents')}", flush=True)


if __name__ == "__main__":
    main()
