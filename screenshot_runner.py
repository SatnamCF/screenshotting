import argparse
import asyncio
import io
import os
import re
import sys
from datetime import datetime, timezone

import yaml
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from playwright.async_api import async_playwright

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.file",
]

VIEWPORT = {"width": 1440, "height": 900}
NAV_TIMEOUT_MS = 45000
POST_LOAD_WAIT_MS = 2500


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_credentials():
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
    return Credentials.from_service_account_file(key_path, scopes=SCOPES)


def read_sheet(sheets_service, sheet_id, range_="A:D"):
    res = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=sheet_id, range=range_)
        .execute()
    )
    rows = res.get("values", [])
    out = []
    for row in rows[1:]:  # skip header
        row = (row + [""] * 4)[:4]
        category, url, country, keyword = (c.strip() for c in row)
        if url and keyword:
            out.append(
                {"category": category, "url": url, "country": country, "keyword": keyword}
            )
    return out


def slugify(s, max_len=40):
    s = re.sub(r"[^\w\-]+", "_", (s or "").strip())[:max_len]
    return s.strip("_") or "x"


def upload_to_drive(drive_service, folder_id, name, data):
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype="image/png", resumable=False)
    drive_service.files().create(
        body={"name": name, "parents": [folder_id]},
        media_body=media,
        fields="id",
        supportsAllDrives=True,
    ).execute()


async def process_row(page, row, drive_service, drive_folder_id, sheet_label):
    url = row["url"]
    keyword = row["keyword"]
    print(f"[{sheet_label}] {url} :: '{keyword}'", flush=True)

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        await page.wait_for_timeout(POST_LOAD_WAIT_MS)
    except Exception as e:
        print(f"  load failed: {e}", flush=True)
        return False

    try:
        body_text = await page.evaluate(
            "() => document.body ? document.body.innerText : ''"
        )
    except Exception as e:
        print(f"  text read failed: {e}", flush=True)
        return False

    if keyword.lower() not in body_text.lower():
        print("  keyword not found", flush=True)
        return False

    try:
        await page.get_by_text(keyword, exact=False).first.scroll_into_view_if_needed(
            timeout=4000
        )
        await page.wait_for_timeout(400)
    except Exception:
        pass  # screenshot top of page if scroll fails

    png = await page.screenshot(full_page=False)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    name = (
        f"{ts}_{slugify(sheet_label)}_{slugify(row['category'])}_"
        f"{slugify(row['country'])}_{slugify(keyword)}.png"
    )
    upload_to_drive(drive_service, drive_folder_id, name, png)
    print(f"  uploaded {name}", flush=True)
    return True


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument(
        "--sheets",
        help="Comma-separated sheet IDs to run instead of those in the config",
    )
    parser.add_argument(
        "--drive-folder-id",
        help="Drive folder ID (overrides config.yaml)",
    )
    args = parser.parse_args()

    sheets_arg = args.sheets or os.environ.get("SHEET_IDS", "").strip()
    drive_folder_arg = args.drive_folder_id or os.environ.get("DRIVE_FOLDER_ID", "").strip()

    cfg = None
    if not drive_folder_arg or not sheets_arg:
        if os.path.exists(args.config):
            cfg = load_config(args.config)

    drive_folder_id = drive_folder_arg or (cfg or {}).get("drive_folder_id")
    if not drive_folder_id:
        print("Drive folder ID not provided (set DRIVE_FOLDER_ID, --drive-folder-id, or config.yaml).", file=sys.stderr)
        sys.exit(1)

    if sheets_arg:
        ids = [s.strip() for s in sheets_arg.split(",") if s.strip()]
        sheets_to_run = [{"id": sid, "name": sid[:8]} for sid in ids]
    else:
        sheets_to_run = (cfg or {}).get("sheets", [])

    if not sheets_to_run:
        print("No sheets configured (set SHEET_IDS, --sheets, or config.yaml).", file=sys.stderr)
        sys.exit(1)

    creds = get_credentials()
    sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)

    total_found = 0
    total_rows = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport=VIEWPORT)
        page = await context.new_page()

        for sheet in sheets_to_run:
            label = sheet.get("name") or sheet["id"][:8]
            try:
                rows = read_sheet(sheets_service, sheet["id"])
            except Exception as e:
                print(f"=== Sheet '{label}' read failed: {e} ===", flush=True)
                continue
            print(f"=== Sheet '{label}' ({len(rows)} rows) ===", flush=True)
            for row in rows:
                total_rows += 1
                if await process_row(page, row, drive_service, drive_folder_id, label):
                    total_found += 1

        await browser.close()

    print(f"Done: {total_found}/{total_rows} screenshots uploaded.", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
