"""Screenshot automation.

Subcommands:
  dispatch  Read the control sheet, find jobs that should run now, create
            per-sheet subfolders in Drive, and emit a JSON matrix of jobs
            to GITHUB_OUTPUT.
  run       Process a single data sheet: visit each URL, screenshot
            viewport when the keyword is found, upload to the given Drive
            subfolder.
"""

import argparse
import asyncio
import io
import json
import os
import re
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from playwright.async_api import async_playwright

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive",
]

VIEWPORT = {"width": 1440, "height": 900}
NAV_TIMEOUT_MS = 45000
POST_LOAD_WAIT_MS = 2500
DEFAULT_RUN_HOURS = [9, 19]


def get_credentials():
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
    return Credentials.from_service_account_file(key_path, scopes=SCOPES)


def slugify(s, max_len=60):
    s = re.sub(r"[^\w\-]+", "_", (s or "").strip())[:max_len]
    return s.strip("_") or "x"


def extract_sheet_id(url_or_id):
    s = (url_or_id or "").strip()
    m = re.search(r"/d/([a-zA-Z0-9_-]+)", s)
    return m.group(1) if m else s


# ---------- Drive ----------

def find_or_create_subfolder(drive_service, parent_id, name):
    safe_name = name.replace("'", "\\'")
    q = (
        f"name = '{safe_name}' "
        f"and mimeType = 'application/vnd.google-apps.folder' "
        f"and '{parent_id}' in parents and trashed = false"
    )
    res = drive_service.files().list(
        q=q,
        fields="files(id,name)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    if res.get("files"):
        return res["files"][0]["id"]
    created = drive_service.files().create(
        body={
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        },
        fields="id",
        supportsAllDrives=True,
    ).execute()
    return created["id"]


def upload_to_drive(drive_service, folder_id, name, data):
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype="image/png", resumable=False)
    drive_service.files().create(
        body={"name": name, "parents": [folder_id]},
        media_body=media,
        fields="id",
        supportsAllDrives=True,
    ).execute()


# ---------- Sheets ----------

def read_data_sheet(sheets_service, sheet_id, range_="A:D"):
    res = sheets_service.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=range_
    ).execute()
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


def get_sheet_title(sheets_service, sheet_id):
    meta = sheets_service.spreadsheets().get(
        spreadsheetId=sheet_id, fields="properties.title"
    ).execute()
    return meta["properties"]["title"]


def read_control_sheet(sheets_service, control_id):
    """Return (jobs, run_hours)."""
    jobs_res = sheets_service.spreadsheets().values().get(
        spreadsheetId=control_id, range="Jobs!A:D"
    ).execute()
    jobs_rows = jobs_res.get("values", [])

    jobs = []
    for i, row in enumerate(jobs_rows[1:], start=2):
        row = (row + [""] * 4)[:4]
        sheet_url, start_str, stop_str, tz_str = (c.strip() for c in row)
        if not sheet_url:
            continue
        try:
            tz = ZoneInfo(tz_str) if tz_str else ZoneInfo("UTC")
            start = datetime.strptime(start_str, "%Y-%m-%d %H:%M").replace(tzinfo=tz)
            stop = datetime.strptime(stop_str, "%Y-%m-%d %H:%M").replace(tzinfo=tz)
        except Exception as e:
            print(f"  Jobs row {i}: bad date/timezone ({e})", file=sys.stderr, flush=True)
            continue
        jobs.append(
            {
                "row": i,
                "sheet_id": extract_sheet_id(sheet_url),
                "start": start,
                "stop": stop,
                "tz": tz,
                "tz_str": tz_str or "UTC",
            }
        )

    run_hours = list(DEFAULT_RUN_HOURS)
    try:
        s_res = sheets_service.spreadsheets().values().get(
            spreadsheetId=control_id, range="Settings!A:B"
        ).execute()
        for srow in s_res.get("values", [])[1:]:
            if len(srow) < 2:
                continue
            key = srow[0].strip().lower()
            val = srow[1].strip()
            if key == "run_hours" and val:
                try:
                    run_hours = [int(h) for h in val.split(",") if h.strip()]
                except ValueError as e:
                    print(f"  bad run_hours value '{val}': {e}", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"  could not read Settings tab (using defaults): {e}", file=sys.stderr, flush=True)

    return jobs, run_hours


def is_active_now(job, run_hours):
    now_in_tz = datetime.now(job["tz"])
    return job["start"] <= now_in_tz <= job["stop"] and now_in_tz.hour in run_hours


# ---------- dispatch ----------

def cmd_dispatch():
    output_path = os.environ.get("GITHUB_OUTPUT")
    active = []

    def write_outputs():
        payload = json.dumps(active)
        if output_path:
            with open(output_path, "a", encoding="utf-8") as f:
                f.write(f"matrix={payload}\n")
                f.write(f"has_jobs={'true' if active else 'false'}\n")
        else:
            print(payload)

    try:
        control_id = os.environ.get("CONTROL_SHEET_ID", "").strip()
        drive_folder_id = os.environ.get("DRIVE_FOLDER_ID", "").strip()
        if not control_id:
            raise RuntimeError("CONTROL_SHEET_ID env var is empty or missing")
        if not drive_folder_id:
            raise RuntimeError("DRIVE_FOLDER_ID env var is empty or missing")

        creds = get_credentials()
        sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)

        jobs, run_hours = read_control_sheet(sheets_service, control_id)
        print(f"Run hours: {run_hours}", flush=True)
        print(f"Jobs in control sheet: {len(jobs)}", flush=True)

        for job in jobs:
            now_in_tz = datetime.now(job["tz"])
            if not is_active_now(job, run_hours):
                print(
                    f"  skip row {job['row']} ({job['sheet_id'][:8]}) "
                    f"now={now_in_tz.strftime('%Y-%m-%d %H:%M')} {job['tz_str']}",
                    flush=True,
                )
                continue
            try:
                title = get_sheet_title(sheets_service, job["sheet_id"])
            except Exception as e:
                print(f"  row {job['row']}: cannot read sheet metadata ({e})", file=sys.stderr, flush=True)
                continue
            try:
                subfolder_id = find_or_create_subfolder(drive_service, drive_folder_id, title)
            except Exception as e:
                print(f"  row {job['row']}: cannot create subfolder for '{title}' ({e})", file=sys.stderr, flush=True)
                continue
            active.append(
                {"sheet_id": job["sheet_id"], "subfolder_id": subfolder_id, "sheet_name": title}
            )
            print(f"  ACTIVE: '{title}' -> subfolder {subfolder_id}", flush=True)

        print(f"Active jobs to dispatch: {len(active)}", flush=True)
    finally:
        write_outputs()


# ---------- run ----------

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
        pass

    png = await page.screenshot(full_page=False)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    name = (
        f"{ts}_{slugify(row['category'])}_"
        f"{slugify(row['country'])}_{slugify(keyword)}.png"
    )
    upload_to_drive(drive_service, drive_folder_id, name, png)
    print(f"  uploaded {name}", flush=True)
    return True


async def cmd_run(sheet_id, drive_folder_id, sheet_name):
    creds = get_credentials()
    sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)

    rows = read_data_sheet(sheets_service, sheet_id)
    label = sheet_name or sheet_id[:8]
    print(f"=== Sheet '{label}' ({len(rows)} rows) ===", flush=True)

    found = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport=VIEWPORT)
        page = await context.new_page()
        for row in rows:
            if await process_row(page, row, drive_service, drive_folder_id, label):
                found += 1
        await browser.close()
    print(f"Done: {found}/{len(rows)} screenshots uploaded.", flush=True)


# ---------- entry ----------

def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("dispatch", help="Read control sheet, output matrix")
    p_run = sub.add_parser("run", help="Process a single data sheet")
    p_run.add_argument("--sheet-id", required=True)
    p_run.add_argument("--drive-folder-id", required=True)
    p_run.add_argument("--sheet-name", default="")

    args = parser.parse_args()
    if args.cmd == "dispatch":
        cmd_dispatch()
    elif args.cmd == "run":
        asyncio.run(cmd_run(args.sheet_id, args.drive_folder_id, args.sheet_name))


if __name__ == "__main__":
    main()
