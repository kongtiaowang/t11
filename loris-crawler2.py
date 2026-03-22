#!/usr/bin/env python3

import os
import csv
import argparse
import subprocess
from pathlib import Path
import requests
import getpass
import sys

# command example:  python3 loris-crawler.py --dataset ./test --api-base https://phantom-dev.loris.ca/api/v0.0.3 --get

# =========================
# 0. get params
# =========================
parser = argparse.ArgumentParser(
    description="Loris → BIDS → DataLad ingest (auto from API, incremental, multi-project)"
)
parser.add_argument("--dataset", required=True, help="Path to DataLad dataset")
parser.add_argument(
    "--api-base",
    required=True,
    help="Phantom API base, e.g. https://phantom.loris.ca/api/v0.0.3-dev",
)
parser.add_argument("--get", action="store_true", help="Download files after addurl")
args = parser.parse_args()

DATASET_DIR = Path(args.dataset).expanduser().resolve()
MANIFEST_OUT = DATASET_DIR / "images_manifest.csv"
API_BASE = args.api_base.rstrip("/")

# =========================
# 1. Login (username/password → token)
# =========================
USERNAME = os.environ.get("LORIS_USERNAME")
PASSWORD = os.environ.get("LORIS_PASSWORD")
if not USERNAME:
    USERNAME = input("Loris username: ")
if not PASSWORD:
    PASSWORD = getpass.getpass("Loris password: ")

print(" Logging in to Loris API...")
try:
    login_resp = requests.post(
        f"{API_BASE}/login",
        json={"username": USERNAME, "password": PASSWORD},
    )
    if login_resp.status_code == 200:
        login_json = login_resp.json()
        TOKEN = login_json.get("token")
        if not TOKEN:
            raise RuntimeError("Login succeeded but no token returned")
        print("Login successful")
    elif login_resp.status_code == 409:
        msg = login_resp.json().get("message", "").lower()
        if "password expired" in msg:
            raise RuntimeError(
                "Login failed: your password has expired. "
                "Please reset it in LORIS before using the crawler."
            )
        else:
            raise RuntimeError(f"Login failed: {msg}")
    elif login_resp.status_code in (401, 403):
        raise RuntimeError("Login failed: invalid credentials or account expired.")
    else:
        login_resp.raise_for_status()
except requests.RequestException as e:
    print(f"Login failed due to network or server error: {e}")
    sys.exit(1)
except RuntimeError as e:
    print(e)
    sys.exit(1)

HEADERS = {"Authorization": f"Bearer {TOKEN}"}

# =========================
# 2. Init DataLad dataset
# =========================
DATASET_DIR.mkdir(parents=True, exist_ok=True)
if not (DATASET_DIR / ".datalad").exists():
    print(f"Creating DataLad dataset at {DATASET_DIR}")
    subprocess.run(["datalad", "create", "-c", "text2git", str(DATASET_DIR)], check=True)

# =========================
# 3. git-annex HTTP auth
# =========================
subprocess.run(
    ["git", "config", "annex.security.allowed-http-addresses", "all"],
    cwd=DATASET_DIR,
    check=True,
)
subprocess.run(
    ["git", "config", "annex.http-headers", f"Authorization: Bearer {TOKEN}"],
    cwd=DATASET_DIR,
    check=True,
)
env = os.environ.copy()
env["GIT_ANNEX_URL_AUTHORIZATION"] = f"Bearer {TOKEN}"

# =========================
# 4. Save CSV
# =========================
existing_files = set()
if MANIFEST_OUT.exists():
    with MANIFEST_OUT.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing_files.add(row["target_path"])

# =========================
# 5. get Project List
# =========================
print("Fetching project list...")
proj_resp = requests.get(f"{API_BASE}/projects", headers=HEADERS)
proj_resp.raise_for_status()
projects = proj_resp.json().get("Projects", {})
if not projects:
    print("No projects returned by API")
    sys.exit(1)
project_names = sorted(projects.keys())
print(f"Found projects: {', '.join(project_names)}")

# =========================
# 6. BIDS path
# =========================
def bids_path(img):
    subj = f"sub-{img['Candidate']}"
    ses = f"ses-{img['Visit']}"
    scan = img["ScanType"].lower()
    if scan.startswith("t1"):
        modality, suffix = "anat", "T1w"
    elif scan.startswith("t2"):
        modality, suffix = "anat", "T2w"
    elif scan.startswith("fieldmap"):
        modality, suffix = "fmap", "epi"
    elif scan.startswith("dwi"):
        modality, suffix = "dwi", "dwi"
    else:
        modality, suffix = "misc", scan
    bids_dir = Path(subj) / ses / modality
    bids_name = f"{subj}_{ses}_{suffix}.mnc"
    return bids_dir / bids_name, modality

# =========================
# 7. CSV write
# =========================
write_header = not MANIFEST_OUT.exists()
MANIFEST_OUT.parent.mkdir(parents=True, exist_ok=True)
f_out = MANIFEST_OUT.open("a", newline="")
writer = csv.DictWriter(
    f_out,
    fieldnames=[
        "project",
        "candidate",
        "visit",
        "filename",
        "modality",
        "target_path",
        "url",
    ],
)
if write_header:
    writer.writeheader()

# =========================
# 8. ingest loop by project
# =========================
for project in project_names:
    print(f"\nFetching images for project: {project}")
    resp = requests.get(f"{API_BASE}/projects/{project}/images", headers=HEADERS)
    resp.raise_for_status()
    images = resp.json().get("Images", [])
    print(f"{project}: {len(images)} images")

    for img in images:
        url = API_BASE + img["Link"]
        rel_target, modality = bids_path(img)
        target = Path("data") / project / rel_target
        if str(target) in existing_files:
            print(f"Already registered: {target}")
            continue

        (DATASET_DIR / target.parent).mkdir(parents=True, exist_ok=True)
        print("Registering addurl:", target)
        try:
            subprocess.run(
                [
                    "git",
                    "annex",
                    "addurl",
                    url,
                    "--file",
                    str(target),
                    "--fast",
                    "--relaxed",
                ],
                cwd=DATASET_DIR,
                env=env,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"Warning: addurl failed for {target}: {e}")
            continue

        writer.writerow(
            {
                "project": project,
                "candidate": img["Candidate"],
                "visit": img["Visit"],
                "filename": target.name,
                "modality": modality,
                "target_path": str(target),
                "url": url,
            }
        )
        existing_files.add(str(target))

        if args.get:
            print("Downloading", target)
            try:
                subprocess.run(["datalad", "get", str(target)], cwd=DATASET_DIR, check=True)
            except subprocess.CalledProcessError as e:
                print(f"Warning: download failed for {target}: {e}")
                continue

f_out.close()

# =========================
# 9. Save DataLad changes
# =========================
subprocess.run(
    [
        "datalad",
        "save",
        "-m",
        "Ingest Loris images via API (multi-project, BIDS, incremental)",
    ],
    cwd=DATASET_DIR,
    check=True,
)

print("\nDone")
print(f"Dataset created at: {DATASET_DIR}")
print("Files have been registered in git-annex")
if not args.get:
    print("To download files, run:")
    print(f"  cd {DATASET_DIR}")
    print(f"  datalad get <file-path>")

