#!/usr/bin/env python3
"""
Ingestion (sans Airflow) :
- Scrape page TLC
- Récupère UNIQUEMENT les liens Yellow Taxi .parquet
- 3 modes :
    1) Manuel : --year + --months 01 02 ...
    2) Latest : --latest (prend le mois le + récent disponible sur la page)
    3) Bootstrap : --bootstrap-3months (prend les 3 mois les + récents)
    4)Bootstrap : --bootstrap-3months (prend les 6 mois les + récents)
- Télécharge en local (staging)
- Upload sur HDFS /datalake/.../raw/...
- Logs : nb fichiers, taille totale, destination HDFS

Exemples :
  # Démo simple sur 1 mois (manuel)
  python nyc_yellow_ingest_to_hdfs.py --year 2025 --months 01 --hdfs-base /datalake/nawres/raw/nyc_taxi/yellow

  # Démo "mois le plus récent" (automatique)
  python nyc_yellow_ingest_to_hdfs.py --latest --hdfs-base /datalake/nawres/raw/nyc_taxi/yellow

  # Bootstrap : 6 derniers mois (automatique)
  python nyc_yellow_ingest_to_hdfs.py --bootstrap-3months --hdfs-base /datalake/nawres/raw/nyc_taxi/yellow

Dépendances :
  pip install requests beautifulsoup4
"""


import argparse
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

TLC_PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
CLOUDFRONT_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
YELLOW_RE = re.compile(r"yellow[_-]tripdata[_-](\d{4})-(\d{2})\.parquet", re.IGNORECASE)


def run(cmd):
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{p.stderr}")
    return p.stdout.strip()


def run_ok(cmd) -> bool:
    """Return True if cmd succeeds (exit code 0), False otherwise."""
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.returncode == 0


def fetch_html(url):
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.text


def extract_links(html):
    soup = BeautifulSoup(html, "html.parser")
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        abs_url = urljoin(TLC_PAGE, href)
        if abs_url.lower().endswith(".parquet") and YELLOW_RE.search(abs_url):
            links.append(abs_url)

    # fallback CloudFront
    if not links:
        text = soup.get_text(" ", strip=True)
        for m in YELLOW_RE.finditer(text):
            y, mm = m.group(1), m.group(2)
            links.append(f"{CLOUDFRONT_PREFIX}yellow_tripdata_{y}-{mm}.parquet")

    return list(dict.fromkeys(links))


def download(url, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() and dest.stat().st_size > 0:
        print(f"[SKIP] Already downloaded locally: {dest.name}")
        return

    print(f"[DOWNLOAD] {dest.name}")
    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int)
    ap.add_argument("--months", nargs="*")
    ap.add_argument("--latest", action="store_true")
    ap.add_argument("--bootstrap-3months", action="store_true")
    ap.add_argument("--bootstrap-6months", action="store_true")
    ap.add_argument("--local-staging", default="/tmp/nyc_taxi_staging/yellow")
    ap.add_argument("--hdfs-base", required=True)
    args = ap.parse_args()

    print("[CHECK] HDFS access")
    run(["hdfs", "ls", args.hdfs_base])

    print("[SCRAPE]", TLC_PAGE)
    html = fetch_html(TLC_PAGE)
    links = extract_links(html)

    if not links:
        print("No parquet links found.")
        sys.exit(1)

    available = []
    for u in links:
        m = YELLOW_RE.search(u)
        if m:
            available.append((int(m.group(1)), m.group(2), u))
    available.sort()

    plan = []

    if args.latest:
        plan = [available[-1]]
    elif args.bootstrap_3months:
        plan = available[-3:]
    elif args.bootstrap_6months:
        plan = available[-6:]
    else:
        if not args.year or not args.months:
            print("Manual mode requires --year and --months")
            sys.exit(1)

        for y, mm, u in available:
            if y == args.year and mm in args.months:
                plan.append((y, mm, u))

    print(f"[PLAN] {len(plan)} file(s) selected")
    for y, mm, _ in plan:
        print(f"  - {y}-{mm}")

    for y, mm, url in plan:
        filename = url.split("/")[-1]

        hdfs_dir = f"{args.hdfs_base}/year={y}/month={mm}"
        hdfs_file = f"{hdfs_dir}/{filename}"

        # SKIP if already in HDFS (works with any 'hdfs' command set)
        if run_ok(["hdfs", "ls", hdfs_file]):
            print(f"[SKIP] Already present in HDFS: {y}-{mm} ({hdfs_file})")
            continue

        local_dir = Path(args.local_staging) / f"year={y}" / f"month={mm}"
        local_path = local_dir / filename

        download(url, local_path)

        print("[HDFS] mkdir", hdfs_dir)
        run(["hdfs", "mkdir", "-p", hdfs_dir])

        print("[HDFS] put", local_path.name)
        run(["hdfs", "put", str(local_path), hdfs_dir])

        print("[HDFS] ls")
        print(run(["hdfs", "ls", "-h", hdfs_dir]))

    print("\n[DONE]")


if __name__ == "__main__":
    main()
