#!/usr/bin/env python3
"""
Ingestion (sans Airflow) :
- Scrape page TLC
- Récupère UNIQUEMENT les liens Yellow Taxi .parquet
- 3 modes :
    1) Manuel : --year + --months 01 02 ...
    2) Latest : --latest (prend le mois le + récent disponible sur la page)
    3) Bootstrap : --bootstrap-3months (prend les 3 mois les + récents)
- Télécharge en local (staging)
- Upload sur HDFS /datalake/.../raw/...
- Logs : nb fichiers, taille totale, destination HDFS

Exemples :
  # Démo simple sur 1 mois (manuel)
  python nyc_yellow_ingest_to_hdfs.py --year 2025 --months 01 --hdfs-base /datalake/nawres/raw/nyc_taxi/yellow

  # Démo "mois le plus récent" (automatique)
  python nyc_yellow_ingest_to_hdfs.py --latest --hdfs-base /datalake/nawres/raw/nyc_taxi/yellow

  # Bootstrap : 3 derniers mois (automatique)
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


def human_size(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(n)
    for u in units:
        if size < 1024 or u == units[-1]:
            return f"{size:.2f} {u}"
        size /= 1024
    return f"{n} B"


def run(cmd: list[str]) -> str:
    """Exécute une commande shell et renvoie stdout, lève une erreur si exit != 0."""
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(
            f"CMD failed: {' '.join(cmd)}\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}"
        )
    return p.stdout.strip()


def fetch_html(url: str) -> str:
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.text


def extract_yellow_links(html: str) -> list[str]:
    """
    Extrait les liens .parquet Yellow directement depuis la page.
    Fallback : si la page ne contient pas de liens directs, reconstruit via CloudFront.
    """
    soup = BeautifulSoup(html, "html.parser")
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        abs_url = urljoin(TLC_PAGE, href)
        if abs_url.lower().endswith(".parquet") and YELLOW_RE.search(abs_url):
            links.append(abs_url)

    # Fallback : texte visible
    if not links:
        text = soup.get_text(" ", strip=True)
        for m in YELLOW_RE.finditer(text):
            y, mm = m.group(1), m.group(2)
            links.append(f"{CLOUDFRONT_PREFIX}yellow_tripdata_{y}-{mm}.parquet")

    # Dedup en conservant l'ordre
    out, seen = [], set()
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def list_available_year_month(links: list[str]) -> list[tuple[int, str, str]]:
    """Retourne [(YYYY, 'MM', url), ...] trié du plus ancien au plus récent."""
    items = []
    for u in links:
        m = YELLOW_RE.search(u)
        if not m:
            continue
        y, mm = int(m.group(1)), m.group(2)
        items.append((y, mm, u))
    items.sort(key=lambda x: (x[0], x[1]))
    return items


def filter_links(links: list[str], year: int, months: set[str] | None) -> list[str]:
    out = []
    for u in links:
        m = YELLOW_RE.search(u)
        if not m:
            continue
        y, mm = int(m.group(1)), m.group(2)
        if y != year:
            continue
        if months is not None and mm not in months:
            continue
        out.append(u)
    return out


def download(url: str, dest: Path) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)

    # Skip si déjà téléchargé
    if dest.exists() and dest.stat().st_size > 0:
        return dest.stat().st_size

    with requests.get(url, stream=True, timeout=180, headers={"User-Agent": "Mozilla/5.0"}) as r:
        r.raise_for_status()
        total = 0
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    total += len(chunk)
    return total


def build_hdfs_partition_path(hdfs_base: str, year: int, month: str | None) -> str:
    """
    Convention :
      - /.../year=YYYY/month=MM si month est donné
      - /.../year=YYYY sinon
    """
    hdfs_dir = f"{hdfs_base}/year={year}"
    if month:
        hdfs_dir += f"/month={month}"
    return hdfs_dir


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, help="Année (ex: 2025) pour mode manuel")
    ap.add_argument("--months", nargs="*", default=None, help="Mode manuel : 01 02 03 ...")
    ap.add_argument("--latest", action="store_true", help="Télécharge le mois le plus récent disponible")
    ap.add_argument("--bootstrap-3months", action="store_true", help="Télécharge les 3 mois les plus récents disponibles")
    ap.add_argument("--local-staging", default="/tmp/nyc_taxi_staging/yellow", help="Dossier staging local")
    ap.add_argument("--hdfs-base", default="/datalake/raw/nyc_taxi/yellow", help="Base HDFS pour raw")
    args = ap.parse_args()

    # Validation des modes
    modes = int(bool(args.months)) + int(args.latest) + int(args.bootstrap_3months)
    if modes == 0:
        print("ERROR: choisis un mode : (--months ...) OU --latest OU --bootstrap-3months", file=sys.stderr)
        sys.exit(1)
    if modes > 1:
        print("ERROR: un seul mode à la fois : (--months ...) OU --latest OU --bootstrap-3months", file=sys.stderr)
        sys.exit(1)

    if args.months and args.year is None:
        print("ERROR: en mode manuel (--months), il faut aussi --year", file=sys.stderr)
        sys.exit(1)

    # Vérif HDFS (évite de télécharger pour rien)
    print("[CHECK] HDFS report...")
    _ = run(["hdfs", "dfsadmin", "-report"])

    print(f"[INFO] Scrape: {TLC_PAGE}")
    html = fetch_html(TLC_PAGE)
    all_links = extract_yellow_links(html)

    if not all_links:
        print("[WARN] Aucun lien .parquet détecté sur la page TLC.", file=sys.stderr)
        sys.exit(2)

    available = list_available_year_month(all_links)
    if not available:
        print("[WARN] Impossible d'extraire (YYYY-MM) depuis les liens.", file=sys.stderr)
        sys.exit(2)

    selected: list[str] = []
    plan: list[tuple[int, str, str]] = []  # (year, month, url)

    # MODE 1 : latest
    if args.latest:
        y, mm, url = available[-1]
        plan = [(y, mm, url)]

    # MODE 2 : bootstrap 3 mois
    elif args.bootstrap_3months:
        plan = available[-3:] if len(available) >= 3 else available

    # MODE 3 : manuel
    else:
        months_set = set()
        for m in args.months:
            mm = str(m).zfill(2)
            if mm < "01" or mm > "12":
                print(f"ERROR: mois invalide: {m}", file=sys.stderr)
                sys.exit(1)
            months_set.add(mm)

        tmp = filter_links(all_links, args.year, months_set)
        if not tmp:
            print("[WARN] Aucun fichier trouvé pour ce filtre (year/month).", file=sys.stderr)
            sys.exit(2)

        # Reconstruire plan (year, month, url) dans l'ordre
        for u in tmp:
            m = YELLOW_RE.search(u)
            if m:
                plan.append((int(m.group(1)), m.group(2), u))

        plan.sort(key=lambda x: (x[0], x[1]))

    selected = [u for (_, _, u) in plan]
    print(f"[INFO] Fichiers sélectionnés: {len(selected)}")
    print("[INFO] Liste:")
    for y, mm, _ in plan:
        print(f"  - {y}-{mm}")

    # Download local + PUT HDFS fichier par fichier, partitionné year/month
    total_bytes = 0
    downloaded_files: list[tuple[int, str, Path]] = []  # (year, month, local_path)

    for y, mm, url in plan:
        local_dir = Path(args.local_staging) / f"year={y}" / f"month={mm}"
        local_dir.mkdir(parents=True, exist_ok=True)

        fname = url.split("/")[-1]
        dest = local_dir / fname

        print(f"[DL] {fname}")
        sz = download(url, dest)
        total_bytes += sz
        downloaded_files.append((y, mm, dest))

    print(f"[OK] Téléchargé: {len(downloaded_files)} fichiers, taille totale: {human_size(total_bytes)}")

    # Upload HDFS
    for y, mm, fpath in downloaded_files:
        hdfs_dir = build_hdfs_partition_path(args.hdfs_base, y, mm)
        print(f"[HDFS] mkdir: {hdfs_dir}")
        run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir])

        print(f"[HDFS PUT] {fpath.name} -> {hdfs_dir}")
        run(["hdfs", "dfs", "-put", "-f", str(fpath), hdfs_dir + "/"])

        print("[HDFS] ls -h:")
        print(run(["hdfs", "dfs", "-ls", "-h", hdfs_dir]))

    print("\n[RESULT]")
    print(f"HDFS base: {args.hdfs_base}")
    print(f"Local staging: {args.local_staging}")


if __name__ == "__main__":
    main()