# nyc_tlc_yellow_poc.py
# Objectif :
# - Automatiser la récupération (scraping) des liens PARQUET "Yellow Taxi" depuis la page TLC
# - Télécharger les fichiers dans ./data/raw/nyc_taxi/yellow/
# - Afficher des logs : nb de fichiers + taille totale

import argparse
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Page officielle TLC qui liste les fichiers par année/mois
TLC_PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# Les fichiers PARQUET sont souvent servis via ce CDN (URL directe standard)
CLOUDFRONT_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

# Pattern pour reconnaître les fichiers Yellow Taxi : yellow_tripdata_YYYY-MM.parquet
YELLOW_PARQUET_RE = re.compile(r"yellow[_-]tripdata[_-](\d{4})-(\d{2})\.parquet", re.IGNORECASE)


def human_size(n_bytes: int) -> str:
    """Convertit un nombre d’octets en taille lisible (KB/MB/GB)."""
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(n_bytes)
    for u in units:
        if size < 1024 or u == units[-1]:
            return f"{size:.2f} {u}"
        size /= 1024
    return f"{n_bytes} B"


def fetch_page_html(url: str, timeout: int = 30) -> str:
    """Télécharge le HTML de la page TLC (une simple requête HTTP GET)."""
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.text


def extract_yellow_parquet_links(html: str) -> list[str]:
    """
    Extrait les URLs de fichiers PARQUET Yellow Taxi depuis le HTML.
    - Cas 1 : la page contient directement des liens .parquet
    - Cas 2 : sinon, on tente de reconstruire des URLs via le pattern CloudFront
    """
    soup = BeautifulSoup(html, "html.parser")
    links = []

    # Parcours de tous les <a href="...">
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()

        # Transformation en URL absolue (au cas où le href est relatif)
        abs_url = urljoin(TLC_PAGE, href)

        # On garde uniquement les liens .parquet qui matchent le pattern Yellow Taxi
        if abs_url.lower().endswith(".parquet") and YELLOW_PARQUET_RE.search(abs_url):
            links.append(abs_url)

    # Fallback : si aucun lien direct n’a été trouvé, on cherche des mentions dans le texte
    # puis on reconstruit des URLs "standard" via CloudFront.
    if not links:
        text = soup.get_text(" ", strip=True)
        for m in YELLOW_PARQUET_RE.finditer(text):
            y, mm = m.group(1), m.group(2)
            links.append(f"{CLOUDFRONT_PREFIX}yellow_tripdata_{y}-{mm}.parquet")

    # Déduplication (éviter les doublons) en conservant l’ordre
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def filter_links(links: list[str], year: int, months: set[str] | None) -> list[str]:
    """Filtre les liens pour ne garder que l’année et les mois demandés."""
    out = []
    for u in links:
        m = YELLOW_PARQUET_RE.search(u)
        if not m:
            continue
        y, mm = m.group(1), m.group(2)

        # Filtre année
        if int(y) != year:
            continue

        # Filtre mois (si months est défini)
        if months is not None and mm not in months:
            continue

        out.append(u)
    return out


def safe_filename_from_url(url: str) -> str:
    """Récupère un nom de fichier à partir de l’URL (dernier segment du chemin)."""
    return url.split("/")[-1]


def head_content_length(url: str, timeout: int = 30) -> int | None:
    """
    Essaie d’estimer la taille du fichier via une requête HEAD.
    Si le serveur ne renvoie pas Content-Length, on retourne None.
    """
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        cl = r.headers.get("Content-Length")
        return int(cl) if cl else None
    except Exception:
        return None


def download_file(url: str, dest_path: Path, chunk_size: int = 1024 * 1024) -> int:
    """
    Télécharge un fichier en streaming (par morceaux).
    - Crée le dossier si besoin
    - Ne retélécharge pas si le fichier existe déjà
    - Retourne la taille finale téléchargée en octets
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    # Si le fichier existe déjà et n’est pas vide, on le garde (évite les doublons)
    if dest_path.exists() and dest_path.stat().st_size > 0:
        return dest_path.stat().st_size

    with requests.get(url, stream=True, timeout=60, headers={"User-Agent": "Mozilla/5.0"}) as r:
        r.raise_for_status()
        total = 0
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    total += len(chunk)
    return total


def main():
    # Lecture des arguments (année + mois)
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True, help="Année (ex: 2025)")
    ap.add_argument("--months", nargs="*", default=None, help="Mois: 01 02 03 ...")
    ap.add_argument("--all-months", action="store_true", help="Télécharge les 12 mois de l’année")
    ap.add_argument("--out", default="./data", help="Dossier de sortie")
    args = ap.parse_args()

    # Obligation : soit on donne des mois, soit on demande tous les mois
    if not args.all_months and not args.months:
        print("ERROR: fournir --months 01 02 ... OU --all-months", file=sys.stderr)
        sys.exit(1)

    # Normalisation/validation des mois (format 01..12)
    months = None
    if not args.all_months:
        months = set()
        for m in args.months:
            mm = str(m).zfill(2)
            if mm < "01" or mm > "12":
                print(f"ERROR: mois invalide {m}", file=sys.stderr)
                sys.exit(1)
            months.add(mm)

    out_dir = Path(args.out)

    # 1) Télécharger le HTML de la page TLC
    print(f"[INFO] Récupération de la page TLC: {TLC_PAGE}")
    html = fetch_page_html(TLC_PAGE)

    # 2) Extraire les liens Yellow Taxi parquet
    print("[INFO] Extraction des liens Yellow Taxi (PARQUET)...")
    all_links = extract_yellow_parquet_links(html)

    # 3) Filtrer selon année/mois
    links = filter_links(all_links, args.year, months)

    # Si on veut tous les mois et qu’on n’a pas trouvé via la page, on génère les 12 URLs standard
    if args.all_months and not links:
        links = [f"{CLOUDFRONT_PREFIX}yellow_tripdata_{args.year}-{mm:02d}.parquet" for mm in range(1, 13)]

    if not links:
        print("[WARN] Aucun lien trouvé pour ces paramètres (année/mois).")
        sys.exit(2)

    print(f"[INFO] Fichiers sélectionnés: {len(links)}")

    # 4) Estimation optionnelle de la taille à télécharger (HEAD)
    total_expected = 0
    unknown = 0
    for u in links:
        cl = head_content_length(u)
        if cl is None:
            unknown += 1
        else:
            total_expected += cl

    if total_expected > 0:
        print(f"[INFO] Taille estimée (HEAD): {human_size(total_expected)} (inconnus: {unknown})")
    else:
        print(f"[INFO] Taille non estimable via HEAD (inconnus: {unknown}/{len(links)})")

    # 5) Téléchargement réel
    downloaded = 0
    total_bytes = 0
    for u in links:
        fname = safe_filename_from_url(u)
        dest = out_dir / fname
        print(f"[DOWNLOAD] {fname}")
        size = download_file(u, dest)
        total_bytes += size
        downloaded += 1

    # 6) Résumé final (logs)
    print("\n[RESULT]")
    print(f"Fichiers téléchargés: {downloaded}")
    print(f"Taille totale sur disque: {human_size(total_bytes)}")
    print(f"Dossier de sortie: {out_dir.resolve()}")


if __name__ == "__main__":
    main()