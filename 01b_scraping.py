"""
╔══════════════════════════════════════════════════════════════════════════════╗
║        PROJET 3 — WILD DATA HUB · FORMULE 1                                 ║
║        Phase 1b : Web Scraping                                               ║
║                                                                              ║
║  Sources scrapées :                                                          ║
║   1. Wikipedia          → Champions du monde, records historiques            ║
║   2. StatsF1.com        → Statistiques avancées pilotes / circuits           ║
║   3. Formula1.com       → Standings officiels saison en cours                ║
║   4. Motorsport.com     → Dernières actualités F1                            ║
║                                                                              ║
║  Données collectées (non disponibles via Ergast/OpenF1) :                   ║
║   • Biographies pilotes (âge, nationalité, équipe actuelle)                  ║
║   • Records historiques (plus de victoires, pôles, points...)                ║
║   • Champions du monde par année                                             ║
║   • Caractéristiques circuits (longueur, type, virages)                      ║
║   • Actualités récentes                                                      ║
╚══════════════════════════════════════════════════════════════════════════════╝

Dépendances :
    pip install requests beautifulsoup4 lxml cloudscraper pandas python-dotenv

Utilisation :
    python 01b_scraping.py               # tout scraper
    python 01b_scraping.py champions     # seulement champions
    python 01b_scraping.py circuits      # seulement circuits
"""

import requests
import time
import random
import logging
import json
import re
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

try:
    import cloudscraper
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False
    print("⚠️  cloudscraper non installé — certains sites protégés seront ignorés")
    print("   → pip install cloudscraper")

# ── Config ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("F1_Scraper")

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Délais aléatoires entre requêtes (respect éthique / rate limiting)
DELAY_MIN = 1.5
DELAY_MAX = 3.5

# Headers pour simuler un navigateur réel
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.google.com/",
    "DNT": "1",
}


# ══════════════════════════════════════════════════════════════════════════════
#  UTILITAIRES
# ══════════════════════════════════════════════════════════════════════════════

def polite_sleep():
    """Pause aléatoire pour ne pas surcharger les serveurs."""
    t = random.uniform(DELAY_MIN, DELAY_MAX)
    time.sleep(t)


def get_soup(url: str, use_cloudscraper: bool = False, timeout: int = 15) -> BeautifulSoup | None:
    """
    Récupère et parse une page HTML.

    Args:
        url              : URL cible
        use_cloudscraper : True pour les sites protégés par Cloudflare
        timeout          : secondes avant abandon

    Returns:
        BeautifulSoup ou None en cas d'erreur
    """
    try:
        if use_cloudscraper and HAS_CLOUDSCRAPER:
            scraper = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False}
            )
            r = scraper.get(url, timeout=timeout)
        else:
            session = requests.Session()
            session.headers.update(HEADERS)
            r = session.get(url, timeout=timeout)

        r.raise_for_status()
        log.info(f"  ✅ {url[:70]}... [{r.status_code}]")
        return BeautifulSoup(r.text, "lxml")

    except requests.HTTPError as e:
        log.error(f"  ❌ HTTP {e.response.status_code} — {url}")
        return None
    except requests.RequestException as e:
        log.error(f"  ❌ Connexion : {e}")
        return None


def save_raw(df: pd.DataFrame, name: str):
    """Sauvegarde CSV dans data/raw/."""
    path = RAW_DIR / f"{name}.csv"
    df.to_csv(path, index=False, encoding="utf-8")
    log.info(f"  💾 Sauvegardé : {path}  ({len(df)} lignes)")


def clean_text(t: str) -> str:
    """Nettoie un texte scraped (espaces, retours, références wiki)."""
    if not t:
        return ""
    # Supprimer les références Wikipedia [1], [note 2], etc.
    t = re.sub(r"\[\d+\]|\[note \d+\]|\[a\]|\[b\]", "", t)
    # Supprimer les annotations entre parenthèses trop longues
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def parse_wiki_table(table) -> pd.DataFrame | None:
    """Parse un tableau Wikipedia en DataFrame."""
    if not table:
        return None
    rows = []
    headers = []
    for i, row in enumerate(table.find_all("tr")):
        cells = row.find_all(["th", "td"])
        if i == 0:
            headers = [clean_text(c.get_text()) for c in cells]
        else:
            rows.append([clean_text(c.get_text()) for c in cells])

    if not headers or not rows:
        return None

    # Harmoniser les longueurs
    max_cols = max(len(headers), max(len(r) for r in rows) if rows else 0)
    headers  = headers + [f"col_{i}" for i in range(len(headers), max_cols)]
    rows     = [r + [""] * (max_cols - len(r)) for r in rows]

    return pd.DataFrame(rows, columns=headers[:max_cols])


# ══════════════════════════════════════════════════════════════════════════════
#  1. WIKIPEDIA — Champions du Monde F1
# ══════════════════════════════════════════════════════════════════════════════

def scraper_champions_wiki() -> pd.DataFrame:
    """
    Scrape la liste des Champions du Monde F1 depuis Wikipedia.
    URL : https://fr.wikipedia.org/wiki/Championnat_du_monde_de_Formule_1

    Données récupérées :
    - Année, Pilote champion, Écurie, Points, Nationalité
    """
    log.info("🌐 [Wikipedia] Scraping champions du monde F1...")
    url  = "https://fr.wikipedia.org/wiki/Championnat_du_monde_de_Formule_1"
    soup = get_soup(url)

    if not soup:
        return pd.DataFrame()

    rows = []

    # Chercher les tables de la page
    tables = soup.find_all("table", class_="wikitable")

    for table in tables:
        headers_raw = [th.get_text(strip=True) for th in table.find_all("th")]
        # On cherche la table avec "Saison" ou "Année" dans les headers
        if any(h in ["Saison", "Année", "Champion"] for h in headers_raw):
            for tr in table.find_all("tr")[1:]:
                tds = tr.find_all(["td", "th"])
                if len(tds) >= 3:
                    row = {
                        "annee"    : clean_text(tds[0].get_text()),
                        "champion" : clean_text(tds[1].get_text()),
                        "ecurie"   : clean_text(tds[2].get_text()) if len(tds) > 2 else "",
                        "points"   : clean_text(tds[3].get_text()) if len(tds) > 3 else "",
                    }
                    # Nettoyer l'année
                    if re.match(r"^\d{4}$", row["annee"]):
                        rows.append(row)
            break

    df = pd.DataFrame(rows)
    if not df.empty:
        df["annee"] = pd.to_numeric(df["annee"], errors="coerce")
        df = df.dropna(subset=["annee"]).sort_values("annee")

    log.info(f"  → {len(df)} champions scrapés")
    save_raw(df, "champions_wiki")
    polite_sleep()
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  2. WIKIPEDIA — Circuits F1 (infos détaillées)
# ══════════════════════════════════════════════════════════════════════════════

CIRCUITS_WIKI = {
    "monaco": {
        "url"  : "https://fr.wikipedia.org/wiki/Circuit_de_Monaco",
        "nom"  : "Circuit de Monaco",
        "ville": "Monaco",
        "pays" : "Monaco",
    },
    "silverstone": {
        "url"  : "https://fr.wikipedia.org/wiki/Circuit_de_Silverstone",
        "nom"  : "Silverstone Circuit",
        "ville": "Silverstone",
        "pays" : "Royaume-Uni",
    },
    "monza": {
        "url"  : "https://fr.wikipedia.org/wiki/Autodromo_Nazionale_di_Monza",
        "nom"  : "Autodromo Nazionale di Monza",
        "ville": "Monza",
        "pays" : "Italie",
    },
    "spa": {
        "url"  : "https://fr.wikipedia.org/wiki/Circuit_de_Spa-Francorchamps",
        "nom"  : "Circuit de Spa-Francorchamps",
        "ville": "Stavelot",
        "pays" : "Belgique",
    },
    "suzuka": {
        "url"  : "https://fr.wikipedia.org/wiki/Circuit_de_Suzuka",
        "nom"  : "Suzuka International Racing Course",
        "ville": "Suzuka",
        "pays" : "Japon",
    },
    "abu_dhabi": {
        "url"  : "https://fr.wikipedia.org/wiki/Circuit_de_Yas_Marina",
        "nom"  : "Yas Marina Circuit",
        "ville": "Abu Dhabi",
        "pays" : "Émirats Arabes Unis",
    },
}


def scraper_circuit_wiki(circuit_key: str, circuit_info: dict) -> dict:
    """
    Scrape les infos techniques d'un circuit depuis sa page Wikipedia.

    Données récupérées depuis l'infobox :
    - Longueur, nombre de virages, altitude, capacité, premier GP, record tour
    """
    log.info(f"  📍 Circuit : {circuit_info['nom']}")
    soup = get_soup(circuit_info["url"])
    if not soup:
        return {}

    result = {
        "circuit_key"  : circuit_key,
        "nom"          : circuit_info["nom"],
        "ville"        : circuit_info["ville"],
        "pays"         : circuit_info["pays"],
        "longueur_km"  : None,
        "nb_virages"   : None,
        "premier_gp"   : None,
        "record_tour"  : None,
        "record_pilote": None,
        "record_annee" : None,
        "type_circuit" : None,
        "source_url"   : circuit_info["url"],
    }

    # Infobox Wikipedia
    infobox = soup.find("table", class_=re.compile(r"infobox"))
    if infobox:
        for tr in infobox.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = clean_text(th.get_text()).lower()
            value = clean_text(td.get_text())

            if "longueur" in label or "length" in label:
                # Extraire km : "5,412 km" ou "5.412 km"
                m = re.search(r"([\d\s,.]+)\s*km", value)
                if m:
                    result["longueur_km"] = m.group(1).strip().replace(" ","").replace(",",".")

            elif "virage" in label or "turn" in label or "courbe" in label:
                m = re.search(r"\d+", value)
                if m:
                    result["nb_virages"] = int(m.group())

            elif "premier" in label and ("grand" in label or "gp" in label or "race" in label):
                result["premier_gp"] = value[:20]

            elif "record" in label and "tour" in label:
                result["record_tour"] = value[:50]

    # Type de circuit (street, permanent, ovale...)
    intro = soup.find("p")
    if intro:
        text = clean_text(intro.get_text()).lower()
        if "urbain" in text or "rue" in text or "street" in text or "city" in text:
            result["type_circuit"] = "Street circuit"
        elif "permanent" in text or "racing circuit" in text:
            result["type_circuit"] = "Circuit permanent"
        else:
            result["type_circuit"] = "Circuit mixte"

    polite_sleep()
    return result


def scraper_circuits_wiki() -> pd.DataFrame:
    """Scrape les infos de tous les circuits définis."""
    log.info("🌐 [Wikipedia] Scraping circuits F1...")
    rows = []
    for key, info in CIRCUITS_WIKI.items():
        row = scraper_circuit_wiki(key, info)
        if row:
            rows.append(row)

    df = pd.DataFrame(rows)
    log.info(f"  → {len(df)} circuits scrapés")
    save_raw(df, "circuits_wiki_details")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  3. WIKIPEDIA — Records F1 (all-time)
# ══════════════════════════════════════════════════════════════════════════════

def scraper_records_wiki() -> pd.DataFrame:
    """
    Scrape les records all-time F1 depuis Wikipedia.
    URL : https://en.wikipedia.org/wiki/List_of_Formula_One_driver_records
    """
    log.info("🌐 [Wikipedia] Scraping records F1...")
    url  = "https://en.wikipedia.org/wiki/List_of_Formula_One_driver_records"
    soup = get_soup(url)
    if not soup:
        return pd.DataFrame()

    records = []
    sections = soup.find_all("h2") + soup.find_all("h3")

    for section in sections:
        title = clean_text(section.get_text())
        # Trouver la table qui suit le titre
        next_table = section.find_next("table", class_="wikitable")
        if next_table:
            # Prendre seulement la première ligne (le record actuel)
            trs = next_table.find_all("tr")
            if len(trs) > 1:
                header_row = trs[0]
                data_row   = trs[1]
                headers = [clean_text(th.get_text()) for th in header_row.find_all(["th","td"])]
                values  = [clean_text(td.get_text()) for td in data_row.find_all(["th","td"])]

                row = {"categorie_record": title}
                for h, v in zip(headers, values):
                    if h:
                        row[h[:30]] = v[:100]  # tronquer pour éviter les très longs textes
                records.append(row)

    df = pd.DataFrame(records)
    log.info(f"  → {len(df)} records scrapés")
    save_raw(df, "records_wiki")
    polite_sleep()
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  4. FORMULA1.COM — Standings officiels saison en cours
# ══════════════════════════════════════════════════════════════════════════════

def scraper_standings_f1com(saison: int = 2024) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Scrape les standings officiels depuis formula1.com.
    ⚠️ Site JavaScript-heavy — utilise cloudscraper.

    Retourne : (df_pilotes, df_constructeurs)
    """
    log.info(f"🌐 [Formula1.com] Standings {saison}...")

    if not HAS_CLOUDSCRAPER:
        log.warning("  ⚠️  cloudscraper requis pour formula1.com")
        return pd.DataFrame(), pd.DataFrame()

    # Pilotes
    url_drivers = f"https://www.formula1.com/en/results/{saison}/drivers"
    soup_d = get_soup(url_drivers, use_cloudscraper=True)

    df_drivers = pd.DataFrame()
    if soup_d:
        table = soup_d.find("table")
        if table:
            rows = []
            headers = [clean_text(th.get_text()) for th in table.find_all("th")]
            for tr in table.find_all("tr")[1:]:
                tds = tr.find_all("td")
                if tds:
                    rows.append([clean_text(td.get_text()) for td in tds])
            df_drivers = pd.DataFrame(rows, columns=headers[:len(rows[0])] if rows else headers)
            df_drivers["saison"] = saison
            log.info(f"  → {len(df_drivers)} pilotes standings scrapés")
            save_raw(df_drivers, f"standings_pilotes_f1com_{saison}")

    polite_sleep()

    # Constructeurs
    url_constructors = f"https://www.formula1.com/en/results/{saison}/team"
    soup_c = get_soup(url_constructors, use_cloudscraper=True)

    df_constructors = pd.DataFrame()
    if soup_c:
        table = soup_c.find("table")
        if table:
            rows = []
            headers = [clean_text(th.get_text()) for th in table.find_all("th")]
            for tr in table.find_all("tr")[1:]:
                tds = tr.find_all("td")
                if tds:
                    rows.append([clean_text(td.get_text()) for td in tds])
            df_constructors = pd.DataFrame(rows, columns=headers[:len(rows[0])] if rows else headers)
            df_constructors["saison"] = saison
            log.info(f"  → {len(df_constructors)} écuries standings scrapés")
            save_raw(df_constructors, f"standings_ecuries_f1com_{saison}")

    polite_sleep()
    return df_drivers, df_constructors


# ══════════════════════════════════════════════════════════════════════════════
#  5. MOTORSPORT.COM — Actualités F1
# ══════════════════════════════════════════════════════════════════════════════

def scraper_news_motorsport(nb_pages: int = 3) -> pd.DataFrame:
    """
    Scrape les dernières actualités F1 depuis motorsport.com.

    Données récupérées :
    - Titre, date, résumé, URL, catégorie

    Args:
        nb_pages : nombre de pages d'actualités à scraper (1 page ≈ 20 articles)
    """
    log.info(f"🌐 [Motorsport.com] Actualités F1 ({nb_pages} pages)...")

    articles = []

    for page in range(1, nb_pages + 1):
        url = f"https://www.motorsport.com/f1/news/" if page == 1 \
              else f"https://www.motorsport.com/f1/news/?page={page}"

        soup = get_soup(url, use_cloudscraper=HAS_CLOUDSCRAPER)
        if not soup:
            continue

        # Chercher les articles (structure variée selon le site)
        for article in soup.find_all("article") or soup.find_all("div", class_=re.compile(r"article|news|item")):
            titre_tag  = article.find(["h1","h2","h3","h4"])
            date_tag   = article.find(["time","span"], class_=re.compile(r"date|time"))
            resume_tag = article.find("p")
            lien_tag   = article.find("a", href=True)

            titre  = clean_text(titre_tag.get_text())  if titre_tag  else ""
            date   = clean_text(date_tag.get_text())   if date_tag   else ""
            resume = clean_text(resume_tag.get_text()) if resume_tag else ""
            lien   = lien_tag["href"]                  if lien_tag   else ""

            if titre and len(titre) > 10:
                # Normaliser le lien
                if lien and not lien.startswith("http"):
                    lien = "https://www.motorsport.com" + lien

                articles.append({
                    "titre"        : titre,
                    "date"         : date,
                    "resume"       : resume[:300],
                    "url"          : lien,
                    "source"       : "motorsport.com",
                    "scraped_at"   : datetime.now().isoformat(),
                })

        log.info(f"  Page {page} : {len(articles)} articles au total")
        polite_sleep()

    df = pd.DataFrame(articles).drop_duplicates(subset="titre")
    log.info(f"  → {len(df)} articles scrapés")
    save_raw(df, "news_motorsport")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  6. WIKIPEDIA — Palmarès pilotes (all-time stats)
# ══════════════════════════════════════════════════════════════════════════════

PILOTES_WIKI = {
    "max_verstappen": "https://fr.wikipedia.org/wiki/Max_Verstappen",
    "lewis_hamilton" : "https://fr.wikipedia.org/wiki/Lewis_Hamilton",
    "charles_leclerc": "https://fr.wikipedia.org/wiki/Charles_Leclerc",
    "fernando_alonso": "https://fr.wikipedia.org/wiki/Fernando_Alonso",
    "lando_norris"   : "https://fr.wikipedia.org/wiki/Lando_Norris",
    "george_russell" : "https://fr.wikipedia.org/wiki/George_Russell_(pilote)",
    "carlos_sainz"   : "https://fr.wikipedia.org/wiki/Carlos_Sainz_Jr.",
    "oscar_piastri"  : "https://fr.wikipedia.org/wiki/Oscar_Piastri",
    "sebastian_vettel": "https://fr.wikipedia.org/wiki/Sebastian_Vettel",
    "michael_schumacher": "https://fr.wikipedia.org/wiki/Michael_Schumacher",
}


def scraper_pilote_wiki(driver_key: str, url: str) -> dict:
    """
    Scrape la fiche d'un pilote sur Wikipedia.

    Données récupérées depuis l'infobox :
    - Nom complet, date naissance, nationalité
    - Victoires, podiums, poles, championnats
    """
    log.info(f"  👤 Pilote : {driver_key}")
    soup = get_soup(url)
    if not soup:
        return {}

    result = {
        "driver_key"     : driver_key,
        "nom_complet"    : "",
        "date_naissance" : "",
        "lieu_naissance" : "",
        "nationalite"    : "",
        "championnats"   : None,
        "courses"        : None,
        "victoires"      : None,
        "podiums"        : None,
        "poles"          : None,
        "meilleurs_tours": None,
        "points_totaux"  : None,
        "premiere_course": "",
        "derniere_course": "",
        "source_url"     : url,
    }

    # Nom complet depuis le titre H1
    h1 = soup.find("h1", id="firstHeading")
    if h1:
        result["nom_complet"] = clean_text(h1.get_text())

    # Infobox
    infobox = soup.find("table", class_=re.compile(r"infobox"))
    if not infobox:
        return result

    for tr in infobox.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        label = clean_text(th.get_text()).lower()
        value = clean_text(td.get_text())

        if "naissance" in label or "né" in label or "born" in label:
            result["date_naissance"] = value[:30]
        elif "nationalité" in label or "nationality" in label:
            result["nationalite"] = value[:50]
        elif "championnats" in label or "championship" in label:
            m = re.search(r"\d+", value)
            result["championnats"] = int(m.group()) if m else None
        elif "victoire" in label or "win" in label:
            m = re.search(r"\d+", value)
            result["victoires"] = int(m.group()) if m else None
        elif "podium" in label:
            m = re.search(r"\d+", value)
            result["podiums"] = int(m.group()) if m else None
        elif "pole" in label:
            m = re.search(r"\d+", value)
            result["poles"] = int(m.group()) if m else None
        elif "meilleur" in label or "fastest" in label or "rapide" in label:
            m = re.search(r"\d+", value)
            result["meilleurs_tours"] = int(m.group()) if m else None
        elif "point" in label:
            m = re.search(r"[\d\s,.]+", value)
            result["points_totaux"] = value[:20] if m else None
        elif "course" in label and ("premiè" in label or "first" in label):
            result["premiere_course"] = value[:30]

    polite_sleep()
    return result


def scraper_pilotes_wiki() -> pd.DataFrame:
    """Scrape les fiches Wikipedia des pilotes principaux."""
    log.info("🌐 [Wikipedia] Scraping fiches pilotes...")
    rows = []
    for key, url in PILOTES_WIKI.items():
        row = scraper_pilote_wiki(key, url)
        if row:
            rows.append(row)

    df = pd.DataFrame(rows)
    log.info(f"  → {len(df)} pilotes scrapés")
    save_raw(df, "pilotes_wiki_details")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  7. WIKIPEDIA — Calendrier saison actuelle
# ══════════════════════════════════════════════════════════════════════════════

def scraper_calendrier_wiki(saison: int = 2025) -> pd.DataFrame:
    """
    Scrape le calendrier de la saison depuis Wikipedia.
    Complète les données Ergast avec statut officiel.
    """
    log.info(f"🌐 [Wikipedia] Calendrier {saison}...")
    url  = f"https://fr.wikipedia.org/wiki/Championnat_du_monde_de_Formule_1_{saison}"
    soup = get_soup(url)
    if not soup:
        return pd.DataFrame()

    rows = []
    # Chercher la table calendrier
    for table in soup.find_all("table", class_="wikitable"):
        ths = [clean_text(th.get_text()).lower() for th in table.find_all("th")]
        if any("grand prix" in t or "circuit" in t for t in ths):
            for tr in table.find_all("tr")[1:]:
                tds = tr.find_all(["td","th"])
                if len(tds) >= 4:
                    rows.append({
                        "round"      : clean_text(tds[0].get_text()),
                        "grand_prix" : clean_text(tds[1].get_text()),
                        "circuit"    : clean_text(tds[2].get_text()) if len(tds)>2 else "",
                        "date"       : clean_text(tds[3].get_text()) if len(tds)>3 else "",
                        "statut"     : clean_text(tds[4].get_text()) if len(tds)>4 else "",
                        "saison"     : saison,
                    })
            break

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["round"].str.match(r"^\d+$", na=False)]  # garder seulement les vrais rounds
    log.info(f"  → {len(df)} rounds calendrier scrapés")
    save_raw(df, f"calendrier_wiki_{saison}")
    polite_sleep()
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

SCRAPERS = {
    "champions"  : scraper_champions_wiki,
    "circuits"   : scraper_circuits_wiki,
    "records"    : scraper_records_wiki,
    "pilotes"    : scraper_pilotes_wiki,
    "calendrier" : lambda: scraper_calendrier_wiki(2025),
    "standings"  : lambda: scraper_standings_f1com(2024),
    "news"       : lambda: scraper_news_motorsport(nb_pages=2),
}


def run_scraping(cibles: list = None):
    """
    Lance le pipeline de scraping.

    Args:
        cibles : liste de scrapers à lancer.
                 None = tous.
                 Ex: ["champions","pilotes","circuits"]
    """
    if cibles is None:
        cibles = list(SCRAPERS.keys())

    log.info("=" * 60)
    log.info(f"🕷️  DÉMARRAGE SCRAPING F1  —  {datetime.now():%Y-%m-%d %H:%M}")
    log.info(f"   Cibles : {', '.join(cibles)}")
    log.info("=" * 60)

    results = {}
    for nom in cibles:
        if nom not in SCRAPERS:
            log.warning(f"  ⚠️  Scraper inconnu : {nom}")
            continue
        log.info(f"\n── {nom.upper()} ─────────────────────────────────────────")
        try:
            result = SCRAPERS[nom]()
            results[nom] = result
        except Exception as e:
            log.error(f"  ❌ Erreur scraper '{nom}': {e}")
            import traceback; traceback.print_exc()

    log.info("\n" + "=" * 60)
    log.info("✅  SCRAPING TERMINÉ")
    log.info(f"📁  Fichiers dans : {RAW_DIR.resolve()}")
    log.info("=" * 60)
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    cibles = sys.argv[1:] if len(sys.argv) > 1 else None

    if cibles and cibles[0] == "liste":
        print("Scrapers disponibles :")
        for nom in SCRAPERS:
            print(f"  • {nom}")
    else:
        run_scraping(cibles)
