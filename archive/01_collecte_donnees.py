"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          PROJET 3 — WILD DATA HUB · API FORMULE 1                           ║
║          Phase 1 : Acquisition des données                                   ║
║                                                                              ║
║  Sources :                                                                   ║
║   • OpenF1 API  → https://api.openf1.org/v1/   (temps réel, saisons récentes)
║   • Ergast API  → http://ergast.com/api/f1/     (historique 1950-2024)       ║
║                                                                              ║
║  Données collectées :                                                        ║
║   • Pilotes (drivers)          • Écuries (constructors)                      ║
║   • Circuits                   • Calendrier des courses (races)              ║
║   • Résultats de courses       • Qualifications                              ║
║   • Classement pilotes/éc.     • Pit stops                                   ║
║   • Temps au tour (lap times)  • Sessions OpenF1                             ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import requests
import pandas as pd
import numpy as np
import json
import time
import os
import logging
from pathlib import Path
from datetime import datetime

# ── Configuration ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("F1_Collector")

# Répertoires de sortie
DATA_DIR  = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── Constantes API ─────────────────────────────────────────────────────────
ERGAST_BASE  = "http://ergast.com/api/f1"
OPENF1_BASE  = "https://api.openf1.org/v1"
SAISONS      = list(range(2018, 2026))   # Modifier selon besoin
DELAY        = 0.5                        # secondes entre requêtes (respect rate limit)


# ══════════════════════════════════════════════════════════════════════════════
#  UTILITAIRES
# ══════════════════════════════════════════════════════════════════════════════

def get_ergast(endpoint: str, params: dict = None, limit: int = 1000) -> list:
    """
    Appel générique à l'API Ergast avec pagination automatique.
    
    Args:
        endpoint : ex. "/drivers" ou "/2024/results"
        params   : paramètres GET supplémentaires
        limit    : nombre d'éléments par page (max 1000)
    
    Returns:
        liste de tous les résultats (toutes pages confondues)
    """
    url    = f"{ERGAST_BASE}{endpoint}.json"
    offset = 0
    all_results = []
    
    while True:
        p = {"limit": limit, "offset": offset}
        if params:
            p.update(params)
        
        try:
            r = requests.get(url, params=p, timeout=15)
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            log.error(f"Erreur Ergast {url}: {e}")
            break
        
        # Navigation dans la réponse Ergast (structure MRData)
        mr_data  = data.get("MRData", {})
        total    = int(mr_data.get("total", 0))
        
        # Chercher la clé de données (varie selon l'endpoint)
        table = mr_data.get("RaceTable") \
             or mr_data.get("DriverTable") \
             or mr_data.get("ConstructorTable") \
             or mr_data.get("CircuitTable") \
             or mr_data.get("StandingsTable") \
             or mr_data.get("QualifyingTable") \
             or mr_data.get("LapTable") \
             or mr_data.get("PitStopTable") \
             or {}
        
        # Récupérer la première liste dans la table
        items = []
        for key, val in table.items():
            if isinstance(val, list):
                items = val
                break
        
        all_results.extend(items)
        
        offset += limit
        if offset >= total:
            break
        
        time.sleep(DELAY)
    
    return all_results


def get_openf1(endpoint: str, params: dict = None) -> list:
    """
    Appel générique à l'API OpenF1.
    
    Args:
        endpoint : ex. "/drivers" ou "/sessions"
        params   : filtres (session_key, driver_number, etc.)
    
    Returns:
        liste JSON de résultats
    """
    url = f"{OPENF1_BASE}{endpoint}"
    try:
        r = requests.get(url, params=params or {}, timeout=20)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        log.error(f"Erreur OpenF1 {url}: {e}")
        return []


def save_raw(df: pd.DataFrame, name: str) -> None:
    """Sauvegarde un DataFrame en CSV dans data/raw/."""
    path = DATA_DIR / f"{name}.csv"
    df.to_csv(path, index=False, encoding="utf-8")
    log.info(f"  ✅ Sauvegardé : {path}  ({len(df)} lignes)")


# ══════════════════════════════════════════════════════════════════════════════
#  COLLECTE ERGAST
# ══════════════════════════════════════════════════════════════════════════════

def collecter_pilotes() -> pd.DataFrame:
    """Collecte tous les pilotes depuis Ergast."""
    log.info("📡 [Ergast] Collecte des pilotes...")
    
    pilotes = get_ergast("/drivers")
    rows = []
    for d in pilotes:
        rows.append({
            "driver_id"       : d.get("driverId"),
            "code"            : d.get("code"),
            "numero"          : d.get("permanentNumber"),
            "prenom"          : d.get("givenName"),
            "nom"             : d.get("familyName"),
            "nom_complet"     : f"{d.get('givenName','')} {d.get('familyName','')}".strip(),
            "date_naissance"  : d.get("dateOfBirth"),
            "nationalite"     : d.get("nationality"),
            "url_wikipedia"   : d.get("url"),
        })
    
    df = pd.DataFrame(rows)
    log.info(f"  → {len(df)} pilotes collectés")
    save_raw(df, "pilotes")
    return df


def collecter_ecuries() -> pd.DataFrame:
    """Collecte tous les constructeurs depuis Ergast."""
    log.info("📡 [Ergast] Collecte des écuries...")
    
    ecuries = get_ergast("/constructors")
    rows = []
    for c in ecuries:
        rows.append({
            "constructor_id" : c.get("constructorId"),
            "nom"            : c.get("name"),
            "nationalite"    : c.get("nationality"),
            "url_wikipedia"  : c.get("url"),
        })
    
    df = pd.DataFrame(rows)
    log.info(f"  → {len(df)} écuries collectées")
    save_raw(df, "ecuries")
    return df


def collecter_circuits() -> pd.DataFrame:
    """Collecte tous les circuits depuis Ergast."""
    log.info("📡 [Ergast] Collecte des circuits...")
    
    circuits = get_ergast("/circuits")
    rows = []
    for c in circuits:
        loc = c.get("Location", {})
        rows.append({
            "circuit_id"   : c.get("circuitId"),
            "nom"          : c.get("circuitName"),
            "localite"     : loc.get("locality"),
            "pays"         : loc.get("country"),
            "latitude"     : loc.get("lat"),
            "longitude"    : loc.get("long"),
            "altitude"     : loc.get("alt"),
            "url_wikipedia": c.get("url"),
        })
    
    df = pd.DataFrame(rows)
    log.info(f"  → {len(df)} circuits collectés")
    save_raw(df, "circuits")
    return df


def collecter_calendrier(saisons: list) -> pd.DataFrame:
    """Collecte le calendrier des courses pour les saisons données."""
    log.info(f"📡 [Ergast] Collecte du calendrier ({saisons[0]}–{saisons[-1]})...")
    
    rows = []
    for saison in saisons:
        races = get_ergast(f"/{saison}")
        for r in races:
            circuit = r.get("Circuit", {})
            loc     = circuit.get("Location", {})
            rows.append({
                "saison"        : r.get("season"),
                "round"         : r.get("round"),
                "nom_gp"        : r.get("raceName"),
                "circuit_id"    : circuit.get("circuitId"),
                "circuit_nom"   : circuit.get("circuitName"),
                "localite"      : loc.get("locality"),
                "pays"          : loc.get("country"),
                "date_course"   : r.get("date"),
                "heure_course"  : r.get("time", ""),
                "date_quali"    : r.get("Qualifying", {}).get("date", ""),
                "date_sprint"   : r.get("Sprint", {}).get("date", ""),
            })
        time.sleep(DELAY)
    
    df = pd.DataFrame(rows)
    log.info(f"  → {len(df)} courses collectées")
    save_raw(df, "calendrier")
    return df


def collecter_resultats(saisons: list) -> pd.DataFrame:
    """Collecte les résultats de toutes les courses."""
    log.info(f"📡 [Ergast] Collecte des résultats ({saisons[0]}–{saisons[-1]})...")
    
    rows = []
    for saison in saisons:
        races = get_ergast(f"/{saison}/results")
        for race in races:
            for res in race.get("Results", []):
                driver      = res.get("Driver", {})
                constructor = res.get("Constructor", {})
                fastest     = res.get("FastestLap", {})
                speed       = fastest.get("AverageSpeed", {})
                rows.append({
                    "saison"          : race.get("season"),
                    "round"           : race.get("round"),
                    "nom_gp"          : race.get("raceName"),
                    "circuit_id"      : race.get("Circuit", {}).get("circuitId"),
                    "date_course"     : race.get("date"),
                    "driver_id"       : driver.get("driverId"),
                    "code_pilote"     : driver.get("code"),
                    "constructor_id"  : constructor.get("constructorId"),
                    "position"        : res.get("position"),
                    "points"          : res.get("points"),
                    "grille_depart"   : res.get("grid"),
                    "tours_completes" : res.get("laps"),
                    "statut"          : res.get("status"),
                    "temps_total"     : res.get("Time", {}).get("time", ""),
                    "millis_total"    : res.get("Time", {}).get("millis", ""),
                    "fastest_lap_rank": fastest.get("rank", ""),
                    "fastest_lap_tour": fastest.get("lap", ""),
                    "fastest_lap_time": fastest.get("Time", {}).get("time", ""),
                    "fastest_lap_vitesse": speed.get("speed", ""),
                })
        time.sleep(DELAY)
    
    df = pd.DataFrame(rows)
    log.info(f"  → {len(df)} résultats collectés")
    save_raw(df, "resultats")
    return df


def collecter_qualifications(saisons: list) -> pd.DataFrame:
    """Collecte les résultats de qualifications."""
    log.info(f"📡 [Ergast] Collecte des qualifications ({saisons[0]}–{saisons[-1]})...")
    
    rows = []
    for saison in saisons:
        races = get_ergast(f"/{saison}/qualifying")
        for race in races:
            for q in race.get("QualifyingResults", []):
                driver      = q.get("Driver", {})
                constructor = q.get("Constructor", {})
                rows.append({
                    "saison"        : race.get("season"),
                    "round"         : race.get("round"),
                    "nom_gp"        : race.get("raceName"),
                    "circuit_id"    : race.get("Circuit", {}).get("circuitId"),
                    "driver_id"     : driver.get("driverId"),
                    "code_pilote"   : driver.get("code"),
                    "constructor_id": constructor.get("constructorId"),
                    "position"      : q.get("position"),
                    "q1"            : q.get("Q1", ""),
                    "q2"            : q.get("Q2", ""),
                    "q3"            : q.get("Q3", ""),
                })
        time.sleep(DELAY)
    
    df = pd.DataFrame(rows)
    log.info(f"  → {len(df)} résultats de qualif collectés")
    save_raw(df, "qualifications")
    return df


def collecter_classements_pilotes(saisons: list) -> pd.DataFrame:
    """Collecte les classements pilotes en fin de saison."""
    log.info(f"📡 [Ergast] Collecte classements pilotes ({saisons[0]}–{saisons[-1]})...")
    
    rows = []
    for saison in saisons:
        data = get_ergast(f"/{saison}/driverStandings")
        for standing_list in data:
            for s in standing_list.get("DriverStandings", []):
                driver = s.get("Driver", {})
                constructors = s.get("Constructors", [{}])
                rows.append({
                    "saison"        : standing_list.get("season"),
                    "round"         : standing_list.get("round"),
                    "driver_id"     : driver.get("driverId"),
                    "code_pilote"   : driver.get("code"),
                    "position"      : s.get("position"),
                    "points"        : s.get("points"),
                    "victoires"     : s.get("wins"),
                    "constructor_id": constructors[0].get("constructorId", "") if constructors else "",
                })
        time.sleep(DELAY)
    
    df = pd.DataFrame(rows)
    log.info(f"  → {len(df)} classements pilotes collectés")
    save_raw(df, "classements_pilotes")
    return df


def collecter_classements_ecuries(saisons: list) -> pd.DataFrame:
    """Collecte les classements constructeurs en fin de saison."""
    log.info(f"📡 [Ergast] Collecte classements constructeurs ({saisons[0]}–{saisons[-1]})...")
    
    rows = []
    for saison in saisons:
        data = get_ergast(f"/{saison}/constructorStandings")
        for standing_list in data:
            for s in standing_list.get("ConstructorStandings", []):
                constructor = s.get("Constructor", {})
                rows.append({
                    "saison"        : standing_list.get("season"),
                    "round"         : standing_list.get("round"),
                    "constructor_id": constructor.get("constructorId"),
                    "nom_ecurie"    : constructor.get("name"),
                    "position"      : s.get("position"),
                    "points"        : s.get("points"),
                    "victoires"     : s.get("wins"),
                })
        time.sleep(DELAY)
    
    df = pd.DataFrame(rows)
    log.info(f"  → {len(df)} classements constructeurs collectés")
    save_raw(df, "classements_ecuries")
    return df


def collecter_pit_stops(saisons: list) -> pd.DataFrame:
    """Collecte les données de pit stops."""
    log.info(f"📡 [Ergast] Collecte des pit stops ({saisons[0]}–{saisons[-1]})...")
    
    rows = []
    for saison in saisons:
        # D'abord récupérer le nombre de rounds de la saison
        races = get_ergast(f"/{saison}")
        for race in races:
            rnd = race.get("round")
            pitstops = get_ergast(f"/{saison}/{rnd}/pitstops")
            for ps_race in pitstops:
                for ps in ps_race.get("PitStops", []):
                    rows.append({
                        "saison"   : saison,
                        "round"    : rnd,
                        "driver_id": ps.get("driverId"),
                        "arret_n"  : ps.get("stop"),
                        "tour"     : ps.get("lap"),
                        "heure"    : ps.get("time"),
                        "duree"    : ps.get("duration"),
                    })
            time.sleep(DELAY)
    
    df = pd.DataFrame(rows)
    log.info(f"  → {len(df)} pit stops collectés")
    save_raw(df, "pit_stops")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  COLLECTE OPENF1 (données temps-réel / récentes)
# ══════════════════════════════════════════════════════════════════════════════

def collecter_sessions_openf1(annee: int = 2024) -> pd.DataFrame:
    """Collecte les sessions OpenF1 pour une année donnée."""
    log.info(f"📡 [OpenF1] Collecte des sessions {annee}...")
    
    data = get_openf1("/sessions", {"year": annee})
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    # Colonnes utiles : session_key, session_name, session_type, date_start,
    #                   date_end, location, country_name, circuit_short_name, year
    cols_keep = [
        "session_key", "session_name", "session_type", "year",
        "date_start", "date_end",
        "location", "country_name", "country_code",
        "circuit_key", "circuit_short_name",
    ]
    cols_keep = [c for c in cols_keep if c in df.columns]
    df = df[cols_keep]
    
    log.info(f"  → {len(df)} sessions collectées")
    save_raw(df, f"sessions_openf1_{annee}")
    return df


def collecter_pilotes_openf1(session_key: int) -> pd.DataFrame:
    """
    Collecte les pilotes d'une session OpenF1.
    
    Args:
        session_key : identifiant de session (récupéré via collecter_sessions_openf1)
    """
    log.info(f"📡 [OpenF1] Pilotes session {session_key}...")
    
    data = get_openf1("/drivers", {"session_key": session_key})
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    log.info(f"  → {len(df)} pilotes dans la session {session_key}")
    save_raw(df, f"pilotes_session_{session_key}")
    return df


def collecter_positions_openf1(session_key: int, driver_number: int = None) -> pd.DataFrame:
    """
    Collecte les données de position en piste.
    ⚠️  Volume élevé — filtrer par pilote recommandé.
    """
    log.info(f"📡 [OpenF1] Positions session {session_key}" +
             (f" pilote {driver_number}" if driver_number else "") + "...")
    
    params = {"session_key": session_key}
    if driver_number:
        params["driver_number"] = driver_number
    
    data = get_openf1("/position", params)
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    log.info(f"  → {len(df)} positions collectées")
    return df


def collecter_meteo_openf1(session_key: int) -> pd.DataFrame:
    """Collecte les données météo d'une session."""
    log.info(f"📡 [OpenF1] Météo session {session_key}...")
    
    data = get_openf1("/weather", {"session_key": session_key})
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    log.info(f"  → {len(df)} mesures météo collectées")
    save_raw(df, f"meteo_session_{session_key}")
    return df


def collecter_tours_openf1(session_key: int, driver_number: int = None) -> pd.DataFrame:
    """Collecte les temps au tour d'une session."""
    log.info(f"📡 [OpenF1] Temps au tour session {session_key}...")
    
    params = {"session_key": session_key}
    if driver_number:
        params["driver_number"] = driver_number
    
    data = get_openf1("/laps", params)
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    log.info(f"  → {len(df)} tours collectés")
    save_raw(df, f"tours_session_{session_key}")
    return df


def collecter_radio_pilotes_openf1(session_key: int) -> pd.DataFrame:
    """Collecte les communications radio des pilotes."""
    log.info(f"📡 [OpenF1] Radio session {session_key}...")
    
    data = get_openf1("/team_radio", {"session_key": session_key})
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    log.info(f"  → {len(df)} messages radio collectés")
    save_raw(df, f"radio_session_{session_key}")
    return df


def collecter_pit_stops_openf1(session_key: int) -> pd.DataFrame:
    """Collecte les pit stops OpenF1 pour une session."""
    log.info(f"📡 [OpenF1] Pit stops session {session_key}...")
    
    data = get_openf1("/pit", {"session_key": session_key})
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    log.info(f"  → {len(df)} pit stops collectés")
    save_raw(df, f"pitstops_session_{session_key}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def run_collecte_complete(saisons=None, annee_openf1=2024):
    """
    Lance la collecte complète de toutes les données.
    
    Args:
        saisons      : liste d'années pour Ergast (défaut: 2018-2025)
        annee_openf1 : année pour les sessions OpenF1
    """
    if saisons is None:
        saisons = SAISONS
    
    log.info("=" * 60)
    log.info(f"🏎️  DÉMARRAGE COLLECTE F1  —  {datetime.now():%Y-%m-%d %H:%M}")
    log.info("=" * 60)
    
    # ── Ergast : données de référence (une seule fois) ────────────────
    log.info("\n── DONNÉES DE RÉFÉRENCE ─────────────────────────────────────")
    df_pilotes   = collecter_pilotes()
    df_ecuries   = collecter_ecuries()
    df_circuits  = collecter_circuits()
    
    # ── Ergast : données par saison ───────────────────────────────────
    log.info(f"\n── DONNÉES SAISONNIÈRES ({saisons[0]}–{saisons[-1]}) ─────────────")
    df_calendrier  = collecter_calendrier(saisons)
    df_resultats   = collecter_resultats(saisons)
    df_qualifs     = collecter_qualifications(saisons)
    df_class_pilots = collecter_classements_pilotes(saisons)
    df_class_ecs   = collecter_classements_ecuries(saisons)
    
    # ── Ergast : pit stops (plus long — requête par round) ────────────
    log.info(f"\n── PIT STOPS ────────────────────────────────────────────────")
    df_pits = collecter_pit_stops(saisons[-2:])   # seulement 2 dernières saisons
    
    # ── OpenF1 : données récentes ─────────────────────────────────────
    log.info(f"\n── OPENF1 — SAISON {annee_openf1} ───────────────────────────────")
    df_sessions = collecter_sessions_openf1(annee_openf1)
    
    # Pour la dernière course de la saison (exemple)
    if not df_sessions.empty:
        # Récupérer la clé de la dernière session de course
        courses = df_sessions[df_sessions.get("session_type", pd.Series()) == "Race"]
        if not courses.empty:
            last_session_key = int(courses.iloc[-1]["session_key"])
            log.info(f"\n── SESSION DÉTAILLÉE (session_key={last_session_key}) ────────────")
            collecter_pilotes_openf1(last_session_key)
            collecter_meteo_openf1(last_session_key)
            collecter_tours_openf1(last_session_key)
            collecter_pit_stops_openf1(last_session_key)
    
    log.info("\n" + "=" * 60)
    log.info("✅  COLLECTE TERMINÉE")
    log.info(f"📁  Fichiers dans : {DATA_DIR.resolve()}")
    log.info("=" * 60)
    
    return {
        "pilotes"           : df_pilotes,
        "ecuries"           : df_ecuries,
        "circuits"          : df_circuits,
        "calendrier"        : df_calendrier,
        "resultats"         : df_resultats,
        "qualifications"    : df_qualifs,
        "classements_pilotes": df_class_pilots,
        "classements_ecuries": df_class_ecs,
        "pit_stops"         : df_pits,
        "sessions_openf1"   : df_sessions,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  COLLECTE LÉGÈRE (pour tests rapides)
# ══════════════════════════════════════════════════════════════════════════════

def run_collecte_test():
    """
    Collecte légère pour tester les connexions API.
    Seulement la saison 2024 + données de référence.
    """
    log.info("🧪 MODE TEST — Saison 2024 seulement")
    saisons_test = [2024]
    
    df_pilotes  = collecter_pilotes()
    df_ecuries  = collecter_ecuries()
    df_circuits = collecter_circuits()
    df_cal      = collecter_calendrier(saisons_test)
    df_res      = collecter_resultats(saisons_test)
    
    log.info("\n── OPENF1 TEST ─────────────────────────────────────────────")
    df_sessions = collecter_sessions_openf1(2024)
    
    log.info("✅  Test terminé — vérifiez data/raw/")
    return df_res


# ══════════════════════════════════════════════════════════════════════════════
#  POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    
    mode = sys.argv[1] if len(sys.argv) > 1 else "test"
    
    if mode == "test":
        # Collecte rapide pour valider les APIs
        run_collecte_test()
    
    elif mode == "complet":
        # Collecte complète 2018-2025
        run_collecte_complete(saisons=list(range(2018, 2026)))
    
    elif mode == "recent":
        # Seulement les 3 dernières saisons
        run_collecte_complete(saisons=[2022, 2023, 2024])
    
    else:
        print("Usage : python 01_collecte_donnees.py [test|complet|recent]")
        print("  test    → saison 2024 uniquement (rapide)")
        print("  recent  → saisons 2022-2024")
        print("  complet → saisons 2018-2025 (long ~30 min)")
