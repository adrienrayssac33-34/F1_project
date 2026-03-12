"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          PROJET 3 — WILD DATA HUB · API FORMULE 1                           ║
║          Phase 2 : Nettoyage et prétraitement des données                    ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import pandas as pd
import numpy as np
import re
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("F1_Cleaner")

RAW_DIR    = Path("data/raw")
CLEAN_DIR  = Path("data/clean")
CLEAN_DIR.mkdir(parents=True, exist_ok=True)


# ── Utilitaires ────────────────────────────────────────────────────────────

def load_raw(name: str) -> pd.DataFrame:
    path = RAW_DIR / f"{name}.csv"
    if not path.exists():
        log.warning(f"  ⚠️  Fichier introuvable : {path}")
        return pd.DataFrame()
    df = pd.read_csv(path, low_memory=False)
    log.info(f"  📂 Chargé : {name} ({len(df)} lignes, {len(df.columns)} colonnes)")
    return df

def save_clean(df: pd.DataFrame, name: str):
    path = CLEAN_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    log.info(f"  ✅ Nettoyé : {path}  ({len(df)} lignes)")

def lap_time_to_seconds(t: str) -> float:
    """Convertit '1:23.456' ou '83.456' en secondes float."""
    if pd.isna(t) or t == "" or t == "\\N":
        return np.nan
    t = str(t).strip()
    # Format m:ss.mmm
    m = re.match(r"^(\d+):(\d+\.\d+)$", t)
    if m:
        return int(m.group(1)) * 60 + float(m.group(2))
    # Format ss.mmm
    try:
        return float(t)
    except ValueError:
        return np.nan

def pit_duration_to_seconds(d: str) -> float:
    """Convertit la durée pit stop (peut être '23.456' ou '1:03.456')."""
    return lap_time_to_seconds(d)

def clean_position(val) -> float:
    """Nettoie les positions (peut contenir 'R', 'D', 'N', etc.)."""
    try:
        return float(val)
    except (ValueError, TypeError):
        return np.nan


# ══════════════════════════════════════════════════════════════════════════════
#  NETTOYAGE PAR TABLE
# ══════════════════════════════════════════════════════════════════════════════

def nettoyer_pilotes() -> pd.DataFrame:
    log.info("🧹 Nettoyage : pilotes")
    df = load_raw("pilotes")
    if df.empty: return df

    # Normalisation
    df["prenom"]   = df["prenom"].str.strip().str.title()
    df["nom"]      = df["nom"].str.strip().str.upper()
    df["nom_complet"] = df["prenom"] + " " + df["nom"]

    # Date de naissance → datetime
    df["date_naissance"] = pd.to_datetime(df["date_naissance"], errors="coerce")

    # Nationalité → normalisée
    df["nationalite"] = df["nationalite"].str.strip().str.title()

    # Numéro permanent → entier (peut être vide pour anciens pilotes)
    df["numero"] = pd.to_numeric(df["numero"], errors="coerce").astype("Int64")

    # Supprimer les doublons sur driver_id
    before = len(df)
    df = df.drop_duplicates(subset="driver_id")
    if len(df) < before:
        log.info(f"  ℹ️  {before - len(df)} doublons supprimés")

    log.info(f"  Nulls par colonne:\n{df.isnull().sum()[df.isnull().sum() > 0].to_string()}")
    save_clean(df, "pilotes")
    return df


def nettoyer_ecuries() -> pd.DataFrame:
    log.info("🧹 Nettoyage : écuries")
    df = load_raw("ecuries")
    if df.empty: return df

    df["nom"]         = df["nom"].str.strip()
    df["nationalite"] = df["nationalite"].str.strip().str.title()
    df = df.drop_duplicates(subset="constructor_id")

    save_clean(df, "ecuries")
    return df


def nettoyer_circuits() -> pd.DataFrame:
    log.info("🧹 Nettoyage : circuits")
    df = load_raw("circuits")
    if df.empty: return df

    df["nom"]      = df["nom"].str.strip()
    df["localite"] = df["localite"].str.strip()
    df["pays"]     = df["pays"].str.strip().str.title()

    # Convertir lat/lon/alt en float
    for col in ["latitude", "longitude", "altitude"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates(subset="circuit_id")
    save_clean(df, "circuits")
    return df


def nettoyer_calendrier() -> pd.DataFrame:
    log.info("🧹 Nettoyage : calendrier")
    df = load_raw("calendrier")
    if df.empty: return df

    df["saison"]      = df["saison"].astype(int)
    df["round"]       = df["round"].astype(int)
    df["date_course"] = pd.to_datetime(df["date_course"], errors="coerce")
    df["date_quali"]  = pd.to_datetime(df["date_quali"],  errors="coerce")
    df["date_sprint"] = pd.to_datetime(df["date_sprint"], errors="coerce")
    df["nom_gp"]      = df["nom_gp"].str.strip()
    df["pays"]        = df["pays"].str.strip().str.title()

    # Clé unique : saison + round
    df = df.drop_duplicates(subset=["saison", "round"])

    # Dériver : année, mois, semaine de la saison
    df["annee"]    = df["date_course"].dt.year
    df["mois"]     = df["date_course"].dt.month
    df["semaine"]  = df["date_course"].dt.isocalendar().week.astype("Int64")

    save_clean(df, "calendrier")
    return df


def nettoyer_resultats() -> pd.DataFrame:
    log.info("🧹 Nettoyage : résultats de courses")
    df = load_raw("resultats")
    if df.empty: return df

    df["saison"]      = df["saison"].astype(int)
    df["round"]       = df["round"].astype(int)
    df["date_course"] = pd.to_datetime(df["date_course"], errors="coerce")

    # Positions numériques (certains statuts comme 'R' → NaN)
    df["position"]       = df["position"].apply(clean_position).astype("Int64")
    df["grille_depart"]  = df["grille_depart"].apply(clean_position).astype("Int64")
    df["tours_completes"]= pd.to_numeric(df["tours_completes"], errors="coerce").astype("Int64")
    df["points"]         = pd.to_numeric(df["points"], errors="coerce")

    # Temps totaux en secondes
    df["millis_total"]   = pd.to_numeric(df["millis_total"], errors="coerce")
    df["secondes_total"] = df["millis_total"] / 1000

    # Fastest lap
    df["fastest_lap_tour"] = pd.to_numeric(df["fastest_lap_tour"], errors="coerce").astype("Int64")
    df["fastest_lap_rank"] = pd.to_numeric(df["fastest_lap_rank"], errors="coerce").astype("Int64")
    df["fastest_lap_time_sec"] = df["fastest_lap_time"].apply(lap_time_to_seconds)
    df["fastest_lap_vitesse"]  = pd.to_numeric(df["fastest_lap_vitesse"], errors="coerce")

    # Indicateurs dérivés
    df["a_fini"]          = df["statut"].str.lower().isin(["finished", "+1 lap", "+2 laps", "+3 laps", "+4 laps", "+5 laps"])
    df["est_abandon"]     = df["statut"].str.contains("Ret|Accident|Engine|Gearbox|Hydraulics|Electrical|Collision", case=False, na=False)
    df["est_podium"]      = df["position"].isin([1, 2, 3])
    df["est_victoire"]    = df["position"] == 1
    df["positions_gagnees"] = (df["grille_depart"] - df["position"]).astype("Int64")

    # Nettoyage statut (normalisation)
    df["statut"] = df["statut"].str.strip()

    # Supprimer doublons
    df = df.drop_duplicates(subset=["saison", "round", "driver_id"])

    log.info(f"  Taux completion courses : {df['a_fini'].mean():.1%}")
    log.info(f"  Taux abandon : {df['est_abandon'].mean():.1%}")
    save_clean(df, "resultats")
    return df


def nettoyer_qualifications() -> pd.DataFrame:
    log.info("🧹 Nettoyage : qualifications")
    df = load_raw("qualifications")
    if df.empty: return df

    df["saison"]   = df["saison"].astype(int)
    df["round"]    = df["round"].astype(int)
    df["position"] = df["position"].apply(clean_position).astype("Int64")

    # Convertir Q1/Q2/Q3 en secondes
    for q in ["q1", "q2", "q3"]:
        df[f"{q}_sec"] = df[q].apply(lap_time_to_seconds)

    # Meilleur temps de qualif
    df["best_quali_sec"] = df[["q1_sec", "q2_sec", "q3_sec"]].min(axis=1)

    # Colonne : session la plus avancée atteinte
    df["session_max"] = np.where(
        df["q3_sec"].notna(), "Q3",
        np.where(df["q2_sec"].notna(), "Q2", "Q1")
    )

    df = df.drop_duplicates(subset=["saison", "round", "driver_id"])
    save_clean(df, "qualifications")
    return df


def nettoyer_classements(type_: str = "pilotes") -> pd.DataFrame:
    log.info(f"🧹 Nettoyage : classements {type_}")
    name = f"classements_{type_}"
    df = load_raw(name)
    if df.empty: return df

    df["saison"]   = df["saison"].astype(int)
    df["round"]    = pd.to_numeric(df["round"], errors="coerce").astype("Int64")
    df["position"] = pd.to_numeric(df["position"], errors="coerce").astype("Int64")
    df["points"]   = pd.to_numeric(df["points"], errors="coerce")
    df["victoires"]= pd.to_numeric(df["victoires"], errors="coerce").astype("Int64")

    save_clean(df, name)
    return df


def nettoyer_pit_stops() -> pd.DataFrame:
    log.info("🧹 Nettoyage : pit stops")
    df = load_raw("pit_stops")
    if df.empty: return df

    df["saison"]   = df["saison"].astype(int)
    df["round"]    = df["round"].astype(int)
    df["tour"]     = pd.to_numeric(df["tour"], errors="coerce").astype("Int64")
    df["arret_n"]  = pd.to_numeric(df["arret_n"], errors="coerce").astype("Int64")

    # Durée en secondes
    df["duree_sec"] = df["duree"].apply(pit_duration_to_seconds)

    # Outliers durée : >120 sec = probablement arrêt technique ou safety car
    df["arret_normal"] = df["duree_sec"].between(2.0, 60.0)
    log.info(f"  Durée médiane pit stop : {df['duree_sec'].median():.2f}s")
    log.info(f"  Arrêts anormaux (>60s) : {(~df['arret_normal']).sum()}")

    save_clean(df, "pit_stops")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  PIPELINE DE NETTOYAGE COMPLET
# ══════════════════════════════════════════════════════════════════════════════

def run_nettoyage():
    log.info("=" * 60)
    log.info("🧹 DÉMARRAGE PIPELINE NETTOYAGE")
    log.info("=" * 60)

    dfs = {}
    dfs["pilotes"]              = nettoyer_pilotes()
    dfs["ecuries"]              = nettoyer_ecuries()
    dfs["circuits"]             = nettoyer_circuits()
    dfs["calendrier"]           = nettoyer_calendrier()
    dfs["resultats"]            = nettoyer_resultats()
    dfs["qualifications"]       = nettoyer_qualifications()
    dfs["classements_pilotes"]  = nettoyer_classements("pilotes")
    dfs["classements_ecuries"]  = nettoyer_classements("ecuries")
    dfs["pit_stops"]            = nettoyer_pit_stops()

    # Rapport de nettoyage
    log.info("\n── RAPPORT FINAL ────────────────────────────────────────────")
    for name, df in dfs.items():
        if not df.empty:
            nulls = df.isnull().sum().sum()
            log.info(f"  {name:<30} {len(df):>7} lignes | {nulls:>5} nulls restants")

    log.info("\n✅  Nettoyage terminé — fichiers dans data/clean/")
    return dfs


if __name__ == "__main__":
    run_nettoyage()
