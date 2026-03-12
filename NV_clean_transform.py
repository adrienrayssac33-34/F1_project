"""
Script 4 — Nettoyage et transformation multi-saisons
Transforme les JSON bruts en DataFrames propres.
Fusionne toutes les saisons en fichiers CSV unifiés.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
import json
import re
from datetime import datetime
from config import cfg


# ── UTILITAIRES ──────────────────────────────────────────────

def load_json(filename: str) -> list:
    path = os.path.join(cfg.RAW_DIR, filename)
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_csv(df: pd.DataFrame, filename: str):
    path = os.path.join(cfg.CLEAN_DIR, filename)
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"    ✓ {filename}  →  {df.shape[0]:,} lignes × {df.shape[1]} col.")


# ── NETTOYAGE JOLPI ──────────────────────────────────────────

def clean_race_results(seasons: list) -> pd.DataFrame:
    """Fusionne et nettoie les résultats de toutes les saisons."""
    print(f"\n[CLEAN] Résultats de courses — saisons {seasons}")
    frames = []
    for year in seasons:
        data = load_json(f"jolpi_results_{year}.json")
        if not data:
            print(f"    ⚠️  Pas de données pour {year}")
            continue
        rows = []
        for r in data:
            drv  = r.get("Driver", {})
            cons = r.get("Constructor", {})
            fl   = r.get("FastestLap", {})
            t    = r.get("Time", {})
            rows.append({
                # Course
                "season":            r.get("season"),
                "round":             r.get("round"),
                "gp_name":           r.get("raceName"),
                "date":              r.get("date"),
                "circuit_id":        r.get("circuitId"),
                "country":           r.get("country"),
                # Pilote
                "driver_id":         drv.get("driverId"),
                "driver_code":       drv.get("code"),
                "driver_forename":   drv.get("givenName"),
                "driver_surname":    drv.get("familyName"),
                "driver_nationality":drv.get("nationality"),
                # Écurie
                "constructor_id":    cons.get("constructorId"),
                "constructor_name":  cons.get("name"),
                "constructor_nationality": cons.get("nationality"),
                # Résultat
                "position":          r.get("position"),
                "grid":              r.get("grid"),
                "laps":              r.get("laps"),
                "status":            r.get("status"),
                "points":            r.get("points"),
                # Temps
                "race_time":         t.get("time"),
                "race_millis":       t.get("millis"),
                # Meilleur tour
                "fastest_lap_rank":  fl.get("rank"),
                "fastest_lap_time":  fl.get("Time", {}).get("time"),
                "fastest_lap_speed": fl.get("AverageSpeed", {}).get("speed"),
            })
        frames.append(pd.DataFrame(rows))

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # Types
    for col in ["season", "round", "grid", "laps"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df["points"]       = pd.to_numeric(df["points"], errors="coerce")
    df["date"]         = pd.to_datetime(df["date"], errors="coerce")
    df["position_num"] = pd.to_numeric(df["position"], errors="coerce").astype("Int64")
    df["finished"]     = df["status"] == "Finished"
    df["on_podium"]    = df["position_num"].isin([1, 2, 3])
    df["in_points"]    = df["position_num"] <= 10

    save_csv(df, "race_results_all.csv")
    return df


def clean_qualifying(seasons: list) -> pd.DataFrame:
    """Fusionne les qualifications."""
    print(f"\n[CLEAN] Qualifications — saisons {seasons}")
    frames = []
    for year in seasons:
        data = load_json(f"jolpi_qualifying_{year}.json")
        if not data:
            continue
        rows = []
        for r in data:
            drv  = r.get("Driver", {})
            cons = r.get("Constructor", {})
            rows.append({
                "season":          r.get("season"),
                "round":           r.get("round"),
                "gp_name":         r.get("raceName"),
                "position":        r.get("position"),
                "driver_id":       drv.get("driverId"),
                "driver_code":     drv.get("code"),
                "driver_surname":  drv.get("familyName"),
                "constructor_id":  cons.get("constructorId"),
                "constructor_name":cons.get("name"),
                "q1":              r.get("Q1"),
                "q2":              r.get("Q2"),
                "q3":              r.get("Q3"),
            })
        frames.append(pd.DataFrame(rows))

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    for col in ["season", "round", "position"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df["pole_position"] = df["position"] == 1

    save_csv(df, "qualifying_all.csv")
    return df


def clean_driver_standings(seasons: list) -> pd.DataFrame:
    """Fusionne les classements pilotes."""
    print(f"\n[CLEAN] Classements pilotes — saisons {seasons}")
    frames = []
    for year in seasons:
        data = load_json(f"jolpi_driver_standings_{year}.json")
        if not data:
            continue
        rows = []
        for s in data:
            drv  = s.get("Driver", {})
            cons = s.get("Constructors", [{}])
            rows.append({
                "season":           s.get("season"),
                "position":         s.get("position"),
                "points":           s.get("points"),
                "wins":             s.get("wins"),
                "driver_id":        drv.get("driverId"),
                "driver_code":      drv.get("code"),
                "driver_forename":  drv.get("givenName"),
                "driver_surname":   drv.get("familyName"),
                "driver_nationality":drv.get("nationality"),
                "constructor":      cons[0].get("name") if cons else None,
            })
        frames.append(pd.DataFrame(rows))

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df["position"] = pd.to_numeric(df["position"], errors="coerce").astype("Int64")
    df["points"]   = pd.to_numeric(df["points"], errors="coerce")
    df["wins"]     = pd.to_numeric(df["wins"], errors="coerce").astype("Int64")
    df["champion"] = df["position"] == 1

    save_csv(df, "driver_standings_all.csv")
    return df


def clean_constructor_standings(seasons: list) -> pd.DataFrame:
    """Fusionne les classements constructeurs."""
    print(f"\n[CLEAN] Classements constructeurs — saisons {seasons}")
    frames = []
    for year in seasons:
        data = load_json(f"jolpi_constructor_standings_{year}.json")
        if not data:
            continue
        rows = []
        for s in data:
            cons = s.get("Constructor", {})
            rows.append({
                "season":           s.get("season"),
                "position":         s.get("position"),
                "points":           s.get("points"),
                "wins":             s.get("wins"),
                "constructor_id":   cons.get("constructorId"),
                "constructor_name": cons.get("name"),
                "constructor_nationality": cons.get("nationality"),
            })
        frames.append(pd.DataFrame(rows))

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df["position"] = pd.to_numeric(df["position"], errors="coerce").astype("Int64")
    df["points"]   = pd.to_numeric(df["points"], errors="coerce")
    df["wins"]     = pd.to_numeric(df["wins"], errors="coerce").astype("Int64")

    save_csv(df, "constructor_standings_all.csv")
    return df


def clean_circuits(seasons: list) -> pd.DataFrame:
    """Circuits uniques toutes saisons confondues."""
    print(f"\n[CLEAN] Circuits")
    all_circuits = []
    for year in seasons:
        data = load_json(f"jolpi_circuits_{year}.json")
        for c in data:
            loc = c.get("Location", {})
            all_circuits.append({
                "circuit_id":   c.get("circuitId"),
                "circuit_name": c.get("circuitName"),
                "locality":     loc.get("locality"),
                "country":      loc.get("country"),
                "latitude":     loc.get("lat"),
                "longitude":    loc.get("long"),
            })

    if not all_circuits:
        return pd.DataFrame()

    df = pd.DataFrame(all_circuits).drop_duplicates(subset=["circuit_id"])
    df["latitude"]  = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    save_csv(df, "circuits_all.csv")
    return df


# ── FEATURES POUR LE MODÈLE IA ───────────────────────────────

def build_ml_features(results_df: pd.DataFrame,
                      quali_df: pd.DataFrame) -> pd.DataFrame:
    """
    Construit la table de features pour le modèle de prédiction.
    Cible : position_num = 1 (victoire en course).

    Features créées :
    - grid             : position sur la grille de départ
    - pole_position    : booléen — partait-il de la pole ?
    - avg_finish_prev3 : moyenne des 3 derniers classements du pilote
    - win_rate_prev    : taux de victoire en carrière (avant cette course)
    - podium_rate_prev : taux de podiums en carrière
    - constructor_wins : victoires du constructeur cette saison (avant cette course)
    - circuit_win_rate : taux de victoires du pilote sur ce circuit
    - season, round    : contexte temporel
    """
    print("\n[CLEAN] Construction des features ML...")

    if results_df.empty:
        print("    ⚠️  Pas de données de résultats")
        return pd.DataFrame()

    df = results_df.copy().sort_values(["season", "round"])

    # ── Feature 1 : position de grille ──────────────────────
    df["grid"] = pd.to_numeric(df["grid"], errors="coerce").fillna(20)

    # ── Feature 2 : pole position (depuis quali) ─────────────
    if not quali_df.empty:
        pole_df = quali_df[quali_df["pole_position"] == True][
            ["season", "round", "driver_id"]
        ].copy()
        pole_df["pole_position"] = True
        df = df.merge(pole_df, on=["season", "round", "driver_id"], how="left")
        df["pole_position"] = df["pole_position"].fillna(False).astype(int)
    else:
        df["pole_position"] = (df["grid"] == 1).astype(int)

    # ── Feature 3 & 4 : stats historiques du pilote ──────────
    df["win"]    = (df["position_num"] == 1).astype(int)
    df["podium"] = (df["position_num"] <= 3).astype(int)

    # Rolling sur les 3 dernières courses (fenêtre glissante)
    def rolling_stat(group, col, window=3):
        return group[col].shift(1).rolling(window, min_periods=1).mean()

    # Sauvegarder driver_id avant le groupby (pandas le retire du df interne)
    driver_id_series = df["driver_id"].copy()

    stats_per_driver = df.groupby("driver_id", group_keys=False).apply(
        lambda g: g.assign(
            avg_finish_prev3 = g["position_num"].shift(1)
                                                .rolling(3, min_periods=1)
                                                .mean(),
            win_rate_prev    = g["win"].shift(1)
                                       .expanding()
                                       .mean(),
            podium_rate_prev = g["podium"].shift(1)
                                          .expanding()
                                          .mean(),
        )
    )
    df = stats_per_driver.copy()

    # Restaurer driver_id si perdu (comportement pandas ≥ 2.0)
    if "driver_id" not in df.columns:
        df["driver_id"] = driver_id_series.values

    # ── Feature 5 : victoires du constructeur (saison en cours) ─
    cons_wins = (df[df["win"] == 1]
                   .groupby(["season", "constructor_id"])
                   .cumcount()
                   .rename("constructor_wins_season"))
    df = df.join(cons_wins)
    df["constructor_wins_season"] = df["constructor_wins_season"].fillna(0)

    # ── Feature 6 : taux de victoire sur ce circuit ──────────
    circuit_wins = (df.groupby(["driver_id", "circuit_id"])
                      .apply(lambda g: g["win"].shift(1).expanding().mean())
                      .reset_index(level=[0, 1], drop=True)
                      .rename("circuit_win_rate"))
    df = df.join(circuit_wins)
    df["circuit_win_rate"] = df["circuit_win_rate"].fillna(0)

    # ── Cible et sélection ───────────────────────────────────
    df["target"] = (df["position_num"] == 1).astype(int)

    feature_cols = [
        "season", "round", "driver_id", "constructor_id", "circuit_id",
        # Features numériques
        "grid", "pole_position",
        "avg_finish_prev3", "win_rate_prev", "podium_rate_prev",
        "constructor_wins_season", "circuit_win_rate",
        # Cible
        "target",
    ]
    feature_cols = [c for c in feature_cols if c in df.columns]
    ml_df = df[feature_cols].dropna(subset=["avg_finish_prev3"])

    save_csv(ml_df, "ml_features.csv")
    print(f"    → {ml_df['target'].sum()} victoires sur {len(ml_df)} entrées "
          f"({ml_df['target'].mean()*100:.1f}% de win rate)")
    return ml_df


# ── RUNNER ───────────────────────────────────────────────────

def run(seasons: list = None):
    seasons = seasons or cfg.SEASONS

    results_df  = clean_race_results(seasons)
    quali_df    = clean_qualifying(seasons)
    clean_driver_standings(seasons)
    clean_constructor_standings(seasons)
    clean_circuits(seasons)

    # Features ML (nécessite résultats + quali)
    if not results_df.empty:
        build_ml_features(results_df, quali_df)

    print(f"\n✅ Nettoyage terminé — {len(seasons)} saison(s) traitées")


if __name__ == "__main__":
    print("=" * 60)
    print("  NETTOYAGE & TRANSFORMATION — MULTI-SAISONS")
    print("=" * 60)
    run()
