"""
Script 7 — Enrichissement IA : prédiction du vainqueur F1
Modèle : RandomForestClassifier (scikit-learn)

Pipeline :
  1. Chargement des features (ml_features.csv)
  2. Encodage des variables catégorielles
  3. Entraînement avec validation croisée temporelle
  4. Évaluation (accuracy, précision, rapport de classification)
  5. Sauvegarde du modèle (.pkl)
  6. Prédiction pour une prochaine course
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
import pickle
import json
from datetime import datetime
from config import cfg

from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import TimeSeriesSplit, cross_val_score
from sklearn.metrics import (classification_report, confusion_matrix,
                              roc_auc_score, precision_score, recall_score)
from sklearn.pipeline import Pipeline
from sklearn.calibration import CalibratedClassifierCV


# ── CHARGEMENT ───────────────────────────────────────────────

def load_features() -> pd.DataFrame:
    path = os.path.join(cfg.CLEAN_DIR, "ml_features.csv")
    if not os.path.exists(path):
        print("  ⚠️  ml_features.csv introuvable — lancez d'abord le script 04")
        return pd.DataFrame()
    df = pd.read_csv(path)
    print(f"  ✓ Features chargées : {df.shape[0]:,} lignes × {df.shape[1]} col.")
    print(f"    Saisons : {sorted(df['season'].unique())}")
    print(f"    Pilotes : {df['driver_id'].nunique()} uniques")
    print(f"    Victoires : {df['target'].sum()} ({df['target'].mean()*100:.1f}%)")
    return df


# ── PRÉPARATION ──────────────────────────────────────────────

FEATURE_COLS = [
    "grid", "pole_position",
    "avg_finish_prev3", "win_rate_prev", "podium_rate_prev",
    "constructor_wins_season", "circuit_win_rate",
    # Encodées
    "driver_enc", "constructor_enc", "circuit_enc",
]


def prepare(df: pd.DataFrame):
    """Encode les variables catégorielles et retourne X, y + encodeurs."""
    df = df.copy()

    encoders = {}
    for col in ["driver_id", "constructor_id", "circuit_id"]:
        le = LabelEncoder()
        enc_col = col.replace("_id", "_enc")
        df[enc_col] = le.fit_transform(df[col].fillna("unknown"))
        encoders[col] = le

    available = [c for c in FEATURE_COLS if c in df.columns]
    X = df[available].fillna(0).values
    y = df["target"].values

    return X, y, encoders, available


# ── ENTRAÎNEMENT ─────────────────────────────────────────────

def train(X, y):
    """
    Entraîne plusieurs modèles et sélectionne le meilleur.
    Utilise TimeSeriesSplit pour respecter l'ordre chronologique.
    """
    print("\n[ML] Entraînement des modèles...")

    tscv = TimeSeriesSplit(n_splits=5)

    candidates = {
        "RandomForest": RandomForestClassifier(
            n_estimators=200,
            max_depth=8,
            min_samples_leaf=5,
            class_weight="balanced",   # Compense le déséquilibre (peu de wins)
            random_state=42,
            n_jobs=-1,
        ),
        "GradientBoosting": GradientBoostingClassifier(
            n_estimators=150,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            random_state=42,
        ),
        "LogisticRegression": LogisticRegression(
            C=0.5,
            class_weight="balanced",
            max_iter=1000,
            random_state=42,
        ),
    }

    results = {}
    best_name, best_score, best_model = None, -1, None

    for name, model in candidates.items():
        scores = cross_val_score(
            model, X, y,
            cv=tscv,
            scoring="roc_auc",
            n_jobs=-1,
        )
        mean_auc = scores.mean()
        results[name] = {"auc_mean": round(mean_auc, 4),
                         "auc_std":  round(scores.std(), 4)}
        print(f"    {name:<25} AUC = {mean_auc:.4f} ± {scores.std():.4f}")

        if mean_auc > best_score:
            best_score = mean_auc
            best_name  = name
            best_model = model

    print(f"\n  🏆 Meilleur modèle : {best_name} (AUC = {best_score:.4f})")

    # Entraîner le meilleur modèle sur toutes les données
    # + calibrer les probabilités (Platt scaling)
    calibrated = CalibratedClassifierCV(best_model, cv=3, method="sigmoid")
    calibrated.fit(X, y)

    return calibrated, best_name, results


# ── ÉVALUATION ───────────────────────────────────────────────

def evaluate(model, X, y, feature_names: list):
    """Affiche les métriques d'évaluation sur le jeu complet."""
    print("\n[ML] Évaluation sur l'ensemble d'entraînement...")

    y_pred  = model.predict(X)
    y_proba = model.predict_proba(X)[:, 1]

    auc = roc_auc_score(y, y_proba)
    pre = precision_score(y, y_pred, zero_division=0)
    rec = recall_score(y, y_pred, zero_division=0)

    print(f"    AUC-ROC   : {auc:.4f}")
    print(f"    Précision : {pre:.4f}")
    print(f"    Rappel    : {rec:.4f}")
    print()
    print(classification_report(y, y_pred,
                                 target_names=["Pas vainqueur", "Vainqueur"],
                                 zero_division=0))

    # Importance des features (si RandomForest accessible)
    try:
        base = model.estimator if hasattr(model, "estimator") else None
        if base and hasattr(base, "feature_importances_"):
            importances = pd.Series(base.feature_importances_, index=feature_names)
            print("  📊 Importance des features :")
            for feat, imp in importances.sort_values(ascending=False).items():
                bar = "█" * int(imp * 40)
                print(f"    {feat:<30} {bar} {imp:.3f}")
    except Exception:
        pass

    return {"auc": auc, "precision": pre, "recall": rec}


# ── SAUVEGARDE ───────────────────────────────────────────────

def save_model(model, encoders: dict, feature_names: list,
               metrics: dict, model_name: str):
    """Sauvegarde le modèle et ses métadonnées."""
    os.makedirs(cfg.MODEL_DIR, exist_ok=True)

    # Modèle binaire
    model_path = cfg.MODEL_PATH
    with open(model_path, "wb") as f:
        pickle.dump({
            "model":         model,
            "encoders":      encoders,
            "feature_names": feature_names,
            "model_name":    model_name,
            "trained_at":    datetime.now().isoformat(),
        }, f)
    print(f"\n  ✓ Modèle sauvegardé : {model_path}")

    # Métadonnées JSON (lisibles sans pickle)
    meta_path = model_path.replace(".pkl", "_meta.json")
    with open(meta_path, "w") as f:
        json.dump({
            "model_name":    model_name,
            "feature_names": feature_names,
            "metrics":       metrics,
            "trained_at":    datetime.now().isoformat(),
            "drivers":       list(encoders["driver_id"].classes_),
            "circuits":      list(encoders["circuit_id"].classes_),
        }, f, indent=2)
    print(f"  ✓ Métadonnées   : {meta_path}")


# ── PRÉDICTION ───────────────────────────────────────────────

def load_model() -> dict | None:
    """Charge le modèle sauvegardé."""
    if not os.path.exists(cfg.MODEL_PATH):
        print(f"  ⚠️  Modèle introuvable : {cfg.MODEL_PATH}")
        return None
    with open(cfg.MODEL_PATH, "rb") as f:
        return pickle.load(f)


def predict_race(driver_stats: list[dict]) -> pd.DataFrame:
    """
    Prédit les probabilités de victoire pour une liste de pilotes.

    Paramètre driver_stats : liste de dicts, un par pilote, avec les mêmes
    colonnes que FEATURE_COLS (avant encodage).

    Exemple :
        predict_race([
            {"driver_id": "verstappen", "constructor_id": "red_bull",
             "circuit_id": "bahrain", "grid": 1, "pole_position": 1,
             "avg_finish_prev3": 2.1, "win_rate_prev": 0.45,
             "podium_rate_prev": 0.78, "constructor_wins_season": 3,
             "circuit_win_rate": 0.5},
            ...
        ])
    """
    bundle = load_model()
    if bundle is None:
        return pd.DataFrame()

    model         = bundle["model"]
    encoders      = bundle["encoders"]
    feature_names = bundle["feature_names"]

    df = pd.DataFrame(driver_stats)

    # Encodage
    for col in ["driver_id", "constructor_id", "circuit_id"]:
        le      = encoders[col]
        enc_col = col.replace("_id", "_enc")
        known   = set(le.classes_)
        df[enc_col] = df[col].apply(
            lambda v: le.transform([v])[0] if v in known else -1
        )

    available = [c for c in feature_names if c in df.columns]
    X = df[available].fillna(0).values

    probs = model.predict_proba(X)[:, 1]
    df["win_probability"] = probs
    df = df.sort_values("win_probability", ascending=False).reset_index(drop=True)
    df["predicted_rank"] = df.index + 1

    return df[["predicted_rank", "driver_id", "constructor_id",
               "win_probability", "grid", "pole_position"]].round({"win_probability": 4})


# ── DÉMONSTRATION ────────────────────────────────────────────

def demo_prediction(encoders: dict):
    """Prédit les favoris pour un prochain GP avec des données exemple."""
    print("\n[ML] 🔮 Démonstration — Prédiction GP Bahrain 2025")
    print("  (données hypothétiques basées sur les stats 2024)\n")

    known_drivers     = list(encoders["driver_id"].classes_)
    known_circuits    = list(encoders["circuit_id"].classes_)
    known_constructors= list(encoders["constructor_id"].classes_)

    # Quelques pilotes avec leurs stats récentes estimées
    demo_grid = [
        {"driver_id": "verstappen",  "constructor_id": "red_bull",
         "circuit_id": "bahrain",    "grid": 1, "pole_position": 1,
         "avg_finish_prev3": 1.8, "win_rate_prev": 0.45,
         "podium_rate_prev": 0.75, "constructor_wins_season": 0,
         "circuit_win_rate": 0.5},
        {"driver_id": "norris",      "constructor_id": "mclaren",
         "circuit_id": "bahrain",    "grid": 2, "pole_position": 0,
         "avg_finish_prev3": 2.5, "win_rate_prev": 0.18,
         "podium_rate_prev": 0.55, "constructor_wins_season": 0,
         "circuit_win_rate": 0.1},
        {"driver_id": "leclerc",     "constructor_id": "ferrari",
         "circuit_id": "bahrain",    "grid": 3, "pole_position": 0,
         "avg_finish_prev3": 3.2, "win_rate_prev": 0.12,
         "podium_rate_prev": 0.48, "constructor_wins_season": 0,
         "circuit_win_rate": 0.15},
        {"driver_id": "russell",     "constructor_id": "mercedes",
         "circuit_id": "bahrain",    "grid": 4, "pole_position": 0,
         "avg_finish_prev3": 4.1, "win_rate_prev": 0.08,
         "podium_rate_prev": 0.35, "constructor_wins_season": 0,
         "circuit_win_rate": 0.05},
        {"driver_id": "hamilton",    "constructor_id": "ferrari",
         "circuit_id": "bahrain",    "grid": 5, "pole_position": 0,
         "avg_finish_prev3": 4.8, "win_rate_prev": 0.33,
         "podium_rate_prev": 0.60, "constructor_wins_season": 0,
         "circuit_win_rate": 0.12},
    ]

    # Filtrer uniquement les pilotes/circuits connus du modèle
    demo_grid = [
        d for d in demo_grid
        if d["driver_id"] in known_drivers and d["circuit_id"] in known_circuits
    ]

    if not demo_grid:
        print("  ⚠️  Aucun pilote connu du modèle dans la démo.")
        print(f"  Pilotes connus : {known_drivers[:5]}...")
        return

    results = predict_race(demo_grid)
    if results.empty:
        return

    print(f"  {'Rang':<5} {'Pilote':<20} {'Écurie':<20} {'Prob. victoire':>15}")
    print("  " + "─" * 65)
    for _, row in results.iterrows():
        bar = "█" * int(row["win_probability"] * 30)
        prob_pct = f"{row['win_probability']*100:.1f}%"
        print(f"  {int(row['predicted_rank']):<5} "
              f"{row['driver_id']:<20} "
              f"{row['constructor_id']:<20} "
              f"{prob_pct:>8}  {bar}")


# ── RUNNER ───────────────────────────────────────────────────

def run():
    print("=" * 60)
    print("  ENRICHISSEMENT IA — PRÉDICTION DU VAINQUEUR F1")
    print("=" * 60)

    # 1. Charger les features
    df = load_features()
    if df.empty:
        return

    # 2. Préparer X, y
    X, y, encoders, feature_names = prepare(df)
    print(f"\n  Features utilisées : {feature_names}")
    print(f"  Taille X : {X.shape}")

    # 3. Entraîner
    model, model_name, cv_results = train(X, y)

    # 4. Évaluer
    metrics = evaluate(model, X, y, feature_names)

    # 5. Sauvegarder
    save_model(model, encoders, feature_names, metrics, model_name)

    # 6. Démo prédiction
    demo_prediction(encoders)

    print("\n✅ Module IA terminé !")


if __name__ == "__main__":
    run()
