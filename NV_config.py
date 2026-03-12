"""
config.py — Chargement centralisé de la configuration
Lit le fichier .env et expose les paramètres à tous les scripts.

Usage dans n'importe quel script :
    from config import cfg
    engine = cfg.get_engine()
    seasons = cfg.SEASONS
"""

import os
from dotenv import load_dotenv

# Charger .env depuis la racine du projet
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"), override=False)


class Config:
    """Configuration centralisée du projet."""

    # ── Répertoires ──────────────────────────────────────────
    BASE_DIR   = BASE_DIR
    DATA_DIR   = os.path.join(BASE_DIR, "data")
    RAW_DIR    = os.path.join(DATA_DIR, "raw")
    CLEAN_DIR  = os.path.join(DATA_DIR, "clean")
    DB_DIR     = os.path.join(DATA_DIR, "db")
    MODEL_DIR  = os.path.join(DATA_DIR, "models")

    # ── Saisons ──────────────────────────────────────────────
    _seasons_raw = os.getenv("SEASONS", "2024")
    SEASONS: list[int] = [int(s.strip()) for s in _seasons_raw.split(",")]

    # ── Paramètres collecte ──────────────────────────────────
    API_DELAY  = float(os.getenv("API_DELAY", "0.3"))
    MAX_ROUNDS = int(os.getenv("MAX_ROUNDS", "0"))   # 0 = tous les GPs

    # ── Modèle IA ────────────────────────────────────────────
    MODEL_PATH            = os.getenv("MODEL_PATH",
                            os.path.join(MODEL_DIR, "winner_predictor.pkl"))
    PREDICTION_THRESHOLD  = float(os.getenv("PREDICTION_THRESHOLD", "0.15"))

    # ── Base de données ──────────────────────────────────────
    DB_MODE = os.getenv("DB_MODE", "sqlite").lower()

    @property
    def DATABASE_URL(self) -> str:
        """Construit l'URL de connexion selon le mode."""
        if self.DB_MODE == "postgresql":
            host     = os.getenv("POSTGRES_HOST", "localhost")
            port     = os.getenv("POSTGRES_PORT", "5432")
            db       = os.getenv("POSTGRES_DB", "f1_hub")
            user     = os.getenv("POSTGRES_USER", "f1_user")
            password = os.getenv("POSTGRES_PASSWORD", "")
            return f"postgresql://{user}:{password}@{host}:{port}/{db}"
        else:
            sqlite_path = os.path.join(self.DB_DIR, "f1_hub.db")
            return f"sqlite:///{sqlite_path}"

    def get_engine(self):
        """Retourne un moteur SQLAlchemy selon le mode configuré."""
        try:
            from sqlalchemy import create_engine
            url = self.DATABASE_URL
            kwargs = {}
            if self.DB_MODE == "postgresql":
                kwargs["pool_pre_ping"] = True
                kwargs["pool_size"] = 5
            engine = create_engine(url, **kwargs)
            return engine
        except ImportError:
            # Fallback sqlite3 natif si SQLAlchemy absent
            import sqlite3
            return sqlite3.connect(os.path.join(self.DB_DIR, "f1_hub.db"))

    def ensure_dirs(self):
        """Crée tous les répertoires nécessaires."""
        for d in [self.RAW_DIR, self.CLEAN_DIR, self.DB_DIR, self.MODEL_DIR]:
            os.makedirs(d, exist_ok=True)

    def summary(self):
        """Affiche la configuration active."""
        print(f"  Mode BDD    : {self.DB_MODE.upper()}")
        print(f"  Saisons     : {self.SEASONS}")
        print(f"  Délai API   : {self.API_DELAY}s")
        print(f"  Max rounds  : {self.MAX_ROUNDS or 'tous'}")
        print(f"  Modèle IA   : {self.MODEL_PATH}")
        if self.DB_MODE == "postgresql":
            host = os.getenv("POSTGRES_HOST", "localhost")
            db   = os.getenv("POSTGRES_DB", "f1_hub")
            user = os.getenv("POSTGRES_USER", "f1_user")
            print(f"  PostgreSQL  : {user}@{host}/{db}")


# Instance globale importable partout
cfg = Config()
cfg.ensure_dirs()
