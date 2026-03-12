"""
Script 5 — Pipeline ETL → PostgreSQL ou SQLite
Utilise config.py pour choisir automatiquement la BDD cible.

Connexion PostgreSQL : renseigner .env avec DB_MODE=postgresql
Connexion SQLite     : DB_MODE=sqlite (par défaut, aucune config)
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import sqlite3
from config import cfg

CLEAN_DIR = cfg.CLEAN_DIR


# ── UTILITAIRES ──────────────────────────────────────────────

def load_csv(filename: str) -> pd.DataFrame:
    path = os.path.join(CLEAN_DIR, filename)
    if not os.path.exists(path):
        print(f"    [SKIP] Fichier manquant : {filename}")
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def load_all_tables() -> dict:
    return {
        "race_results":           load_csv("race_results_all.csv"),
        "qualifying":             load_csv("qualifying_all.csv"),
        "driver_standings":       load_csv("driver_standings_all.csv"),
        "constructor_standings":  load_csv("constructor_standings_all.csv"),
        "circuits":               load_csv("circuits_all.csv"),
        "ml_features":            load_csv("ml_features.csv"),
    }


# ── CHARGEMENT ───────────────────────────────────────────────

def load_sqlite(tables: dict):
    """Charge toutes les tables dans SQLite (sans SQLAlchemy)."""
    db_path = os.path.join(cfg.DB_DIR, "f1_hub.db")
    conn    = sqlite3.connect(db_path)

    for table_name, df in tables.items():
        if df.empty:
            continue
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"    ✓ {table_name:<35}  {len(df):>6} lignes")

    create_sqlite_views(conn)
    conn.commit()

    # Rapport final
    print("\n  Tables & vues en base :")
    rows = conn.execute(
        "SELECT name, type FROM sqlite_master WHERE type IN ('table','view') ORDER BY type, name"
    ).fetchall()
    for name, kind in rows:
        count = conn.execute(f"SELECT COUNT(*) FROM [{name}]").fetchone()[0]
        print(f"    [{kind:<5}] {name:<35}  {count:>6} lignes")

    conn.close()
    print(f"\n  ✓ BDD SQLite : {db_path}")


def load_postgresql(tables: dict):
    """Charge toutes les tables dans PostgreSQL via SQLAlchemy."""
    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        print("  ⚠️  SQLAlchemy non installé — pip install sqlalchemy psycopg2-binary")
        return

    url = cfg.DATABASE_URL
    print(f"  Connexion : {url.split('@')[-1]}")  # Cacher mot de passe

    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("  ✓ Connexion PostgreSQL réussie")
    except Exception as e:
        print(f"  ❌ Connexion impossible : {e}")
        print("  → Vérifiez POSTGRES_HOST / USER / PASSWORD dans .env")
        return

    for table_name, df in tables.items():
        if df.empty:
            continue
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"    ✓ {table_name:<35}  {len(df):>6} lignes")

    create_postgresql_views(engine)
    print(f"\n  ✓ BDD PostgreSQL : {cfg.DATABASE_URL.split('@')[-1]}")


# ── VUES ANALYTIQUES ─────────────────────────────────────────

VIEWS = {
    "v_race_winners": """
        CREATE VIEW {prefix}v_race_winners AS
        SELECT season, round, gp_name, date, country,
               driver_forename || ' ' || driver_surname AS winner,
               constructor_name AS winning_team,
               driver_nationality AS winner_nationality,
               race_time, grid AS winner_start_position, points
        FROM race_results
        WHERE position_num = 1
    """,
    "v_driver_career": """
        CREATE VIEW {prefix}v_driver_career AS
        SELECT driver_id,
               driver_forename || ' ' || driver_surname AS driver_name,
               driver_nationality AS nationality,
               COUNT(DISTINCT season) AS seasons,
               COUNT(*) AS races,
               SUM(CASE WHEN position_num = 1 THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN position_num <= 3 THEN 1 ELSE 0 END) AS podiums,
               SUM(CAST(points AS FLOAT)) AS total_points,
               MIN(season) AS first_season,
               MAX(season) AS last_season
        FROM race_results
        WHERE driver_id IS NOT NULL
        GROUP BY driver_id
    """,
    "v_constructor_career": """
        CREATE VIEW {prefix}v_constructor_career AS
        SELECT constructor_id, constructor_name,
               constructor_nationality AS nationality,
               COUNT(DISTINCT season) AS seasons,
               COUNT(*) AS race_entries,
               SUM(CASE WHEN position_num = 1 THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN position_num <= 3 THEN 1 ELSE 0 END) AS podiums,
               SUM(CAST(points AS FLOAT)) AS total_points
        FROM race_results
        WHERE constructor_id IS NOT NULL
        GROUP BY constructor_id
    """,
    "v_season_summary": """
        CREATE VIEW {prefix}v_season_summary AS
        SELECT r.season,
               COUNT(DISTINCT r.round) AS races,
               COUNT(DISTINCT r.driver_id) AS drivers,
               COUNT(DISTINCT r.constructor_id) AS teams,
               ds.driver_forename || ' ' || ds.driver_surname AS champion,
               ds.constructor AS champion_team,
               ds.points AS champion_points,
               ds.wins AS champion_wins
        FROM race_results r
        LEFT JOIN driver_standings ds
               ON ds.season = r.season AND ds.position = 1
        GROUP BY r.season
    """,
}


def create_sqlite_views(conn: sqlite3.Connection):
    """Crée les vues dans SQLite."""
    for view_name, sql_tpl in VIEWS.items():
        sql = sql_tpl.format(prefix="")
        conn.execute(f"DROP VIEW IF EXISTS {view_name}")
        try:
            conn.execute(sql)
            print(f"    ✓ Vue : {view_name}")
        except Exception as e:
            print(f"    ⚠️  Vue {view_name} : {e}")


def create_postgresql_views(engine):
    """Crée les vues dans PostgreSQL (OR REPLACE)."""
    from sqlalchemy import text
    with engine.connect() as conn:
        for view_name, sql_tpl in VIEWS.items():
            sql = sql_tpl.format(prefix="").replace(
                "CREATE VIEW", "CREATE OR REPLACE VIEW"
            )
            try:
                conn.execute(text(sql))
                conn.commit()
                print(f"    ✓ Vue : {view_name}")
            except Exception as e:
                print(f"    ⚠️  Vue {view_name} : {e}")


# ── RUNNER ───────────────────────────────────────────────────

def run():
    print("=" * 60)
    print("  PIPELINE ETL — CHARGEMENT EN BASE DE DONNÉES")
    print("=" * 60)
    print(f"\n  Mode : {cfg.DB_MODE.upper()}")
    cfg.summary()

    print("\n[ETL] Chargement des CSV nettoyés...")
    tables = load_all_tables()

    print("\n[ETL] Insertion en base de données...")
    if cfg.DB_MODE == "postgresql":
        load_postgresql(tables)
    else:
        load_sqlite(tables)

    print("\n✅ Pipeline ETL terminé !")


if __name__ == "__main__":
    run()
