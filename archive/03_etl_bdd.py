"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          PROJET 3 — WILD DATA HUB · API FORMULE 1                           ║
║          Phase 3 : ETL + Schéma PostgreSQL                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝

  Schéma relationnel (Star Schema) :

  ┌─────────────┐     ┌──────────────────┐     ┌──────────────────┐
  │   circuits  │◄────│     courses      │────►│    calendrier    │
  └─────────────┘     └──────────────────┘     └──────────────────┘
                               │
               ┌───────────────┼──────────────────┐
               ▼               ▼                  ▼
  ┌──────────────────┐  ┌─────────────┐  ┌──────────────────┐
  │    resultats     │  │ qualifs     │  │   pit_stops      │
  └──────────────────┘  └─────────────┘  └──────────────────┘
       │        │
       ▼        ▼
  ┌─────────┐  ┌──────────────┐
  │ pilotes │  │   ecuries    │
  └─────────┘  └──────────────┘
"""

import os
import pandas as pd
import logging
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, text, Column, Integer, Float, String,
    Boolean, Date, DateTime, ForeignKey, UniqueConstraint,
    MetaData, Table, inspect
)
from sqlalchemy.orm import declarative_base, Session

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("F1_ETL")

CLEAN_DIR = Path("data/clean")
Base = declarative_base()


# ══════════════════════════════════════════════════════════════════════════════
#  MODÈLES ORM (= Schéma de la BDD)
# ══════════════════════════════════════════════════════════════════════════════

class Circuit(Base):
    __tablename__ = "circuits"
    id            = Column(Integer, primary_key=True, autoincrement=True)
    circuit_id    = Column(String(100), unique=True, nullable=False)
    nom           = Column(String(200))
    localite      = Column(String(100))
    pays          = Column(String(100))
    latitude      = Column(Float)
    longitude     = Column(Float)
    altitude      = Column(Float)
    url_wikipedia = Column(String(500))


class Pilote(Base):
    __tablename__ = "pilotes"
    id              = Column(Integer, primary_key=True, autoincrement=True)
    driver_id       = Column(String(100), unique=True, nullable=False)
    code            = Column(String(10))
    numero          = Column(Integer)
    prenom          = Column(String(100))
    nom             = Column(String(100))
    nom_complet     = Column(String(200))
    date_naissance  = Column(Date)
    nationalite     = Column(String(100))
    url_wikipedia   = Column(String(500))


class Ecurie(Base):
    __tablename__ = "ecuries"
    id              = Column(Integer, primary_key=True, autoincrement=True)
    constructor_id  = Column(String(100), unique=True, nullable=False)
    nom             = Column(String(200))
    nationalite     = Column(String(100))
    url_wikipedia   = Column(String(500))


class Course(Base):
    __tablename__ = "courses"
    __table_args__ = (UniqueConstraint("saison", "round", name="uq_course"),)
    id            = Column(Integer, primary_key=True, autoincrement=True)
    saison        = Column(Integer, nullable=False)
    round         = Column(Integer, nullable=False)
    nom_gp        = Column(String(200))
    circuit_id    = Column(String(100), ForeignKey("circuits.circuit_id"))
    date_course   = Column(Date)
    date_quali    = Column(Date)
    date_sprint   = Column(Date)
    annee         = Column(Integer)
    mois          = Column(Integer)


class Resultat(Base):
    __tablename__ = "resultats"
    __table_args__ = (UniqueConstraint("saison", "round", "driver_id", name="uq_resultat"),)
    id                   = Column(Integer, primary_key=True, autoincrement=True)
    saison               = Column(Integer, nullable=False)
    round                = Column(Integer, nullable=False)
    driver_id            = Column(String(100), ForeignKey("pilotes.driver_id"))
    constructor_id       = Column(String(100), ForeignKey("ecuries.constructor_id"))
    position             = Column(Integer)
    points               = Column(Float)
    grille_depart        = Column(Integer)
    tours_completes      = Column(Integer)
    statut               = Column(String(100))
    secondes_total       = Column(Float)
    fastest_lap_tour     = Column(Integer)
    fastest_lap_rank     = Column(Integer)
    fastest_lap_time_sec = Column(Float)
    fastest_lap_vitesse  = Column(Float)
    a_fini               = Column(Boolean)
    est_abandon          = Column(Boolean)
    est_podium           = Column(Boolean)
    est_victoire         = Column(Boolean)
    positions_gagnees    = Column(Integer)


class Qualification(Base):
    __tablename__ = "qualifications"
    __table_args__ = (UniqueConstraint("saison", "round", "driver_id", name="uq_qualif"),)
    id              = Column(Integer, primary_key=True, autoincrement=True)
    saison          = Column(Integer, nullable=False)
    round           = Column(Integer, nullable=False)
    driver_id       = Column(String(100), ForeignKey("pilotes.driver_id"))
    constructor_id  = Column(String(100), ForeignKey("ecuries.constructor_id"))
    position        = Column(Integer)
    q1              = Column(String(20))
    q2              = Column(String(20))
    q3              = Column(String(20))
    q1_sec          = Column(Float)
    q2_sec          = Column(Float)
    q3_sec          = Column(Float)
    best_quali_sec  = Column(Float)
    session_max     = Column(String(5))


class PitStop(Base):
    __tablename__ = "pit_stops"
    __table_args__ = (UniqueConstraint("saison", "round", "driver_id", "arret_n", name="uq_pitstop"),)
    id          = Column(Integer, primary_key=True, autoincrement=True)
    saison      = Column(Integer, nullable=False)
    round       = Column(Integer, nullable=False)
    driver_id   = Column(String(100), ForeignKey("pilotes.driver_id"))
    arret_n     = Column(Integer)
    tour        = Column(Integer)
    heure       = Column(String(20))
    duree       = Column(String(20))
    duree_sec   = Column(Float)
    arret_normal = Column(Boolean)


class ClassementPilote(Base):
    __tablename__ = "classements_pilotes"
    __table_args__ = (UniqueConstraint("saison", "round", "driver_id", name="uq_class_pilote"),)
    id          = Column(Integer, primary_key=True, autoincrement=True)
    saison      = Column(Integer, nullable=False)
    round       = Column(Integer)
    driver_id   = Column(String(100), ForeignKey("pilotes.driver_id"))
    constructor_id = Column(String(100), ForeignKey("ecuries.constructor_id"))
    position    = Column(Integer)
    points      = Column(Float)
    victoires   = Column(Integer)


class ClassementEcurie(Base):
    __tablename__ = "classements_ecuries"
    __table_args__ = (UniqueConstraint("saison", "round", "constructor_id", name="uq_class_ecurie"),)
    id              = Column(Integer, primary_key=True, autoincrement=True)
    saison          = Column(Integer, nullable=False)
    round           = Column(Integer)
    constructor_id  = Column(String(100), ForeignKey("ecuries.constructor_id"))
    nom_ecurie      = Column(String(200))
    position        = Column(Integer)
    points          = Column(Float)
    victoires       = Column(Integer)


# ══════════════════════════════════════════════════════════════════════════════
#  CONNEXION BDD
# ══════════════════════════════════════════════════════════════════════════════

def get_engine():
    """
    Crée le moteur SQLAlchemy.
    Lit les variables depuis le fichier .env :
      DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    """
    host     = os.getenv("DB_HOST",     "localhost")
    port     = os.getenv("DB_PORT",     "5432")
    name     = os.getenv("DB_NAME",     "f1_db")
    user     = os.getenv("DB_USER",     "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    engine = create_engine(url, echo=False)
    log.info(f"🔌 Connexion BDD : {host}:{port}/{name}")
    return engine


def create_schema(engine):
    """Crée toutes les tables si elles n'existent pas."""
    Base.metadata.create_all(engine)
    log.info("✅ Schéma créé (toutes les tables)")


# ══════════════════════════════════════════════════════════════════════════════
#  CHARGEMENT (LOAD)
# ══════════════════════════════════════════════════════════════════════════════

def load_csv(name: str) -> pd.DataFrame:
    path = CLEAN_DIR / f"{name}.csv"
    if not path.exists():
        log.warning(f"  ⚠️  Fichier manquant : {path}")
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def upsert_df(df: pd.DataFrame, table_name: str, engine,
              conflict_cols: list, update_cols: list = None):
    """
    Insère ou met à jour (upsert) un DataFrame dans PostgreSQL.
    Utilise INSERT ... ON CONFLICT DO UPDATE.
    """
    if df.empty:
        log.warning(f"  ⚠️  DataFrame vide pour {table_name}")
        return

    with engine.begin() as conn:
        for _, row in df.iterrows():
            row_dict = {k: (None if pd.isna(v) else v)
                        for k, v in row.items()
                        if k in [c.name for c in Base.metadata.tables[table_name].columns]}

            # Construire requête upsert
            cols    = list(row_dict.keys())
            vals    = list(row_dict.values())
            placeholders = ", ".join([f":{c}" for c in cols])
            col_str      = ", ".join(cols)
            conflict_str = ", ".join(conflict_cols)

            if update_cols:
                update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
                sql = text(f"""
                    INSERT INTO {table_name} ({col_str})
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}
                """)
            else:
                sql = text(f"""
                    INSERT INTO {table_name} ({col_str})
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_str}) DO NOTHING
                """)
            conn.execute(sql, row_dict)

    log.info(f"  ✅ {table_name} : {len(df)} lignes insérées/mises à jour")


def load_all(engine):
    """Charge toutes les tables nettoyées dans PostgreSQL."""
    log.info("\n── CHARGEMENT DANS POSTGRESQL ───────────────────────────────")

    # Ordre important : respecter les FK
    mapping = [
        # (fichier_clean, table_sql, colonnes_conflit, colonnes_update)
        ("circuits",             "circuits",             ["circuit_id"],                 ["nom","pays","latitude","longitude"]),
        ("pilotes",              "pilotes",              ["driver_id"],                  ["code","numero","nationalite"]),
        ("ecuries",              "ecuries",              ["constructor_id"],              ["nom","nationalite"]),
        ("calendrier",           "courses",              ["saison","round"],              ["nom_gp","date_course","circuit_id"]),
        ("resultats",            "resultats",            ["saison","round","driver_id"],  ["position","points","statut"]),
        ("qualifications",       "qualifications",       ["saison","round","driver_id"],  ["position","q1_sec","q2_sec","q3_sec"]),
        ("pit_stops",            "pit_stops",            ["saison","round","driver_id","arret_n"], ["duree_sec"]),
        ("classements_pilotes",  "classements_pilotes",  ["saison","round","driver_id"],  ["position","points","victoires"]),
        ("classements_ecuries",  "classements_ecuries",  ["saison","round","constructor_id"], ["position","points","victoires"]),
    ]

    for csv_name, table_name, conflict, update in mapping:
        df = load_csv(csv_name)
        if not df.empty:
            upsert_df(df, table_name, engine, conflict, update)


# ══════════════════════════════════════════════════════════════════════════════
#  VUES SQL UTILES (à créer manuellement dans PostgreSQL)
# ══════════════════════════════════════════════════════════════════════════════

VUES_SQL = {

    "v_palmares_pilotes": """
    -- Palmarès global par pilote
    CREATE OR REPLACE VIEW v_palmares_pilotes AS
    SELECT
        p.driver_id,
        p.nom_complet,
        p.nationalite,
        COUNT(DISTINCT r.saison)                        AS saisons,
        COUNT(r.id)                                     AS courses,
        SUM(r.points)                                   AS points_total,
        SUM(CASE WHEN r.est_victoire THEN 1 ELSE 0 END) AS victoires,
        SUM(CASE WHEN r.est_podium   THEN 1 ELSE 0 END) AS podiums,
        SUM(CASE WHEN r.grille_depart = 1 THEN 1 ELSE 0 END) AS poles,
        ROUND(AVG(r.position)::numeric, 2)              AS position_moyenne,
        SUM(CASE WHEN r.a_fini THEN 1 ELSE 0 END)       AS finishes,
        SUM(CASE WHEN r.est_abandon THEN 1 ELSE 0 END)  AS abandons
    FROM resultats r
    JOIN pilotes p ON r.driver_id = p.driver_id
    GROUP BY p.driver_id, p.nom_complet, p.nationalite
    ORDER BY points_total DESC;
    """,

    "v_palmares_ecuries": """
    -- Palmarès global par écurie
    CREATE OR REPLACE VIEW v_palmares_ecuries AS
    SELECT
        e.constructor_id,
        e.nom                                           AS ecurie,
        e.nationalite,
        COUNT(DISTINCT r.saison)                        AS saisons,
        COUNT(r.id)                                     AS courses_participees,
        SUM(r.points)                                   AS points_total,
        SUM(CASE WHEN r.est_victoire THEN 1 ELSE 0 END) AS victoires,
        SUM(CASE WHEN r.est_podium   THEN 1 ELSE 0 END) AS podiums
    FROM resultats r
    JOIN ecuries e ON r.constructor_id = e.constructor_id
    GROUP BY e.constructor_id, e.nom, e.nationalite
    ORDER BY points_total DESC;
    """,

    "v_resultats_complets": """
    -- Vue enrichie des résultats (JOIN toutes tables)
    CREATE OR REPLACE VIEW v_resultats_complets AS
    SELECT
        r.saison,
        r.round,
        c.nom_gp,
        ci.nom           AS circuit,
        ci.pays          AS pays_circuit,
        p.nom_complet    AS pilote,
        p.nationalite    AS nationalite_pilote,
        e.nom            AS ecurie,
        r.grille_depart  AS grille,
        r.position,
        r.points,
        r.tours_completes,
        r.statut,
        r.secondes_total,
        r.fastest_lap_time_sec,
        r.est_victoire,
        r.est_podium,
        r.est_abandon,
        r.positions_gagnees,
        c.date_course
    FROM resultats r
    JOIN courses  c  ON r.saison = c.saison AND r.round = c.round
    JOIN circuits ci ON c.circuit_id = ci.circuit_id
    JOIN pilotes  p  ON r.driver_id = p.driver_id
    JOIN ecuries  e  ON r.constructor_id = e.constructor_id;
    """,

    "v_classement_saison_pilotes": """
    -- Classement pilotes en fin de chaque saison
    CREATE OR REPLACE VIEW v_classement_saison_pilotes AS
    SELECT
        cp.saison,
        cp.position,
        p.nom_complet     AS pilote,
        e.nom             AS ecurie,
        cp.points,
        cp.victoires
    FROM classements_pilotes cp
    JOIN pilotes p ON cp.driver_id = p.driver_id
    LEFT JOIN ecuries e ON cp.constructor_id = e.constructor_id
    WHERE cp.round = (
        SELECT MAX(round) FROM classements_pilotes cp2 WHERE cp2.saison = cp.saison
    )
    ORDER BY cp.saison DESC, cp.position;
    """,

    "v_stats_pit_stops": """
    -- Statistiques pit stops par écurie et saison
    CREATE OR REPLACE VIEW v_stats_pit_stops AS
    SELECT
        ps.saison,
        e.nom                   AS ecurie,
        COUNT(ps.id)            AS nb_arrets,
        ROUND(AVG(ps.duree_sec)::numeric, 3) AS duree_moy_sec,
        MIN(ps.duree_sec)       AS duree_min_sec,
        MAX(ps.duree_sec)       AS duree_max_sec
    FROM pit_stops ps
    JOIN resultats r ON ps.saison = r.saison AND ps.round = r.round AND ps.driver_id = r.driver_id
    JOIN ecuries   e ON r.constructor_id = e.constructor_id
    WHERE ps.arret_normal = TRUE
    GROUP BY ps.saison, e.nom
    ORDER BY ps.saison DESC, duree_moy_sec;
    """,
}


def create_views(engine):
    """Crée les vues SQL dans PostgreSQL."""
    log.info("\n── CRÉATION DES VUES SQL ────────────────────────────────────")
    with engine.begin() as conn:
        for view_name, sql in VUES_SQL.items():
            try:
                conn.execute(text(sql))
                log.info(f"  ✅ Vue créée : {view_name}")
            except Exception as e:
                log.error(f"  ❌ Erreur vue {view_name}: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  PIPELINE ETL COMPLET
# ══════════════════════════════════════════════════════════════════════════════

def run_etl():
    """Lance le pipeline ETL complet : Extract → Transform → Load."""
    log.info("=" * 60)
    log.info("🔄 DÉMARRAGE PIPELINE ETL")
    log.info("=" * 60)

    # 1. Connexion
    engine = get_engine()

    # 2. Créer le schéma
    create_schema(engine)

    # 3. Charger les données nettoyées
    load_all(engine)

    # 4. Créer les vues analytiques
    create_views(engine)

    log.info("\n✅  ETL terminé — Base de données prête !")


if __name__ == "__main__":
    run_etl()
