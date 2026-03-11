# 🏎️  Projet 3 — Wild Data Hub · API Formule 1

## Architecture

```
projet_f1/
├── 01_collecte_donnees.py   # Phase 1 : Appels API Ergast + OpenF1
├── 02_nettoyage.py          # Phase 2 : Nettoyage pandas
├── 03_etl_bdd.py            # Phase 3 : ETL + BDD PostgreSQL
├── 04_dashboard.py          # Phase 4 : Dashboard Streamlit
├── .env                     # Variables d'environnement (BDD)
├── requirements.txt         # Dépendances Python
├── data/
│   ├── raw/                 # Données brutes (CSV)
│   └── clean/               # Données nettoyées (CSV)
└── README.md
```

## Installation

```bash
pip install -r requirements.txt
```

## Configuration BDD

Créer un fichier `.env` à la racine :

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=f1_db
DB_USER=postgres
DB_PASSWORD=votre_mot_de_passe
```

Créer la base dans PostgreSQL :
```sql
CREATE DATABASE f1_db;
```

## Utilisation

### Étape 1 — Collecte (test rapide saison 2024)
```bash
python 01_collecte_donnees.py test
```

### Étape 1 — Collecte complète (2018-2025, ~30 min)
```bash
python 01_collecte_donnees.py complet
```

### Étape 2 — Nettoyage
```bash
python 02_nettoyage.py
```

### Étape 3 — ETL et création BDD
```bash
python 03_etl_bdd.py
```

### Étape 4 — Dashboard
```bash
streamlit run 04_dashboard.py
```

## Sources de données

| API | URL | Données |
|-----|-----|---------|
| **Ergast** | http://ergast.com/api/f1 | Historique 1950-2024, résultats, classements |
| **OpenF1** | https://api.openf1.org/v1 | Temps réel, positions GPS, météo, radio |

## Modèle de données

```
circuits ◄── courses ──► qualifications
               │
        ┌──────┴──────┐
        ▼             ▼
    resultats     pit_stops
        │
   ┌────┴────┐
   ▼         ▼
pilotes   ecuries
```

## KPIs principaux du dashboard

- Points cumulés par saison (pilotes + écuries)
- Victoires, podiums, pole positions
- Durées de pit stops par écurie
- Carte mondiale des circuits
- Corrélation grille de départ → position finale
- Domination des écuries saison par saison
