# 🏎️ F1 Data Hub

Application d'analyse de données **Formula 1** développée dans le cadre du Projet 3 Wild Code School.

Pipeline complet de la collecte à la visualisation, enrichi par un module d'intelligence artificielle pour la prédiction du vainqueur.

---

## 📐 Architecture

```
f1_data_hub/
├── config.py                      # Configuration centralisée (lit .env)
├── .env.example                   # Template de configuration
├── .gitignore
├── requirements.txt
├── run_pipeline.sh                # Lancement en une commande
│
├── scripts/
│   ├── 01_collect_openf1.py       # API OpenF1 (sessions, météo, pit stops)
│   ├── 02_collect_jolpi.py        # API jolpi.ca / Ergast (résultats, classements)
│   ├── 03_scrape_wikipedia.py     # Web scraping (circuits, pilotes)
│   ├── 04_clean_transform.py      # Nettoyage, transformation, features ML
│   ├── 05_etl_pipeline.py         # ETL → SQLite ou PostgreSQL
│   ├── 06_run_all.py              # Orchestrateur du pipeline
│   └── 07_ml_predict.py           # Entraînement & prédiction IA
│
├── dashboard/
│   └── app.py                     # Dashboard Streamlit (6 pages)
│
├── data/
│   ├── raw/                       # Données brutes (JSON)
│   ├── clean/                     # Données nettoyées (CSV)
│   ├── db/                        # Base de données SQLite
│   └── models/                    # Modèle IA sérialisé (.pkl)
│
└── docs/
    └── schema.sql                 # Schéma PostgreSQL documenté
```

---

## 🚀 Installation et lancement

### 1. Cloner le projet et installer les dépendances

```bash
git clone https://github.com/votre-user/f1-data-hub.git
cd f1-data-hub
pip install -r requirements.txt
```

### 2. Configurer l'environnement

```bash
cp .env.example .env
# Éditer .env selon vos besoins (saisons, BDD, etc.)
```

### 3. Lancer le pipeline complet

```bash
bash run_pipeline.sh
```

Ou étape par étape :

```bash
python scripts/01_collect_openf1.py     # Collecte OpenF1
python scripts/02_collect_jolpi.py      # Collecte jolpi.ca
python scripts/03_scrape_wikipedia.py   # Scraping Wikipedia
python scripts/04_clean_transform.py    # Nettoyage + features ML
python scripts/05_etl_pipeline.py       # Chargement en BDD
python scripts/07_ml_predict.py         # Entraînement du modèle IA
```

Options du runner :

```bash
# Ignorer la collecte (réutiliser les données brutes existantes)
bash run_pipeline.sh --skip-collect

# Ignorer l'entraînement IA
bash run_pipeline.sh --skip-ml

# Choisir les saisons à la volée
bash run_pipeline.sh --seasons 2022 2023 2024
```

### 4. Lancer le dashboard

```bash
streamlit run dashboard/app.py
```

---

## ⚙️ Configuration `.env`

| Variable | Valeur par défaut | Description |
|---|---|---|
| `DB_MODE` | `sqlite` | Mode BDD : `sqlite` ou `postgresql` |
| `SEASONS` | `2024` | Saisons à collecter (ex: `2022,2023,2024`) |
| `API_DELAY` | `0.3` | Délai entre requêtes API (secondes) |
| `MAX_ROUNDS` | `0` | Nombre de GPs max par saison (`0` = tous) |
| `POSTGRES_HOST` | `localhost` | Hôte PostgreSQL |
| `POSTGRES_PORT` | `5432` | Port PostgreSQL |
| `POSTGRES_DB` | `f1_hub` | Nom de la base |
| `POSTGRES_USER` | `f1_user` | Utilisateur PostgreSQL |
| `POSTGRES_PASSWORD` | *(vide)* | Mot de passe PostgreSQL |
| `MODEL_PATH` | `data/models/winner_predictor.pkl` | Chemin du modèle IA |
| `PREDICTION_THRESHOLD` | `0.15` | Seuil de probabilité minimum |

### Passer en PostgreSQL

```bash
# Dans .env
DB_MODE=postgresql
POSTGRES_HOST=localhost
POSTGRES_DB=f1_hub
POSTGRES_USER=f1_user
POSTGRES_PASSWORD=votre_mot_de_passe
```

Créer la base au préalable :

```sql
CREATE DATABASE f1_hub;
CREATE USER f1_user WITH PASSWORD 'votre_mot_de_passe';
GRANT ALL PRIVILEGES ON DATABASE f1_hub TO f1_user;
```

---

## 📡 Sources de données

| Source | Type | Données collectées |
|---|---|---|
| [OpenF1 API](https://openf1.org/) | API REST (gratuite, sans clé) | Sessions, télémétrie, météo, pit stops, positions |
| [jolpi.ca / Ergast](https://jolpi.ca/) | API REST (gratuite, sans clé) | Résultats, classements, circuits, pilotes, qualifications |
| [Wikipedia](https://wikipedia.org/) | Web scraping (BeautifulSoup) | Biographies pilotes, stats circuits |

---

## 🗄️ Schéma de la base de données

### Tables principales

| Table | Description |
|---|---|
| `race_results` | Résultats détaillés de chaque course (table de faits) |
| `driver_standings` | Classement pilotes en fin de saison |
| `constructor_standings` | Classement constructeurs en fin de saison |
| `circuits` | Informations géographiques des circuits |
| `qualifying` | Résultats des qualifications (Q1, Q2, Q3) |
| `sessions_f1` | Sessions OpenF1 (FP1, FP2, FP3, Q, R) |
| `weather` | Météo par Grand Prix |
| `pit_stops` | Détail des arrêts aux stands |
| `ml_features` | Features préparées pour le modèle IA |

### Vues analytiques

| Vue | Description |
|---|---|
| `v_race_winners` | Vainqueur de chaque Grand Prix |
| `v_driver_career` | Stats de carrière agrégées par pilote |
| `v_constructor_career` | Stats de carrière agrégées par constructeur |
| `v_season_summary` | Résumé de chaque saison (champion, GPs, équipes) |

---

## 🤖 Module IA — Prédiction du vainqueur

Le script `07_ml_predict.py` entraîne un modèle de classification binaire
(victoire / pas de victoire) et estime la probabilité de victoire de chaque pilote
avant une course.

### Features utilisées

| Feature | Description |
|---|---|
| `grid` | Position de départ sur la grille |
| `pole_position` | Parti de la pole position (booléen) |
| `avg_finish_prev3` | Moyenne des 3 dernières positions en course |
| `win_rate_prev` | Taux de victoire historique du pilote |
| `podium_rate_prev` | Taux de podiums historique du pilote |
| `constructor_wins_season` | Victoires de l'écurie dans la saison en cours |
| `circuit_win_rate` | Taux de victoire du pilote sur ce circuit précis |

### Modèles comparés

- `RandomForestClassifier` (200 arbres, class_weight=balanced)
- `GradientBoostingClassifier` (150 estimateurs, learning_rate=0.05)
- `LogisticRegression` (régularisation L2)

Le meilleur modèle (sélectionné par AUC-ROC en validation croisée temporelle `TimeSeriesSplit`)
est ensuite calibré avec **Platt scaling** pour obtenir des probabilités fiables.

### Utilisation programmatique

```python
from scripts.ml_predict import predict_race

resultats = predict_race([
    {
        "driver_id": "verstappen", "constructor_id": "red_bull",
        "circuit_id": "bahrain",   "grid": 1, "pole_position": 1,
        "avg_finish_prev3": 1.8,   "win_rate_prev": 0.45,
        "podium_rate_prev": 0.75,  "constructor_wins_season": 3,
        "circuit_win_rate": 0.50,
    },
    # ... autres pilotes
])
print(resultats[["driver_id", "win_probability", "predicted_rank"]])
```

---

## 📊 Dashboard Streamlit

Le dashboard comprend **6 pages** :

| Page | Contenu |
|---|---|
| 🏆 Classements | Classements pilotes et constructeurs de la saison sélectionnée |
| 📊 Stats par GP | Résultats détaillés par Grand Prix + évolution des points cumulés |
| 📈 Comparaison historique | Champions par saison, évolution des constructeurs, carrière d'un pilote |
| 🌦️ Météo & Circuits | Températures, conditions météo, carte interactive des circuits |
| 👤 Profil Pilote | Fiche individuelle : classements course par course, stats clés |
| 🤖 Prédiction IA | Simulation d'un Grand Prix et prédiction des probabilités de victoire |

---

## 🧪 Tests

```bash
# Vérifier la syntaxe de tous les scripts
for f in scripts/*.py dashboard/app.py config.py; do
    python -m py_compile $f && echo "✅ $f"
done

# Lancer avec données mock (sans connexion internet)
python scripts/04_clean_transform.py
python scripts/05_etl_pipeline.py
python scripts/07_ml_predict.py
```

---

## 📁 Livrables (conformité Projet 3 Wild Code School)

- [x] Scripts de collecte et d'extraction des données
- [x] Pipeline de nettoyage et prétraitement
- [x] Infrastructure ETL opérationnelle
- [x] Base de données optimisée et documentée (`docs/schema.sql`)
- [x] Tableaux de bord interactifs (Streamlit)
- [x] Interface utilisateur fonctionnelle et intuitive
- [x] Enrichissement IA (prédiction du vainqueur)
- [x] Documentation technique (ce fichier)

---

## 👤 Auteur

Projet réalisé dans le cadre de la certification **Data Analyst** — Wild Code School
