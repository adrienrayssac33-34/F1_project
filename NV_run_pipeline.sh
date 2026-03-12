#!/bin/bash
# ============================================================
# F1 Data Hub — Lancement du pipeline complet
# Usage : bash run_pipeline.sh [OPTIONS]
#
# Options :
#   --skip-collect   Utiliser les données brutes déjà collectées
#   --skip-ml        Ne pas entraîner le modèle IA
#   --seasons "2022 2023 2024"   Saisons à traiter
# ============================================================

set -e  # Arrêter si une commande échoue

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Couleurs terminal
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo ""
echo "🏎️  ========================================== 🏎️"
echo "         F1 DATA HUB — PIPELINE COMPLET"
echo "🏎️  ========================================== 🏎️"
echo ""

# Vérifier que .env existe
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}⚠️  Fichier .env manquant — copie depuis .env.example${NC}"
    cp .env.example .env
    echo "   → Modifiez .env si nécessaire (notamment DB_MODE et SEASONS)"
fi

# Charger les variables d'environnement
export $(grep -v '^#' .env | xargs 2>/dev/null) 2>/dev/null || true

echo "  Mode BDD : ${DB_MODE:-sqlite}"
echo "  Saisons  : ${SEASONS:-2024}"
echo ""

# Vérifier Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python3 non trouvé${NC}"
    exit 1
fi

# Vérifier les dépendances
echo "📦 Vérification des dépendances..."
python3 -c "import requests,pandas,bs4,sklearn" 2>/dev/null || {
    echo -e "${YELLOW}Installation des dépendances...${NC}"
    pip install -r requirements.txt --break-system-packages -q
}

# Lancer le pipeline Python
echo ""
python3 scripts/06_run_all.py "$@"

echo ""
echo -e "${GREEN}✅ Pipeline terminé !${NC}"
echo ""
echo "  👉 Lancez le dashboard :"
echo "     streamlit run dashboard/app.py"
echo ""
