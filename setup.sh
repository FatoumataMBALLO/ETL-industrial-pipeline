#!/bin/bash
set -e

echo "🔧 Installation de l'environnement ETL Pipeline"
echo "================================================"

# Vérification des prérequis
echo "📋 Vérification des prérequis..."
command -v docker >/dev/null 2>&1 || { echo "❌ Docker non installé"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "❌ Python3 non installé"; exit 1; }
echo "   ✅ Docker et Python3 disponibles"

# Création du .env si absent
if [ ! -f .env ]; then
    echo "📝 Création du fichier .env depuis .env.example..."
    cp .env.example .env
    echo "   ⚠️  Pensez à modifier .env avec vos valeurs !"
else
    echo "   ✅ .env déjà présent"
fi

# Installation des dépendances Python
echo "📦 Installation des dépendances Python..."
pip install -r requirements.txt -q
echo "   ✅ Dépendances installées"

# Création des dossiers nécessaires
echo "📁 Création des dossiers..."
mkdir -p data/raw data/input data/processed logs
echo "   ✅ Dossiers créés"

# Démarrage des containers Docker
echo "🐳 Démarrage des containers Docker..."
docker compose -f docker/docker-compose.yml up -d
sleep 5
echo "   ✅ Containers démarrés"

# Lancement des tests
echo "🧪 Lancement des tests..."
pytest tests/ -q --cov=src --cov-report=term-missing
echo "   ✅ Tests OK"

echo ""
echo "✅ Environnement prêt ! Lance le pipeline avec :"
echo "   python run_pipeline.py"
