#!/bin/bash
echo "================================================"
echo "🎓 DÉMONSTRATION ETL PIPELINE"
echo "================================================"

echo ""
echo "📋 1. TESTS UNITAIRES"
echo "------------------------------------------------"
pytest tests/ -v --cov=src --cov-report=term-missing

echo ""
echo "🚀 2. EXÉCUTION DU PIPELINE"
echo "------------------------------------------------"
python run_pipeline.py

echo ""
echo "🗄️  3. DONNÉES DANS POSTGRESQL"
echo "------------------------------------------------"
docker exec etl-postgres psql -U postgres -d etl -c "SELECT COUNT(*) as nb_transactions FROM transactions;"
docker exec etl-postgres psql -U postgres -d etl -c "SELECT * FROM transactions LIMIT 3;"

echo ""
echo "✅ DÉMONSTRATION TERMINÉE"
echo "   MinIO   : http://localhost:9001  (admin/password123)"
echo "   Airflow : http://localhost:8080  (admin/admin)"
echo "================================================"
