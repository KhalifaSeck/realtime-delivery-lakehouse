#!/bin/bash
# ============================================================
# Pipeline batch : ADLS -> Snowflake -> dbt
# Exécuté toutes les 5 min par un CronJob Kubernetes.
# ============================================================

set -e  # Arrêter au premier échec

echo "=== [$(date)] Pipeline démarré ==="

# Étape 1 : COPY INTO Snowflake
echo "--- Étape 1 : COPY INTO Snowflake ---"
cd /app
python -c "
import sys
sys.path.insert(0, '/app')
from ingestion.copy_to_snowflake import load_events
results = load_events(truncate=False)
total = sum(r['n_loaded'] for r in results)
print(f'Lignes chargées : {total}')
"

# Étape 2 : dbt run staging
echo "--- Étape 2 : dbt run staging ---"
cd /app/dbt/delivery_dbt
dbt run --profiles-dir /app/dbt/profiles --select staging

# Étape 3 : dbt test staging
echo "--- Étape 3 : dbt test staging ---"
dbt test --profiles-dir /app/dbt/profiles --select staging

# Étape 4 : dbt run marts
echo "--- Étape 4 : dbt run marts ---"
dbt run --profiles-dir /app/dbt/profiles --select marts

# Étape 5 : dbt test marts
echo "--- Étape 5 : dbt test marts ---"
dbt test --profiles-dir /app/dbt/profiles --select marts

echo "=== [$(date)] Pipeline terminé avec succès ==="