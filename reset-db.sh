#!/bin/bash
# Drop and recreate all Lakebase tables
# Usage: ./reset-db.sh

cd "$(dirname "$0")"

PROFILE=fe-vm-serverless-stable-1dpktm

export PGHOST=ep-little-wind-d2xv9pgg.database.us-east-1.cloud.databricks.com
export PGDATABASE=databricks_postgres
export PGPORT=5432
export PGSSLMODE=require
export PGUSER=$(databricks current-user me -p $PROFILE -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])")
export PGPASSWORD=$(databricks postgres generate-database-credential \
  projects/mlops/branches/production/endpoints/primary \
  -p $PROFILE -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

PSQL=/opt/homebrew/Cellar/postgresql@16/16.13/bin/psql

echo "Dropping all tables..."
$PSQL -c "
  DROP TABLE IF EXISTS app.evaluation_run CASCADE;
  DROP TABLE IF EXISTS app.training_run CASCADE;
  DROP TABLE IF EXISTS app.run CASCADE;
  DROP TABLE IF EXISTS app.dataset CASCADE;
  DROP TABLE IF EXISTS app.feature_entry CASCADE;
  DROP TABLE IF EXISTS app.feature_definition CASCADE;
  DROP TABLE IF EXISTS app.entity_observation_label CASCADE;
  DROP TABLE IF EXISTS app.project CASCADE;
"

echo "Done. Restart dev server to recreate tables."
