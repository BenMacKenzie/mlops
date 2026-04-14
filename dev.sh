#!/bin/bash
# Run MLOps app locally with Lakebase connectivity
# Usage: ./dev.sh
# Note: Tokens expire in ~1 hour — restart to refresh

cd "$(dirname "$0")"

PROFILE=fe-vm-serverless-stable-1dpktm

export DATABRICKS_HOST=https://fevm-serverless-stable-1dpktm.cloud.databricks.com
export DATABRICKS_WAREHOUSE_ID=c3adcf234afed63a
export DATABRICKS_CONFIG_PROFILE=$PROFILE
export NODE_ENV=development

# Generate Databricks API token (for SQL warehouse, jobs, MLflow calls)
echo "Generating credentials..."
export DATABRICKS_TOKEN=$(databricks auth token -p $PROFILE 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null || echo "")
if [ -z "$DATABRICKS_TOKEN" ]; then
  # Fallback: use the token from auth env
  export DATABRICKS_TOKEN=$(databricks auth env -p $PROFILE 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('env',{}).get('DATABRICKS_TOKEN',''))" 2>/dev/null || echo "")
fi

# Lakebase connection — use native auth (static token)
export PGHOST=ep-little-wind-d2xv9pgg.database.us-east-1.cloud.databricks.com
export PGDATABASE=databricks_postgres
export PGPORT=5432
export PGSSLMODE=require
export PGUSER=$(databricks current-user me -p $PROFILE -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])")
export PGPASSWORD=$(databricks postgres generate-database-credential \
  projects/mlops/branches/production/endpoints/primary \
  -p $PROFILE -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

echo "PGUSER=$PGUSER"
echo "DATABRICKS_TOKEN=${DATABRICKS_TOKEN:0:20}..."
echo "Starting dev server on http://localhost:8000"
echo "(Tokens expire in ~1 hour — restart to refresh)"
npx tsx server/server.ts
