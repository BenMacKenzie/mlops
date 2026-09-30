#!/bin/bash
# Canonical AppKit deploy with a minimal runtime package.json.
#
# The Apps platform runs a plain `npm install` on whatever package.json is uploaded and
# ignores NODE_ENV / omit=dev, so the full devDependency tree (playwright+Chromium, sharp,
# rolldown-vite, ...) blows past the ~8-minute build window. Fix: build the client locally
# with the full deps, then upload a MINIMAL package.json containing only the server runtime
# deps (@databricks/appkit + tsx). The platform installs just those; the prebuilt client/dist
# is served in production; the server runs from source via tsx.
set -e

PROFILE="fe-vm-serverless-stable-77rg2n"
APP_NAME="mlops"
SOURCE_PATH="/Workspace/Users/ben.mackenzie@databricks.com/.bundle/mlops/default/files"

echo "==> Building client (with full local deps)..."
npm run build:client

echo "==> Swapping in minimal runtime package.json for upload..."
cp package.json package.json.bak
cat > package.json << 'EOF'
{
  "name": "mlops",
  "version": "1.0.0",
  "type": "module",
  "dependencies": {
    "@databricks/appkit": "0.11.0",
    "tsx": "^4.20.6"
  }
}
EOF

echo "==> Deploying bundle..."
databricks bundle deploy --force-lock --profile "$PROFILE"

echo "==> Restoring full package.json..."
mv package.json.bak package.json

echo "==> Ensuring app compute is started..."
databricks apps start "$APP_NAME" --profile "$PROFILE" > /dev/null 2>&1 || true
for i in $(seq 1 30); do
  STATE=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('compute_status',{}).get('state',''))" 2>/dev/null || echo "")
  [ "$STATE" = "ACTIVE" ] && break
  sleep 10
done

echo "==> Re-adding lakebase resource (bundle overwrites app resources; postgres type unsupported in DABs)..."
databricks api patch /api/2.0/apps/"$APP_NAME" --profile "$PROFILE" --json '{
  "resources": [
    {"name": "sql-warehouse", "sql_warehouse": {"id": "fec84293300374a5", "permission": "CAN_USE"}},
    {"name": "lakebase-endpoint", "postgres": {"branch": "projects/mlops/branches/production", "database": "projects/mlops/branches/production/databases/databricks-postgres", "permission": "CAN_CONNECT_AND_CREATE"}}
  ],
  "description": "ML model lifecycle manager",
  "user_api_scopes": ["sql"]
}' > /dev/null

echo "==> Deploying app (platform installs minimal deps + starts)..."
databricks apps deploy "$APP_NAME" --source-code-path "$SOURCE_PATH" --profile "$PROFILE"

# The app connects to Lakebase as its service-principal Postgres role. The `app` schema is
# owned by the deploying user, and Lakebase re-provisions the SP role on redeploy, so grant
# the SP access to the schema on every deploy (otherwise: "permission denied for schema app").
echo "==> Granting the app service principal access to the Lakebase 'app' schema..."
SP_ID=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json | python3 -c "import sys,json; print(json.load(sys.stdin).get('service_principal_client_id',''))")
PGHOST_DEP=$(databricks postgres list-endpoints projects/mlops/branches/production --profile "$PROFILE" -o json | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['status']['hosts']['host'])")
PGTOKEN=$(databricks postgres generate-database-credential projects/mlops/branches/production/endpoints/primary --profile "$PROFILE" -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")
PGME=$(databricks current-user me --profile "$PROFILE" -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])")
PSQL=$(command -v psql || echo /opt/homebrew/opt/postgresql@16/bin/psql)
PGPASSWORD="$PGTOKEN" "$PSQL" "host=$PGHOST_DEP port=5432 dbname=databricks_postgres user=$PGME sslmode=require" >/dev/null 2>&1 <<SQL || echo "   (grant step failed — run it manually if the app shows 'permission denied for schema app')"
GRANT USAGE ON SCHEMA app TO "$SP_ID";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA app TO "$SP_ID";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA app TO "$SP_ID";
ALTER DEFAULT PRIVILEGES FOR ROLE "$PGME" IN SCHEMA app GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "$SP_ID";
ALTER DEFAULT PRIVILEGES FOR ROLE "$PGME" IN SCHEMA app GRANT USAGE, SELECT ON SEQUENCES TO "$SP_ID";
SQL

APP_URL=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))" 2>/dev/null || echo "")
echo "==> Done! App URL: ${APP_URL:-(run 'databricks apps get $APP_NAME --profile $PROFILE' to see the URL)}"
