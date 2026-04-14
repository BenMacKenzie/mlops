#!/bin/bash
# Deploy script: builds, creates a minimal package.json for remote (no deps = no npm install),
# deploys, then restores the real package.json.
set -e

PROFILE="fe-vm-serverless-stable-1dpktm"
APP_NAME="mlops"
SOURCE_PATH="/Workspace/Users/ben.mackenzie@databricks.com/.bundle/mlops/default/files"

echo "==> Building..."
npm run build:server
npm run build:client
npx esbuild server/server.ts --bundle --platform=node --target=node22 --format=esm \
  --outfile=build/server.mjs \
  --external:sharp --external:@babel/preset-typescript --external:lightningcss \
  --external:@ast-grep/napi --external:playwright

echo "==> Swapping package.json for deploy (empty deps)..."
cp package.json package.json.bak
cat > package.json << 'EOF'
{
  "name": "mlops",
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "start": "node build/server.mjs"
  }
}
EOF

echo "==> Deploying bundle..."
databricks bundle deploy --force-lock --profile "$PROFILE"

echo "==> Restoring package.json..."
mv package.json.bak package.json

echo "==> Re-adding lakebase resource..."
databricks api patch /api/2.0/apps/"$APP_NAME" --profile "$PROFILE" --json '{
  "resources": [
    {"name": "sql-warehouse", "sql_warehouse": {"id": "c3adcf234afed63a", "permission": "CAN_USE"}},
    {"name": "lakebase-endpoint", "postgres": {"branch": "projects/mlops/branches/production", "database": "projects/mlops/branches/production/databases/db-i2v5-bd2cb2q58n", "permission": "CAN_CONNECT_AND_CREATE"}}
  ],
  "description": "ML model lifecycle manager",
  "user_api_scopes": ["sql"]
}' > /dev/null

echo "==> Deploying app..."
databricks apps deploy "$APP_NAME" --source-code-path "$SOURCE_PATH" --profile "$PROFILE"

echo "==> Done! App URL: https://mlops-7474652257397569.aws.databricksapps.com"
