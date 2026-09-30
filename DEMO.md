# MLOps Model Manager — Demo Data & Walkthrough (workspace: fe-vm-serverless-stable-77rg2n)

Synthetic credit-card-fraud dataset created 2026-09-29 to exercise the full app path
(direct features + table lookups + on-demand function + serving).

## Workspace facts
- **Profile / host:** `fe-vm-serverless-stable-77rg2n` — https://fevm-serverless-stable-77rg2n.cloud.databricks.com
- **SQL warehouse:** `fec84293300374a5` (Serverless Starter)
- **Lakebase project:** `mlops` (app state + online store) — endpoint host `ep-ancient-unit-d20ko36y.database.us-east-1.cloud.databricks.com`, db `databricks_postgres`
- **Demo catalog/schema:** `serverless_stable_77rg2n_catalog.credit_card_fraud_demo`

## Objects
| Object | Type | Key columns |
|--------|------|-------------|
| `transactions` | Delta (EOL spine source) | `transaction_id`, `customer_id`, `merchant_id`, `transaction_ts`, `amount`, `pos_entry_mode` (STRING), `security_code` (STRING), `is_fraud` (INT label) |
| `customer_features` | Feature table (PK `customer_id`, CDF on) | `customer_zip`, `customer_age`, `account_age_days`, `avg_transaction_amount`, `num_transactions_30d` |
| `merchant_features` | Feature table (PK `merchant_id`, CDF on) | `merchant_zip`, `merchant_category`, `merchant_risk_score` |
| `distance(customer_zip STRING, merchant_zip STRING) → DOUBLE` | UC function | on-demand feature; `abs(cz - mz)/1000` |

- 5,000 transactions, ~18% fraud. `is_fraud` is correlated with `amount`, `pos_entry_mode='4'`,
  and `merchant_risk_score`, so a model learns real signal.
- `pos_entry_mode` / `security_code` are STRING (categorical codes) — avoids the int32/int64
  serving-signature mismatch documented in the project memory.

## App inputs for the end-to-end walkthrough

**Project:** catalog `serverless_stable_77rg2n_catalog`, schema `credit_card_fraud_demo`,
model name e.g. `cc_fraud`.

**EOL (spine):**
- SQL: `SELECT transaction_id, customer_id, merchant_id, transaction_ts, amount, pos_entry_mode, security_code, is_fraud FROM serverless_stable_77rg2n_catalog.credit_card_fraud_demo.transactions`
- Label column: `is_fraud`
- Entity columns: `customer_id`, `merchant_id`
- Timestamp column: `transaction_ts`

**Training spec:** task `classification`; split `none` (CV) to start (uses `train_cv.py`).

**Feature entries:**
1. Lookup — table `customer_features`, keys `customer_id`, features
   `customer_age, account_age_days, avg_transaction_amount, num_transactions_30d` (+ `customer_zip` for the on-demand binding)
2. Lookup — table `merchant_features`, keys `merchant_id`, features
   `merchant_category, merchant_risk_score` (+ `merchant_zip` for the on-demand binding)
3. On-demand — function `...credit_card_fraud_demo.distance`, bindings
   `customer_zip` ← customer_features.customer_zip, `merchant_zip` ← merchant_features.merchant_zip,
   output `zip_distance`
   (`customer_zip`/`merchant_zip` are lookup features only to feed the function; can be excluded from the model if desired)

Direct features from the spine (`amount`, `pos_entry_mode`, `security_code`) are used as-is.

## Deployment note
Both lookup tables must be published online before serving. `pos_entry_mode`/`security_code`
being STRING is the deliberate fix for the int32→int64 downcast MLflow rejects at serving.

## End-to-end result (2026-09-29, verified)
Full path exercised via the app API against 77rg2n: project → EOL → spec → train → register
→ publish online tables → serving endpoint → test inference.
- Training (CV): test-AUC-mean **0.9616**.
- Serving returned `predictions: [1,1,0]` for three sampled transactions, matching true labels;
  serving auto-resolved both lookups + computed `zip_distance` on-demand.
- Model `cc_fraud` v2 deployed to endpoint `cc-fraud`.

## Deploying the app to Databricks Apps (canonical pattern)
Deployed and verified 2026-09-30 at https://mlops-7474643951125249.aws.databricksapps.com
(reads Lakebase + Unity Catalog). Run `./deploy.sh`. Hard-won lessons baked into the config:

1. **No esbuild bundling / no deps-stripping.** Ship the source; the Apps platform runs
   `npm install`. (The old esbuild approach failed on AppKit's per-plugin `manifest.json`, which
   each plugin reads from its own `node_modules` dir — a single bundle can't satisfy all three.)
2. **`.databricksignore` is NOT honored by `databricks bundle deploy`** — only `.gitignore` and
   `sync.exclude` in `databricks.yml` are. Exclusions must go in `databricks.yml sync.exclude`.
3. **Never ship the dev-proxy `.npmrc`.** `registry=https://npm-proxy.dev.databricks.com/` is
   unreachable from the Apps build env → every `npm install` hangs ~8 min then is killed
   ("Exit handler never called"). Excluded via `sync.exclude`; the platform's default registry
   serves `@databricks/appkit` fine (~16s install). Also exclude `package-lock.json` so it can't
   override the minimal uploaded `package.json`.
4. **Minimal runtime `package.json`.** `deploy.sh` swaps in a package.json with only
   `@databricks/appkit` + `tsx`; the client is prebuilt locally into `client/dist`.
5. **`app.yaml`:** `npx tsx server/server.ts`, `NODE_ENV=production`, and an explicit
   `LAKEBASE_ENDPOINT=projects/mlops/branches/production/endpoints/primary` (PGHOST/PGUSER/… auto-inject
   from the postgres resource; only LAKEBASE_ENDPOINT must be set).
6. **`DATABRICKS_HOST` is injected without a scheme** on the platform → `server.ts` prepends
   `https://` at startup so REST calls parse.
7. **Grant the app SP the `app` schema** every deploy (Lakebase re-provisions the SP role on
   redeploy → "permission denied for schema app"). `deploy.sh` does this automatically.
8. The postgres app resource is re-added via REST after `bundle deploy` (DABs can't declare it),
   and a first-time app must be started before `apps deploy` — both handled in `deploy.sh`.
9. **Force-include `client/dist`.** `.gitignore`'s `dist/` pattern also matches `client/dist`, so the
   prebuilt SPA is excluded from the upload → root serves "Cannot GET /". `databricks.yml` uses
   `sync.include: client/dist/**` to ship it.

### Two gotchas fixed during the run (important for the demo)
1. **On-demand `FeatureFunction` requires a Python UDF, not SQL.** A SQL `distance()` failed at
   `create_training_set` with "is not a Python UDF. Only Python UDFs are supported." Recreated as
   `LANGUAGE PYTHON`. → on-demand feature functions must be `LANGUAGE PYTHON` in UC.
2. **int32/int64 serving mismatch** (same as the historical fix). Feature-table integer columns
   created as Spark `INT` → int32 model signature; online-store lookup returns int64 → MLflow
   refuses the narrowing at serving. Fix: make numeric feature columns **BIGINT** (or DOUBLE) so
   the signature is int64. Applied to `customer_features` (customer_age, account_age_days,
   num_transactions_30d). Re-publish over an existing online view via `fe.publish_table` (TRIGGERED)
   works without dropping the view.
