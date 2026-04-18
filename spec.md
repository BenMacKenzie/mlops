# MLOps Model Manager

**Created:** 2026-04-11
**Status:** In Progress
**Workspace:** https://fevm-serverless-stable-1dpktm.cloud.databricks.com
**Default Catalog:** serverless_stable_1dpktm_catalog (user-configurable per project)
**Lakebase Instance:** mlops
**Owner:** Ben MacKenzie
**Prior Art:** https://github.com/BenMacKenzie/mlops/tree/main (Dash-based prototype — reference for data model, job patterns, feature lookup UI)

---

## Problem Statement

Managing the full lifecycle of ML models — from feature definition through training to deployment — requires coordinating across multiple systems (Unity Catalog feature tables, MLflow experiments, model registry, serving endpoints, Databricks Jobs). The current prototype (Dash/Python) proved the concept but needs a rewrite as a proper Databricks App using React + AppKit with Lakebase for persistence.

The app should let a user:
1. Define a **project** with a catalog/schema for ML assets
2. Define an **EOL** (entity/observation/label) — the SQL "spine" for training data
3. Create a **training spec** — features, split strategy, task type, and parameters — all in one place
4. **Run** training — a single job that materializes the dataset, trains the model, and logs with feature specs via `fe.log_model()`
5. **Register** models to Unity Catalog and **deploy** to serving endpoints with auto feature lookup

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Databricks App (AppKit)                            │
│  ┌────────────┐  ┌──────────────────────────────┐   │
│  │  Express    │  │  React Frontend (Vite)       │   │
│  │  Backend    │  │  - Project management         │   │
│  │  (API)      │  │  - Dataset builder            │   │
│  │             │  │  - Training dashboard          │   │
│  │  Plugins:   │  │  - MLflow experiment viewer    │   │
│  │  - lakebase │  │  - Model registry browser      │   │
│  │  - analytics│  │  - Deployment & serving mgmt   │   │
│  └──────┬──────┘  └──────────────────────────────┘   │
│         │                                            │
└─────────┼────────────────────────────────────────────┘
          │
          ▼
┌─────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  Lakebase        │  │  Unity Catalog   │  │  MLflow          │
│  (mlops)         │  │                  │  │                  │
│  - projects      │  │  - Feature tables│  │  - Experiments   │
│  - training specs│  │  - Models        │  │  - Runs          │
│  - runs          │  │  - Synced tables │  │  - Model Registry│
│  - synced tables │  │                  │  │                  │
│  - deployments   │  │                  │  │                  │
└─────────────────┘  └──────────────────┘  └──────────────────┘
          │                    │                      │
          └────────────────────┼──────────────────────┘
                               ▼
┌──────────────────┐  ┌──────────────────────────────┐
│  Databricks Jobs │  │  Databricks Model Serving    │
│  - Train         │  │  - Auto feature lookup       │
│  (materialize +  │  │    (from synced tables)       │
│   train + log)   │  │                               │
└──────────────────┘  └──────────────────────────────┘
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React + TypeScript + Vite (via AppKit) |
| Backend | Express (via AppKit) |
| App Framework | `@databricks/appkit` with lakebase + analytics plugins |
| OLTP Store | Lakebase (Postgres) — instance: `mlops` |
| Feature Tables | Unity Catalog (Databricks Feature Engineering) |
| Experiment Tracking | MLflow (via Databricks workspace) |
| Model Registry | Unity Catalog Model Registry |
| Compute | Databricks Jobs (app-managed notebooks: train, register) |
| Online Feature Store | Lakebase Synced Tables (synced from UC feature tables) |
| Serving | Databricks Model Serving endpoints (with auto feature lookup) |
| Auth | AppKit dual-identity (service principal + user OBO) |

## Data Model (Lakebase)

The data model is designed around a consolidated **training spec** that combines feature definitions, split configuration, and training parameters into a single entity. This eliminates the previous separate feature_definition and dataset tables, simplifying the workflow from EOL → training spec → run → model.

### Tables

#### `app.project`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| name | VARCHAR(255) | Unique project name |
| description | TEXT | |
| catalog | VARCHAR(255) | Unity Catalog catalog for this project's assets |
| schema | VARCHAR(255) | UC schema within catalog |
| model_name | VARCHAR(255) | Name for UC model registry (defaults to project name) |

#### `app.entity_observation_label`
Defines the base entity/observation/label SQL — the "spine" of a training set. Reusable across multiple training specs within a project.

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| project_id | BIGINT FK → project | |
| name | VARCHAR(255) | |
| sql_definition | TEXT | SQL query that produces entity keys + label |
| label_column | VARCHAR(255) | Name of the label column |
| entity_columns | TEXT[] | Primary key / entity columns |
| timestamp_column | VARCHAR(255) | Timestamp column (for point-in-time joins) |

#### `app.training_spec`
A complete, immutable training configuration: features + split + task type. Combines what was previously feature_definition + dataset. To experiment, copy an existing spec and modify.

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| project_id | BIGINT FK → project | |
| eol_id | BIGINT FK → entity_observation_label | The spine |
| name | VARCHAR(255) | e.g. `customer_churn_v2` |
| task_type | VARCHAR(50) | `'classification'` or `'regression'` |
| split_strategy | VARCHAR(50) | `'none'`, `'train_eval'`, `'train_eval_test'` |
| split_method | VARCHAR(50) | `'random'`, `'temporal'` (null when strategy is `none`) |
| split_config | JSONB | See below |
| parameters | JSONB | Hyperparameters passed to the training notebook |

**Split strategies:**
- **None** — no split, single dataset. Training notebook uses cross-validation internally.
- **Train / Eval** — two-way split. Standard model selection: train to fit, eval to assess.
- **Train / Eval / Test** — three-way split. For hyperparameter search: eval for tuning, test for final unbiased estimate.

**Split methods** (when strategy is not `none`):
- **Random** — stratified random split on the label column (from EOL). Maintains class proportions. Seeded for reproducibility.
- **Temporal** — sort by EOL's timestamp column, latest records to eval/test. Prevents future data leaking into training. Not stratified.

**`split_config` JSONB:**
```json
{ "eval_pct": 20, "test_pct": 10, "seed": 42 }
```

**Split strategy determines which app-managed notebook runs:**
- `none` → `notebooks/train_cv.py` (cross-validation)
- `train_eval` → `notebooks/train_standard.py` (train + eval)
- `train_eval_test` → `notebooks/train_hpsearch.py` (hyperparameter search, future)

Each notebook handles both classification and regression via the `task_type` parameter.

#### `app.feature_entry`
An individual feature lookup or declarative feature within a training spec. A training spec can have many entries, each pulling from a different table.

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| training_spec_id | BIGINT FK → training_spec | Parent spec |
| feature_type | VARCHAR(50) | `'lookup'` or `'declarative'` |
| table_name | VARCHAR(255) | UC feature table (for lookup type) |
| feature_names | TEXT[] | Columns to look up |
| lookup_key | TEXT[] | Join keys mapping to EOL entity columns |
| timestamp_lookup_key | VARCHAR(255) | For point-in-time lookups |
| default_values | JSONB | Default values for missing features |
| declarative_spec | JSONB | Full declarative Feature spec (for declarative type) |

#### `app.run`
A single end-to-end execution: materialize dataset → train model → log with `fe.log_model()`. One Databricks Job does everything.

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| project_id | BIGINT FK → project | |
| training_spec_id | BIGINT FK → training_spec | |
| job_id | BIGINT | Databricks job ID |
| run_id | BIGINT | Databricks run ID (for this execution) |
| mlflow_experiment_id | VARCHAR(255) | MLflow experiment ID |
| mlflow_run_id | VARCHAR(255) | |
| training_metrics | JSONB | Metrics from training |
| eval_metrics | JSONB | Metrics from evaluation |
| status | VARCHAR(50) | `'PENDING'`, `'RUNNING'`, `'SUCCESS'`, `'FAILED'` |
| model_uri | TEXT | MLflow model URI if registered |
| model_name | VARCHAR(255) | Registered model name in UC |
| model_version | INTEGER | Version in model registry |
| training_table | VARCHAR(255) | Materialized training table |
| eval_table | VARCHAR(255) | Materialized eval table (null for `none` split) |
| test_table | VARCHAR(255) | Materialized test table (null unless 3-way split) |
| databricks_run_url | TEXT | |
| error_message | TEXT | |
| started_at | TIMESTAMP | |
| ended_at | TIMESTAMP | |

#### `app.synced_table`
Tracks feature tables synced to Lakebase for online serving. Project-level — shared across all deployments.

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| project_id | BIGINT FK → project | |
| source_table | VARCHAR(255) | Fully-qualified UC table name |
| synced_table_name | VARCHAR(255) | Destination UC name (`{catalog}.{schema}.{name}_online`) |
| status | VARCHAR(50) | `'PROVISIONING'`, `'ONLINE'`, `'FAILED'` |
| pipeline_id | VARCHAR(255) | Lakebase sync pipeline URL |

#### `app.deployment`
Links a registered model version to a serving endpoint. All feature tables from the model's training spec must be synced before the endpoint can be created.

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| project_id | BIGINT FK → project | |
| name | VARCHAR(255) | e.g. `'production'`, `'staging'` |
| run_id | BIGINT FK → run | Must have `model_name`/`model_version` set |
| endpoint_name | VARCHAR(255) | Serving endpoint name |
| endpoint_status | VARCHAR(50) | `'NOT_CREATED'`, `'CREATING'`, `'READY'`, `'FAILED'` |
| endpoint_config | JSONB | Instance type, scaling, etc. |
| created_at | TIMESTAMP | |

### Entity Relationship Summary

```
project 1──* entity_observation_label
project 1──* training_spec
project 1──* run
project 1──* synced_table
project 1──* deployment

entity_observation_label 1──* training_spec
training_spec 1──* feature_entry
training_spec 1──* run
run 1──* deployment
```

## UI Pages

### 1. Projects List
- Table of all projects with status summary (training specs, runs)
- Create / edit / delete projects
- Project fields: name, description, catalog, schema, model name

### 2. Project Detail
- Overview: catalog/schema, model name, summary counts
- Tabs: **EOL**, **Training**, **Deployment**

### 3. EOL Builder (within project)
- Write SQL definition for the entity/observation/label "spine"
- Specify label column, entity columns, timestamp column
- Preview SQL results against the warehouse (validate before saving)
- **View:** Click an existing EOL to expand and see its full definition (read-only)
- **Copy:** Duplicate an existing EOL to create a new version that can be modified before saving

### 4. Training (within project)
Consolidates feature definitions, dataset configuration, and model training into a single workflow. A **training spec** defines everything needed to produce a model: features, split, task type, and parameters.

**Creating a training spec:**
1. Give it a name (e.g. `customer_churn_v2`)
2. Select an **EOL** from dropdown
3. Select **task type**: classification or regression
4. Configure **split strategy**: None (CV), Train/Eval, or Train/Eval/Test
5. Configure **split method** (when not None): Random (stratified) or Temporal
6. Set split percentages and seed
7. Optionally set hyperparameters (JSON)

**Adding feature entries** (one or more per spec):

*Standard FeatureLookup entry:*
1. Select a **feature table** via cascading dropdowns: Catalog → Schema → Table
   - Dropdowns populated from Unity Catalog metadata via SQL warehouse
2. Select **feature columns** from the chosen table (multi-select)
3. Select **lookup key(s)** from the EOL's entity columns
4. Optionally set **timestamp lookup key** and **default values**

*Declarative Feature entry (beta):*
1. Configure via guided form: source table, input column, function, time window

**View & Copy:**
- **View:** Click an existing spec to expand and see all details (read-only)
- **Copy:** Duplicate an existing spec (with all entries) to create a new version. Use this to experiment with different features, splits, or parameters.

Training specs are **locked** once a run has been created against them — no edits to features, split, or parameters after that point. This preserves lineage: a model always traces back to the exact spec that produced it. To iterate, copy the spec.

**Running:**
- Click **Run** on a training spec → launches a single Databricks Job that:
  1. Executes EOL SQL → spine DataFrame
  2. Builds `FeatureLookup` objects → `fe.create_training_set()` → `load_df()`
  3. Splits per strategy/method
  4. Trains model (CatBoost, configured by task_type + parameters)
  5. Logs with `fe.log_model()` (embeds feature specs for serving)
- View runs with status, metrics, job/MLflow links
- **Register model:** One-click registration via app-managed notebook (`register_model.py`)
- Auto-polls running jobs every 10s
- Delete runs to clean up

**App-managed training notebooks** (in `notebooks/` folder, uploaded to workspace before each run):
- `train_cv.py` — cross-validation (split_strategy = `none`)
- `train_standard.py` — standard train/eval (split_strategy = `train_eval`)
- `train_hpsearch.py` — hyperparameter search (split_strategy = `train_eval_test`, future)

Each notebook handles both classification and regression via the `task_type` parameter. The app selects the right notebook automatically based on split_strategy.

Server endpoints for cascading dropdowns:
- `GET /api/uc/catalogs` — list catalogs via SQL warehouse
- `GET /api/uc/schemas?catalog=X` — list schemas in catalog
- `GET /api/uc/tables?catalog=X&schema=Y` — list tables in schema
- `GET /api/uc/columns?catalog=X&schema=Y&table=Z` — list columns in table

### 5. Deployment (within project)

Manages synced tables (online feature store) and serving endpoints.

Since training notebooks use `fe.log_model()`, models have feature specs embedded. Databricks serving endpoints automatically resolve feature lookups from synced tables at inference time. If the requesting system supplies a feature in the request payload, it is used instead of the lookup — built-in Databricks behavior.

**Two sections in the tab:**

1. **Synced Tables** (project-level) — all distinct source tables from feature entries across all training specs. Shows sync status, pipeline link, and which training specs reference each table. When a deployment is selected, highlights tables required by that model.
2. **Deployments** — each links a registered model to a serving endpoint.

**Creating a deployment:**
1. Give it a name (e.g. `production`)
2. Select a registered model version from dropdown
3. All required feature tables must be synced (ONLINE) before endpoint creation
4. Link to Databricks endpoint UI for management (stop/start)

**Syncing tables:**
- One-click sync via Lakebase Projects API (`POST /api/2.0/postgres/synced_tables`)
- PK columns read from UC `information_schema.constraint_column_usage`
- CDF auto-enabled on source tables before sync
- Naming: `{project_catalog}.{project_schema}.{short_name}_online`
- "Sync All Required" button per deployment

**Test interface:**
- Entity key inputs populated from sample data (queried from EOL SQL)
- Constructs `{"dataframe_records": [{...}]}` payload automatically
- Displays prediction response

## MLflow Integration Details

The app interacts with MLflow via the Databricks REST API (from the Express backend using the app's service principal or user OBO token):

| Capability | API | Notes |
|------------|-----|-------|
| List experiments | `GET /api/2.0/mlflow/experiments/list` | Filter by project tag |
| Get runs | `POST /api/2.0/mlflow/runs/search` | Filter by experiment, order by metric |
| Compare runs | Client-side from run data | Chart metrics across runs |
| Get model versions | UC Model Registry API | `GET /api/2.0/mlflow/databricks/registered-models/get` |
| Register model | Done by training notebook | App reads the result |
| Transition stage | UC Model Registry API | Promote versions |
| Create serving endpoint | `POST /api/2.0/serving-endpoints` | From registered model version |
| Get endpoint status | `GET /api/2.0/serving-endpoints/{name}` | Poll for readiness |
| Query endpoint | `POST /serving-endpoints/{name}/invocations` | Test inference with entity keys |
| Create online table | `POST /api/2.0/online-tables` | Publish feature table to online store |
| Get online table status | `GET /api/2.0/online-tables/{name}` | Poll provisioning status |
| Delete online table | `DELETE /api/2.0/online-tables/{name}` | Remove online table |

## App-Managed Notebooks

All training notebooks live in this codebase under `notebooks/` and are uploaded to the workspace via the Workspace Import API before each job run. Users do not provide their own notebooks.

### Training Notebooks

Each notebook handles the full pipeline: materialize → split → train → log with `fe.log_model()`.

| Notebook | Split Strategy | Description |
|----------|---------------|-------------|
| `train_cv.py` | `none` | Cross-validation. No separate eval set — CV handles splitting internally. |
| `train_standard.py` | `train_eval` | Standard train/eval. Train on one split, evaluate on the other. |
| `train_hpsearch.py` | `train_eval_test` | Hyperparameter search. Eval for tuning, test for final assessment. (Future) |

Each notebook handles both classification and regression via the `task_type` parameter.

### Notebook Parameters (widget contract)

All training notebooks accept:
- `eol_sql` — the spine SQL (entity keys + label)
- `label_column` — target column name
- `entity_columns_json` — JSON array of entity key column names
- `feature_lookups_json` — JSON array of feature lookup definitions
- `task_type` — `'classification'` or `'regression'`
- `split_method` — `'random'` or `'temporal'` (ignored for CV)
- `split_config_json` — `{"eval_pct": 20, "test_pct": 10, "seed": 42}`
- `parameters_json` — hyperparameters (e.g. `{"iterations": 100}`)
- `catalog` — Unity Catalog catalog
- `schema` — UC schema
- `experiment_name` — MLflow experiment name (short; notebook prepends `/Users/<username>/`)

### Notebook Pipeline (what each notebook does)

1. Execute EOL SQL → spine DataFrame
2. Build `FeatureLookup` objects from `feature_lookups_json`
3. `fe.create_training_set(spine, feature_lookups, label)` → training_set
4. `training_set.load_df()` → full DataFrame with features
5. Split per `split_method` + `split_config` (or skip for CV)
6. Train model (CatBoost, configured by `task_type` + `parameters_json`)
7. `fe.log_model(model, training_set=training_set, ...)` → model with feature specs embedded
8. Return metrics + table names via `dbutils.notebook.exit()`

### Registration Notebook

`register_model.py` — separate app-managed notebook for model registration. Called when user clicks "Register" on a successful run.
- **Params:** `run_id`, `model_name`
- **Logic:** `mlflow.register_model(f"runs:/{run_id}/model", model_name)`
- Required because the REST API cannot resolve artifact paths when DBFS root is disabled.

### Job Execution Pattern

All compute runs as **Databricks Jobs** (serverless), not in the app process:

1. **Upload notebook** to workspace via `POST /api/2.0/workspace/import`
2. **Create job** (`POST /api/2.1/jobs/create`) — workspace notebook, environment with `databricks-feature-engineering`
3. **Run job** (`POST /api/2.1/jobs/run-now`) with `notebook_params`
4. **Poll for completion** (`GET /api/2.1/jobs/runs/get`)
5. **Parse output** (`GET /api/2.1/jobs/runs/get-output` on task run_id)
6. **Update Lakebase** with metrics, model info, table names

## app.yaml

```yaml
command: ['npx', 'tsx', 'server/server.ts']
env:
  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom: sql-warehouse
```

Note: `LAKEBASE_ENDPOINT` is set via the app's postgres resource (added via REST API after bundle deploy, since DABs doesn't support the `postgres` resource type yet). For local dev, `PGPASSWORD` is generated from a CLI token and passed to the lakebase plugin directly.

## Local Development

```bash
./dev.sh  # Generates Lakebase token, starts dev server on http://localhost:8000
```

The dev script sets `PGPASSWORD` via `databricks postgres generate-database-credential` (token expires in ~1 hour). The server detects `PGPASSWORD` and passes it to the AppKit lakebase plugin as native auth, bypassing the `LAKEBASE_ENDPOINT` OAuth token refresh.

## Deployment

Deployment to Databricks Apps requires:
1. Bundling the server with esbuild (minified, <10MB file size limit)
2. Using a minimal `package.json` with no dependencies (to avoid npm install on the remote, which can't reach the registry)
3. Re-adding the Lakebase postgres resource after each `databricks bundle deploy` (bundle overwrites app resources)

See `deploy.sh` for the full deployment script.

## Key Design Decisions

1. **AppKit over APX** — AppKit is the official Databricks SDK with built-in Lakebase plugin, type-safe queries, and active maintenance. React + Express is the standard pattern.

2. **Lakebase for app state, UC for ML artifacts** — App metadata (projects, training specs, run tracking) lives in Lakebase. Actual data (feature tables, models) lives in Unity Catalog. Clean separation of concerns.

3. **Consolidated training spec** — Feature definitions, dataset split config, task type, and parameters are combined into a single `training_spec` entity. This eliminates the previous separate `feature_definition` and `dataset` tables and reduces the UI from 4 tabs (EOL, Features, Datasets, Training) to 2 tabs (EOL, Training). Experimentation is done by copying a training spec and modifying it.

4. **App-managed training notebooks** — Training notebooks live in the app's `notebooks/` folder, not in a user's git repo. The app selects the right notebook based on `split_strategy` (CV, standard, or hyperparameter search). Each notebook handles both classification and regression via the `task_type` parameter. This gives the app full control over the training pipeline and ensures `fe.log_model()` is always used correctly.

5. **Single-job training pipeline** — Each run executes one Databricks Job that does everything: EOL SQL → feature lookups → `fe.create_training_set()` → split → train → `fe.log_model()`. No separate materialize step. The `training_set` object flows through the entire pipeline, ensuring the model's feature specs are embedded for serving.

6. **`fe.log_model()` enables auto-feature-lookup** — Training notebooks log models with `FeatureEngineeringClient.log_model()`, embedding the feature spec. Databricks serving endpoints automatically look up features from synced tables at inference time.

7. **Declarative features stored as JSONB** — Since the declarative feature API is in beta, storing the full definition as JSONB in `feature_entry.declarative_spec` is more flexible than normalizing.

8. **Training specs lock on first run** — A training spec can be edited freely until its first run is created. After that, it's locked — features, split config, and parameters are frozen to preserve lineage. The UI disables editing and shows a "Copy" action instead. This is enforced server-side: PUT/POST endpoints reject changes to specs that have runs.

9. **Synced tables (not online tables)** — Feature tables are synced to Lakebase via `POST /api/2.0/postgres/synced_tables`. PK columns are read from UC `information_schema.constraint_column_usage`. CDF is auto-enabled on source tables. Synced tables are project-level, shared across deployments.

10. **Notebook-based model registration** — Model registration uses an app-managed notebook (`register_model.py`) that calls `mlflow.register_model()`. The REST API cannot resolve artifact paths when DBFS root is disabled — the Python SDK has internal access.

11. **MLflow via REST API for reads, notebooks for writes** — The Express backend calls MLflow REST endpoints for reading (experiments, runs, metrics). Model logging and registration require the Python SDK and run inside notebooks.

---

## Action Items
- [x] Scaffold AppKit project with `databricks apps init` — 2026-04-11
- [x] Design and create Lakebase schema — 2026-04-11
- [x] Build Express API routes for CRUD on all entities — 2026-04-11
- [x] Build React UI: Projects list + detail page (incl. GitHub notebook auto-fetch) — 2026-04-11
- [x] Build React UI: Dataset builder, Runs tab — 2026-04-11
- [x] Build job launcher logic (submit runs to Databricks Jobs API) — 2026-04-11
- [x] Create Lakebase tables via psql — 2026-04-12
- [x] Fix materialize notebook upload (Workspace Import API) — 2026-04-12
- [x] Add `databricks-feature-engineering` to serverless job environment — 2026-04-12
- [x] Refactor feature_definition → container + feature_entry model (multi-table support) — 2026-04-12
- [x] Add view/expand for existing EOLs and feature definitions (read-only detail view) — 2026-04-12
- [x] Add copy-to-new-version for EOLs and feature definitions — 2026-04-12
- [x] Remove DROP TABLE statements from schema init — 2026-04-13
- [x] Merge training_run + evaluation_run into single `run` table — 2026-04-13
- [x] Single-task training job (training notebook includes evaluation) — 2026-04-14
- [x] MLflow experiment link resolved by numeric ID via API — 2026-04-14
- [x] Dynamic username resolution (PGUSER local, SCIM API deployed) — 2026-04-14
- [x] Read-only detail view for feature lookup entries — 2026-04-14
- [x] Pushed to GitHub: `appkit-rewrite` branch on BenMacKenzie/mlops — 2026-04-14
- [x] Fix DeltaTableSource namespace — use short table_name with separate catalog_name/schema_name — 2026-04-17
- [x] Rename Runs tab → Training — 2026-04-17
- [x] Add model registration to Unity Catalog (one-click from training runs) — 2026-04-17
- [x] Add model_name as project-level field (backfill existing projects with project name) — 2026-04-17
- [x] Pass catalog/schema as training notebook params (for volume TMPDIR) — 2026-04-17
- [x] Consolidate metrics display to single column (test/eval only, hide training metrics) — 2026-04-17
- [x] Build Deployment tab: synced tables, endpoint creation, test inference — 2026-04-17
- [x] Synced table provisioning via Lakebase Projects API — 2026-04-17
- [x] Model registration via app-managed notebook (register_model.py) — 2026-04-17
- [x] Serving endpoint creation + status polling — 2026-04-17
- [x] Test inference interface with entity key inputs and sample data — 2026-04-17
- [ ] Consolidate UI: merge Features/Datasets/Training tabs into single Training tab — 2026-04-17
- [ ] Create `app.training_spec` table, migrate from feature_definition + dataset — 2026-04-17
- [ ] Write `train_cv.py` notebook (full pipeline: EOL → features → CV → fe.log_model) — 2026-04-17
- [ ] Write `train_standard.py` notebook (train/eval split) — 2026-04-17
- [ ] Remove git_url/notebook_path from project, remove GitHub notebook listing — 2026-04-17
- [ ] End-to-end test: create project → EOL → training spec → run → register → deploy — 2026-04-17
- [ ] Deploy to workspace (blocked: npm registry unreachable from app runtime) — 2026-04-12
- [ ] Build MLflow experiment viewer + run comparison UI — 2026-04-12
- [ ] Fix local dev Lakebase auth (SASL issue with AppKit token refresh) — 2026-04-12

## Decisions
- 2026-04-11: Use AppKit (not APX) for React app framework — official SDK, built-in Lakebase plugin
- 2026-04-11: Store declarative feature specs as JSONB — beta API may change, flexibility > normalization
- 2026-04-11: Rewrite from scratch in React/TS rather than porting Dash app — cleaner architecture, better Databricks Apps integration
- 2026-04-11: Notebooks are user-provided via git, not baked into the app — keeps app generic, data scientists write training code in their preferred way
- 2026-04-12: Use `appkit.server.extend()` for custom routes (not `configure` callback which doesn't exist)
- 2026-04-12: For local dev, pass `PGPASSWORD` directly to lakebase plugin as native auth — AppKit's `LAKEBASE_ENDPOINT` OAuth token refresh doesn't work with CLI profile auth
- 2026-04-12: esbuild bundling for deploy has plugin manifest issues — using `npx tsx server/server.ts` as app command instead; deployment still blocked on npm registry access from app runtime
- 2026-04-12: Refactored feature_definition into container + feature_entry — a definition is a named spec (name + EOL), entries are individual lookups/declarative features that can reference different tables. Materialize gathers all entries. Definitions are immutable; copy to create new versions.
- 2026-04-12: Materialize notebook auto-uploaded to workspace via Import API (`/api/2.0/workspace/import`) before each job run — strips `/Workspace` prefix for API, uses `format: SOURCE`
- 2026-04-12: Serverless jobs need `databricks-feature-engineering` in environment dependencies spec
- 2026-04-13: Merged training_run + evaluation_run into single `run` table — eliminated separate evaluation tab
- 2026-04-13: Git source jobs use job-level `git_source` (not task-level) and relative notebook paths (no leading `/`); workspace jobs use absolute paths
- 2026-04-14: Dropped separate evaluation task — training notebook includes `mlflow.evaluate()`, so a single-task job is sufficient
- 2026-04-14: MLflow experiment names are short (`project_dataset`); notebook prepends `/Users/<username>/`. Server resolves numeric experiment ID via `GET /api/2.0/mlflow/experiments/get-by-name` (note: MLflow API is at 2.0, not 2.1)
- 2026-04-14: Username resolved dynamically — `PGUSER` env var for local dev, Databricks SCIM API (`/api/2.1/preview/scim/v2/Me`) for deployed app, cached after first call
- 2026-04-14: Code pushed to `appkit-rewrite` branch on https://github.com/BenMacKenzie/mlops
- 2026-04-14: MLflow experiment scoped to project (not dataset) so all runs appear in one experiment; experiment ID lookup falls back to null instead of name to avoid broken links
- 2026-04-14: Materialize notebook now excludes `timestamp_lookup_key` columns (observation dates) alongside entity columns
- 2026-04-14: Dataset tab now links to UC table explorer and job run URL (visible in all statuses, not just MATERIALIZING)
- 2026-04-14: **Future enhancement** — consolidate to a single Databricks Job per project (store `job_id` on project record, reuse across all runs/datasets). Currently each run creates its own job on first launch; `notebook_params` already vary per run so a shared job would work.
- 2026-04-17: DeltaTableSource requires `catalog_name` and `schema_name` as separate params; `table_name` must be the short name only — passing a fully qualified name caused garbled namespace resolution
- 2026-04-17: `fe.create_feature()` also requires `catalog_name`/`schema_name` — these specify the output feature catalog/schema
- 2026-04-17: Model registration uses an app-managed notebook (`notebooks/register_model.py`) launched as a Databricks Job. The notebook calls `mlflow.register_model(f"runs:/{run_id}/model", model_name)`. This is required because the REST API (`unity-catalog/model-versions/create`) cannot resolve artifact paths when DBFS root is disabled — the Python SDK has internal access to the artifact store.
- 2026-04-17: MLflow `runs/get` is a GET endpoint (not POST) — must pass `run_id` as query param
- 2026-04-17: Metrics bucketing handles both `mlflow.evaluate()` prefixes (`eval_`, `evaluation_`) and CatBoost CV prefixes (`test-`, `train-`). UI shows only test/eval metrics.
- 2026-04-17: `model_name` added to project table as UC model registry name; defaults to project name. Backfilled via `UPDATE app.project SET model_name = name WHERE model_name = ''`
- 2026-04-17: Training notebook receives `catalog` and `schema` as widget params — needed for constructing volume paths (e.g. TMPDIR for CatBoost on serverless)
- 2026-04-17: Serverless jobs cannot write to `/tmp` — use UC volumes for temp storage. CatBoost `cv()` needs `train_dir` or `logging_dir` set to a volume path.
- 2026-04-17: Online tables are project-level, shared across deployments. A source table published once serves all deployments referencing it. Multiple models/datasets can share the same online table.
- 2026-04-17: All feature tables from a dataset must be published as online tables before the model can be deployed to a serving endpoint. No per-feature request-time selection needed — if the requesting system supplies a feature in the request payload, Databricks serving uses it instead of looking it up. This is built-in behavior.
- 2026-04-17: Training notebooks already use `FeatureEngineeringClient.log_model()` — models have feature specs embedded. Serving endpoints auto-resolve feature lookups from online tables at inference time. No notebook contract changes needed for deployment.
- 2026-04-17: Online table naming convention: `{project_catalog}.{project_schema}.{short_table_name}_online`. Synced table creation uses `POST /api/2.0/postgres/synced_tables` with PK columns read from UC `information_schema.constraint_column_usage`. CDF enabled automatically on source tables before sync.
- 2026-04-17: Dataset split redesign — three strategies (none, train/eval, train/eval/test) × two methods (random stratified, temporal). Random splits always stratified on label column to maintain class proportions. Temporal splits sorted by EOL timestamp column, latest records to eval/test. "None" strategy for CV workflows where the notebook handles splitting internally. Replaces the original `eval_split_type`/`eval_split_config` design.
- 2026-04-17: Consolidated training spec — merged feature_definition + dataset + training config into single `training_spec` entity. UI reduced from 4 tabs to 2 (EOL + Training). Split strategy determines notebook: none→CV, train_eval→standard, train_eval_test→hpsearch. Copy workflow for experimentation.
- 2026-04-17: App-managed training notebooks — training notebooks live in `notebooks/` folder, not user git repos. App selects notebook based on split_strategy. Each notebook handles full pipeline: EOL → features → create_training_set → split → train → fe.log_model(). Ensures feature specs always embedded correctly. Project no longer needs git_url/notebook_path fields.
- 2026-04-17: Single-job training — no separate materialize step. Each run does everything in one notebook/job. The `training_set` object from `fe.create_training_set()` flows through to `fe.log_model()`, embedding feature specs for serving. Eliminates the need to cache/manage materialized tables separately.

## Meeting Notes
See `meetings/` folder for dated meeting notes.
