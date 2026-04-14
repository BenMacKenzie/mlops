# MLOps Model Manager

**Created:** 2026-04-11
**Status:** In Progress
**Workspace:** https://fevm-serverless-stable-1dpktm.cloud.databricks.com
**Default Catalog:** serverless_stable_1dpktm_catalog (user-configurable per project)
**Lakebase Instance:** mlops
**Owner:** Ben MacKenzie
**Prior Art:** https://github.com/BenMacKenzie/mlops/tree/main (Dash-based prototype — reference for data model, job patterns, feature lookup UI)
**Reference Notebooks:** https://github.com/BenMacKenzie/db-model-trainer/tree/main/notebooks (training/evaluation notebook contract)

---

## Problem Statement

Managing the full lifecycle of ML models — from feature definition through training to deployment — requires coordinating across multiple systems (Unity Catalog feature tables, MLflow experiments, model registry, serving endpoints, Databricks Jobs). The current prototype (Dash/Python) proved the concept but needs a rewrite as a proper Databricks App using React + AppKit with Lakebase for persistence.

The app should let a user:
1. Define a **project** that points to training/evaluation notebooks in a git repo
2. Define **datasets** using either standard `FeatureLookup` or declarative `Feature` definitions (beta)
3. **Materialize** datasets as Unity Catalog tables via Databricks Jobs
4. **Train** models by launching notebook jobs against materialized datasets
5. **Track** experiments, compare runs, and register models via MLflow
6. **Deploy** registered models to serving endpoints

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
│  │  - analytics│  │  - Serving endpoint mgmt       │   │
│  └──────┬──────┘  └──────────────────────────────┘   │
│         │                                            │
└─────────┼────────────────────────────────────────────┘
          │
          ▼
┌─────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  Lakebase        │  │  Unity Catalog   │  │  MLflow          │
│  (mlops)         │  │                  │  │                  │
│  - projects      │  │  - Feature tables│  │  - Experiments   │
│  - datasets      │  │  - Training tbls │  │  - Runs          │
│  - feature defs  │  │  - Models        │  │  - Model Registry│
│  - training runs │  │                  │  │  - Serving       │
│                  │  │                  │  │                  │
└─────────────────┘  └──────────────────┘  └──────────────────┘
          │                    │                      │
          └────────────────────┼──────────────────────┘
                               ▼
                    ┌──────────────────┐
                    │  Databricks Jobs │
                    │  - Materialize   │
                    │  - Train         │
                    └──────────────────┘
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
| Compute | Databricks Jobs (materialize + train notebooks) |
| Serving | Databricks Model Serving endpoints |
| Auth | AppKit dual-identity (service principal + user OBO) |

## Data Model (Lakebase)

Evolves the existing schema from the prototype. Key changes:
- Feature definitions are **containers** (name + EOL reference) with multiple **feature entries** inside
- Each feature entry is either a standard FeatureLookup or a declarative Feature — entries can reference different tables
- A dataset materializes a single feature definition (which may contain many entries from many tables)
- Model version tracking linked to training runs
- Serving endpoints queried live from Databricks API (no local table)

### Tables

#### `app.project`
| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| name | VARCHAR(255) | Unique project name |
| description | TEXT | |
| catalog | VARCHAR(255) | Unity Catalog catalog for this project's assets |
| schema | VARCHAR(255) | UC schema within catalog |
| git_url | TEXT | GitHub repo URL (e.g. `https://github.com/user/repo`) |
| notebook_path | TEXT | Path within repo where notebooks live (e.g. `notebooks`) |
| training_notebook | TEXT | Notebook filename selected from git repo (e.g. `01_Train_Classification_Model.py`) |
| evaluation_notebook | TEXT | Notebook filename selected from git repo (e.g. `02_Eval_Model.py`) |

#### `app.entity_observation_label` (was `eol_definition`)
Defines the base entity/observation/label SQL — the "spine" of a training set.

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| project_id | BIGINT FK → project | |
| name | VARCHAR(255) | |
| sql_definition | TEXT | SQL query that produces entity keys + label |
| label_column | VARCHAR(255) | Name of the label column |
| entity_columns | TEXT[] | Primary key / entity columns |
| timestamp_column | VARCHAR(255) | Timestamp column (for point-in-time joins) |

#### `app.feature_definition`
A named feature spec container. References an EOL and contains multiple feature entries. Immutable after creation — to modify, copy to a new version.

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| project_id | BIGINT FK → project | |
| eol_id | BIGINT FK → entity_observation_label | The spine this feature spec is built against |
| name | VARCHAR(255) | e.g. `customer_churn_features_v2` |

#### `app.feature_entry`
An individual feature lookup or declarative feature within a feature definition. A feature definition can have many entries, each pulling from a different table.

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| feature_definition_id | BIGINT FK → feature_definition | Parent container |
| feature_type | VARCHAR(50) | `'lookup'` or `'declarative'` |
| table_name | VARCHAR(255) | UC feature table (for lookup type) |
| feature_names | TEXT[] | Columns to look up |
| lookup_key | TEXT[] | Join keys mapping to EOL entity columns |
| timestamp_lookup_key | VARCHAR(255) | For point-in-time lookups |
| default_values | JSONB | Default values for missing features |
| declarative_spec | JSONB | Full declarative Feature spec (for declarative type) |

#### `app.dataset`
A materialized feature definition, split into train/eval UC tables.

| Column                 | Type                                 | Notes                                                     |
| ---------------------- | ------------------------------------ | --------------------------------------------------------- |
| id                     | BIGSERIAL PK                         |                                                           |
| project_id             | BIGINT FK → project                  |                                                           |
| name                   | VARCHAR(255)                         |                                                           |
| feature_definition_id  | BIGINT FK → feature_definition       | The feature spec to materialize (includes EOL + entries)  |
| eval_split_type        | VARCHAR(50)                          | `'percentage'`, `'time'`, `'custom'`                      |
| eval_split_config      | JSONB                                | Split parameters                                          |
| status                 | VARCHAR(50)                          | `'NOT_STARTED'`, `'MATERIALIZING'`, `'READY'`, `'FAILED'` |
| training_table         | VARCHAR(255)                         | UC table name once materialized                           |
| eval_table             | VARCHAR(255)                         | UC table name once materialized                           |
| materialize_job_id     | BIGINT                               | Databricks job ID                                         |
| materialize_run_id     | BIGINT                               | Databricks run ID                                         |
| materialize_run_url    | TEXT                                 |                                                           |
| row_count              | BIGINT                               | Row count after materialization                           |

#### `app.run`
A single run that trains a model then evaluates it, executed as a multi-task Databricks job (train → evaluate) within one MLflow experiment.

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| project_id | BIGINT FK → project | |
| dataset_id | BIGINT FK → dataset | |
| job_id | BIGINT | Databricks job ID (reused across runs) |
| run_id | BIGINT | Databricks run ID (for this execution) |
| mlflow_experiment_id | VARCHAR(255) | MLflow experiment name |
| mlflow_run_id | VARCHAR(255) | |
| parameters | JSONB | Hyperparameters passed to training notebook |
| training_metrics | JSONB | Metrics from training task |
| eval_metrics | JSONB | Metrics from evaluation task |
| status | VARCHAR(50) | `'PENDING'`, `'RUNNING'`, `'SUCCESS'`, `'FAILED'` |
| model_uri | TEXT | MLflow model URI if registered |
| model_name | VARCHAR(255) | Registered model name in UC |
| model_version | INTEGER | Version in model registry |
| databricks_run_url | TEXT | |
| error_message | TEXT | |
| started_at | TIMESTAMP | |
| ended_at | TIMESTAMP | |

### Entity Relationship Summary

```
project 1──* entity_observation_label
project 1──* feature_definition
project 1──* dataset
project 1──* run

entity_observation_label 1──* feature_definition
feature_definition 1──* feature_entry
feature_definition 1──* dataset
dataset 1──* run
```

## UI Pages

### 1. Projects List
- Table of all projects with status summary (datasets, runs)
- Create / edit / delete projects
- When creating a project, user enters a GitHub repo URL; the app uses the GitHub API to list `.py` and `.ipynb` files in the repo's notebook path, then presents dropdowns for selecting training and evaluation notebooks

### 2. Project Detail
- Overview: git repo, catalog/schema, notebooks
- Tabs for the sub-entities below

### 3. EOL Builder (within project)
- Write SQL definition for the entity/observation/label "spine"
- Specify label column, entity columns, timestamp column
- Preview SQL results against the warehouse (validate before saving)
- **View:** Click an existing EOL to expand and see its full definition (read-only)
- **Copy:** Duplicate an existing EOL to create a new version that can be modified before saving

### 4. Feature Definition Builder (within project)
A feature definition is a **named container** that references an EOL and holds one or more **feature entries**. Each entry pulls features from a different table. This lets you build a complete feature spec in one place.

**Creating a feature definition:**
1. Give it a name (e.g. `customer_churn_features_v2`)
2. Select an **EOL** from dropdown (scoped to project) — this is the spine all entries join against

**Adding feature entries** (one or more per definition):

*Standard FeatureLookup entry:*
1. Select a **feature table** via cascading dropdowns: Catalog → Schema → Table
   - Dropdowns populated from Unity Catalog metadata via SQL warehouse (`SHOW CATALOGS`, `SHOW SCHEMAS IN {catalog}`, `SHOW TABLES IN {catalog}.{schema}`)
2. Select **feature columns** from the chosen table (multi-select, populated from `DESCRIBE TABLE`)
3. Select **lookup key(s)** from the EOL's entity columns (maps feature table to EOL)
4. Optionally set **timestamp lookup key** and **default values**

*Declarative Feature entry (beta):*
1. Enter declarative feature spec as JSON (source tables, entity/time columns, feature SQL)

**View & Copy:**
- **View:** Click an existing feature definition to expand and see its EOL reference and all entries (read-only)
- **Copy:** Duplicate an existing feature definition (with all its entries) to create a new version that can be modified before saving

Feature definitions are immutable after creation — to change the feature spec, copy to a new version, modify, and save. This preserves lineage: a dataset always points to the exact feature spec that produced it.

### 5. Dataset Builder (within project)
- Select a **feature definition** from dropdown (which already references an EOL and contains all feature entries)
- Configure **eval split** (percentage, time-based, custom)
- **Materialize:** Launch Databricks job that executes all feature entries in the definition, poll for status, show results

Server endpoints needed for the cascading dropdowns:
- `GET /api/uc/catalogs` — list catalogs via SQL warehouse
- `GET /api/uc/schemas?catalog=X` — list schemas in catalog
- `GET /api/uc/tables?catalog=X&schema=Y` — list tables in schema
- `GET /api/uc/columns?catalog=X&schema=Y&table=Z` — list columns in table

### 6. Runs (within project)
A single "run" trains a model then evaluates it as a multi-task Databricks job (train → evaluate), both logging to the same MLflow experiment.

- Select a materialized dataset
- Configure hyperparameters (or use notebook defaults)
- Launch run — creates a two-task job: train notebook → evaluation notebook (sequential, with `depends_on`)
- View running/completed runs with status, training metrics, and eval metrics
- Links to Databricks job and MLflow experiment for each run
- Auto-polls running jobs every 10s; status updated to SUCCESS/FAILED once terminal
- Job reuse: first run creates the multi-task job definition, subsequent runs reuse it with `run-now`

### 7. MLflow Integration
- **Experiment Viewer:** List experiments for the project, show runs with metrics/params
- **Run Comparison:** Side-by-side metric comparison for selected runs
- **Model Registry:** List registered models, versions, stage transitions
- Use MLflow REST API / Databricks SDK (not embedded iframe)

### 8. Serving Endpoints (within project)
- Query serving endpoints from Databricks API for models registered under this project
- Create endpoint from a registered model version
- Monitor endpoint status
- Basic test interface (send JSON, see prediction)
- No local Lakebase table — all state read live from Databricks serving API

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
| Create serving endpoint | `POST /api/2.0/serving-endpoints` | From registered model |
| Get endpoint status | `GET /api/2.0/serving-endpoints/{name}` | Poll for readiness |

## Databricks Jobs

The app does NOT contain training or evaluation notebooks. Users provide their own notebooks via a git repo URL + path (configured per project). The app launches these notebooks as Databricks Jobs, passing standardized widget parameters.

### Notebook Contract

Training and evaluation notebooks must accept these `dbutils.widgets`:

**Training notebooks:**
- `target` — label column name
- `training_table_name` — UC table with training data
- `eval_table_name` — UC table with eval data
- `experiment_name` — MLflow experiment name
- Must set task values: `dbutils.jobs.taskValues.set(key="model_uri", value=model_info.model_uri)`

**Evaluation notebooks:**
- `target` — label column name
- `eval_table_name` — UC table with eval data
- `model_uri` — MLflow model URI from training task (e.g. `runs:/<run_id>/model`)
- `experiment_name` — MLflow experiment name

See https://github.com/BenMacKenzie/db-model-trainer/tree/main/notebooks for reference implementations.

### Job Execution Pattern

All compute-intensive work (materialize, train, evaluate) runs as **Databricks Jobs**, not in the app process. The app server uses the Databricks REST API to:

1. **Create a job** (`POST /api/2.1/jobs/create`) — persistent job definition with git source, notebook path, and base parameters
2. **Run the job** (`POST /api/2.1/jobs/run-now`) — trigger execution with `notebook_params`
3. **Poll for completion** (`GET /api/2.1/jobs/runs/get`) — check status, update Lakebase when done
4. **Store the job_id** in Lakebase so it can be rerun without recreating

Note: Notebook paths should strip `.py` extension when passed to the Databricks Jobs API (the API expects the path without extension).

### Materialize Job
- **Notebook:** App-managed — lives in this codebase at `notebooks/materialize.py`, uploaded to workspace by the app
- **Params:** `eol_sql`, `feature_lookup_json`, `catalog`, `schema`, `training_table_name`, `eval_table_name`, `eval_split_percentage`
- **Process:** Executes EOL SQL, applies FeatureLookups via `FeatureEngineeringClient.create_training_set()`, splits into train/eval, writes to UC tables
- **Output:** App updates dataset status + table names + row count in Lakebase

### Training Job
- **Notebook:** User-provided, referenced from the user's git repo
- **Git source:** Project's `git_url` + `notebook_path/training_notebook`
- **Params:** `target`, `training_table_name`, `eval_table_name`, `experiment_name`
- **Output:** Notebook logs to MLflow; app reads experiment/run data via REST API

### Evaluation Job
- **Notebook:** User-provided, referenced from the user's git repo
- **Git source:** Project's `git_url` + `notebook_path/evaluation_notebook`
- **Params:** `target`, `eval_table_name`, `model_name`, `model_version`, `experiment_name`
- **Output:** Notebook logs eval metrics to MLflow; app reads results via REST API

### Job creation and execution code
The Python SDK wrappers that create and run Databricks Jobs are **app server logic**, implemented as Express API endpoints. The user's git repo contains only the training/evaluation notebooks — not any job orchestration code. The reference implementation at https://github.com/BenMacKenzie/db-model-trainer/tree/main/notebooks shows the notebook contract (widget params) that user notebooks must follow.

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

2. **Lakebase for app state, UC for ML artifacts** — App metadata (projects, dataset definitions, run tracking) lives in Lakebase. Actual data (feature tables, training tables, models) lives in Unity Catalog. Clean separation of concerns.

3. **Declarative features stored as JSONB** — Since the declarative feature API is in beta and the spec may evolve, storing the full definition as JSONB in `feature_entry.declarative_spec` is more flexible than normalizing into separate tables. The `feature_type` discriminator column lets the UI and backend handle both paths.

4. **MLflow via REST API, not SDK** — The Express backend calls MLflow REST endpoints directly. This avoids Python SDK dependency and works naturally with the Node.js/TypeScript stack. The training notebooks (Python) use the MLflow Python SDK directly.

5. **Notebook-driven training** — The app doesn't contain training or evaluation logic. Users provide their own notebooks (in a git repo) that follow a standard widget parameter contract (`target`, `training_table_name`, `eval_table_name`, `experiment_name`, etc.). The app launches them as Databricks Jobs. See [db-model-trainer](https://github.com/BenMacKenzie/db-model-trainer/tree/main/notebooks) for reference implementations.

6. **Feature definition is a container with entries** — A `feature_definition` is a named container (with EOL reference) that holds multiple `feature_entry` rows. Each entry can be a lookup or declarative feature from a different table. This lets users build a complete feature spec in one place. Datasets point to one feature definition, which may have many entries.

7. **EOL renamed for clarity** — `eol_definition` → `entity_observation_label` to be more self-documenting.

8. **Immutable feature definitions** — Feature definitions cannot be edited after creation. To modify, users copy an existing definition to a new version. This preserves lineage: a dataset always points to the exact feature spec that produced it.

9. **Materialize notebook auto-uploaded** — The app-managed `notebooks/materialize.py` is uploaded to the workspace via the Workspace Import API before each materialize job runs. This ensures the notebook is always in sync with the app code.

---

## Action Items
- [x] Scaffold AppKit project with `databricks apps init` — 2026-04-11
- [x] Design and create Lakebase schema — 2026-04-11
- [x] Build Express API routes for CRUD on all entities — 2026-04-11
- [x] Build React UI: Projects list + detail page (incl. GitHub notebook auto-fetch) — 2026-04-11
- [x] Build React UI: Dataset builder, Training, Evaluation tabs — 2026-04-11
- [x] Build job launcher logic (submit runs to Databricks Jobs API) — 2026-04-11
- [x] Create Lakebase tables via psql — 2026-04-12
- [x] Fix materialize notebook upload (Workspace Import API) — 2026-04-12
- [x] Add `databricks-feature-engineering` to serverless job environment — 2026-04-12
- [x] Refactor feature_definition → container + feature_entry model (multi-table support) — 2026-04-12
- [x] Add view/expand for existing EOLs and feature definitions (read-only detail view) — 2026-04-12
- [x] Add copy-to-new-version for EOLs and feature definitions — 2026-04-12
- [ ] Remove DROP TABLE statements from schema init (added for migration, should be one-time) — 2026-04-12
- [ ] Fix local dev Lakebase auth (SASL issue with AppKit token refresh) — 2026-04-12
- [ ] End-to-end test: create project, create EOL, create dataset, train, evaluate — 2026-04-12
- [ ] Deploy to workspace (blocked: npm registry unreachable from app runtime, esbuild bundle has plugin manifest issue) — 2026-04-12
- [ ] Build MLflow experiment viewer + run comparison UI — 2026-04-12
- [ ] Build Serving endpoints UI (live from Databricks API) — 2026-04-12

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
- 2026-04-13: Merged training_run + evaluation_run into single `run` table — train and evaluate are two tasks in one Databricks job, same MLflow experiment. Eliminated separate evaluation tab.
- 2026-04-13: Git source jobs use job-level `git_source` (not task-level) and relative notebook paths (no leading `/`); workspace jobs use absolute paths

## Meeting Notes
See `meetings/` folder for dated meeting notes.
