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
│  - datasets      │  │  - Training tbls │  │  - Runs          │
│  - feature defs  │  │  - Models        │  │  - Model Registry│
│  - training runs │  │  - Online tables │  │                  │
│  - online tables │  │                  │  │                  │
│  - deployments   │  │                  │  │                  │
└─────────────────┘  └──────────────────┘  └──────────────────┘
          │                    │                      │
          └────────────────────┼──────────────────────┘
                               ▼
┌──────────────────┐  ┌──────────────────────────────┐
│  Databricks Jobs │  │  Databricks Model Serving    │
│  - Materialize   │  │  - Auto feature lookup       │
│  - Train         │  │    (from online tables)       │
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
| Compute | Databricks Jobs (materialize + train notebooks) |
| Online Feature Store | Databricks Online Tables (synced from UC feature tables) |
| Serving | Databricks Model Serving endpoints (with auto feature lookup) |
| Auth | AppKit dual-identity (service principal + user OBO) |

## Data Model (Lakebase)

Evolves the existing schema from the prototype. Key changes:
- Feature definitions are **containers** (name + EOL reference) with multiple **feature entries** inside
- Each feature entry is either a standard FeatureLookup or a declarative Feature — entries can reference different tables
- A dataset materializes a single feature definition (which may contain many entries from many tables)
- Model version tracking linked to training runs
- Online table tracking for feature tables published to Databricks Online Tables
- Deployment tracking links registered models to serving endpoints with request-time feature configuration

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
A materialized feature definition. Split strategy controls how the data is divided for training and evaluation.

**Split strategies:**
- **None** — single output table, no split. For small datasets with cross-validation (CV handles train/val internally in the notebook).
- **Train / Eval** — two tables. Standard model selection: train to fit, eval to assess.
- **Train / Eval / Test** — three tables. For hyperparameter search: eval for tuning, test for final unbiased performance estimate (never used during development).

**Split methods** (when strategy is not `none`):
- **Random** — percentage-based stratified random split. Always stratified on the label column (from EOL) to maintain class proportions across splits. Uses a fixed seed for reproducibility.
- **Temporal** — sort by the EOL's timestamp column, latest records go to eval (and test). Most realistic for production ML — prevents future data leaking into training. Not stratified (time ordering takes precedence).

| Column                 | Type                                 | Notes                                                     |
| ---------------------- | ------------------------------------ | --------------------------------------------------------- |
| id                     | BIGSERIAL PK                         |                                                           |
| project_id             | BIGINT FK → project                  |                                                           |
| name                   | VARCHAR(255)                         |                                                           |
| feature_definition_id  | BIGINT FK → feature_definition       | The feature spec to materialize (includes EOL + entries)  |
| split_strategy         | VARCHAR(50)                          | `'none'`, `'train_eval'`, `'train_eval_test'`             |
| split_method           | VARCHAR(50)                          | `'random'`, `'temporal'` (null when strategy is `none`)   |
| split_config           | JSONB                                | See below                                                 |
| status                 | VARCHAR(50)                          | `'NOT_STARTED'`, `'MATERIALIZING'`, `'READY'`, `'FAILED'` |
| training_table         | VARCHAR(255)                         | UC table name once materialized (always present)          |
| eval_table             | VARCHAR(255)                         | UC table name (null when strategy is `none`)              |
| test_table             | VARCHAR(255)                         | UC table name (only for `train_eval_test`)                |
| materialize_job_id     | BIGINT                               | Databricks job ID                                         |
| materialize_run_id     | BIGINT                               | Databricks run ID                                         |
| materialize_run_url    | TEXT                                 |                                                           |
| row_count              | BIGINT                               | Row count after materialization                           |

**`split_config` JSONB structure by method:**

*Random:*
```json
{
  "eval_pct": 20,
  "test_pct": 10,
  "seed": 42
}
```
`test_pct` only present for `train_eval_test` strategy. Train gets the remainder.
Stratification on the label column (from EOL) is automatic — no config needed.

*Temporal:*
```json
{
  "eval_pct": 20,
  "test_pct": 10
}
```
Rows sorted by EOL's `timestamp_column`. Latest `test_pct`% → test, next `eval_pct`% → eval, rest → train. Percentages define the proportion of rows, not date ranges.

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

#### `app.online_table`
Tracks feature tables published to Databricks Online Tables. Project-level — shared across all deployments that reference the same source table.

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| project_id | BIGINT FK → project | |
| source_table | VARCHAR(255) | Fully-qualified UC table name (e.g. `catalog.schema.customer_features`) |
| online_table_name | VARCHAR(255) | Online table name in UC (convention: `{source_table}_online`) |
| primary_key_columns | TEXT[] | Entity/lookup key columns (from feature entry's `lookup_key`) |
| timeseries_key | VARCHAR(255) | For point-in-time lookups (from feature entry's `timestamp_lookup_key`) |
| sync_mode | VARCHAR(50) | `'triggered'` or `'continuous'` |
| status | VARCHAR(50) | `'NOT_PUBLISHED'`, `'PROVISIONING'`, `'ONLINE'`, `'FAILED'` |
| pipeline_id | VARCHAR(255) | Databricks pipeline ID for the online table |

#### `app.deployment`
Links a registered model version to a serving endpoint. All feature tables from the model's dataset must have online tables before the endpoint can be created.

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| project_id | BIGINT FK → project | |
| name | VARCHAR(255) | e.g. `'production'`, `'staging'` |
| run_id | BIGINT FK → run | Must have `model_name`/`model_version` set |
| endpoint_name | VARCHAR(255) | Serving endpoint name |
| endpoint_status | VARCHAR(50) | `'NOT_CREATED'`, `'CREATING'`, `'READY'`, `'FAILED'` |
| endpoint_config | JSONB | Instance type, scaling (min/max instances), etc. |
| created_at | TIMESTAMP | |

### Entity Relationship Summary

```
project 1──* entity_observation_label
project 1──* feature_definition
project 1──* dataset
project 1──* run
project 1──* online_table
project 1──* deployment

entity_observation_label 1──* feature_definition
feature_definition 1──* feature_entry
feature_definition 1──* dataset
dataset 1──* run
run 1──* deployment
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
- Configure **split strategy**:
  - **None** — no split, single output table. Best for small datasets where the training notebook uses cross-validation internally.
  - **Train / Eval** — two-way split. Standard for model selection.
  - **Train / Eval / Test** — three-way split. For hyperparameter search workflows where eval is used during tuning and test is held out for final assessment.
- Configure **split method** (when strategy is not "none"):
  - **Random** — stratified random split on the label column (from EOL). Maintains class proportions across splits. User sets eval % (and test % for 3-way) plus a seed for reproducibility.
  - **Temporal** — sort by EOL's timestamp column, latest records go to eval/test. Recommended when data has a time dimension to prevent future leakage. User sets eval % (and test %).
  - If the EOL has no timestamp column, temporal is disabled in the UI.
- **Materialize:** Launch Databricks job that executes all feature entries in the definition, splits per config, poll for status, show results
- Display: show output table names with links, row counts per split, job link

Server endpoints needed for the cascading dropdowns:
- `GET /api/uc/catalogs` — list catalogs via SQL warehouse
- `GET /api/uc/schemas?catalog=X` — list schemas in catalog
- `GET /api/uc/tables?catalog=X&schema=Y` — list tables in schema
- `GET /api/uc/columns?catalog=X&schema=Y&table=Z` — list columns in table

### 6. Training (within project)
A single "run" trains and evaluates a model in one Databricks Job, logging to one MLflow experiment.

- Select a materialized dataset
- Configure hyperparameters (or use notebook defaults)
- Launch run — creates a single-task job running the training notebook (which includes evaluation via `mlflow.evaluate()` or cross-validation)
- View running/completed runs with status and test/eval metrics (single Metrics column; training metrics hidden)
- **Register model:** One-click registration to Unity Catalog as `{catalog}.{schema}.{model_name}` (model name configured at project level). Creates registered model if needed, then creates a version from the MLflow run artifact. Registered models link to UC model explorer.
- Links to Databricks job and MLflow experiment (resolved by numeric ID) for each run
- Delete runs to clean up stale/failed entries
- Auto-polls running jobs every 10s; status updated to SUCCESS/FAILED once terminal
- Job reuse: first run creates the job definition, subsequent runs reuse it with `run-now`

### 7. MLflow Integration
- **Experiment Viewer:** List experiments for the project, show runs with metrics/params
- **Run Comparison:** Side-by-side metric comparison for selected runs
- **Model Registry:** List registered models, versions, stage transitions
- Use MLflow REST API / Databricks SDK (not embedded iframe)

### 8. Deployment (within project)

Manages the full workflow from registered model to live serving endpoint: specifying request-time features, publishing feature tables to the online store, and creating/monitoring serving endpoints.

Since training notebooks use `FeatureEngineeringClient.log_model()`, models have their feature specs embedded. When served, Databricks automatically resolves feature lookups from online tables at inference time. If the requesting system supplies a feature in the request payload, it is used instead of looking it up from the online table — this is built-in Databricks serving behavior.

**Two sections in the tab:**

1. **Online Tables** (project-level) — shows all distinct source tables referenced by feature entries across **all** datasets in the project. For each table: online table status (not published / provisioning / online / failed) and which datasets reference it. This gives a full picture of the project's feature table landscape and online readiness.
2. **Deployments** — each deployment links a registered model version to a serving endpoint. When viewing a deployment, the online tables list highlights which tables are required for that deployment's model (derived from the model's run → dataset → feature_definition → feature_entries). Tables not needed by the selected deployment are shown but de-emphasized.

**Creating a deployment:**
1. Give it a name (e.g. `production`, `staging-v2`)
2. Select a **registered model version** from dropdown (runs that have `model_name`/`model_version`)
3. Online tables list highlights the required tables for this model's dataset and shows their status
4. All required feature tables must be ONLINE before the endpoint can be created

**Publishing online tables:**
- One-click publish for tables that need online sync (creates Databricks Online Table via `POST /api/2.0/online-tables`)
- Online table spec derived from feature entry metadata:
  - `source_table_full_name` — from feature entry's `table_name`
  - `primary_key_columns` — from feature entry's `lookup_key`
  - `timeseries_key` — from feature entry's `timestamp_lookup_key` (if present)
  - `run_triggered` or `run_continuously` — user-selectable sync mode
- Online table naming convention: `{source_table}_online` (configurable)
- Status tracking: PROVISIONING → ONLINE (or FAILED), polled via `GET /api/2.0/online-tables/{name}`
- A "Publish All Required" button to publish all missing tables at once

**Creating the serving endpoint:**
- Prereq: all required online tables must be ONLINE
- Create via `POST /api/2.0/serving-endpoints` with the registered model's UC name and version
- Endpoint config: workload size, scale-to-zero, traffic routing
- The model's embedded feature spec (from `fe.log_model()`) handles feature resolution automatically

**Monitoring & testing:**
- Poll endpoint status via `GET /api/2.0/serving-endpoints/{name}`: NOT_READY → READY (or FAILED)
- Show endpoint URL when ready
- **Test interface:** send JSON payload with entity keys, display prediction response
- Delete deployment: tears down endpoint (online tables are project-level and remain)

**Online table view (project-level section):**
- Lists all distinct source tables from feature entries across all datasets in the project
- Columns: source table, online table name, sync mode, status, datasets that reference this table
- When a deployment is selected, rows needed by that deployment's model are visually highlighted
- Per-row actions: **Publish** (create online table) or **Remove** (delete online table, with warning if active deployments depend on it)
- "Publish All Required" button when a deployment is selected — publishes all missing tables needed by that model
- Status is interactive: user can publish or remove online tables at any time to control what's in the online store

Server endpoints needed:
- `GET /api/projects/:projectId/online-tables` — list online tables for project
- `POST /api/projects/:projectId/online-tables` — publish a new online table
- `POST /api/online-tables/:id/check-status` — poll provisioning status
- `DELETE /api/online-tables/:id` — remove an online table
- `GET /api/projects/:projectId/deployments` — list deployments
- `POST /api/projects/:projectId/deployments` — create a deployment
- `POST /api/deployments/:id/publish-tables` — publish all required online tables for this deployment
- `POST /api/deployments/:id/create-endpoint` — create the serving endpoint
- `POST /api/deployments/:id/check-status` — poll endpoint status
- `POST /api/deployments/:id/test` — send test inference request to endpoint
- `DELETE /api/deployments/:id` — delete deployment and endpoint

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

## Databricks Jobs

The app does NOT contain training or evaluation notebooks. Users provide their own notebooks via a git repo URL + path (configured per project). The app launches these notebooks as Databricks Jobs, passing standardized widget parameters.

### Notebook Contract

Training and evaluation notebooks must accept these `dbutils.widgets`:

**Training notebook** (includes evaluation via `mlflow.evaluate()` or cross-validation):
- `target` — label column name
- `training_table_name` — UC table with training data
- `eval_table_name` — UC table with eval data (empty string if split strategy is `none`)
- `test_table_name` — UC table with held-out test data (empty string if not 3-way split)
- `split_strategy` — `'none'`, `'train_eval'`, or `'train_eval_test'` — tells the notebook how to handle evaluation
- `experiment_name` — MLflow experiment name (short name; notebook prepends `/Users/<username>/`)
- `catalog` — Unity Catalog catalog (for volume access, e.g. TMPDIR)
- `schema` — UC schema within catalog
- `feature_lookups_json` — JSON array of feature lookup definitions from the dataset's feature entries. Used by `fe.log_model()` to embed feature specs into the model for auto feature lookup at serving time. Format: `[{"table_name": "...", "feature_names": [...], "lookup_key": [...], "timestamp_lookup_key": "..."}]`

**Notebook behavior by split strategy:**
- `none` — notebook receives a single table; should use cross-validation internally for evaluation
- `train_eval` — train on `training_table_name`, evaluate on `eval_table_name`
- `train_eval_test` — train on `training_table_name`, tune/evaluate on `eval_table_name`, final assessment on `test_table_name`

**Important:** Training notebooks must log models using `FeatureEngineeringClient.log_model()` (not `mlflow.sklearn.log_model()` or other flavor-specific methods). This embeds the feature spec into the model, enabling Databricks serving endpoints to automatically look up features from online tables at inference time. Without `fe.log_model()`, the deployment workflow cannot create auto-feature-lookup endpoints.

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
- **Params:** `eol_sql`, `feature_lookup_json`, `catalog`, `schema`, `training_table_name`, `eval_table_name`, `test_table_name`, `split_strategy`, `split_method`, `split_config_json`
- **Process:** Executes EOL SQL, applies FeatureLookups via `FeatureEngineeringClient.create_training_set()`, splits per strategy/method, writes to UC tables
- **Split logic in notebook:**
  - `none` — write full dataset to `training_table_name`, skip eval/test
  - `random` — stratified split on label column using `sampleBy` (maintains class proportions), seeded for reproducibility. 2-way or 3-way per strategy.
  - `temporal` — sort by timestamp column, latest rows to eval/test by percentage. No stratification.
- **Output:** App updates dataset status + table names + row count in Lakebase

### Training Job
- **Notebook:** User-provided, referenced from the user's git repo
- **Git source:** Project's `git_url` + `notebook_path/training_notebook` (job-level `git_source`, relative paths)
- **Params:** `target`, `training_table_name`, `eval_table_name`, `test_table_name`, `split_strategy`, `experiment_name`, `catalog`, `schema`
- **Output:** Notebook trains model, evaluates via `mlflow.evaluate()` or cross-validation (depending on `split_strategy`), and logs everything to MLflow. App resolves the MLflow experiment numeric ID via `GET /api/2.0/mlflow/experiments/get-by-name` for direct linking.

### Job creation and execution code
The job orchestration is **app server logic**, implemented as Express API endpoints. The user's git repo contains only the training notebook — not any job orchestration code. The reference implementation at https://github.com/BenMacKenzie/db-model-trainer/tree/main/notebooks shows the notebook contract (widget params) that user notebooks must follow.

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

10. **Online tables are project-level, not per-deployment** — A feature table (e.g. `catalog.schema.customer_features`) is published to the online store once and shared by all deployments that reference it. This avoids duplicate syncs when multiple models use the same source tables. The `online_table` table tracks these at the project level.

11. **All feature tables must be online before deployment** — All source tables from a dataset's feature entries must be published as online tables before the model can be deployed. No per-feature request-time selection is needed — if the requesting system supplies a feature in the request payload, Databricks serving uses it instead of the online table lookup. This is built-in serving behavior, so the app doesn't need to track which features are request-time.

12. **`fe.log_model()` in training notebooks enables auto-feature-lookup** — Because training notebooks log models with `FeatureEngineeringClient.log_model()`, the model's feature spec is embedded. Databricks serving endpoints automatically look up features from online tables at inference time — no custom serving logic needed in the app.

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
- [ ] End-to-end test: create project → EOL → features → dataset → train — 2026-04-12
- [ ] Deploy to workspace (blocked: npm registry unreachable from app runtime, esbuild bundle has plugin manifest issue) — 2026-04-12
- [ ] Build MLflow experiment viewer + run comparison UI — 2026-04-12
- [ ] Add `app.online_table` and `app.deployment` Lakebase tables — 2026-04-17
- [ ] Build server endpoints: online table CRUD, publish, status polling — 2026-04-17
- [ ] Build server endpoints: deployment CRUD, endpoint creation, test inference — 2026-04-17
- [ ] Build Deployment tab UI: online tables view + deployment creation + status monitoring — 2026-04-17
- [ ] Online table provisioning via Databricks Online Tables API — 2026-04-17
- [ ] Serving endpoint creation + status polling — 2026-04-17
- [ ] Test inference interface (send entity keys, show prediction) — 2026-04-17
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

## Meeting Notes
See `meetings/` folder for dated meeting notes.
