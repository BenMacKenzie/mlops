# MLOps Model Manager

**Created:** 2026-04-11
**Status:** Deployed to Databricks Apps + end-to-end verified (2026-09-30) on fe-vm-serverless-stable-77rg2n. Migrated to the Feature View API for training + serving (2026-10-13); legacy dataset/publish paths removed (2026-10-16).
**Workspace:** https://fevm-serverless-stable-77rg2n.cloud.databricks.com
**Default Catalog:** serverless_stable_77rg2n_catalog (user-configurable per project)
**Lakebase Project:** mlops (app state + online store)
**Owner:** Ben MacKenzie
**Prior Art:** https://github.com/BenMacKenzie/mlops/tree/main (Dash-based prototype — reference for data model, job patterns, feature lookup UI)

---

## Problem Statement

Managing the full lifecycle of ML models — from feature definition through training to deployment — requires coordinating across multiple systems (Unity Catalog feature tables, MLflow experiments, model registry, serving endpoints, Databricks Jobs). The current prototype (Dash/Python) proved the concept but needs a rewrite as a proper Databricks App using React + AppKit with Lakebase for persistence.

The app should let a user:
1. Define a **project** with a catalog/schema for ML assets
2. Define an **EOL** (entity/observation/label) — the SQL "spine" for training data
3. Create a **training spec** — features, split strategy, task type, and parameters — all in one place
4. **Run** training — a single job that builds Feature View features, resolves + persists the training set, trains the model, and logs with feature specs via `fe.log_model()`
5. **Register** models to Unity Catalog and **deploy** to serving endpoints with auto feature lookup

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Databricks App (AppKit)                            │
│  ┌────────────┐  ┌──────────────────────────────┐   │
│  │  Express    │  │  React Frontend (Vite)       │   │
│  │  Backend    │  │  - Project management         │   │
│  │  (API)      │  │  - Training spec builder      │   │
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
│  - runs          │  │  - Features      │  │  - Model Registry│
│  - online tables │  │  - Online tables │  │                  │
│  - deployments   │  │                  │  │                  │
└─────────────────┘  └──────────────────┘  └──────────────────┘
          │                    │                      │
          └────────────────────┼──────────────────────┘
                               ▼
┌──────────────────┐  ┌──────────────────────────────┐
│  Databricks Jobs │  │  Databricks Model Serving    │
│  - Train         │  │  - Auto feature lookup       │
│  - Materialize   │  │    (from online tables)       │
│    (online)      │  │                               │
└──────────────────┘  └──────────────────────────────┘
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React + TypeScript + Vite (via AppKit) |
| Backend | Express (via AppKit) |
| App Framework | `@databricks/appkit` with lakebase + analytics plugins |
| OLTP Store | Lakebase (Postgres) — instance: `mlops` |
| Features | Unity Catalog Feature Views (`databricks-feature-engineering>=0.18`: `ColumnSelection`, `AggregationFunction`, `CustomUDF`) |
| Experiment Tracking | MLflow (via Databricks workspace) |
| Model Registry | Unity Catalog Model Registry |
| Compute | Databricks Jobs (app-managed notebooks: train, register, materialize online) |
| Online Feature Store | Lakebase online store `mlops`, populated by `fe.materialize_features` (managed pipelines) |
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
An individual feature entry within a training spec. Each entry is one of three types: a table lookup, an on-demand function, or a declarative feature. A training spec can have many entries of mixed types.

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| training_spec_id | BIGINT FK → training_spec | Parent spec |
| feature_type | VARCHAR(50) | `'lookup'`, `'on_demand'`, or `'declarative'` |
| table_name | VARCHAR(255) | UC feature table (for lookup type) |
| feature_names | TEXT[] | Columns to look up (for lookup type) |
| lookup_key | TEXT[] | Join keys mapping to EOL entity columns (for lookup type) |
| timestamp_lookup_key | VARCHAR(255) | For point-in-time lookups (for lookup type) |
| default_values | JSONB | Default values for missing features (for lookup type) |
| function_name | VARCHAR(255) | Fully-qualified UC function name (for on_demand type) |
| input_bindings | JSONB | Map of function param → source column (for on_demand type). Sources can be columns from any lookup entry in the spec or EOL columns. |
| output_name | VARCHAR(255) | Name of the computed output column (for on_demand type) |
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

#### `app.online_table`
Tracks online-store tables created by a deployment's **Publish Tables** (`materialize_serving.py`). Project-level; a table can be shared by several deployments (see `deployment.serving_tables`).

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| project_id | BIGINT FK → project | |
| source_table | VARCHAR(255) | Same as `online_table_name` for materialized tables (legacy rows: the published source table) |
| online_table_name | VARCHAR(255) | e.g. `{catalog}.{schema}.mlops_srv_cs__xxxx` (ColumnSelections by entity) / `mlops_srv_aggon__xxxx` (aggregations) |
| status | VARCHAR(50) | `'PROVISIONING'`, `'ONLINE'`, `'OFFLINE'` — reconciled by `check-status` |
| pipeline_id | VARCHAR(255) | URL of the materialize job run |

#### `app.materialized_feature`
One Online Features tab materialization per declarative feature entry (schedule + live pipeline state). Its own tables use prefix `mlops_feat_`; operations never touch the serving path's `mlops_srv_*` materializations of a same-named feature.

#### `app.deployment`
Links a registered model version to a serving endpoint. **Publish Tables** must have run and every table in `serving_tables` must be ONLINE before the endpoint can be created.

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| project_id | BIGINT FK → project | |
| name | VARCHAR(255) | e.g. `'production'`, `'staging'` |
| run_id | BIGINT FK → run | Must have `model_name`/`model_version` set |
| endpoint_name | VARCHAR(255) | Serving endpoint name |
| endpoint_status | VARCHAR(50) | `'NOT_CREATED'`, `'CREATING'`, `'READY'`, `'FAILED'` |
| endpoint_config | JSONB | Instance type, scaling, etc. |
| serving_tables | TEXT[] | Online tables the model resolves features from; written by Publish Tables (NULL = not yet published) |
| created_at | TIMESTAMP | |

### Entity Relationship Summary

```
project 1──* entity_observation_label
project 1──* training_spec
project 1──* run
project 1──* online_table
project 1──* materialized_feature
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
- Tabs: **Overview**, **EOLs**, **Training**, **Online Features**, **Deployment**

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

**Feature Builder (master-detail UI)**

The feature section of a training spec uses a two-panel master-detail layout for managing all feature entries (lookups, on-demand, declarative).

*Left panel — entry list:*
- Shows all feature entries in the spec, each displaying: type badge, name/summary, source info
- **"+ Add"** button with type selector: Lookup | On-Demand | Declarative
- Click an entry to select it → loads in the right panel for editing
- Delete button per entry (trash icon)
- Entries are editable until the spec is locked (first run created). After locking, the list is read-only.

*Right panel — detail form:*
Adapts to the selected entry's type. Catalog and schema **default to the most recently used values** across entries (so you only pick catalog/schema once if all features come from the same schema).

*Lookup detail:*
1. Catalog → Schema → Table cascade (catalog/schema pre-filled from last entry)
2. Feature columns — checkbox list from the selected table
3. Lookup keys — checkbox list from EOL entity columns
4. Optional: timestamp lookup key, default values (JSON)
5. All fields editable until spec is locked

*On-Demand detail:*
1. Catalog → Schema → Function cascade (catalog/schema pre-filled from last entry)
   - Functions listed from `information_schema.routines`
2. Function parameters read from `information_schema.parameters`, displayed as binding form
3. For each parameter, bind to a source via dropdown:
   - **Columns from any lookup entry** in the spec (grouped by source table)
   - **EOL columns** (entity keys, timestamp, label)
4. Output column name
5. Bridged to a Feature View `CustomUDF` whose upstreams are input-only `ColumnSelection` features for each bound column. At serving time the endpoint resolves the upstreams from the online store and computes the UDF; the UDF output itself is not materialized.

*Declarative detail (beta):*
1. Source table (catalog/schema/table cascade), input column, function, time window
2. Same form as today, adapted to the right-panel layout

**Editing & locking:**
- All entries are fully editable (add columns, change bindings, modify parameters) until the spec's first run is created
- After first run → spec is **locked**: entry list and detail forms become read-only
- **Copy** duplicates the entire spec (with all entries) under a new name for further editing
- Enforced server-side: PUT/POST/DELETE on entries rejected for specs that have runs

**Running:**
- Click **Run** on a training spec → launches a single Databricks Job that:
  1. Executes EOL SQL → spine DataFrame
  2. Builds declarative Feature View features from the spec's entries (the server's `buildFvEntries` bridge: lookup → one `ColumnSelection` per column, declarative → `AggregationFunction`, on-demand → `CustomUDF` over input-only `ColumnSelection` upstreams) → `fe.create_training_set(features=[...])` → `load_df()`
  3. Persists the resolved training set to a unique timestamped table (`{spec}_train_{YYYYMMDDHHMMSS}`) for reproducibility
  4. Splits per strategy/method
  5. Trains model (CatBoost, configured by task_type + parameters; only string columns are categorical)
  6. Logs with `fe.log_model()` (embeds the feature spec for serving; **no `input_example`** — it would attach a spine signature to the raw model and break online inference)
- View runs with status, metrics, job/MLflow links
- **Register model:** One-click registration via app-managed notebook (`register_model.py`)
- Auto-polls running jobs every 10s
- Delete runs to clean up

**App-managed training notebooks** (in `notebooks/` folder, uploaded to workspace before each run):
- `train_cv.py` — cross-validation (split_strategy = `none`)
- `train_standard.py` — standard train/eval (split_strategy = `train_eval`)
- `train_hpsearch.py` — hyperparameter search (split_strategy = `train_eval_test`, future)

Each notebook handles both classification and regression via the `task_type` parameter. The app selects the right notebook automatically based on split_strategy.

Server endpoints for UC metadata:
- `GET /api/uc/catalogs` — list catalogs via SQL warehouse
- `GET /api/uc/schemas?catalog=X` — list schemas in catalog
- `GET /api/uc/tables?catalog=X&schema=Y` — list tables in schema
- `GET /api/uc/columns?catalog=X&schema=Y&table=Z` — list columns in table
- `GET /api/uc/functions?catalog=X&schema=Y` — list UC functions in schema (from `information_schema.routines`)
- `GET /api/uc/function-params?catalog=X&schema=Y&function=Z` — get function parameter names and types (from `information_schema.parameters`)
- `PUT /api/training-spec-entries/:id` — update an existing entry (blocked if spec has runs)

### 5. Online Features (within project)

Schedules Databricks-managed online materialization of a spec's **declarative** features via
`fe.materialize_features` (`materialize_online.py`): derived cadence, custom Quartz cron, or on-table-change;
pause / resume / refresh / remove. Its tables use prefix `mlops_feat_`. Remove deletes only this tab's
materializations (offline first — that cascades to the online one) and drops the UC feature only if nothing
else (e.g. a deployment's serving pipeline) still materializes it.

### 6. Deployment (within project)

Manages the online tables a model serves from and its serving endpoint.

Training logs models with `fe.log_model()` over Feature View features, so the feature spec is embedded and the
serving endpoint resolves features from the online store at inference time. The caller sends only the spine
(entity keys + observation timestamp + pass-through columns); `CustomUDF` features (e.g. `distance`) are
computed at serving from their upstream features.

**Two sections in the tab:**

1. **Online Tables** (project-level) — every `app.online_table` row, its status, job link, and which
   deployments use it (from `deployment.serving_tables`). Selecting a deployment highlights its tables.
   **Remove** only stops tracking the row; it does not drop the table or its pipeline.
2. **Deployments** — each links a registered model to a serving endpoint.

**Creating a deployment:**
1. Give it a name (e.g. `production`)
2. Select a registered model version from dropdown
3. Click **Publish Tables** — runs `materialize_serving.py` on the spec's feature set:
   - `ColumnSelection` features → online only, `TableTrigger` (features sharing an entity co-locate in one table, `mlops_srv_cs__*`)
   - Aggregations → offline + online, `CronSchedule` (`mlops_srv_aggoff__*` / `mlops_srv_aggon__*`)
   - `CustomUDF` → not materialized
   - Waits for each online table's pipeline to create it, then grants USAGE + SELECT to `PUBLIC` (serving role) via psycopg.
   - Records the online tables in `deployment.serving_tables`. Idempotent — re-running reports existing tables.
4. **Create Endpoint** appears once every serving table is ONLINE. First provisioning is slow (~20–25 min, feature-serving sidecar).
5. Link to Databricks endpoint UI for management (stop/start)

**Gotchas:**
- Every feature source table needs a timestamp column with the **same name as the spine's observation timestamp** (the point-in-time join key); Type-1 dimension tables were converted to SCD2 with `transaction_ts`.
- Feature-table integer columns must be `BIGINT`/`DOUBLE`, not `INT` (online lookup returns int64; MLflow rejects int64→int32).
- Endpoints created via local dev (as the user) aren't on the app SP's ACL — grant the SP CAN_MANAGE or the deployed app shows them FAILED.

**Test interface (schema-driven, dynamic to the model signature):**
- Reads the endpoint's OpenAPI (`GET /api/deployments/:id/schema`) and renders one input
  per caller-supplied input from the request schema — required ones (entity keys) plus optional
  spine columns (`transaction_ts`, `amount`, `pos_entry_mode`, `security_code`), marked optional.
  Resolved features never appear in the request schema and are not asked for. Adapts automatically
  to whatever model/version the endpoint serves.
- Sample records (from the EOL spine, minus label — `GET /api/deployments/:id/samples`)
  prefill the required fields on click.
- Values are coerced to their declared types, then sent as `{"dataframe_records": [{...}]}`.
- Displays the prediction response.

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
| Query endpoint | `POST /serving-endpoints/{name}/invocations` | Test inference; caller supplies the required request-time inputs (entity keys + pass-through features), lookups auto-resolved |
| Get serving input schema | `GET /api/2.0/serving-endpoints/{name}/openapi` | Drives the dynamic test form: `required` = caller-provided inputs, the rest = looked-up features |
| Materialize features online | `fe.materialize_features(features, offline_config, online_config, trigger)` (in `materialize_serving.py` / `materialize_online.py`) | Managed pipelines populate the `mlops` online store. Aggregations require `offline_config`; offline/online `table_name_prefix` must differ. |
| Pause / resume / list materializations | `w.feature_engineering.update_materialized_feature(..., update_mask="pipeline_schedule_state")` / `fe.list_materialized_features(feature_name=)` | Only the online materialization is pausable; the list lags right after a PATCH (use the PATCH's return). Only offline materializations are directly deletable. |
| Poll online table status | `GET /api/2.0/database/synced_tables/{name}` (requires pipeline View — the app SP lacks it) → falls back to `SELECT count(*)` via the warehouse | The app row is written `PROVISIONING` at publish time; `check-status` reconciles it to `ONLINE` when the table has rows. |

> **Online store note:** the serving path uses `fe.materialize_features` with `OnlineStoreConfig(online_store_name="mlops")`,
> **not** `fe.publish_table` or the older `POST /api/2.0/online-tables` / `synced_tables` REST endpoints. The online
> store and the app-state DB share the single Lakebase project `mlops`.

## App-Managed Notebooks

All training notebooks live in this codebase under `notebooks/` and are uploaded to the workspace via the Workspace Import API before each job run. Users do not provide their own notebooks.

### Training Notebooks

Each notebook handles the full pipeline: build features → resolve + persist training set → split → train → log with `fe.log_model()`. Other app-managed notebooks: `register_model.py`, `materialize_serving.py` (Publish Tables), `materialize_online.py` (Online Features tab).

| Notebook | Split Strategy | Description |
|----------|---------------|-------------|
| `train_cv.py` | `none` | Cross-validation. No separate eval set — CV handles splitting internally. |
| `train_standard.py` | `train_eval` | Standard train/eval. Train on one split, evaluate on the other. |
| `train_hpsearch.py` | `train_eval_test` | Hyperparameter search. Eval for tuning, test for final assessment. (Future) |

Each notebook handles both classification and regression via the `task_type` parameter.

### Notebook Parameters (widget contract)

All training notebooks accept:
- `eol_sql` — the spine SQL (entity keys + observation timestamp + label)
- `label_column` — target column name
- `entity_columns_json` — JSON array of entity key column names
- `feature_entries_json` — JSON array of Feature View entries built by the server (`buildFvEntries`). Each has a `"type"`:
  `column_selection` (`source_table`, `column`, `entity`), `aggregation` (`source_table`, `input`, `function`, `window`, `entity`, optional `filter_condition`),
  or `custom_udf` (`function_name`, `bindings` param → upstream feature name). `include: false` marks input-only upstreams.
- `excluded_features_json` — feature names to drop from the model's training columns
- `timeseries_column` — the shared observation-timestamp column (in the spine AND every source table)
- `task_type` — `'classification'` or `'regression'`
- `split_method`, `split_config_json` — `train_standard.py` only
- `parameters_json` — hyperparameters (e.g. `{"iterations": 100}`)
- `catalog`, `schema` — Unity Catalog location for features/tables
- `experiment_name` — MLflow experiment name (short; notebook prepends `/Users/<username>/`)
- `training_table_name` (+ `eval_table_name` for train/eval) — unique timestamped tables the resolved sets are written to

### Notebook Pipeline (what each notebook does)

1. Execute EOL SQL → spine DataFrame
2. Create (idempotently) a UC feature per entry: `ColumnSelection` / `AggregationFunction` over `DeltaTableSource`
   (with `entity` + `timeseries_column`), and `CustomUDF` over `FeatureViewSource(features=[upstreams])` with
   `input_bindings` values as **full three-part** upstream names
3. `fe.create_training_set(df=spine, features=[...], label=...)` → `load_df()` (point-in-time join on `timeseries_column`)
4. Persist the resolved set to `training_table_name` (and eval table)
5. Split per `split_method` + `split_config` (or CV)
6. Train CatBoost, wrapped in a pyfunc that reindexes incoming columns to the training order (CatBoost categoricals are positional)
7. `fe.log_model(model, training_set=training_set, flavor=mlflow.pyfunc, ...)` — no `input_example`
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

Deployment to Databricks Apps uses the **canonical AppKit pattern** (no esbuild bundling):
1. Build the client locally to `client/dist`; ship source. The Apps platform runs a plain
   `npm install` on a **minimal runtime `package.json`** (`{@databricks/appkit, tsx}`).
   esbuild single-file bundling was abandoned — it breaks on AppKit's per-plugin
   `manifest.json` (each plugin reads it from its own `node_modules`).
2. **Never ship the dev-proxy `.npmrc`** or `package-lock.json` — unreachable from the Apps
   build env, so `npm install` hangs. Exclude them via `sync.exclude` in `databricks.yml`;
   force-include `client/dist` via `sync.include` (`.gitignore`'s `dist/` otherwise drops
   the SPA → "Cannot GET /"). Note `.databricksignore` is ignored by `bundle deploy`.
3. Re-add the Lakebase postgres resource after each `databricks bundle deploy` (bundle
   overwrites app resources; DABs can't declare the `postgres` type). Start the app before
   `apps deploy`.
4. Grant the app service principal the Lakebase `app` schema on every deploy (Lakebase
   re-provisions the SP Postgres role per deploy).
   Schema DDL lives in `server/schema.sql` and is applied by `deploy.sh` via `psql` **as the table
   owner, before `apps deploy`** — the SP can't run DDL (CREATE needs schema CREATE, ALTER needs
   ownership), so the server only self-applies it in local dev. AppKit's cache is given an in-memory
   store (otherwise it tries to create tables in `public` and logs `permission denied for schema public`).
5. **The deployed app authenticates as its service principal** for management APIs
   (client-credentials OAuth from the injected `DATABRICKS_CLIENT_ID/SECRET`); the user OBO
   token only carries the app's limited `user_api_scopes` (no serving/jobs/mlflow). Grant the
   SP CAN_MANAGE on serving endpoints and USE/SELECT/EXECUTE on the model's catalog/schema.
   Local dev keeps the CLI token.

See `deploy.sh` for the full deployment script.

## Key Design Decisions

1. **AppKit over APX** — AppKit is the official Databricks SDK with built-in Lakebase plugin, type-safe queries, and active maintenance. React + Express is the standard pattern.

2. **Lakebase for app state, UC for ML artifacts** — App metadata (projects, training specs, run tracking) lives in Lakebase. Actual data (feature tables, models) lives in Unity Catalog. Clean separation of concerns.

3. **Consolidated training spec** — Feature definitions, dataset split config, task type, and parameters are combined into a single `training_spec` entity. This eliminates the previous separate `feature_definition` and `dataset` tables and reduces the UI from 4 tabs (EOL, Features, Datasets, Training) to 2 tabs (EOL, Training). Experimentation is done by copying a training spec and modifying it.

4. **App-managed training notebooks** — Training notebooks live in the app's `notebooks/` folder, not in a user's git repo. The app selects the right notebook based on `split_strategy` (CV, standard, or hyperparameter search). Each notebook handles both classification and regression via the `task_type` parameter. This gives the app full control over the training pipeline and ensures `fe.log_model()` is always used correctly.

5. **Single-job training pipeline** — Each run executes one Databricks Job that does everything: EOL SQL → Feature View features → `fe.create_training_set()` → persist training set → split → train → `fe.log_model()`. No separate materialize step. The `training_set` object flows through the entire pipeline, ensuring the model's feature specs are embedded for serving.

6. **`fe.log_model()` enables auto-feature-lookup** — Training notebooks log models with `FeatureEngineeringClient.log_model()`, embedding the feature spec. Databricks serving endpoints automatically resolve features from the online store at inference time.

7. **Declarative features stored as JSONB** — Since the declarative feature API is in beta, storing the full definition as JSONB in `feature_entry.declarative_spec` is more flexible than normalizing.

8. **Training specs lock on first run** — A training spec can be edited freely until its first run is created. After that, it's locked — features, split config, and parameters are frozen to preserve lineage. The UI disables editing and shows a "Copy" action instead. This is enforced server-side: PUT/POST endpoints reject changes to specs that have runs.

9. **Feature Views end to end** — Spec entries keep their UI types (lookup / on-demand / declarative) and the server bridges them to Feature View features for both training and serving. Serving tables are materialized per deployment with `fe.materialize_features` into the `mlops` online store (same Lakebase project as app state) and recorded in `deployment.serving_tables`. `create_training_set` can't mix `features=` with `FeatureLookup`s, so everything goes through Feature Views. UC features are shared by name between the Online Features tab and the serving path, so each path only operates on materializations under its own table prefix (`mlops_feat_` vs `mlops_srv_`).

10. **Notebook-based model registration** — Model registration uses an app-managed notebook (`register_model.py`) that calls `mlflow.register_model()`. The REST API cannot resolve artifact paths when DBFS root is disabled — the Python SDK has internal access.

11. **MLflow via REST API for reads, notebooks for writes** — The Express backend calls MLflow REST endpoints for reading (experiments, runs, metrics). Model logging and registration require the Python SDK and run inside notebooks.

12. **On-demand features as peer entries with cross-table bindings** — On-demand features are their own `feature_entry` rows (`feature_type = 'on_demand'`) with `function_name`, `input_bindings`, and `output_name` columns. Input bindings can reference columns from *any* lookup entry in the spec (not just one table) + EOL columns. This supports functions like `distance(customer_zip, merchant_zip)` that need inputs from multiple tables. The app references existing UC functions — it does not author them. The server bridges each to a `CustomUDF` over input-only `ColumnSelection` upstreams; the output is computed at serving time, not materialized. **The referenced UC function must be a Python UDF (`LANGUAGE PYTHON`)** — SQL UDFs are rejected at `create_training_set` ("Only Python UDFs are supported").

13. **Master-detail feature builder** — Feature entries (lookup, on-demand, declarative) are managed via a two-panel layout: left panel shows the entry list, right panel shows the editable detail form for the selected entry. Catalog/schema default to the most recently used values across entries. All entries are fully editable (add/remove columns, change bindings) until the spec's first run locks it. Copy-to-edit after locking.

---

## Action Items

**Delivered** (initial build Apr 2026 + re-home 2026-09-30): scaffold, Lakebase schema, full
CRUD API + React UI, consolidated `training_spec` model, app-managed training notebooks
(`train_cv.py`, `train_standard.py`), on-demand + declarative features, master-detail feature
builder, single-job training pipeline, model registration (`register_model.py`), Deployment tab
(online tables + serving endpoint + schema-driven test UI), deploy to Databricks Apps, and
end-to-end verification (train → register → publish → serve → predict). The ES-1849341
online-store discovery blocker is **resolved** (feature-engineering library bump + `fe.*`
online-store API + SP grants).

**Open / future:**
- [ ] `train_hpsearch.py` — hyperparameter-search notebook for the `train_eval_test` split (not yet written)
- [x] Remove legacy git-notebook fields from the create-project form (git_url/notebook_path/training/eval notebook selectors + GitHub auto-fetch) and the stale `serverless_stable_1dpktm_catalog` default — done 2026-10-01 (commit a2acd87). **Remaining:** the DB columns (`git_url`, `notebook_path`, `training_notebook`, `evaluation_notebook` on `app.project`, NOT NULL DEFAULT '') and the unused legacy `git_source` training path in `server.ts` can be dropped in a later migration.
- [ ] MLflow experiment viewer + run-comparison UI
- [x] ~~Re-publish `customer_features_online`~~ — moot: Feature View models serve from `mlops_srv_*`; the old `*_features_online` rows are unused (Remove them from the Deployment tab)
- [ ] (Optional) UI refactor: author the three Feature View types directly instead of bridging lookup/on-demand/declarative entries server-side
- [ ] (Later) Drop the legacy `app.feature_definition` / `app.dataset` tables and `run.dataset_id` (code paths removed 2026-10-16; DDL still creates them)
- [ ] (Optional) Populate the native "Query endpoint" example (needs a retrain; the app's schema-driven test UI already covers testing)
- [ ] (Future) One shared Databricks Job per project, reused across runs (`job_id` on project; `notebook_params` already vary per run)

## Decisions

Curated: durable choices, still-relevant implementation gotchas, and the 2026-09-30 operational
learnings. Superseded build-churn (the earlier `feature_definition`/`dataset`/materialize model,
the git-source-notebook and esbuild-bundling attempts) has been pruned — current architecture lives
in **Key Design Decisions** above. Git history has the full trail.

**Framework / architecture:**
- Use AppKit (not APX) — official SDK, built-in Lakebase plugin; full React/TS rewrite of the Dash prototype.
- Declarative feature specs stored as JSONB (beta API; flexibility > normalization).
- Custom routes via `appkit.server.extend()` (there is no `configure` callback).

**Jobs / notebooks:**
- Serverless jobs need `databricks-feature-engineering` in the environment dependencies spec.
- App-managed notebooks are uploaded via the Workspace Import API (`/api/2.0/workspace/import`) before each run — strip the `/Workspace` prefix for the API call, use `format: SOURCE`.
- Serverless jobs cannot write to `/tmp` — point CatBoost `cv()` `train_dir`/`logging_dir` at a UC **volume**; the notebook receives `catalog`/`schema` widgets to build the volume path.
- `DeltaTableSource` / `fe.create_feature()` need `catalog_name` + `schema_name` as separate params; `table_name` must be the short name only (a fully-qualified name garbles namespace resolution).
- Model registration must run in a notebook (`register_model.py` → `mlflow.register_model`) — the REST `model-versions/create` can't resolve artifact paths when DBFS root is disabled.

**MLflow / metrics:**
- Experiment names are short (`project`); the notebook prepends `/Users/<username>/`. Resolve the numeric id via `GET /api/2.0/mlflow/experiments/get-by-name` (MLflow API is **2.0**, not 2.1); fall back to null (not name) to avoid broken links.
- `GET /api/2.0/mlflow/runs/get` is a GET — pass `run_id` as a query param.
- Metric bucketing handles both `mlflow.evaluate()` prefixes (`eval_`, `evaluation_`) and CatBoost CV prefixes (`test-`, `train-`); UI shows test/eval only.

**Auth / identity:**
- Local dev: pass `PGPASSWORD` (from a CLI token) directly to the lakebase plugin as native auth — AppKit's `LAKEBASE_ENDPOINT` OAuth refresh doesn't work with CLI-profile auth.
- Username resolved dynamically: `PGUSER` locally, SCIM `/api/2.1/preview/scim/v2/Me` when deployed; cached after first call.

**2026-09-30 (re-home to fe-vm-serverless-stable-77rg2n):**
- Canonical AppKit deploy (no esbuild): ship source + minimal runtime `package.json`; platform runs `npm install`. Exclude dev-proxy `.npmrc`/`package-lock.json` via `sync.exclude`, force-include `client/dist` via `sync.include`. `DATABRICKS_HOST` arrives schemeless → prepend `https://` in server. Full recipe: `reference_appkit_deploy_recipe` memory.
- Deployed app auths as its **service principal** for management APIs (client-credentials OAuth via injected `DATABRICKS_CLIENT_ID/SECRET`); the user OBO token lacks serving/jobs/mlflow scopes. `getToken()` is async: dev → CLI token, deployed → SP token. Grant the SP CAN_MANAGE on endpoints + USE/SELECT/EXECUTE on the model catalog/schema, else deployment shows FAILED + inference 403.
- Serving inputs = the full spine, not just entity keys. The model signature marks the spine's pass-through columns (e.g. `amount`, `pos_entry_mode`, `security_code`) REQUIRED alongside the lookup keys; sending only entity keys → `BAD_REQUEST … Model is missing inputs [...]`. `/samples` returns the full spine row (minus label).
- Test form is **schema-driven** — `GET /api/deployments/:id/schema` reads the endpoint OpenAPI and returns `{required, optional, types}`. UI renders a typed field per required input (dynamic to the served model signature); optional = FeatureLookup features shown as auto-resolved. Robust when `/samples` returns nothing.
- Online-table status reconciliation — the synced-table state API (`/api/2.0/database/synced_tables/{name}`) needs pipeline View perms the app SP lacks (`PERMISSION_DENIED … pipeline`), so `check-status` falls back to `SELECT count(*)` via the warehouse (SP-safe, survives re-publishes): rows > 0 → ONLINE. Client re-checks PROVISIONING rows on load.
- Recreating a feature source table (e.g. INT→BIGINT retype) gives it a new Delta table id, permanently breaking the existing synced-table sync pipeline (`DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`). The initial snapshot keeps serving, but incremental sync dies — drop the online table and re-publish fresh.
- `fe.log_model(input_example=...)` does not reliably populate the native "Query endpoint" example — the served model's OpenAPI carries the signature but no example value (the spine example has fewer columns than the resolved feature signature, so MLflow drops it). The app's schema-driven test UI is the reliable path.

## Meeting Notes
See `meetings/` folder for dated meeting notes.
