-- MLOps app schema (Lakebase `app` schema). Idempotent — safe to re-run.
-- Applied by the TABLE OWNER: deploy.sh runs it via psql as the deploying user before
-- `apps deploy`; local dev (./dev.sh, connected as that same user) runs it at server startup.
-- The deployed app's service principal can't run it (CREATE/ALTER need schema CREATE + ownership).
CREATE SCHEMA IF NOT EXISTS app;
CREATE TABLE IF NOT EXISTS app.project (
  id BIGSERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, description TEXT NOT NULL DEFAULT '',
  catalog VARCHAR(255) NOT NULL, schema VARCHAR(255) NOT NULL, model_name VARCHAR(255) NOT NULL DEFAULT '',
  git_url TEXT NOT NULL DEFAULT '',
  notebook_path TEXT NOT NULL DEFAULT '', training_notebook TEXT NOT NULL DEFAULT '', evaluation_notebook TEXT NOT NULL DEFAULT ''
);
ALTER TABLE app.project ADD COLUMN IF NOT EXISTS model_name VARCHAR(255) NOT NULL DEFAULT '';
UPDATE app.project SET model_name = name WHERE model_name = '';
CREATE TABLE IF NOT EXISTS app.entity_observation_label (
  id BIGSERIAL PRIMARY KEY, project_id BIGINT NOT NULL REFERENCES app.project(id) ON DELETE CASCADE,
  name VARCHAR(255) NOT NULL, sql_definition TEXT NOT NULL, label_column VARCHAR(255),
  entity_columns TEXT[], timestamp_column VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS app.feature_definition (
  id BIGSERIAL PRIMARY KEY, project_id BIGINT NOT NULL REFERENCES app.project(id) ON DELETE CASCADE,
  eol_id BIGINT REFERENCES app.entity_observation_label(id) ON DELETE SET NULL,
  name VARCHAR(255) NOT NULL
);
CREATE TABLE IF NOT EXISTS app.training_spec (
  id BIGSERIAL PRIMARY KEY, project_id BIGINT NOT NULL REFERENCES app.project(id) ON DELETE CASCADE,
  eol_id BIGINT REFERENCES app.entity_observation_label(id) ON DELETE SET NULL,
  name VARCHAR(255) NOT NULL,
  task_type VARCHAR(50) NOT NULL DEFAULT 'classification',
  split_strategy VARCHAR(50) NOT NULL DEFAULT 'none',
  split_method VARCHAR(50),
  split_config JSONB,
  parameters JSONB,
  excluded_features TEXT[]
);
ALTER TABLE app.training_spec ADD COLUMN IF NOT EXISTS excluded_features TEXT[];
CREATE TABLE IF NOT EXISTS app.feature_entry (
  id BIGSERIAL PRIMARY KEY, feature_definition_id BIGINT REFERENCES app.feature_definition(id) ON DELETE CASCADE,
  training_spec_id BIGINT REFERENCES app.training_spec(id) ON DELETE CASCADE,
  feature_type VARCHAR(50) NOT NULL DEFAULT 'lookup',
  table_name VARCHAR(255), feature_names TEXT[], lookup_key TEXT[],
  timestamp_lookup_key VARCHAR(255), output_name VARCHAR(255), default_values JSONB, declarative_spec JSONB,
  function_name VARCHAR(255), input_bindings JSONB
);
ALTER TABLE app.feature_entry ADD COLUMN IF NOT EXISTS training_spec_id BIGINT REFERENCES app.training_spec(id) ON DELETE CASCADE;
ALTER TABLE app.feature_entry ALTER COLUMN feature_definition_id DROP NOT NULL;
ALTER TABLE app.feature_entry ADD COLUMN IF NOT EXISTS function_name VARCHAR(255);
ALTER TABLE app.feature_entry ADD COLUMN IF NOT EXISTS input_bindings JSONB;
CREATE TABLE IF NOT EXISTS app.dataset (
  id BIGSERIAL PRIMARY KEY, project_id BIGINT NOT NULL REFERENCES app.project(id) ON DELETE CASCADE,
  name VARCHAR(255) NOT NULL, feature_definition_id BIGINT REFERENCES app.feature_definition(id) ON DELETE SET NULL,
  eval_split_type VARCHAR(50) DEFAULT 'percentage', eval_split_config JSONB,
  status VARCHAR(50) DEFAULT 'NOT_STARTED', training_table VARCHAR(255), eval_table VARCHAR(255),
  materialize_job_id BIGINT, materialize_run_id BIGINT, materialize_run_url TEXT, row_count BIGINT
);
CREATE TABLE IF NOT EXISTS app.run (
  id BIGSERIAL PRIMARY KEY, project_id BIGINT NOT NULL REFERENCES app.project(id) ON DELETE CASCADE,
  dataset_id BIGINT REFERENCES app.dataset(id) ON DELETE CASCADE,
  training_spec_id BIGINT REFERENCES app.training_spec(id) ON DELETE CASCADE,
  job_id BIGINT, run_id BIGINT, mlflow_experiment_id VARCHAR(255), mlflow_run_id VARCHAR(255),
  parameters JSONB, training_metrics JSONB, eval_metrics JSONB,
  status VARCHAR(50) DEFAULT 'PENDING',
  model_uri TEXT, model_name VARCHAR(255), model_version INTEGER,
  training_table VARCHAR(255), eval_table VARCHAR(255), test_table VARCHAR(255),
  databricks_run_url TEXT, error_message TEXT,
  started_at TIMESTAMP, ended_at TIMESTAMP
);
ALTER TABLE app.run ADD COLUMN IF NOT EXISTS training_spec_id BIGINT REFERENCES app.training_spec(id) ON DELETE CASCADE;
ALTER TABLE app.run ADD COLUMN IF NOT EXISTS training_table VARCHAR(255);
ALTER TABLE app.run ADD COLUMN IF NOT EXISTS eval_table VARCHAR(255);
ALTER TABLE app.run ADD COLUMN IF NOT EXISTS test_table VARCHAR(255);
ALTER TABLE app.run ALTER COLUMN dataset_id DROP NOT NULL;
CREATE TABLE IF NOT EXISTS app.online_table (
  id BIGSERIAL PRIMARY KEY, project_id BIGINT NOT NULL REFERENCES app.project(id) ON DELETE CASCADE,
  source_table VARCHAR(255) NOT NULL, online_table_name VARCHAR(255) NOT NULL,
  primary_key_columns TEXT[], timeseries_key VARCHAR(255),
  sync_mode VARCHAR(50) DEFAULT 'triggered',
  status VARCHAR(50) DEFAULT 'NOT_PUBLISHED', pipeline_id VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS app.deployment (
  id BIGSERIAL PRIMARY KEY, project_id BIGINT NOT NULL REFERENCES app.project(id) ON DELETE CASCADE,
  name VARCHAR(255) NOT NULL, run_id BIGINT NOT NULL REFERENCES app.run(id) ON DELETE CASCADE,
  endpoint_name VARCHAR(255) NOT NULL, endpoint_status VARCHAR(50) DEFAULT 'NOT_CREATED',
  endpoint_config JSONB, created_at TIMESTAMP DEFAULT NOW()
);
-- Online tables a deployment's model resolves features from (set by publish-tables;
-- NULL = not yet published, [] = published, nothing to serve from the online store).
ALTER TABLE app.deployment ADD COLUMN IF NOT EXISTS serving_tables TEXT[];
-- Online deployments own getting their model's features online (Deploy). deploy_stage tracks the
-- background flow FEATURES → ENDPOINT → DONE | FAILED; pending_release = {feature: online_table} the
-- previous model used, torn down once the endpoint is READY on the new version.
ALTER TABLE app.deployment ADD COLUMN IF NOT EXISTS deploy_stage VARCHAR(40);
ALTER TABLE app.deployment ADD COLUMN IF NOT EXISTS deploy_error TEXT;
ALTER TABLE app.deployment ADD COLUMN IF NOT EXISTS pending_release JSONB;
-- Which features (and the online table each lives in) every online deployment uses. An online table
-- is released only when no row references it. CustomUDF features have online_table NULL (computed at serving).
CREATE TABLE IF NOT EXISTS app.deployment_feature (
  deployment_id BIGINT NOT NULL REFERENCES app.deployment(id) ON DELETE CASCADE,
  feature_full_name VARCHAR(512) NOT NULL,
  feature_type VARCHAR(40),
  online_table VARCHAR(512),
  last_materialization_time TIMESTAMPTZ,
  PRIMARY KEY (deployment_id, feature_full_name)
);
-- One Databricks-managed online materialization per declarative feature entry.
-- Tracks the chosen schedule (trigger) and the live pipeline state read back from the
-- feature-engineering API. Distinct from app.online_table (which tracks the per-deployment
-- serving tables created by publish-tables).
CREATE TABLE IF NOT EXISTS app.materialized_feature (
  id BIGSERIAL PRIMARY KEY,
  project_id BIGINT NOT NULL REFERENCES app.project(id) ON DELETE CASCADE,
  feature_entry_id BIGINT REFERENCES app.feature_entry(id) ON DELETE SET NULL,
  feature_name VARCHAR(255) NOT NULL,
  feature_full_name VARCHAR(767),
  online_table_name VARCHAR(767),
  trigger_type VARCHAR(50) NOT NULL DEFAULT 'derived',
  cron_expression VARCHAR(255),
  timezone_id VARCHAR(100),
  resolved_cron VARCHAR(255),
  desired_state VARCHAR(50) NOT NULL DEFAULT 'ACTIVE',
  schedule_state VARCHAR(50),
  materialized_feature_id VARCHAR(255),
  job_id BIGINT, run_id BIGINT, pipeline_id VARCHAR(255),
  status VARCHAR(50) NOT NULL DEFAULT 'NOT_MATERIALIZED',
  last_materialization_time VARCHAR(64),
  materialize_run_url TEXT,
  error_message TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE (feature_entry_id)
);

-- Batch deployments: a scheduled Databricks job (owned by the app SP) that scores an input table of
-- request fields with models:/<model_name>@<model_alias> via notebooks/score_batch.py and appends
-- input + features + prediction to output_table. Every parameter lives in the job definition, so the
-- job runs without the app; run history / pause state are read live from the Jobs API.
CREATE TABLE IF NOT EXISTS app.batch_deployment (
  id BIGSERIAL PRIMARY KEY,
  project_id BIGINT NOT NULL REFERENCES app.project(id) ON DELETE CASCADE,
  name VARCHAR(255) NOT NULL,
  model_name VARCHAR(512) NOT NULL,
  model_alias VARCHAR(255) NOT NULL,
  input_table VARCHAR(512),
  input_sql TEXT,
  ts_col VARCHAR(255) NOT NULL,
  output_table VARCHAR(512) NOT NULL,
  cron_expression VARCHAR(255),
  timezone_id VARCHAR(64) DEFAULT 'UTC',
  job_id BIGINT,
  created_at TIMESTAMP DEFAULT NOW()
);
