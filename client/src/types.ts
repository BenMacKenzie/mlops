export interface Project {
  id: number;
  name: string;
  description: string;
  catalog: string;
  schema: string;
  model_name: string;
  // Legacy git fields (deprecated — notebooks are now app-managed)
  git_url?: string;
  notebook_path?: string;
  training_notebook?: string;
  evaluation_notebook?: string;
}

export interface EOL {
  id: number;
  project_id: number;
  name: string;
  sql_definition: string;
  label_column: string;
  entity_columns: string[];
  timestamp_column: string;
}

export interface FeatureEntry {
  id: number;
  training_spec_id: number;
  feature_type: 'lookup' | 'on_demand' | 'declarative';
  // Lookup fields
  table_name: string | null;
  feature_names: string[] | null;
  lookup_key: string[] | null;
  timestamp_lookup_key: string | null;
  default_values: Record<string, any> | null;
  // On-demand fields
  function_name: string | null;
  input_bindings: Record<string, string> | null;
  output_name: string | null;
  // Declarative fields
  declarative_spec: Record<string, any> | null;
}

export interface TrainingSpec {
  id: number;
  project_id: number;
  eol_id: number | null;
  name: string;
  task_type: 'classification' | 'regression';
  split_strategy: 'none' | 'train_eval' | 'train_eval_test';
  split_method: 'random' | 'temporal' | null;
  split_config: Record<string, any> | null;
  parameters: Record<string, any> | null;
  excluded_features: string[] | null;
  entries: FeatureEntry[];
}

export interface Run {
  id: number;
  project_id: number;
  training_spec_id: number | null;
  dataset_id?: number; // Legacy
  job_id: number | null;
  run_id: number | null;
  mlflow_experiment_id: string | null;
  mlflow_run_id: string | null;
  parameters: Record<string, any> | null;
  training_metrics: Record<string, any> | null;
  eval_metrics: Record<string, any> | null;
  status: 'PENDING' | 'RUNNING' | 'SUCCESS' | 'FAILED';
  model_uri: string | null;
  model_name: string | null;
  model_version: number | null;
  training_table: string | null;
  eval_table: string | null;
  test_table: string | null;
  databricks_run_url: string | null;
  error_message: string | null;
  started_at: string | null;
  ended_at: string | null;
}

export interface Deployment {
  id: number;
  project_id: number;
  name: string;
  run_id: number;
  endpoint_name: string;
  endpoint_status: 'NOT_CREATED' | 'CREATING' | 'READY' | 'FAILED';
  endpoint_config: Record<string, any> | null;
  // Online tables the model serves from (null = never deployed)
  serving_tables: string[] | null;
  // Background Deploy / Update-model flow: FEATURES → ENDPOINT → DONE | FAILED (null = never deployed)
  deploy_stage: 'FEATURES' | 'ENDPOINT' | 'DONE' | 'FAILED' | null;
  deploy_error: string | null;
  // {feature: online_table} the previous model used — torn down once the endpoint is READY
  pending_release: Record<string, string> | null;
  created_at: string | null;
}

// A feature an online deployment serves from (registry row). online_table is null for CustomUDF
// features, which are computed at serving from their upstream features.
export interface DeploymentFeature {
  deployment_id: number;
  feature_full_name: string;
  feature_type: 'column_selection' | 'aggregation' | 'custom_udf' | string;
  online_table: string | null;
  last_materialization_time: string | null;
  // Live from the feature-engineering API (GET /api/deployments/:id/features)
  schedule?: FeatureSchedule | null;
  pipeline_state?: 'ACTIVE' | 'PAUSED' | 'MISSING' | string | null;
  live_error?: string;
}

// How an online feature is kept fresh: a cron (mode DERIVED = "Auto", computed by Databricks from the
// feature's window; MANUAL = user cron), a trigger on source-table change, or computed per request.
export type FeatureSchedule =
  | { kind: 'cron'; cron_expression: string; timezone_id: string; mode?: 'DERIVED' | 'MANUAL' | string }
  | { kind: 'table_trigger' }
  | { kind: 'on_request' };

// A scheduled batch-inference job (scores input rows with model@alias, appends to output_table).
export interface BatchRun {
  run_id: number;
  start_time: string | null;
  state: string;                 // SUCCESS | FAILED | RUNNING | PENDING | ...
  trigger?: string;              // PERIODIC (schedule) | ONE_TIME (run now) | ...
  url: string;
  rows?: number;
  model_version?: string;
  scored_at?: string;
  error?: string;
}

export interface BatchDeployment {
  id: number;
  project_id: number;
  name: string;
  model_name: string;
  model_alias: string;
  input_table: string | null;
  input_sql: string | null;
  ts_col: string;
  output_table: string;
  cron_expression: string | null;
  timezone_id: string;
  job_id: number | null;
  created_at: string | null;
  // live from the Jobs API
  paused: boolean | null;
  runs: BatchRun[];
  job_error: string | null;
}

export interface ModelAliases {
  aliases: { alias: string; version: number }[];
  versions: number[];
}
