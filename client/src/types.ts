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
  entries: FeatureEntry[];
}

// Legacy — kept for backward compat with old runs
export interface FeatureDefinition {
  id: number;
  project_id: number;
  eol_id: number | null;
  name: string;
  entries: FeatureEntry[];
}

export interface Dataset {
  id: number;
  project_id: number;
  name: string;
  feature_definition_id: number | null;
  eval_split_type: string;
  eval_split_config: Record<string, any> | null;
  status: 'NOT_STARTED' | 'MATERIALIZING' | 'READY' | 'FAILED';
  training_table: string | null;
  eval_table: string | null;
  materialize_job_id: number | null;
  materialize_run_id: number | null;
  materialize_run_url: string | null;
  row_count: number | null;
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

export interface OnlineTable {
  id: number;
  project_id: number;
  source_table: string;
  online_table_name: string;
  primary_key_columns: string[];
  timeseries_key: string | null;
  sync_mode: 'triggered' | 'continuous';
  status: 'NOT_PUBLISHED' | 'PROVISIONING' | 'ONLINE' | 'FAILED';
  pipeline_id: string | null;
}

export interface Deployment {
  id: number;
  project_id: number;
  name: string;
  run_id: number;
  endpoint_name: string;
  endpoint_status: 'NOT_CREATED' | 'CREATING' | 'READY' | 'FAILED';
  endpoint_config: Record<string, any> | null;
  created_at: string | null;
}
