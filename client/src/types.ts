export interface Project {
  id: number;
  name: string;
  description: string;
  catalog: string;
  schema: string;
  git_url: string;
  notebook_path: string;
  training_notebook: string;
  evaluation_notebook: string;
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
  feature_definition_id: number;
  feature_type: 'lookup' | 'declarative';
  table_name: string | null;
  feature_names: string[] | null;
  lookup_key: string[] | null;
  timestamp_lookup_key: string | null;
  output_name: string | null;
  default_values: Record<string, any> | null;
  declarative_spec: Record<string, any> | null;
}

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
  dataset_id: number;
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
  databricks_run_url: string | null;
  error_message: string | null;
  started_at: string | null;
  ended_at: string | null;
}
