import type { Project, EOL, FeatureEntry, Run, Deployment, TrainingSpec, DeploymentFeature, BatchDeployment, ModelAliases } from './types';

async function request<T>(url: string, options?: RequestInit): Promise<T> {
  const res = await fetch(url, {
    ...options,
    headers: { 'Content-Type': 'application/json', ...options?.headers },
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || res.statusText);
  }
  return res.json();
}

// ── Projects ──
export const getProjects = () => request<Project[]>('/api/projects');
export const getProject = (id: number) => request<Project>(`/api/projects/${id}`);
export const createProject = (data: Omit<Project, 'id'>) =>
  request<Project>('/api/projects', { method: 'POST', body: JSON.stringify(data) });
export const updateProject = (id: number, data: Partial<Project>) =>
  request<Project>(`/api/projects/${id}`, { method: 'PUT', body: JSON.stringify(data) });
export const deleteProject = (id: number) =>
  request<{ ok: boolean }>(`/api/projects/${id}`, { method: 'DELETE' });

// ── Unity Catalog metadata ──
export const getCatalogs = () => request<string[]>('/api/uc/catalogs');
export const getSchemas = (catalog: string) => request<string[]>(`/api/uc/schemas?catalog=${encodeURIComponent(catalog)}`);
export const getTables = (catalog: string, schema: string) => request<string[]>(`/api/uc/tables?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}`);
export const getColumns = (catalog: string, schema: string, table: string) =>
  request<{ name: string; type: string }[]>(`/api/uc/columns?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`);
export const getFunctions = (catalog: string, schema: string) =>
  request<string[]>(`/api/uc/functions?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}`);
export const getFunctionParams = (catalog: string, schema: string, functionName: string) =>
  request<{ name: string; type: string }[]>(`/api/uc/function-params?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}&function=${encodeURIComponent(functionName)}`);
export type EolColumns = {
  columns: { name: string; type: string }[];
  suggested: { label_column: string; timestamp_column: string; entity_columns: string[] };
};
export const detectEolColumns = (sql: string) =>
  request<EolColumns>('/api/sql/columns', { method: 'POST', body: JSON.stringify({ sql }) });
export const previewSql = (sql: string) =>
  request<{ rows: any[]; count: number }>('/api/sql/preview', { method: 'POST', body: JSON.stringify({ sql }) });

// ── Training Specs ──
export const getTrainingSpecs = (projectId: number) =>
  request<(TrainingSpec & { run_count: number; locked?: boolean })[]>(`/api/projects/${projectId}/training-specs`);
export const createTrainingSpec = (projectId: number, data: Partial<TrainingSpec>) =>
  request<TrainingSpec>(`/api/projects/${projectId}/training-specs`, { method: 'POST', body: JSON.stringify(data) });
export const updateTrainingSpec = (id: number, data: Partial<TrainingSpec>) =>
  request<TrainingSpec>(`/api/training-specs/${id}`, { method: 'PUT', body: JSON.stringify(data) });
// Deletes the spec + its runs + every run's assets (model versions/aliases, MLflow runs, tables, jobs) + UC features
// nothing else uses. 409 if blocked.
export const deleteTrainingSpec = (id: number) =>
  request<{ ok: boolean }>(`/api/training-specs/${id}`, { method: 'DELETE' });
export const getTrainingSpecDeletePlan = (id: number) =>
  request<{ runs: number[]; assets: RunAsset[]; blockers: string[]; kept: string[]; features: { delete: string[]; keep: string[] } }>(`/api/training-specs/${id}/delete-plan`);
export const copyTrainingSpec = (id: number, overrides?: Partial<TrainingSpec>) =>
  request<TrainingSpec>(`/api/training-specs/${id}/copy`, { method: 'POST', body: JSON.stringify(overrides || {}) });
export const createTrainingSpecEntry = (specId: number, data: any) =>
  request<FeatureEntry>(`/api/training-specs/${specId}/entries`, { method: 'POST', body: JSON.stringify(data) });
export const updateTrainingSpecEntry = (id: number, data: any) =>
  request<FeatureEntry>(`/api/training-spec-entries/${id}`, { method: 'PUT', body: JSON.stringify(data) });
export const deleteTrainingSpecEntry = (id: number) =>
  request<{ ok: boolean }>(`/api/training-spec-entries/${id}`, { method: 'DELETE' });

// ── Spec Runs ──
export const getSpecRuns = (projectId: number) =>
  request<Run[]>(`/api/projects/${projectId}/spec-runs`);
export const createSpecRun = (specId: number) =>
  request<Run>(`/api/training-specs/${specId}/runs`, { method: 'POST' });
export const launchSpecRun = (runId: number) =>
  request<{ job_id: number; run_id: number; run_url: string }>(`/api/spec-runs/${runId}/launch`, { method: 'POST' });
export const checkSpecRunStatus = (runId: number) =>
  request<Run>(`/api/spec-runs/${runId}/check-status`, { method: 'POST' });

// ── EOLs ──
export const getEOLs = (projectId: number) =>
  request<EOL[]>(`/api/projects/${projectId}/eols`);
export const createEOL = (projectId: number, data: Omit<EOL, 'id' | 'project_id'>) =>
  request<EOL>(`/api/projects/${projectId}/eols`, { method: 'POST', body: JSON.stringify(data) });
export const updateEOL = (id: number, data: Partial<EOL>) =>
  request<EOL>(`/api/eols/${id}`, { method: 'PUT', body: JSON.stringify(data) });
export const deleteEOL = (id: number) =>
  request<{ ok: boolean }>(`/api/eols/${id}`, { method: 'DELETE' });
export const copyEOL = (id: number, name?: string) =>
  request<EOL>(`/api/eols/${id}/copy`, { method: 'POST', body: JSON.stringify({ name }) });

// ── Runs (shared) ──
export type RunAsset = { kind: 'alias' | 'model_version' | 'mlflow_run' | 'table' | 'job'; name: string };
// What deleting a run would remove, and anything (deployments, a live training job) blocking it.
export const getRunDeletePlan = (id: number) =>
  request<{ assets: RunAsset[]; blockers: string[]; kept: string[] }>(`/api/runs/${id}/delete-plan`);
// Deletes the run AND its assets (model version + aliases, MLflow run, tables, job). 409 if blocked.
export const deleteRun = (id: number) =>
  request<{ ok: boolean }>(`/api/runs/${id}`, { method: 'DELETE' });
export const registerModel = (runId: number) =>
  request<Run>(`/api/runs/${runId}/register-model`, { method: 'POST' });

// ── Deployments ──
export const getDeployments = (projectId: number) =>
  request<Deployment[]>(`/api/projects/${projectId}/deployments`);
export const createDeployment = (projectId: number, data: { name: string; run_id: number; endpoint_name: string; endpoint_config?: Record<string, any> }) =>
  request<Deployment>(`/api/projects/${projectId}/deployments`, { method: 'POST', body: JSON.stringify(data) });
// Deploy = put the model's features online (only what's missing) + create/update the endpoint (background).
export const deployDeployment = (deploymentId: number) =>
  request<Deployment>(`/api/deployments/${deploymentId}/deploy`, { method: 'POST' });
export const getDeploymentFeatures = (deploymentId: number) =>
  request<DeploymentFeature[]>(`/api/deployments/${deploymentId}/features`);
export const refreshDeploymentFeatures = (deploymentId: number) =>
  request<DeploymentFeature[]>(`/api/deployments/${deploymentId}/features/refresh`, { method: 'POST' });
export const checkDeploymentStatus = (deploymentId: number) =>
  request<Deployment>(`/api/deployments/${deploymentId}/check-status`, { method: 'POST' });
export const getDeploymentSamples = (deploymentId: number) =>
  request<Record<string, any>[]>(`/api/deployments/${deploymentId}/samples`);
export const getDeploymentSchema = (deploymentId: number) =>
  request<{ required: string[]; optional: string[]; types: Record<string, string> }>(
    `/api/deployments/${deploymentId}/schema`,
  );
export const testDeploymentEndpoint = (deploymentId: number, data: Record<string, any>) =>
  request<any>(`/api/deployments/${deploymentId}/test`, { method: 'POST', body: JSON.stringify(data) });
export const updateDeploymentEndpoint = (id: number, runId: number) =>
  request<Deployment>(`/api/deployments/${id}/update-endpoint`, { method: 'POST', body: JSON.stringify({ run_id: runId }) });
export const stopDeploymentEndpoint = (id: number) =>
  request<Deployment>(`/api/deployments/${id}/stop-endpoint`, { method: 'POST' });
export const deleteDeployment = (id: number) =>
  request<{ ok: boolean }>(`/api/deployments/${id}`, { method: 'DELETE' });

// ── Databricks proxy ──
export const databricksGet = (path: string, params?: Record<string, string>) => {
  const qs = params ? '?' + new URLSearchParams(params).toString() : '';
  return request<any>(`/api/databricks/${path}${qs}`);
};
export const databricksPost = (path: string, body: any) =>
  request<any>(`/api/databricks/${path}`, { method: 'POST', body: JSON.stringify(body) });

// ── Batch deployments ──
export type BatchDeploymentInput = {
  name: string; model_name: string; model_alias: string;
  input_table?: string | null; input_sql?: string | null; output_table: string;
  cron_expression?: string | null; timezone_id?: string;
};
export const getBatchDeployments = (projectId: number) =>
  request<BatchDeployment[]>(`/api/projects/${projectId}/batch-deployments`);
export const createBatchDeployment = (projectId: number, data: BatchDeploymentInput) =>
  request<BatchDeployment>(`/api/projects/${projectId}/batch-deployments`, { method: 'POST', body: JSON.stringify(data) });
export const updateBatchDeployment = (id: number, data: Partial<BatchDeploymentInput>) =>
  request<BatchDeployment>(`/api/batch-deployments/${id}`, { method: 'PUT', body: JSON.stringify(data) });
export const runBatchNow = (id: number) =>
  request<{ run_id: number }>(`/api/batch-deployments/${id}/run-now`, { method: 'POST' });
export const pauseBatch = (id: number) =>
  request<BatchDeployment>(`/api/batch-deployments/${id}/pause`, { method: 'POST' });
export const resumeBatch = (id: number) =>
  request<BatchDeployment>(`/api/batch-deployments/${id}/resume`, { method: 'POST' });
export const previewBatch = (id: number) =>
  request<Record<string, any>[]>(`/api/batch-deployments/${id}/preview`);
export const deleteBatchDeployment = (id: number) =>
  request<{ ok: boolean }>(`/api/batch-deployments/${id}`, { method: 'DELETE' });
export type BatchRequirements = {
  ts_col: string; label: string; entity_columns: string[]; request_fields: string[];
  eol_name: string; spec_name: string; model_version: number | null;
};
export const getBatchRequirements = (projectId: number, model: string) =>
  request<BatchRequirements>(`/api/projects/${projectId}/batch-requirements?model=${encodeURIComponent(model)}`);
export const getModelAliases = (name: string) =>
  request<ModelAliases>(`/api/models/aliases?name=${encodeURIComponent(name)}`);
export const setModelAlias = (name: string, alias: string, version: number) =>
  request<{ alias: string; version: number }>(`/api/models/aliases`, { method: 'PUT', body: JSON.stringify({ name, alias, version }) });
