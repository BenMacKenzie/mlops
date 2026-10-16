import type { Project, EOL, FeatureEntry, Run, OnlineTable, Deployment, TrainingSpec, DeclarativeFeature, MaterializedFeature } from './types';

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
export const previewSql = (sql: string) =>
  request<{ rows: any[]; count: number }>('/api/sql/preview', { method: 'POST', body: JSON.stringify({ sql }) });

// ── Training Specs ──
export const getTrainingSpecs = (projectId: number) =>
  request<(TrainingSpec & { run_count: number })[]>(`/api/projects/${projectId}/training-specs`);
export const createTrainingSpec = (projectId: number, data: Partial<TrainingSpec>) =>
  request<TrainingSpec>(`/api/projects/${projectId}/training-specs`, { method: 'POST', body: JSON.stringify(data) });
export const updateTrainingSpec = (id: number, data: Partial<TrainingSpec>) =>
  request<TrainingSpec>(`/api/training-specs/${id}`, { method: 'PUT', body: JSON.stringify(data) });
export const deleteTrainingSpec = (id: number) =>
  request<{ ok: boolean }>(`/api/training-specs/${id}`, { method: 'DELETE' });
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
export const deleteRun = (id: number) =>
  request<{ ok: boolean }>(`/api/runs/${id}`, { method: 'DELETE' });
export const registerModel = (runId: number) =>
  request<Run>(`/api/runs/${runId}/register-model`, { method: 'POST' });

// ── Online Tables ──
export const getOnlineTables = (projectId: number) =>
  request<OnlineTable[]>(`/api/projects/${projectId}/online-tables`);
export const checkOnlineTableStatus = (id: number) =>
  request<OnlineTable>(`/api/online-tables/${id}/check-status`, { method: 'POST' });
export const deleteOnlineTable = (id: number) =>
  request<{ ok: boolean }>(`/api/online-tables/${id}`, { method: 'DELETE' });

// ── Materialized Features (declarative → online store, on a schedule) ──
export const getMaterializedFeatures = (projectId: number) =>
  request<DeclarativeFeature[]>(`/api/projects/${projectId}/materialized-features`);
export const materializeFeature = (
  projectId: number,
  data: { feature_entry_id: number; trigger_type: string; cron_expression?: string | null; timezone_id?: string | null },
) =>
  request<MaterializedFeature>(`/api/projects/${projectId}/materialized-features`, { method: 'POST', body: JSON.stringify(data) });
export const pauseMaterializedFeature = (id: number) =>
  request<MaterializedFeature>(`/api/materialized-features/${id}/pause`, { method: 'POST' });
export const resumeMaterializedFeature = (id: number) =>
  request<MaterializedFeature>(`/api/materialized-features/${id}/resume`, { method: 'POST' });
export const refreshMaterializedFeature = (id: number) =>
  request<MaterializedFeature>(`/api/materialized-features/${id}/refresh`, { method: 'POST' });
export const deleteMaterializedFeature = (id: number) =>
  request<{ ok: boolean }>(`/api/materialized-features/${id}`, { method: 'DELETE' });

// ── Deployments ──
export const getDeployments = (projectId: number) =>
  request<Deployment[]>(`/api/projects/${projectId}/deployments`);
export const createDeployment = (projectId: number, data: { name: string; run_id: number; endpoint_name: string; endpoint_config?: Record<string, any> }) =>
  request<Deployment>(`/api/projects/${projectId}/deployments`, { method: 'POST', body: JSON.stringify(data) });
export const publishDeploymentTables = (deploymentId: number) =>
  request<{ published: number }>(`/api/deployments/${deploymentId}/publish-tables`, { method: 'POST' });
export const createDeploymentEndpoint = (deploymentId: number) =>
  request<Deployment>(`/api/deployments/${deploymentId}/create-endpoint`, { method: 'POST' });
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
