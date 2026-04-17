import type { Project, EOL, FeatureDefinition, FeatureEntry, Dataset, Run } from './types';

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
export const previewSql = (sql: string) =>
  request<{ rows: any[]; count: number }>('/api/sql/preview', { method: 'POST', body: JSON.stringify({ sql }) });

// ── GitHub ──
export const listNotebooks = (repoUrl: string, notebookPath: string) =>
  request<{ name: string; path: string }[]>(
    `/api/github/notebooks?repo_url=${encodeURIComponent(repoUrl)}&notebook_path=${encodeURIComponent(notebookPath)}`
  );

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

// ── Feature Definitions ──
export const getFeatures = (projectId: number) =>
  request<FeatureDefinition[]>(`/api/projects/${projectId}/features`);
export const createFeature = (projectId: number, data: { eol_id: number | null; name: string }) =>
  request<FeatureDefinition>(`/api/projects/${projectId}/features`, { method: 'POST', body: JSON.stringify(data) });
export const deleteFeature = (id: number) =>
  request<{ ok: boolean }>(`/api/features/${id}`, { method: 'DELETE' });
export const copyFeature = (id: number, name?: string) =>
  request<FeatureDefinition>(`/api/features/${id}/copy`, { method: 'POST', body: JSON.stringify({ name }) });

// ── Feature Entries ──
export const createFeatureEntry = (featureId: number, data: Omit<FeatureEntry, 'id' | 'feature_definition_id'>) =>
  request<FeatureEntry>(`/api/features/${featureId}/entries`, { method: 'POST', body: JSON.stringify(data) });
export const deleteFeatureEntry = (id: number) =>
  request<{ ok: boolean }>(`/api/feature-entries/${id}`, { method: 'DELETE' });

// ── Datasets ──
export const getDatasets = (projectId: number) =>
  request<Dataset[]>(`/api/projects/${projectId}/datasets`);
export const createDataset = (projectId: number, data: { name: string; feature_definition_id: number; eval_split_type: string; eval_split_config: any }) =>
  request<Dataset>(`/api/projects/${projectId}/datasets`, { method: 'POST', body: JSON.stringify(data) });
export const updateDataset = (id: number, data: Partial<Dataset>) =>
  request<Dataset>(`/api/datasets/${id}`, { method: 'PUT', body: JSON.stringify(data) });
export const deleteDataset = (id: number) =>
  request<{ ok: boolean }>(`/api/datasets/${id}`, { method: 'DELETE' });

// ── Runs (train + evaluate) ──
export const getRuns = (projectId: number) =>
  request<Run[]>(`/api/projects/${projectId}/runs`);
export const createRun = (projectId: number, data: { dataset_id: number; parameters?: Record<string, any> }) =>
  request<Run>(`/api/projects/${projectId}/runs`, { method: 'POST', body: JSON.stringify(data) });
export const launchRun = (projectId: number, runId: number) =>
  request<{ job_id: number; run_id: number; run_url: string }>(`/api/projects/${projectId}/runs/${runId}/launch`, { method: 'POST' });
export const checkRunStatus = (runId: number) =>
  request<Run>(`/api/runs/${runId}/check-status`, { method: 'POST' });
export const deleteRun = (id: number) =>
  request<{ ok: boolean }>(`/api/runs/${id}`, { method: 'DELETE' });
export const registerModel = (runId: number) =>
  request<Run>(`/api/runs/${runId}/register-model`, { method: 'POST' });

// ── Job launchers ──
export const materializeDataset = (projectId: number, datasetId: number) =>
  request<{ job_id: number; run_id: number; run_url: string }>(`/api/projects/${projectId}/datasets/${datasetId}/materialize`, { method: 'POST' });
export const checkDatasetStatus = (projectId: number, datasetId: number) =>
  request<Dataset>(`/api/projects/${projectId}/datasets/${datasetId}/check-status`, { method: 'POST' });

// ── Databricks proxy ──
export const databricksGet = (path: string, params?: Record<string, string>) => {
  const qs = params ? '?' + new URLSearchParams(params).toString() : '';
  return request<any>(`/api/databricks/${path}${qs}`);
};
export const databricksPost = (path: string, body: any) =>
  request<any>(`/api/databricks/${path}`, { method: 'POST', body: JSON.stringify(body) });
