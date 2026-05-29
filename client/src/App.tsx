import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import type { Project, EOL, FeatureDefinition, FeatureEntry, Dataset, Run, OnlineTable, Deployment, TrainingSpec } from './types';
import * as api from './api';

// ── Simple hash-based routing ──
type Page =
  | { view: 'projects' }
  | { view: 'project'; id: number; tab: string };

function useHashRoute(): [Page, (p: Page) => void] {
  const parse = (): Page => {
    const hash = window.location.hash.slice(1);
    const m = hash.match(/^\/projects\/(\d+)(?:\/(\w+))?/);
    if (m) return { view: 'project', id: parseInt(m[1]), tab: m[2] || 'overview' };
    return { view: 'projects' };
  };
  const [page, setPage] = useState<Page>(parse);
  useEffect(() => {
    const handler = () => setPage(parse());
    window.addEventListener('hashchange', handler);
    return () => window.removeEventListener('hashchange', handler);
  }, []);
  const navigate = (p: Page) => {
    if (p.view === 'projects') window.location.hash = '/';
    else window.location.hash = `/projects/${p.id}/${p.tab}`;
  };
  return [page, navigate];
}

// ════════════════════════════════════════════
//  STATUS BADGE
// ════════════════════════════════════════════
function StatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    NOT_STARTED: 'bg-gray-200 text-gray-700',
    PENDING: 'bg-yellow-100 text-yellow-800',
    MATERIALIZING: 'bg-blue-100 text-blue-800',
    RUNNING: 'bg-blue-100 text-blue-800',
    READY: 'bg-green-100 text-green-800',
    SUCCESS: 'bg-green-100 text-green-800',
    FAILED: 'bg-red-100 text-red-800',
  };
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${colors[status] || 'bg-gray-100'}`}>
      {status}
    </span>
  );
}

// ════════════════════════════════════════════
//  PROJECTS LIST
// ════════════════════════════════════════════
function ProjectsList({ navigate }: { navigate: (p: Page) => void }) {
  const [projects, setProjects] = useState<Project[]>([]);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({ name: '', description: '', catalog: 'serverless_stable_1dpktm_catalog', schema: '', model_name: '', git_url: 'https://github.com/BenMacKenzie/db-model-trainer/tree/main/notebooks', notebook_path: '', training_notebook: '', evaluation_notebook: '' });
  const [notebooks, setNotebooks] = useState<{ name: string; path: string }[]>([]);
  const [loadingNotebooks, setLoadingNotebooks] = useState(false);

  const load = useCallback(() => { api.getProjects().then(setProjects); }, []);
  useEffect(() => { load(); }, [load]);

  // Parse git_url to extract repo URL and notebook path
  // e.g. "https://github.com/BenMacKenzie/db-model-trainer/notebooks" ->
  //   repo: "https://github.com/BenMacKenzie/db-model-trainer", path: "notebooks"
  const parseGitUrl = (url: string) => {
    const match = url.match(/^(https:\/\/github\.com\/[^/]+\/[^/]+)(?:\/(.+))?$/);
    if (match) {
      let path = match[2] || '';
      // Strip tree/main/ or tree/branch/ (GitHub web UI artifact)
      path = path.replace(/^tree\/[^/]+\//, '');
      return { repoUrl: match[1], path };
    }
    return null;
  };

  const fetchNotebooks = async (gitUrl: string) => {
    const parsed = parseGitUrl(gitUrl);
    if (!parsed) { setNotebooks([]); return; }
    setLoadingNotebooks(true);
    try {
      const nbs = await api.listNotebooks(parsed.repoUrl, parsed.path);
      setNotebooks(nbs);
      setForm((f) => ({ ...f, notebook_path: parsed.path }));
    } catch {
      setNotebooks([]);
    }
    setLoadingNotebooks(false);
  };

  // Auto-fetch notebooks when git_url changes and looks valid (debounced)
  useEffect(() => {
    const parsed = parseGitUrl(form.git_url);
    if (parsed && parsed.repoUrl) {
      const timer = setTimeout(() => fetchNotebooks(form.git_url), 500);
      return () => clearTimeout(timer);
    } else {
      setNotebooks([]);
    }
    return undefined;
  }, [form.git_url]);

  const submit = async () => {
    const parsed = parseGitUrl(form.git_url);
    const submitData = {
      ...form,
      git_url: parsed?.repoUrl || form.git_url,
      notebook_path: parsed?.path || form.notebook_path,
    };
    await api.createProject(submitData as Omit<Project, 'id'>);
    setForm({ name: '', description: '', catalog: '', schema: '', model_name: '', git_url: '', notebook_path: '', training_notebook: '', evaluation_notebook: '' });
    setNotebooks([]);
    setShowForm(false);
    load();
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">Projects</h1>
        <button onClick={() => setShowForm(!showForm)} className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
          New Project
        </button>
      </div>

      {showForm && (
        <div className="mb-6 p-4 border rounded-lg bg-white shadow">
          <div className="grid grid-cols-2 gap-3">
            <div><label className="block text-sm font-medium text-gray-700 mb-1">name</label><input className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500 focus:outline-none" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} /></div>
            <div className="col-span-2"><label className="block text-sm font-medium text-gray-700 mb-1">description</label><input className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500 focus:outline-none" value={form.description} onChange={(e) => setForm({ ...form, description: e.target.value })} /></div>
            <div><label className="block text-sm font-medium text-gray-700 mb-1">catalog</label><input className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500 focus:outline-none" value={form.catalog} onChange={(e) => setForm({ ...form, catalog: e.target.value })} placeholder="serverless_stable_1dpktm_catalog" /></div>
            <div><label className="block text-sm font-medium text-gray-700 mb-1">schema</label><input className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500 focus:outline-none" value={form.schema} onChange={(e) => setForm({ ...form, schema: e.target.value })} /></div>
            <div className="col-span-2"><label className="block text-sm font-medium text-gray-700 mb-1">model name <span className="text-xs text-gray-400">(for UC model registry — defaults to project name)</span></label><input className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500 focus:outline-none" value={form.model_name} onChange={(e) => setForm({ ...form, model_name: e.target.value })} placeholder={form.name || 'project name'} /></div>
            <div className="col-span-2">
              <label className="block text-sm font-medium text-gray-700 mb-1">git url (include path to notebooks folder)</label>
              <input
                className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500 focus:outline-none font-mono text-sm"
                value={form.git_url}
                onChange={(e) => setForm({ ...form, git_url: e.target.value })}
                placeholder="https://github.com/user/repo/notebooks"
              />
              {loadingNotebooks && <div className="text-xs text-blue-500 mt-1">Fetching notebooks...</div>}
            </div>
            {notebooks.length > 0 && (
              <>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">training notebook</label>
                  <select className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500 focus:outline-none" value={form.training_notebook} onChange={(e) => setForm({ ...form, training_notebook: e.target.value })}>
                    <option value="">Select training notebook...</option>
                    {notebooks.map((nb) => <option key={nb.name} value={nb.name}>{nb.name}</option>)}
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">evaluation notebook</label>
                  <select className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500 focus:outline-none" value={form.evaluation_notebook} onChange={(e) => setForm({ ...form, evaluation_notebook: e.target.value })}>
                    <option value="">Select evaluation notebook...</option>
                    {notebooks.map((nb) => <option key={nb.name} value={nb.name}>{nb.name}</option>)}
                  </select>
                </div>
              </>
            )}
            {notebooks.length === 0 && form.git_url && !loadingNotebooks && parseGitUrl(form.git_url) && (
              <div className="col-span-2 text-sm text-gray-400">No notebooks found at this path</div>
            )}
          </div>
          <div className="mt-3 flex gap-2">
            <button onClick={submit} className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700">Create</button>
            <button onClick={() => setShowForm(false)} className="px-4 py-2 bg-gray-200 rounded hover:bg-gray-300">Cancel</button>
          </div>
        </div>
      )}

      <div className="bg-white rounded-lg shadow overflow-hidden">
        <table className="w-full text-left">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-sm font-medium text-gray-500">Name</th>
              <th className="px-4 py-3 text-sm font-medium text-gray-500">Catalog</th>
              <th className="px-4 py-3 text-sm font-medium text-gray-500">Schema</th>
              <th className="px-4 py-3 text-sm font-medium text-gray-500">Description</th>
              <th className="px-4 py-3 text-sm font-medium text-gray-500"></th>
            </tr>
          </thead>
          <tbody className="divide-y">
            {projects.map((p) => (
              <tr key={p.id} className="hover:bg-gray-50 cursor-pointer" onClick={() => navigate({ view: 'project', id: p.id, tab: 'overview' })}>
                <td className="px-4 py-3 font-medium text-blue-600">{p.name}</td>
                <td className="px-4 py-3 text-sm text-gray-500">{p.catalog}</td>
                <td className="px-4 py-3 text-sm text-gray-500">{p.schema}</td>
                <td className="px-4 py-3 text-sm text-gray-500">{p.description}</td>
                <td className="px-4 py-3">
                  <button
                    className="text-red-500 hover:text-red-700 text-sm"
                    onClick={(e) => { e.stopPropagation(); api.deleteProject(p.id).then(load); }}
                  >Delete</button>
                </td>
              </tr>
            ))}
            {projects.length === 0 && (
              <tr><td colSpan={5} className="px-4 py-8 text-center text-gray-400">No projects yet</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ════════════════════════════════════════════
//  PROJECT DETAIL
// ════════════════════════════════════════════
function ProjectDetail({ projectId, tab, navigate }: { projectId: number; tab: string; navigate: (p: Page) => void }) {
  const [project, setProject] = useState<Project | null>(null);
  const [eols, setEols] = useState<EOL[]>([]);
  const [trainingSpecs, setTrainingSpecs] = useState<(TrainingSpec & { run_count: number })[]>([]);
  const [specRuns, setSpecRuns] = useState<Run[]>([]);

  const load = useCallback(async () => {
    const [p, e, ts, sr] = await Promise.all([
      api.getProject(projectId),
      api.getEOLs(projectId),
      api.getTrainingSpecs(projectId),
      api.getSpecRuns(projectId),
    ]);
    setProject(p); setEols(e); setTrainingSpecs(ts); setSpecRuns(sr);
  }, [projectId]);

  useEffect(() => { load(); }, [load]);

  if (!project) return <div className="text-center py-8">Loading...</div>;

  const tabs = ['overview', 'eols', 'training', 'deployment'];

  return (
    <div>
      <div className="flex items-center gap-3 mb-4">
        <button onClick={() => navigate({ view: 'projects' })} className="text-blue-600 hover:text-blue-800">&larr; Projects</button>
        <h1 className="text-2xl font-bold">{project.name}</h1>
      </div>

      <div className="flex gap-1 mb-6 border-b">
        {tabs.map((t) => (
          <button
            key={t}
            className={`px-4 py-2 text-sm font-medium capitalize ${tab === t ? 'border-b-2 border-blue-600 text-blue-600' : 'text-gray-500 hover:text-gray-700'}`}
            onClick={() => navigate({ view: 'project', id: projectId, tab: t })}
          >{t}</button>
        ))}
      </div>

      {tab === 'overview' && <OverviewTab project={project} eols={eols} trainingSpecs={trainingSpecs} specRuns={specRuns} />}
      {tab === 'eols' && <EOLsTab projectId={projectId} eols={eols} reload={load} />}
      {tab === 'training' && <TrainingSpecTab projectId={projectId} eols={eols} specs={trainingSpecs} runs={specRuns} reload={load} />}
      {tab === 'deployment' && <DeploymentTab projectId={projectId} eols={eols} trainingSpecs={trainingSpecs} runs={specRuns} />}
    </div>
  );
}

// ── Overview Tab ──
function OverviewTab({ project, eols, trainingSpecs, specRuns }: {
  project: Project; eols: EOL[]; trainingSpecs: TrainingSpec[]; specRuns: Run[];
}) {
  return (
    <div className="grid grid-cols-2 gap-6">
      <div className="bg-white p-4 rounded-lg shadow">
        <h3 className="font-medium mb-3">Project Info</h3>
        <dl className="space-y-2 text-sm">
          <div><dt className="text-gray-500">Catalog</dt><dd className="font-mono">{project.catalog}</dd></div>
          <div><dt className="text-gray-500">Schema</dt><dd className="font-mono">{project.schema}</dd></div>
          <div><dt className="text-gray-500">Model Name</dt><dd className="font-mono">{project.model_name || project.name}</dd></div>
        </dl>
      </div>
      <div className="bg-white p-4 rounded-lg shadow">
        <h3 className="font-medium mb-3">Summary</h3>
        <div className="grid grid-cols-3 gap-4 text-center">
          <div className="p-3 bg-gray-50 rounded"><div className="text-2xl font-bold">{eols.length}</div><div className="text-xs text-gray-500">EOLs</div></div>
          <div className="p-3 bg-gray-50 rounded"><div className="text-2xl font-bold">{trainingSpecs.length}</div><div className="text-xs text-gray-500">Training Specs</div></div>
          <div className="p-3 bg-gray-50 rounded"><div className="text-2xl font-bold">{specRuns.length}</div><div className="text-xs text-gray-500">Runs</div></div>
        </div>
      </div>
    </div>
  );
}

// ── EOLs Tab — with SQL preview ──
function EOLForm({ projectId, initial, onSaved, onCancel }: {
  projectId: number;
  initial?: { name: string; sql_definition: string; label_column: string; entity_columns: string; timestamp_column: string };
  onSaved: () => void;
  onCancel: () => void;
}) {
  const [form, setForm] = useState(initial || { name: '', sql_definition: '', label_column: '', entity_columns: '', timestamp_column: '' });
  const [preview, setPreview] = useState<{ rows: any[]; count: number } | null>(null);
  const [previewError, setPreviewError] = useState('');
  const [previewing, setPreviewing] = useState(false);

  const runPreview = async (sql: string) => {
    setPreviewing(true); setPreviewError(''); setPreview(null);
    try { setPreview(await api.previewSql(sql)); } catch (e: any) { setPreviewError(e.message); }
    setPreviewing(false);
  };

  const submit = async () => {
    await api.createEOL(projectId, {
      ...form,
      entity_columns: form.entity_columns.split(',').map((s) => s.trim()).filter(Boolean),
    } as any);
    onSaved();
  };

  return (
    <div className="mb-4 p-4 border rounded-lg bg-white shadow">
      <div className="grid grid-cols-2 gap-3">
        <div><label className="block text-sm font-medium mb-1">Name</label><input className="w-full px-3 py-2 border rounded" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} /></div>
        <div><label className="block text-sm font-medium mb-1">Label Column</label><input className="w-full px-3 py-2 border rounded" value={form.label_column} onChange={(e) => setForm({ ...form, label_column: e.target.value })} /></div>
        <div><label className="block text-sm font-medium mb-1">Entity Columns (comma-separated)</label><input className="w-full px-3 py-2 border rounded" value={form.entity_columns} onChange={(e) => setForm({ ...form, entity_columns: e.target.value })} /></div>
        <div><label className="block text-sm font-medium mb-1">Timestamp Column</label><input className="w-full px-3 py-2 border rounded" value={form.timestamp_column} onChange={(e) => setForm({ ...form, timestamp_column: e.target.value })} /></div>
        <div className="col-span-2">
          <label className="block text-sm font-medium mb-1">SQL Definition</label>
          <textarea className="w-full px-3 py-2 border rounded font-mono text-sm h-32" value={form.sql_definition} onChange={(e) => setForm({ ...form, sql_definition: e.target.value })} />
          <button onClick={() => runPreview(form.sql_definition)} disabled={previewing || !form.sql_definition}
            className="mt-1 px-3 py-1 bg-gray-600 text-white text-xs rounded hover:bg-gray-700 disabled:bg-gray-300"
          >{previewing ? 'Running...' : 'Preview SQL'}</button>
        </div>
        {previewError && <div className="col-span-2 text-sm text-red-500 bg-red-50 p-2 rounded">{previewError}</div>}
        {preview && (
          <div className="col-span-2 max-h-48 overflow-auto border rounded">
            <table className="w-full text-xs font-mono">
              <thead className="bg-gray-100 sticky top-0">
                <tr>{preview.rows[0] && Object.keys(preview.rows[0]).map((k) => <th key={k} className="px-2 py-1 text-left">{k}</th>)}</tr>
              </thead>
              <tbody>
                {preview.rows.slice(0, 20).map((row, i) => (
                  <tr key={i} className="border-t">{Object.values(row).map((v: any, j) => <td key={j} className="px-2 py-1">{String(v)}</td>)}</tr>
                ))}
              </tbody>
            </table>
            <div className="text-xs text-gray-400 p-1">{preview.count} rows (showing first 20)</div>
          </div>
        )}
      </div>
      <div className="mt-3 flex gap-2">
        <button onClick={submit} className="px-4 py-2 bg-green-600 text-white rounded text-sm">Save</button>
        <button onClick={onCancel} className="px-4 py-2 bg-gray-200 rounded text-sm">Cancel</button>
      </div>
    </div>
  );
}

function EOLsTab({ projectId, eols, reload }: { projectId: number; eols: EOL[]; reload: () => void }) {
  const [showForm, setShowForm] = useState(false);
  const [copyingFrom, setCopyingFrom] = useState<EOL | null>(null);
  const [expandedId, setExpandedId] = useState<number | null>(null);

  const parseEntityCols = (cols: any): string => {
    if (Array.isArray(cols)) return cols.join(', ');
    if (typeof cols === 'string') return cols.replace(/^\{|\}$/g, '');
    return '';
  };

  return (
    <div>
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-lg font-medium">Entity Observation Labels</h2>
        <button onClick={() => { setShowForm(!showForm); setCopyingFrom(null); }} className="px-3 py-1.5 bg-blue-600 text-white text-sm rounded hover:bg-blue-700">
          {showForm ? 'Cancel' : 'Add EOL'}
        </button>
      </div>

      {(showForm || copyingFrom) && (
        <EOLForm
          projectId={projectId}
          initial={copyingFrom ? {
            name: `${copyingFrom.name} (copy)`,
            sql_definition: copyingFrom.sql_definition,
            label_column: copyingFrom.label_column,
            entity_columns: parseEntityCols(copyingFrom.entity_columns),
            timestamp_column: copyingFrom.timestamp_column || '',
          } : undefined}
          onSaved={() => { setShowForm(false); setCopyingFrom(null); reload(); }}
          onCancel={() => { setShowForm(false); setCopyingFrom(null); }}
        />
      )}

      <div className="space-y-3">
        {eols.map((eol) => {
          const isExpanded = expandedId === eol.id;
          return (
            <div key={eol.id} className="p-4 bg-white rounded-lg shadow">
              <div className="flex justify-between items-start">
                <div className="cursor-pointer flex-1" onClick={() => setExpandedId(isExpanded ? null : eol.id)}>
                  <h3 className="font-medium">
                    <span className="text-gray-400 mr-1">{isExpanded ? '▾' : '▸'}</span>
                    {eol.name}
                  </h3>
                  <div className="text-sm text-gray-500 mt-1">
                    Label: <span className="font-mono">{eol.label_column}</span> | Entity: <span className="font-mono">{parseEntityCols(eol.entity_columns)}</span>
                    {eol.timestamp_column && <> | Timestamp: <span className="font-mono">{eol.timestamp_column}</span></>}
                  </div>
                </div>
                <div className="flex gap-2">
                  <button onClick={() => { setCopyingFrom(eol); setShowForm(false); }} className="px-2 py-1 bg-gray-100 text-gray-600 text-xs rounded hover:bg-gray-200">Copy</button>
                  <button onClick={() => api.deleteEOL(eol.id).then(reload)} className="text-red-500 text-sm">Delete</button>
                </div>
              </div>
              {isExpanded && (
                <div className="mt-3 border-t pt-3">
                  <div className="grid grid-cols-2 gap-2 text-sm mb-2">
                    <div><span className="text-gray-500">Label Column:</span> <span className="font-mono">{eol.label_column}</span></div>
                    <div><span className="text-gray-500">Entity Columns:</span> <span className="font-mono">{parseEntityCols(eol.entity_columns)}</span></div>
                    {eol.timestamp_column && <div><span className="text-gray-500">Timestamp Column:</span> <span className="font-mono">{eol.timestamp_column}</span></div>}
                  </div>
                  <div>
                    <div className="text-sm text-gray-500 mb-1">SQL Definition:</div>
                    <pre className="p-2 bg-gray-50 rounded text-xs font-mono overflow-x-auto whitespace-pre-wrap">{eol.sql_definition}</pre>
                  </div>
                </div>
              )}
            </div>
          );
        })}
        {eols.length === 0 && <div className="text-center py-8 text-gray-400">No EOLs defined yet</div>}
      </div>
    </div>
  );
}

// ── Feature Summary — global include/exclude across all entries ──
function FeatureSummary({ spec, onChanged, locked }: {
  spec: TrainingSpec & { run_count: number };
  onChanged: () => void;
  locked: boolean;
}) {
  type Output = { name: string; source: string; sourceType: 'lookup' | 'on_demand' };
  const outputs: Output[] = [];
  for (const e of spec.entries) {
    if (e.feature_type === 'lookup') {
      const tbl = e.table_name?.split('.').pop() || 'lookup';
      for (const col of e.feature_names || []) {
        outputs.push({ name: col, source: tbl, sourceType: 'lookup' });
      }
    } else if (e.feature_type === 'on_demand' && e.output_name) {
      const fn = e.function_name?.split('.').pop() || 'function';
      outputs.push({ name: e.output_name, source: fn, sourceType: 'on_demand' });
    }
  }

  const persisted = useMemo(() => new Set(spec.excluded_features || []), [spec.excluded_features]);
  const [pending, setPending] = useState<Set<string>>(persisted);
  const [saving, setSaving] = useState(false);
  const [open, setOpen] = useState(false);

  // Reset pending state when the spec's persisted value changes (e.g. after save/reload)
  useEffect(() => { setPending(new Set(persisted)); }, [persisted]);

  const dirty = pending.size !== persisted.size ||
    [...pending].some(x => !persisted.has(x));

  const included = outputs.filter(o => !pending.has(o.name));

  const toggle = (name: string) => {
    setPending(prev => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name); else next.add(name);
      return next;
    });
  };

  const save = async () => {
    setSaving(true);
    try {
      await api.updateTrainingSpec(spec.id, { excluded_features: [...pending] });
      onChanged();
    } catch (err: any) {
      alert(`Update failed: ${err.message}`);
    }
    setSaving(false);
  };

  const reset = () => setPending(new Set(persisted));

  if (outputs.length === 0) return null;

  return (
    <div className="mt-4 border-t pt-3">
      <div className="flex items-center justify-between mb-2">
        <button
          onClick={() => setOpen(o => !o)}
          className="text-xs font-medium text-gray-500 hover:text-gray-700 flex items-center gap-1"
        >
          <span className="text-gray-400">{open ? '▾' : '▸'}</span>
          Training Features ({included.length} of {outputs.length} included)
          {locked && <span className="ml-2 text-gray-400">— locked</span>}
          {dirty && !locked && <span className="ml-2 text-amber-600">— unsaved</span>}
        </button>
        {open && !locked && (
          <div className="flex gap-2">
            {dirty && (
              <button onClick={reset} className="px-2 py-1 text-xs text-gray-500 hover:text-gray-700">Reset</button>
            )}
            <button
              onClick={save}
              disabled={!dirty || saving}
              className="px-3 py-1 bg-blue-600 text-white text-xs rounded hover:bg-blue-700 disabled:bg-gray-300"
            >{saving ? 'Saving...' : 'Save'}</button>
          </div>
        )}
      </div>
      {open && (
      <table className="w-full text-left text-sm border rounded overflow-hidden">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-3 py-1.5 font-medium text-gray-500 w-16">Include</th>
            <th className="px-3 py-1.5 font-medium text-gray-500">Column</th>
            <th className="px-3 py-1.5 font-medium text-gray-500">Source</th>
            <th className="px-3 py-1.5 font-medium text-gray-500">Type</th>
          </tr>
        </thead>
        <tbody className="divide-y">
          {outputs.map((o, i) => {
            const isExcluded = pending.has(o.name);
            return (
              <tr key={`${o.name}-${o.source}-${i}`} className={isExcluded ? 'bg-gray-50 text-gray-400' : ''}>
                <td className="px-3 py-1.5">
                  <input
                    type="checkbox"
                    checked={!isExcluded}
                    onChange={() => toggle(o.name)}
                    disabled={locked}
                  />
                </td>
                <td className="px-3 py-1.5 font-mono text-xs">{o.name}</td>
                <td className="px-3 py-1.5 font-mono text-xs">{o.source}</td>
                <td className="px-3 py-1.5">
                  {o.sourceType === 'lookup'
                    ? <span className="text-xs bg-green-100 text-green-700 px-1.5 py-0.5 rounded">Lookup</span>
                    : <span className="text-xs bg-blue-100 text-blue-700 px-1.5 py-0.5 rounded">On-Demand</span>}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      )}
    </div>
  );
}

// ── Feature Builder — master-detail UI for managing feature entries ──
function FeatureBuilder({ specId, eolId, eols, entries, onChanged, locked }: {
  specId: number; eolId: number | null; eols: EOL[];
  entries: FeatureEntry[]; onChanged: () => void; locked: boolean;
}) {
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [adding, setAdding] = useState<'lookup' | 'on_demand' | 'declarative' | null>(null);
  const [lastCatalog, setLastCatalog] = useState('');
  const [lastSchema, setLastSchema] = useState('');
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  const selectedEntry = entries.find(e => e.id === selectedId) || null;

  useEffect(() => {
    if (!menuOpen) return;
    const onDown = (ev: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(ev.target as Node)) setMenuOpen(false);
    };
    document.addEventListener('mousedown', onDown);
    return () => document.removeEventListener('mousedown', onDown);
  }, [menuOpen]);

  // Track last used catalog/schema from entries
  useEffect(() => {
    if (entries.length > 0) {
      const last = [...entries].reverse().find(e => e.table_name || e.function_name);
      if (last) {
        const parts = (last.table_name || last.function_name || '').split('.');
        if (parts.length >= 2) { setLastCatalog(parts[0]); setLastSchema(parts[1]); }
      }
    }
  }, [entries]);

  const handleAdd = async (data: any) => {
    try {
      await api.createTrainingSpecEntry(specId, data);
      setAdding(null);
      onChanged();
    } catch (e: any) {
      alert(`Add failed: ${e.message}`);
    }
  };

  const handleUpdate = async (id: number, data: any) => {
    try {
      await api.updateTrainingSpecEntry(id, data);
      onChanged();
    } catch (e: any) {
      alert(`Update failed: ${e.message}`);
    }
  };

  const handleDelete = async (id: number) => {
    if (!confirm('Delete this feature entry?')) return;
    await api.deleteTrainingSpecEntry(id);
    if (selectedId === id) setSelectedId(null);
    onChanged();
  };

  const entryLabel = (e: FeatureEntry) => {
    if (e.feature_type === 'lookup') {
      const short = e.table_name?.split('.').pop() || 'untitled';
      return short;
    }
    if (e.feature_type === 'on_demand') return e.output_name || e.function_name?.split('.').pop() || 'untitled';
    return 'declarative';
  };

  const typeBadge = (type: string) => {
    if (type === 'lookup') return <span className="text-xs bg-green-100 text-green-700 px-1.5 py-0.5 rounded">Lookup</span>;
    if (type === 'on_demand') return <span className="text-xs bg-blue-100 text-blue-700 px-1.5 py-0.5 rounded">On-Demand</span>;
    return <span className="text-xs bg-purple-100 text-purple-700 px-1.5 py-0.5 rounded">Declarative</span>;
  };

  return (
    <div className="flex gap-4 mt-3" style={{ minHeight: 300 }}>
      {/* Left panel — entry list */}
      <div className="w-1/3 border rounded bg-white">
        <div className="p-2 border-b flex items-center justify-between">
          <span className="text-sm font-medium">Features ({entries.length})</span>
          {!locked && (
            <div className="relative" ref={menuRef}>
              <button onClick={() => setMenuOpen(o => !o)} className="px-2 py-1 text-xs bg-green-600 text-white rounded">+ Add</button>
              {menuOpen && (
                <div className="absolute right-0 mt-1 bg-white border rounded shadow-lg z-10 min-w-[140px]">
                  <button onClick={() => { setAdding('lookup'); setSelectedId(null); setMenuOpen(false); }} className="block w-full text-left px-3 py-1.5 text-sm hover:bg-gray-50">Lookup</button>
                  <button onClick={() => { setAdding('on_demand'); setSelectedId(null); setMenuOpen(false); }} className="block w-full text-left px-3 py-1.5 text-sm hover:bg-gray-50">On-Demand</button>
                  <button onClick={() => { setAdding('declarative'); setSelectedId(null); setMenuOpen(false); }} className="block w-full text-left px-3 py-1.5 text-sm hover:bg-gray-50">Declarative</button>
                </div>
              )}
            </div>
          )}
        </div>
        <div className="divide-y max-h-[400px] overflow-y-auto">
          {entries.map(e => (
            <div
              key={e.id}
              onClick={() => { setSelectedId(e.id); setAdding(null); }}
              className={`flex items-center justify-between px-3 py-2 cursor-pointer text-sm ${selectedId === e.id ? 'bg-blue-50 border-l-2 border-blue-500' : 'hover:bg-gray-50'}`}
            >
              <div className="flex items-center gap-2 min-w-0">
                {typeBadge(e.feature_type)}
                <span className="font-mono text-xs truncate">{entryLabel(e)}</span>
              </div>
              {!locked && (
                <button onClick={(ev) => { ev.stopPropagation(); handleDelete(e.id); }} className="text-red-400 hover:text-red-600 text-xs ml-2 shrink-0">x</button>
              )}
            </div>
          ))}
          {adding && (
            <div className="flex items-center justify-between px-3 py-2 text-sm bg-blue-50 border-l-2 border-blue-500 border-dashed">
              <div className="flex items-center gap-2 min-w-0">
                {typeBadge(adding)}
                <span className="font-mono text-xs italic text-gray-500 truncate">new {adding.replace('_', '-')}…</span>
              </div>
            </div>
          )}
          {entries.length === 0 && !adding && <div className="p-3 text-xs text-gray-400 text-center">No features yet</div>}
        </div>
      </div>

      {/* Right panel — detail form */}
      <div className="flex-1 border rounded bg-white p-3 overflow-y-auto max-h-[500px]">
        {adding && (
          <FeatureEntryEditor
            mode="create"
            featureType={adding}
            eolId={eolId}
            eols={eols}
            allEntries={entries}
            defaultCatalog={lastCatalog}
            defaultSchema={lastSchema}
            onSave={handleAdd}
            onCancel={() => setAdding(null)}
          />
        )}
        {!adding && selectedEntry && (
          <FeatureEntryEditor
            key={selectedEntry.id}
            mode={locked ? 'view' : 'edit'}
            featureType={selectedEntry.feature_type}
            entry={selectedEntry}
            eolId={eolId}
            eols={eols}
            allEntries={entries}
            defaultCatalog={lastCatalog}
            defaultSchema={lastSchema}
            onSave={(data) => handleUpdate(selectedEntry.id, data)}
            onCancel={() => setSelectedId(null)}
          />
        )}
        {!adding && !selectedEntry && (
          <div className="flex items-center justify-center h-full text-sm text-gray-400">
            {entries.length > 0 ? 'Select an entry to view or edit' : 'Click + Add to create a feature entry'}
          </div>
        )}
      </div>
    </div>
  );
}

// ── Feature Entry Editor — handles create/edit/view for all entry types ──
function FeatureEntryEditor({ mode, featureType, entry, eolId, eols, allEntries, defaultCatalog, defaultSchema, onSave, onCancel }: {
  mode: 'create' | 'edit' | 'view';
  featureType: 'lookup' | 'on_demand' | 'declarative';
  entry?: FeatureEntry;
  eolId: number | null; eols: EOL[];
  allEntries: FeatureEntry[];
  defaultCatalog: string; defaultSchema: string;
  onSave: (data: any) => void;
  onCancel: () => void;
}) {
  const isReadOnly = mode === 'view';

  // Parse existing entry for edit/view
  const existingParts = entry?.table_name?.split('.') || entry?.function_name?.split('.') || [];
  const initCatalog = existingParts[0] || defaultCatalog;
  const initSchema = existingParts[1] || defaultSchema;
  const initTable = existingParts[2] || '';
  const initFunction = existingParts[2] || '';

  const [form, setForm] = useState({
    catalog: initCatalog, schema: initSchema, table: initTable,
    selectedFeatures: (entry?.feature_names || []) as string[],
    selectedLookupKeys: (entry?.lookup_key || []) as string[],
    timestamp_lookup_key: entry?.timestamp_lookup_key || '',
    default_values: entry?.default_values ? JSON.stringify(entry.default_values) : '',
    // On-demand fields
    selectedFunction: initFunction,
    outputName: entry?.output_name || '',
    bindings: (entry?.input_bindings || {}) as Record<string, string>,
    // Declarative fields
    declarative_input: (entry?.declarative_spec as any)?.input || '',
    declarative_function: (entry?.declarative_spec as any)?.function || '',
    declarative_window_type: (entry?.declarative_spec as any)?.time_window?.type || '',
    declarative_window_duration: (entry?.declarative_spec as any)?.time_window?.window_duration || '',
    declarative_slide_duration: (entry?.declarative_spec as any)?.time_window?.slide_duration || '',
    declarative_filter: (entry?.declarative_spec as any)?.filter_condition || '',
  });

  const [catalogs, setCatalogs] = useState<string[]>([]);
  const [schemas, setSchemas] = useState<string[]>([]);
  const [tables, setTables] = useState<string[]>([]);
  const [columns, setColumns] = useState<{ name: string; type: string }[]>([]);
  const [ucFunctions, setUcFunctions] = useState<string[]>([]);
  const [funcParams, setFuncParams] = useState<{ name: string; type: string }[]>([]);
  const [loadingUc, setLoadingUc] = useState('');
  void setLoadingUc;

  // EOL columns for lookup key selection
  const selectedEol = eols.find((e) => e.id === eolId);
  const rawCols: any = selectedEol?.entity_columns;
  const eolColumns: string[] = Array.isArray(rawCols)
    ? rawCols
    : typeof rawCols === 'string'
      ? rawCols.replace(/^\{|\}$/g, '').split(',').filter(Boolean)
      : [];

  // Collect all feature columns from sibling lookup entries (for on-demand bindings)
  const lookupColumns: { table: string; columns: string[] }[] = allEntries
    .filter(e => e.feature_type === 'lookup' && e.feature_names && e.id !== entry?.id)
    .map(e => ({ table: e.table_name?.split('.').pop() || '', columns: e.feature_names || [] }));

  // Load catalogs on mount
  useEffect(() => {
    api.getCatalogs().then(setCatalogs).catch(() => setCatalogs([]));
  }, []);

  // Load schemas when catalog changes (only reset for new entries or actual catalog change)
  const catalogRef = useRef(initCatalog);
  useEffect(() => {
    if (form.catalog) {
      api.getSchemas(form.catalog).then(setSchemas).catch(() => setSchemas([]));
      if (form.catalog !== catalogRef.current) {
        catalogRef.current = form.catalog;
        setTables([]); setColumns([]); setUcFunctions([]);
        setForm(f => ({ ...f, schema: '', table: '', selectedFunction: '', selectedFeatures: [] }));
      }
    }
  }, [form.catalog]);

  // Load tables + functions when schema changes
  const schemaRef = useRef(initSchema);
  useEffect(() => {
    if (form.catalog && form.schema) {
      api.getTables(form.catalog, form.schema).then(setTables).catch(() => setTables([]));
      api.getFunctions(form.catalog, form.schema).then(setUcFunctions).catch(() => setUcFunctions([]));
      if (form.schema !== schemaRef.current) {
        schemaRef.current = form.schema;
        setColumns([]);
        setForm(f => ({ ...f, table: '', selectedFunction: '', selectedFeatures: [] }));
      }
    }
  }, [form.catalog, form.schema]);

  // Load columns when table changes (for lookup)
  useEffect(() => {
    if (form.catalog && form.schema && form.table) {
      api.getColumns(form.catalog, form.schema, form.table).then(setColumns).catch(() => setColumns([]));
    }
  }, [form.catalog, form.schema, form.table]);

  // Load function params when function changes (for on-demand)
  useEffect(() => {
    if (form.catalog && form.schema && form.selectedFunction) {
      api.getFunctionParams(form.catalog, form.schema, form.selectedFunction)
        .then(setFuncParams).catch(() => setFuncParams([]));
    } else {
      setFuncParams([]);
    }
  }, [form.catalog, form.schema, form.selectedFunction]);

  const toggleFeatureCol = (col: string) => {
    setForm(f => ({
      ...f,
      selectedFeatures: f.selectedFeatures.includes(col)
        ? f.selectedFeatures.filter(c => c !== col)
        : [...f.selectedFeatures, col],
    }));
  };

  const toggleLookupKey = (col: string) => {
    setForm(f => ({
      ...f,
      selectedLookupKeys: f.selectedLookupKeys.includes(col)
        ? f.selectedLookupKeys.filter(c => c !== col)
        : [...f.selectedLookupKeys, col],
    }));
  };

  const submit = () => {
    const fullName = `${form.catalog}.${form.schema}`;

    if (featureType === 'lookup') {
      if (!form.table) { alert('Select a table'); return; }
      if (form.selectedFeatures.length === 0) { alert('Select at least one feature column'); return; }
      if (form.selectedLookupKeys.length === 0) { alert('Select at least one lookup key'); return; }
      onSave({
        feature_type: 'lookup',
        table_name: `${fullName}.${form.table}`,
        feature_names: form.selectedFeatures,
        lookup_key: form.selectedLookupKeys,
        timestamp_lookup_key: form.timestamp_lookup_key || null,
        default_values: form.default_values ? JSON.parse(form.default_values) : null,
      });
    } else if (featureType === 'on_demand') {
      if (!form.selectedFunction) { alert('Select a function'); return; }
      if (!form.outputName) { alert('Enter an output column name'); return; }
      const unboundParams = funcParams.filter(p => !form.bindings[p.name]);
      if (unboundParams.length > 0) { alert(`Bind all parameters: ${unboundParams.map(p => p.name).join(', ')}`); return; }
      onSave({
        feature_type: 'on_demand',
        function_name: `${fullName}.${form.selectedFunction}`,
        input_bindings: form.bindings,
        output_name: form.outputName,
      });
    } else if (featureType === 'declarative') {
      if (!form.table || !form.declarative_input || !form.declarative_function || !form.declarative_window_type || !form.declarative_window_duration) {
        alert('Fill in all required fields'); return;
      }
      const windowSpec: Record<string, any> = { type: form.declarative_window_type, window_duration: form.declarative_window_duration };
      if (form.declarative_window_type === 'sliding' && form.declarative_slide_duration) windowSpec.slide_duration = form.declarative_slide_duration;
      onSave({
        feature_type: 'declarative',
        table_name: `${fullName}.${form.table}`,
        declarative_spec: {
          source_table: `${fullName}.${form.table}`, input: form.declarative_input, function: form.declarative_function,
          time_window: windowSpec, lookup_key: form.selectedLookupKeys.length > 0 ? form.selectedLookupKeys : null,
          timestamp_lookup_key: form.timestamp_lookup_key || null, filter_condition: form.declarative_filter || null,
        },
      });
    }
  };

  const sel = (v: string) => isReadOnly ? v : undefined;
  void sel;
  const dis = isReadOnly;

  return (
    <div>
      <div className="flex items-center justify-between mb-3">
        <div className="text-sm font-medium">
          {mode === 'create' ? 'New' : ''} {featureType === 'lookup' ? 'Feature Lookup' : featureType === 'on_demand' ? 'On-Demand Feature' : 'Declarative Feature'}
        </div>
        <button onClick={onCancel} className="text-xs text-gray-400 hover:text-gray-600">Close</button>
      </div>

      {/* Catalog / Schema — shared across all types */}
      <div className="grid grid-cols-2 gap-3 mb-3">
        <div>
          <label className="block text-xs font-medium mb-1">Catalog {loadingUc === 'catalogs' && <span className="text-blue-500 text-xs">loading...</span>}</label>
          <select className="w-full px-2 py-1.5 border rounded text-sm" value={form.catalog} onChange={e => setForm({ ...form, catalog: e.target.value })} disabled={dis}>
            <option value="">Select...</option>
            {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium mb-1">Schema</label>
          <select className="w-full px-2 py-1.5 border rounded text-sm" value={form.schema} onChange={e => setForm({ ...form, schema: e.target.value })} disabled={dis || !form.catalog}>
            <option value="">Select...</option>
            {schemas.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
        </div>
      </div>

      {/* Lookup detail */}
      {featureType === 'lookup' && (
        <div className="space-y-3">
          <div>
            <label className="block text-xs font-medium mb-1">Table</label>
            <select className="w-full px-2 py-1.5 border rounded text-sm" value={form.table} onChange={e => setForm({ ...form, table: e.target.value })} disabled={dis || !form.schema}>
              <option value="">Select...</option>
              {tables.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium mb-1">Feature Columns</label>
            <div className="max-h-40 overflow-y-auto border rounded p-2 space-y-1">
              {columns.map(col => (
                <label key={col.name} className="flex items-center gap-1.5 text-sm cursor-pointer hover:bg-gray-50 px-1 rounded">
                  <input type="checkbox" checked={form.selectedFeatures.includes(col.name)} onChange={() => toggleFeatureCol(col.name)} disabled={dis} />
                  <span className="font-mono text-xs">{col.name}</span> <span className="text-gray-400 text-xs">({col.type})</span>
                </label>
              ))}
              {columns.length === 0 && <span className="text-xs text-gray-400">{form.table ? 'Loading...' : 'Select a table'}</span>}
            </div>
            {form.selectedFeatures.length > 0 && <div className="text-xs text-gray-500 mt-1">{form.selectedFeatures.length} selected</div>}
          </div>
          <div>
            <label className="block text-xs font-medium mb-1">Lookup Key(s) <span className="text-gray-400">(EOL entity columns)</span></label>
            <div className="border rounded p-2 space-y-1">
              {eolColumns.map(col => (
                <label key={col} className="flex items-center gap-1.5 text-sm cursor-pointer hover:bg-gray-50 px-1 rounded">
                  <input type="checkbox" checked={form.selectedLookupKeys.includes(col)} onChange={() => toggleLookupKey(col)} disabled={dis} />
                  <span className="font-mono text-xs">{col}</span>
                </label>
              ))}
            </div>
          </div>
          {selectedEol?.timestamp_column && (
            <div>
              <label className="block text-xs font-medium mb-1">Timestamp Lookup Key</label>
              <label className="flex items-center gap-1.5 text-sm cursor-pointer px-1">
                <input type="checkbox" checked={form.timestamp_lookup_key === selectedEol.timestamp_column}
                  onChange={e => setForm({ ...form, timestamp_lookup_key: e.target.checked ? selectedEol!.timestamp_column : '' })} disabled={dis} />
                <span className="font-mono text-xs">{selectedEol.timestamp_column}</span>
                <span className="text-gray-400 text-xs">(point-in-time join)</span>
              </label>
            </div>
          )}
          <div>
            <label className="block text-xs font-medium mb-1">Default Values (JSON)</label>
            <input className="w-full px-2 py-1.5 border rounded font-mono text-sm" value={form.default_values}
              onChange={e => setForm({ ...form, default_values: e.target.value })} placeholder='{"col": 0}' disabled={dis} />
          </div>
        </div>
      )}

      {/* On-demand detail */}
      {featureType === 'on_demand' && (
        <div className="space-y-3">
          <div>
            <label className="block text-xs font-medium mb-1">Function</label>
            <select className="w-full px-2 py-1.5 border rounded text-sm" value={form.selectedFunction}
              onChange={e => setForm({ ...form, selectedFunction: e.target.value, bindings: {} })} disabled={dis || !form.schema}>
              <option value="">Select...</option>
              {ucFunctions.map(f => <option key={f} value={f}>{f}</option>)}
            </select>
            {ucFunctions.length === 0 && form.schema && <div className="text-xs text-gray-400 mt-1">No functions in {form.catalog}.{form.schema}</div>}
          </div>
          <div>
            <label className="block text-xs font-medium mb-1">Output Column Name</label>
            <input className="w-full px-2 py-1.5 border rounded font-mono text-sm" value={form.outputName}
              onChange={e => setForm({ ...form, outputName: e.target.value })} placeholder="e.g. risk_score" disabled={dis} />
          </div>
          {funcParams.length > 0 && (
            <div>
              <label className="block text-xs font-medium mb-2">Input Bindings</label>
              <div className="border rounded divide-y">
                {funcParams.map(param => (
                  <div key={param.name} className="flex items-center gap-2 px-3 py-2">
                    <div className="w-1/3">
                      <span className="font-mono text-xs font-medium">{param.name}</span>
                      <span className="text-gray-400 text-xs ml-1">({param.type})</span>
                    </div>
                    <span className="text-gray-400 text-xs">&larr;</span>
                    <select className="flex-1 px-2 py-1 border rounded text-sm" value={form.bindings[param.name] || ''}
                      onChange={e => setForm(f => ({ ...f, bindings: { ...f.bindings, [param.name]: e.target.value } }))} disabled={dis}>
                      <option value="">Select source...</option>
                      {lookupColumns.map(lk => (
                        <optgroup key={lk.table} label={lk.table}>
                          {lk.columns.map(col => <option key={`${lk.table}-${col}`} value={col}>{col}</option>)}
                        </optgroup>
                      ))}
                      <optgroup label="EOL Columns">
                        {eolColumns.map(col => <option key={`eol-${col}`} value={col}>{col}</option>)}
                        {selectedEol?.timestamp_column && !eolColumns.includes(selectedEol.timestamp_column) && (
                          <option value={selectedEol.timestamp_column}>{selectedEol.timestamp_column} (timestamp)</option>
                        )}
                        {selectedEol?.label_column && !eolColumns.includes(selectedEol.label_column) && (
                          <option value={selectedEol.label_column}>{selectedEol.label_column} (label)</option>
                        )}
                      </optgroup>
                    </select>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Declarative detail */}
      {featureType === 'declarative' && (
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="block text-xs font-medium mb-1">Source Table</label>
            <select className="w-full px-2 py-1.5 border rounded text-sm" value={form.table} onChange={e => setForm({ ...form, table: e.target.value })} disabled={dis || !form.schema}>
              <option value="">Select...</option>
              {tables.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium mb-1">Input Column</label>
            <select className="w-full px-2 py-1.5 border rounded text-sm" value={form.declarative_input} onChange={e => setForm({ ...form, declarative_input: e.target.value })} disabled={dis || columns.length === 0}>
              <option value="">Select...</option>
              {columns.map(col => <option key={col.name} value={col.name}>{col.name} ({col.type})</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium mb-1">Function</label>
            <select className="w-full px-2 py-1.5 border rounded text-sm" value={form.declarative_function} onChange={e => setForm({ ...form, declarative_function: e.target.value })} disabled={dis}>
              <option value="">Select...</option>
              {['sum','avg','count','min','max','stddev_pop','stddev_samp','var_pop','var_samp','approx_count_distinct','first','last'].map(f => <option key={f} value={f}>{f}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium mb-1">Window Type</label>
            <select className="w-full px-2 py-1.5 border rounded text-sm" value={form.declarative_window_type} onChange={e => setForm({ ...form, declarative_window_type: e.target.value })} disabled={dis}>
              <option value="">Select...</option>
              <option value="continuous">Continuous</option>
              <option value="tumbling">Tumbling</option>
              <option value="sliding">Sliding</option>
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium mb-1">Window Duration</label>
            <input className="w-full px-2 py-1.5 border rounded font-mono text-sm" value={form.declarative_window_duration} onChange={e => setForm({ ...form, declarative_window_duration: e.target.value })} placeholder="30d" disabled={dis} />
          </div>
          {form.declarative_window_type === 'sliding' && (
            <div>
              <label className="block text-xs font-medium mb-1">Slide Duration</label>
              <input className="w-full px-2 py-1.5 border rounded font-mono text-sm" value={form.declarative_slide_duration} onChange={e => setForm({ ...form, declarative_slide_duration: e.target.value })} placeholder="1d" disabled={dis} />
            </div>
          )}
          <div className="col-span-2">
            <label className="block text-xs font-medium mb-1">Filter Condition</label>
            <input className="w-full px-2 py-1.5 border rounded font-mono text-sm" value={form.declarative_filter} onChange={e => setForm({ ...form, declarative_filter: e.target.value })} placeholder="e.g. amount > 100" disabled={dis} />
          </div>
        </div>
      )}

      {!isReadOnly && (
        <div className="mt-4 flex gap-2">
          <button onClick={submit} className="px-4 py-2 bg-green-600 text-white rounded text-sm">{mode === 'create' ? 'Add' : 'Save'}</button>
          <button onClick={onCancel} className="px-4 py-2 border rounded text-sm">Cancel</button>
        </div>
      )}
    </div>
  );
}

// ── Features Tab (legacy, replaced by TrainingSpecTab) ──
// eslint-disable-next-line @typescript-eslint/no-unused-vars
function FeaturesTab({ projectId, eols, features, reload }: { projectId: number; eols: EOL[]; features: FeatureDefinition[]; reload: () => void }) {
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [createForm, setCreateForm] = useState({ name: '', eol_id: '' });
  const [addingEntryTo, setAddingEntryTo] = useState<number | null>(null);
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [viewingEntryId, setViewingEntryId] = useState<number | null>(null);

  const submitCreate = async () => {
    await api.createFeature(projectId, {
      eol_id: createForm.eol_id ? parseInt(createForm.eol_id) : null,
      name: createForm.name,
    });
    setCreateForm({ name: '', eol_id: '' });
    setShowCreateForm(false);
    reload();
  };

  const copyFeature = async (f: FeatureDefinition) => {
    await api.copyFeature(f.id, `${f.name} (copy)`);
    reload();
  };

  return (
    <div>
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-lg font-medium">Feature Definitions</h2>
        <button onClick={() => setShowCreateForm(!showCreateForm)} className="px-3 py-1.5 bg-blue-600 text-white text-sm rounded hover:bg-blue-700">
          {showCreateForm ? 'Cancel' : 'New Feature Definition'}
        </button>
      </div>

      {showCreateForm && (
        <div className="mb-4 p-4 border rounded-lg bg-white shadow">
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-sm font-medium mb-1">Name</label>
              <input className="w-full px-3 py-2 border rounded" value={createForm.name} onChange={(e) => setCreateForm({ ...createForm, name: e.target.value })} placeholder="e.g. customer_churn_features" />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">EOL (spine)</label>
              <select className="w-full px-3 py-2 border rounded" value={createForm.eol_id} onChange={(e) => setCreateForm({ ...createForm, eol_id: e.target.value })}>
                <option value="">Select EOL...</option>
                {eols.map((e) => <option key={e.id} value={e.id}>{e.name}</option>)}
              </select>
            </div>
          </div>
          <div className="mt-3">
            <button onClick={submitCreate} className="px-4 py-2 bg-green-600 text-white rounded text-sm" disabled={!createForm.name || !createForm.eol_id}>Create</button>
          </div>
        </div>
      )}

      <div className="space-y-4">
        {features.map((f) => {
          const eol = eols.find((e) => e.id === f.eol_id);
          const isExpanded = expandedId === f.id;
          return (
            <div key={f.id} className="p-4 bg-white rounded-lg shadow">
              <div className="flex justify-between items-start">
                <div className="cursor-pointer flex-1" onClick={() => setExpandedId(isExpanded ? null : f.id)}>
                  <h3 className="font-medium">
                    <span className="text-gray-400 mr-1">{isExpanded ? '▾' : '▸'}</span>
                    {f.name}
                  </h3>
                  <div className="text-sm text-gray-500 ml-4">
                    EOL: <span className="font-mono">{eol?.name || '—'}</span>
                    <span className="ml-2 text-gray-400">{f.entries.length} {f.entries.length === 1 ? 'entry' : 'entries'}</span>
                  </div>
                </div>
                <div className="flex gap-2">
                  <button onClick={() => setAddingEntryTo(addingEntryTo === f.id ? null : f.id)} className="px-2 py-1 bg-blue-50 text-blue-600 text-xs rounded hover:bg-blue-100">
                    {addingEntryTo === f.id ? 'Cancel' : '+ Add Lookup'}
                  </button>
                  <button onClick={() => copyFeature(f)} className="px-2 py-1 bg-gray-100 text-gray-600 text-xs rounded hover:bg-gray-200">Copy</button>
                  <button onClick={() => api.deleteFeature(f.id).then(reload)} className="text-red-500 text-sm">Delete</button>
                </div>
              </div>

              {/* Expanded view — read-only detail */}
              {isExpanded && (
                <div className="mt-3 border-t pt-3">
                  {eol && (
                    <div className="mb-3 p-2 bg-blue-50 rounded text-sm">
                      <div className="font-medium text-blue-800 text-xs mb-1">EOL: {eol.name}</div>
                      <div className="text-blue-700 text-xs">
                        Label: <span className="font-mono">{eol.label_column}</span> | Entity: <span className="font-mono">{eol.entity_columns?.join?.(', ') || String(eol.entity_columns)}</span>
                        {eol.timestamp_column && <> | Timestamp: <span className="font-mono">{eol.timestamp_column}</span></>}
                      </div>
                    </div>
                  )}
                  {f.entries.length > 0 ? (
                    <div className="space-y-2">
                      {f.entries.map((entry) => (
                        <div key={entry.id}>
                          <div className="flex justify-between items-center p-2 bg-gray-50 rounded text-sm cursor-pointer hover:bg-gray-100"
                            onClick={() => setViewingEntryId(viewingEntryId === entry.id ? null : entry.id)}>
                            <div>
                              <span className="text-gray-400 mr-1">{viewingEntryId === entry.id ? '▾' : '▸'}</span>
                              <span className="text-xs px-1.5 py-0.5 bg-purple-100 text-purple-700 rounded mr-2">{entry.feature_type}</span>
                              {entry.feature_type === 'lookup' && (
                                <span className="text-gray-600">
                                  <span className="font-mono">{entry.table_name}</span>
                                  {' → '}
                                  <span className="font-mono">{entry.feature_names?.join(', ')}</span>
                                  {' (key: '}<span className="font-mono">{entry.lookup_key?.join(', ')}</span>{')'}
                                </span>
                              )}
                              {entry.feature_type === 'declarative' && entry.declarative_spec && (
                                <span className="font-mono text-gray-500 text-xs">
                                  {(entry.declarative_spec as any).function}({(entry.declarative_spec as any).input}) over {(entry.declarative_spec as any).time_window?.type} {(entry.declarative_spec as any).time_window?.window_duration} — {(entry.declarative_spec as any).source_table}
                                </span>
                              )}
                            </div>
                            <button onClick={(e) => { e.stopPropagation(); api.deleteFeatureEntry(entry.id).then(reload); }} className="text-red-400 text-xs hover:text-red-600">remove</button>
                          </div>
                          {viewingEntryId === entry.id && (
                            <div className="mt-2 p-2 bg-gray-50 rounded text-xs font-mono">{JSON.stringify(entry, null, 2)}</div>
                          )}
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="text-xs text-gray-400">No feature entries yet — click "+ Add Lookup" to add lookups or declarative features</div>
                  )}
                </div>
              )}

              {/* Collapsed summary — show entry count only */}
              {!isExpanded && f.entries.length === 0 && (
                <div className="mt-2 ml-4 text-xs text-gray-400">No feature entries yet</div>
              )}

              {/* Add entry form */}
              {addingEntryTo === f.id && (
                <div className="p-3 text-sm text-gray-400">Legacy feature entry form removed — use Training Specs</div>
              )}
            </div>
          );
        })}
        {features.length === 0 && <div className="text-center py-8 text-gray-400">No feature definitions yet</div>}
      </div>
    </div>
  );
}

// ── Datasets Tab (legacy, replaced by TrainingSpecTab) ──
// eslint-disable-next-line @typescript-eslint/no-unused-vars
function DatasetsTab({ projectId, features, datasets, reload }: {
  projectId: number; features: FeatureDefinition[]; datasets: Dataset[]; reload: () => void;
}) {
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({ name: '', feature_definition_id: '', eval_split_type: 'percentage', eval_percentage: '20' });
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Poll for status on any MATERIALIZING datasets
  useEffect(() => {
    const materializing = datasets.filter((d) => d.status === 'MATERIALIZING' && d.materialize_run_id);
    if (materializing.length === 0) {
      if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
      return;
    }
    if (pollRef.current) return; // already polling
    pollRef.current = setInterval(async () => {
      let changed = false;
      for (const d of materializing) {
        try {
          const updated = await api.checkDatasetStatus(projectId, d.id);
          if (updated.status !== 'MATERIALIZING') changed = true;
        } catch { /* ignore */ }
      }
      if (changed) reload();
    }, 10000);
    return () => { if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; } };
  }, [datasets, projectId, reload]);

  const submit = async () => {
    if (!form.feature_definition_id) { alert('Select a feature definition'); return; }
    await api.createDataset(projectId, {
      name: form.name,
      feature_definition_id: parseInt(form.feature_definition_id),
      eval_split_type: form.eval_split_type,
      eval_split_config: form.eval_split_type === 'percentage' ? { percentage: parseFloat(form.eval_percentage) } : null,
    });
    setForm({ name: '', feature_definition_id: '', eval_split_type: 'percentage', eval_percentage: '20' });
    setShowForm(false);
    reload();
  };

  // Manual refresh for a single dataset
  const checkNow = async (d: Dataset) => {
    try {
      await api.checkDatasetStatus(projectId, d.id);
      reload();
    } catch (e: any) { alert(e.message); }
  };

  return (
    <div>
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-lg font-medium">Datasets</h2>
        <button onClick={() => setShowForm(!showForm)} className="px-3 py-1.5 bg-blue-600 text-white text-sm rounded hover:bg-blue-700">New Dataset</button>
      </div>

      {showForm && (
        <div className="mb-4 p-4 border rounded-lg bg-white shadow">
          <div className="grid grid-cols-2 gap-3">
            <div><label className="block text-sm font-medium mb-1">Name</label><input className="w-full px-3 py-2 border rounded" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} /></div>
            <div>
              <label className="block text-sm font-medium mb-1">Feature Definition</label>
              <select className="w-full px-3 py-2 border rounded" value={form.feature_definition_id} onChange={(e) => setForm({ ...form, feature_definition_id: e.target.value })}>
                <option value="">Select feature definition...</option>
                {features.map((f) => <option key={f.id} value={f.id}>{f.name} ({f.entries.length} entries)</option>)}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Eval Split Type</label>
              <select className="w-full px-3 py-2 border rounded" value={form.eval_split_type} onChange={(e) => setForm({ ...form, eval_split_type: e.target.value })}>
                <option value="percentage">Percentage</option>
                <option value="time">Time-based</option>
                <option value="custom">Custom</option>
              </select>
            </div>
            {form.eval_split_type === 'percentage' && (
              <div>
                <label className="block text-sm font-medium mb-1">Eval Percentage</label>
                <input type="number" className="w-full px-3 py-2 border rounded" value={form.eval_percentage} onChange={(e) => setForm({ ...form, eval_percentage: e.target.value })} />
              </div>
            )}
          </div>
          <div className="mt-3 flex gap-2">
            <button onClick={submit} className="px-4 py-2 bg-green-600 text-white rounded text-sm">Create</button>
            <button onClick={() => setShowForm(false)} className="px-4 py-2 bg-gray-200 rounded text-sm">Cancel</button>
          </div>
        </div>
      )}

      <div className="space-y-3">
        {datasets.map((d) => (
          <div key={d.id} className="p-4 bg-white rounded-lg shadow">
            <div className="flex justify-between items-start">
              <div>
                <h3 className="font-medium">{d.name} <StatusBadge status={d.status} /></h3>
                <div className="text-sm text-gray-500 mt-1">
                  Split: {d.eval_split_type}{d.eval_split_config?.percentage ? ` (${d.eval_split_config.percentage}%)` : ''}
                  {d.row_count !== null && <> | Rows: {d.row_count.toLocaleString()}</>}
                </div>
                {d.training_table && (() => {
                  const tableUrl = (t: string) => {
                    if (!d.materialize_run_url) return null;
                    const host = new URL(d.materialize_run_url).origin;
                    const parts = t.split('.');
                    if (parts.length === 3) return `${host}/explore/data/${parts[0]}/${parts[1]}/${parts[2]}`;
                    return null;
                  };
                  const trainUrl = tableUrl(d.training_table!);
                  const evalUrl = d.eval_table ? tableUrl(d.eval_table) : null;
                  return (
                    <div className="text-sm font-mono text-gray-500">
                      Train: {trainUrl ? <a href={trainUrl} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">{d.training_table}</a> : d.training_table}
                      {d.eval_table && <> | Eval: {evalUrl ? <a href={evalUrl} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">{d.eval_table}</a> : d.eval_table}</>}
                    </div>
                  );
                })()}
              </div>
              <div className="flex gap-2">
                {d.status === 'NOT_STARTED' && (
                  <button
                    onClick={() => api.materializeDataset(projectId, d.id).then(reload).catch((e) => alert(e.message))}
                    className="px-2 py-1 bg-blue-600 text-white text-xs rounded hover:bg-blue-700"
                  >Materialize</button>
                )}
                {d.materialize_run_url && (
                  <a href={d.materialize_run_url} target="_blank" rel="noopener noreferrer" className="text-blue-600 text-xs hover:underline">View Job</a>
                )}
                {d.status === 'MATERIALIZING' && (
                  <button onClick={() => checkNow(d)} className="px-2 py-1 bg-gray-100 text-gray-600 text-xs rounded hover:bg-gray-200">Check Status</button>
                )}
                {d.status === 'FAILED' && (
                  <button
                    onClick={() => api.materializeDataset(projectId, d.id).then(reload).catch((e) => alert(e.message))}
                    className="px-2 py-1 bg-orange-600 text-white text-xs rounded hover:bg-orange-700"
                  >Retry</button>
                )}
                <button onClick={() => api.deleteDataset(d.id).then(reload)} className="text-red-500 text-sm">Delete</button>
              </div>
            </div>
          </div>
        ))}
        {datasets.length === 0 && <div className="text-center py-8 text-gray-400">No datasets yet</div>}
      </div>
    </div>
  );
}

// ── Runs Tab (train + evaluate) (legacy, replaced by TrainingSpecTab) ──
// eslint-disable-next-line @typescript-eslint/no-unused-vars
function RunsTab({ projectId, datasets, runs, reload }: {
  projectId: number; datasets: Dataset[]; runs: Run[]; reload: () => void;
}) {
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({ dataset_id: '', parameters: '' });
  const [registerError, setRegisterError] = useState('');
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Poll for status on RUNNING runs
  useEffect(() => {
    const active = runs.filter((r) => r.status === 'RUNNING');
    if (active.length === 0) {
      if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
      return;
    }
    if (pollRef.current) return;
    pollRef.current = setInterval(async () => {
      let changed = false;
      for (const r of active) {
        try {
          const updated = await api.checkRunStatus(r.id);
          if (updated.status !== 'RUNNING') changed = true;
        } catch { /* ignore */ }
      }
      if (changed) reload();
    }, 10000);
    return () => { if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; } };
  }, [runs, reload]);

  const submit = async () => {
    await api.createRun(projectId, {
      dataset_id: parseInt(form.dataset_id),
      parameters: form.parameters ? JSON.parse(form.parameters) : undefined,
    });
    setForm({ dataset_id: '', parameters: '' });
    setShowForm(false);
    reload();
  };

  const readyDatasets = datasets.filter((d) => d.status === 'READY');
  const host = runs[0]?.databricks_run_url?.match(/^https?:\/\/[^/]+/)?.[0] || '';

  const formatMetrics = (metrics: Record<string, any> | null) => {
    if (!metrics) return '-';
    return Object.entries(metrics).map(([k, v]) => `${k}=${typeof v === 'number' ? v.toFixed(4) : v}`).join(', ');
  };

  return (
    <div>
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-lg font-medium">Runs</h2>
        <button onClick={() => setShowForm(!showForm)} className="px-3 py-1.5 bg-blue-600 text-white text-sm rounded hover:bg-blue-700">New Run</button>
      </div>

      {showForm && (
        <div className="mb-4 p-4 border rounded-lg bg-white shadow">
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-sm font-medium mb-1">Dataset</label>
              <select className="w-full px-3 py-2 border rounded" value={form.dataset_id} onChange={(e) => setForm({ ...form, dataset_id: e.target.value })}>
                <option value="">Select dataset...</option>
                {readyDatasets.map((d) => <option key={d.id} value={d.id}>{d.name}</option>)}
                {datasets.filter((d) => d.status !== 'READY').map((d) => <option key={d.id} value={d.id} disabled>{d.name} ({d.status})</option>)}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Parameters (JSON, optional)</label>
              <input className="w-full px-3 py-2 border rounded font-mono text-sm" value={form.parameters} onChange={(e) => setForm({ ...form, parameters: e.target.value })} placeholder='{"n_estimators": 100}' />
            </div>
          </div>
          <div className="mt-3 flex gap-2">
            <button onClick={submit} className="px-4 py-2 bg-green-600 text-white rounded text-sm">Create</button>
            <button onClick={() => setShowForm(false)} className="px-4 py-2 bg-gray-200 rounded text-sm">Cancel</button>
          </div>
        </div>
      )}

      {registerError && (
        <div className="mb-3 p-3 bg-red-50 border border-red-200 rounded text-sm text-red-700 flex justify-between items-center">
          <span>Registration failed: {registerError}</span>
          <button onClick={() => setRegisterError('')} className="text-red-400 hover:text-red-600 text-xs ml-4">Dismiss</button>
        </div>
      )}

      <div className="bg-white rounded-lg shadow overflow-hidden">
        <table className="w-full text-left text-sm">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 font-medium text-gray-500">ID</th>
              <th className="px-4 py-3 font-medium text-gray-500">Dataset</th>
              <th className="px-4 py-3 font-medium text-gray-500">Status</th>
              <th className="px-4 py-3 font-medium text-gray-500">Model</th>
              <th className="px-4 py-3 font-medium text-gray-500">Metrics</th>
              <th className="px-4 py-3 font-medium text-gray-500">Links</th>
            </tr>
          </thead>
          <tbody className="divide-y">
            {runs.map((r) => {
              const ds = datasets.find((d) => d.id === r.dataset_id);
              return (
                <tr key={r.id} className="hover:bg-gray-50">
                  <td className="px-4 py-3">{r.id}</td>
                  <td className="px-4 py-3">{ds?.name || r.dataset_id}</td>
                  <td className="px-4 py-3">
                    <StatusBadge status={r.status} />
                    {r.status === 'PENDING' && (
                      <button
                        className="ml-2 px-2 py-0.5 bg-green-600 text-white text-xs rounded hover:bg-green-700"
                        onClick={() => api.launchRun(projectId, r.id).then(reload)}
                      >Launch</button>
                    )}
                    {r.status === 'RUNNING' && (
                      <button
                        className="ml-2 px-2 py-0.5 bg-gray-200 text-gray-600 text-xs rounded hover:bg-gray-300"
                        onClick={() => api.checkRunStatus(r.id).then(reload)}
                      >Check</button>
                    )}
                    {r.error_message && <div className="text-xs text-red-500 mt-1">{r.error_message}</div>}
                  </td>
                  <td className="px-4 py-3 font-mono text-xs">
                    {r.model_name ? (
                      <a
                        href={`${host}/explore/data/models/${r.model_name.replace(/\./g, '/')}${r.model_version ? `/version/${r.model_version}` : ''}`}
                        target="_blank" rel="noopener noreferrer"
                        className="text-blue-600 hover:underline"
                      >{r.model_name.split('.').pop()} v{r.model_version}</a>
                    ) : r.status === 'SUCCESS' ? (
                      <button
                        className="px-2 py-1 bg-purple-600 text-white text-xs rounded hover:bg-purple-700"
                        onClick={() => {
                          setRegisterError('');
                          api.registerModel(r.id).then(reload).catch((e) => setRegisterError(e.message));
                        }}
                      >Register</button>
                    ) : '-'}
                  </td>
                  <td className="px-4 py-3 font-mono text-xs">{formatMetrics(r.eval_metrics || r.training_metrics)}</td>
                  <td className="px-4 py-3 text-xs space-x-2">
                    {r.databricks_run_url && <a href={r.databricks_run_url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">Job</a>}
                    {r.mlflow_experiment_id && <a href={`${host}/ml/experiments/${r.mlflow_experiment_id}`} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">MLflow</a>}
                    <button onClick={() => api.deleteRun(r.id).then(reload)} className="text-red-500 hover:underline">Delete</button>
                  </td>
                </tr>
              );
            })}
            {runs.length === 0 && (
              <tr><td colSpan={6} className="px-4 py-8 text-center text-gray-400">No runs yet</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ════════════════════════════════════════════
//  TRAINING SPEC TAB
// ════════════════════════════════════════════
function TrainingSpecTab({ projectId, eols, specs, runs, reload }: {
  projectId: number; eols: EOL[]; specs: (TrainingSpec & { run_count: number })[]; runs: Run[]; reload: () => void;
}) {
  const [showForm, setShowForm] = useState(false);
  const [copyingFromId, setCopyingFromId] = useState<number | null>(null);
  const emptyForm = { name: '', eol_id: '', task_type: 'classification', split_strategy: 'none', split_method: 'random', eval_pct: '20', seed: '42', parameters: '' };
  const [form, setForm] = useState(emptyForm);
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [registerError, setRegisterError] = useState('');
  const [renamingId, setRenamingId] = useState<number | null>(null);
  const [renameValue, setRenameValue] = useState('');
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const commitRename = async (specId: number, original: string) => {
    const next = renameValue.trim();
    setRenamingId(null);
    if (!next || next === original) return;
    try {
      await api.updateTrainingSpec(specId, { name: next });
      reload();
    } catch (e: any) {
      alert(`Rename failed: ${e.message}`);
    }
  };

  // Poll running runs
  useEffect(() => {
    const active = runs.filter(r => r.status === 'RUNNING');
    if (active.length === 0) {
      if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
      return;
    }
    if (pollRef.current) return;
    pollRef.current = setInterval(async () => {
      let changed = false;
      for (const r of active) {
        try {
          const updated = await api.checkSpecRunStatus(r.id);
          if (updated.status !== 'RUNNING') changed = true;
        } catch { /* ignore */ }
      }
      if (changed) reload();
    }, 10000);
    return () => { if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; } };
  }, [runs, reload]);

  const submitSpec = async () => {
    const splitConfig = form.split_strategy !== 'none' ? { eval_pct: parseFloat(form.eval_pct), seed: parseInt(form.seed) } : null;
    const payload = {
      name: form.name,
      eol_id: form.eol_id ? parseInt(form.eol_id) : null,
      task_type: form.task_type as any,
      split_strategy: form.split_strategy as any,
      split_method: form.split_strategy !== 'none' ? form.split_method as any : null,
      split_config: splitConfig,
      parameters: form.parameters ? JSON.parse(form.parameters) : null,
    };
    try {
      if (copyingFromId != null) {
        await api.copyTrainingSpec(copyingFromId, payload as any);
      } else {
        await api.createTrainingSpec(projectId, payload);
      }
    } catch (e: any) {
      alert(`Save failed: ${e.message}`);
      return;
    }
    setForm(emptyForm);
    setShowForm(false);
    setCopyingFromId(null);
    reload();
  };

  const startCopy = (spec: TrainingSpec & { run_count: number }) => {
    setCopyingFromId(spec.id);
    setForm({
      name: `${spec.name} (copy)`,
      eol_id: spec.eol_id != null ? String(spec.eol_id) : '',
      task_type: spec.task_type,
      split_strategy: spec.split_strategy,
      split_method: spec.split_method || 'random',
      eval_pct: String(spec.split_config?.eval_pct ?? '20'),
      seed: String(spec.split_config?.seed ?? '42'),
      parameters: spec.parameters ? JSON.stringify(spec.parameters) : '',
    });
    setShowForm(true);
  };

  const cancelForm = () => {
    setShowForm(false);
    setCopyingFromId(null);
    setForm(emptyForm);
  };

  const launchRun = async (specId: number) => {
    try {
      const run = await api.createSpecRun(specId);
      await api.launchSpecRun(run.id);
      reload();
    } catch (e: any) { alert(`Launch failed: ${e.message}`); }
  };

  const registerModel = async (runId: number) => {
    setRegisterError('');
    try {
      await api.registerModel(runId);
      reload();
    } catch (e: any) { setRegisterError(e.message); }
  };

  const host = runs[0]?.databricks_run_url?.match(/^https?:\/\/[^/]+/)?.[0] || '';

  const formatMetrics = (metrics: Record<string, any> | null) => {
    if (!metrics) return '-';
    return Object.entries(metrics).map(([k, v]) => `${k}=${typeof v === 'number' ? v.toFixed(4) : v}`).join(', ');
  };

  return (
    <div>
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-lg font-medium">Training Specs</h2>
        <button onClick={() => showForm ? cancelForm() : setShowForm(true)} className="px-3 py-1.5 bg-blue-600 text-white text-sm rounded hover:bg-blue-700">
          {showForm ? 'Cancel' : 'New Training Spec'}
        </button>
      </div>

      {showForm && (
        <div className="mb-4 p-4 border rounded-lg bg-white shadow">
          {copyingFromId != null && (
            <div className="text-sm text-gray-600 mb-3">
              Copying from <span className="font-mono">{specs.find(s => s.id === copyingFromId)?.name}</span> — edit fields before saving.
            </div>
          )}
          <div className="grid grid-cols-3 gap-3">
            <div><label className="block text-sm font-medium mb-1">Name</label><input className="w-full px-3 py-2 border rounded" value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} placeholder="e.g. customer_churn_v1" /></div>
            <div>
              <label className="block text-sm font-medium mb-1">EOL (spine)</label>
              <select className="w-full px-3 py-2 border rounded" value={form.eol_id} onChange={e => setForm({ ...form, eol_id: e.target.value })}>
                <option value="">Select EOL...</option>
                {eols.map(e => <option key={e.id} value={e.id}>{e.name}</option>)}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Task Type</label>
              <select className="w-full px-3 py-2 border rounded" value={form.task_type} onChange={e => setForm({ ...form, task_type: e.target.value })}>
                <option value="classification">Classification</option>
                <option value="regression">Regression</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Split Strategy</label>
              <select className="w-full px-3 py-2 border rounded" value={form.split_strategy} onChange={e => setForm({ ...form, split_strategy: e.target.value })}>
                <option value="none">None (CV)</option>
                <option value="train_eval">Train / Eval</option>
                <option value="train_eval_test">Train / Eval / Test</option>
              </select>
            </div>
            {form.split_strategy !== 'none' && (
              <>
                <div>
                  <label className="block text-sm font-medium mb-1">Split Method</label>
                  <select className="w-full px-3 py-2 border rounded" value={form.split_method} onChange={e => setForm({ ...form, split_method: e.target.value })}>
                    <option value="random">Random (stratified)</option>
                    <option value="temporal">Temporal</option>
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium mb-1">Eval % / Seed</label>
                  <div className="flex gap-2">
                    <input type="number" className="w-20 px-2 py-2 border rounded" value={form.eval_pct} onChange={e => setForm({ ...form, eval_pct: e.target.value })} />
                    <input type="number" className="w-20 px-2 py-2 border rounded" value={form.seed} onChange={e => setForm({ ...form, seed: e.target.value })} placeholder="seed" />
                  </div>
                </div>
              </>
            )}
            <div className="col-span-3">
              <label className="block text-sm font-medium mb-1">Parameters (JSON, optional)</label>
              <input className="w-full px-3 py-2 border rounded font-mono text-sm" value={form.parameters} onChange={e => setForm({ ...form, parameters: e.target.value })} placeholder='{"iterations": 100}' />
            </div>
          </div>
          <div className="mt-3">
            <button onClick={submitSpec} className="px-4 py-2 bg-green-600 text-white rounded text-sm" disabled={!form.name || !form.eol_id}>
              {copyingFromId != null ? 'Save Copy' : 'Create'}
            </button>
          </div>
        </div>
      )}

      {registerError && (
        <div className="mb-3 p-3 bg-red-50 border border-red-200 rounded text-sm text-red-700 flex justify-between items-center">
          <span>Registration failed: {registerError}</span>
          <button onClick={() => setRegisterError('')} className="text-red-400 hover:text-red-600 text-xs ml-4">Dismiss</button>
        </div>
      )}

      <div className="space-y-4">
        {specs.map(spec => {
          const eol = eols.find(e => e.id === spec.eol_id);
          const isExpanded = expandedId === spec.id;
          const isLocked = spec.run_count > 0;
          const specRuns = runs.filter(r => r.training_spec_id === spec.id);

          return (
            <div key={spec.id} className="bg-white rounded-lg shadow">
              {/* Header */}
              <div className="p-4 flex justify-between items-start">
                <div className="cursor-pointer flex-1" onClick={() => setExpandedId(isExpanded ? null : spec.id)}>
                  <h3 className="font-medium">
                    <span className="text-gray-400 mr-1">{isExpanded ? '▾' : '▸'}</span>
                    {renamingId === spec.id ? (
                      <input
                        autoFocus
                        className="border rounded px-1 py-0.5 text-sm font-medium"
                        value={renameValue}
                        onChange={e => setRenameValue(e.target.value)}
                        onClick={e => e.stopPropagation()}
                        onBlur={() => commitRename(spec.id, spec.name)}
                        onKeyDown={e => {
                          if (e.key === 'Enter') { e.preventDefault(); commitRename(spec.id, spec.name); }
                          if (e.key === 'Escape') { e.preventDefault(); setRenamingId(null); }
                        }}
                      />
                    ) : (
                      <span
                        onClick={e => {
                          if (isLocked) return;
                          e.stopPropagation();
                          setRenameValue(spec.name);
                          setRenamingId(spec.id);
                        }}
                        className={isLocked ? '' : 'cursor-text hover:bg-gray-100 px-1 rounded'}
                        title={isLocked ? undefined : 'Click to rename'}
                      >
                        {spec.name}
                      </span>
                    )}
                    {isLocked && <span className="ml-2 text-xs bg-gray-200 text-gray-600 px-1.5 py-0.5 rounded">locked</span>}
                  </h3>
                  <div className="text-sm text-gray-500 ml-4">
                    EOL: <span className="font-mono">{eol?.name || '—'}</span>
                    <span className="mx-2">|</span>{spec.task_type}
                    <span className="mx-2">|</span>{spec.split_strategy === 'none' ? 'CV' : spec.split_strategy.replace('_', '/')}
                    <span className="mx-2">|</span>{spec.entries.length} {spec.entries.length === 1 ? 'feature' : 'features'}
                    <span className="mx-2">|</span>{spec.run_count} {spec.run_count === 1 ? 'run' : 'runs'}
                  </div>
                </div>
                <div className="flex gap-2">
                  {!isLocked && spec.entries.length > 0 && (
                    <button onClick={() => launchRun(spec.id)} className="px-2 py-1 bg-green-600 text-white text-xs rounded hover:bg-green-700">Run</button>
                  )}
                  <button onClick={() => startCopy(spec)} className="px-2 py-1 bg-gray-100 text-gray-600 text-xs rounded hover:bg-gray-200">Copy</button>
                  {!isLocked && <button onClick={() => api.deleteTrainingSpec(spec.id).then(reload)} className="text-red-500 text-sm">Delete</button>}
                </div>
              </div>

              {/* Expanded detail */}
              {isExpanded && (
                <div className="border-t px-4 pb-4 pt-3">
                  {/* Config summary */}
                  <div className="grid grid-cols-4 gap-2 text-sm mb-3">
                    <div><span className="text-gray-500">Task:</span> {spec.task_type}</div>
                    <div><span className="text-gray-500">Split:</span> {spec.split_strategy === 'none' ? 'None (CV)' : `${spec.split_strategy.replace('_', '/')} — ${spec.split_method}`}</div>
                    {spec.split_config && <div><span className="text-gray-500">Eval:</span> {spec.split_config.eval_pct}% (seed {spec.split_config.seed})</div>}
                    {spec.parameters && Object.keys(spec.parameters).length > 0 && (
                      <div><span className="text-gray-500">Params:</span> <span className="font-mono text-xs">{JSON.stringify(spec.parameters)}</span></div>
                    )}
                  </div>

                  {/* Feature Builder — master-detail */}
                  <FeatureBuilder specId={spec.id} eolId={spec.eol_id} eols={eols} entries={spec.entries} onChanged={reload} locked={isLocked} />

                  {/* Training Features summary — global include/exclude */}
                  <FeatureSummary spec={spec} onChanged={reload} locked={isLocked} />

                  {/* Runs */}
                  {specRuns.length > 0 && (
                    <div>
                      <div className="text-xs font-medium text-gray-500 mb-2">Runs</div>
                      <table className="w-full text-left text-sm">
                        <thead className="bg-gray-50">
                          <tr>
                            <th className="px-3 py-2 font-medium text-gray-500">ID</th>
                            <th className="px-3 py-2 font-medium text-gray-500">Status</th>
                            <th className="px-3 py-2 font-medium text-gray-500">Model</th>
                            <th className="px-3 py-2 font-medium text-gray-500">Metrics</th>
                            <th className="px-3 py-2 font-medium text-gray-500">Links</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y">
                          {specRuns.map(r => (
                            <tr key={r.id} className="hover:bg-gray-50">
                              <td className="px-3 py-2">{r.id}</td>
                              <td className="px-3 py-2">
                                <StatusBadge status={r.status} />
                                {r.status === 'RUNNING' && (
                                  <button className="ml-2 px-2 py-0.5 bg-gray-200 text-gray-600 text-xs rounded" onClick={() => api.checkSpecRunStatus(r.id).then(reload)}>Check</button>
                                )}
                                {r.error_message && <div className="text-xs text-red-500 mt-1">{r.error_message}</div>}
                              </td>
                              <td className="px-3 py-2 font-mono text-xs">
                                {r.model_name ? (
                                  <a href={`${host}/explore/data/models/${r.model_name.replace(/\./g, '/')}`} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">
                                    {r.model_name.split('.').pop()} v{r.model_version}
                                  </a>
                                ) : r.status === 'SUCCESS' ? (
                                  <button className="px-2 py-1 bg-purple-600 text-white text-xs rounded hover:bg-purple-700" onClick={() => registerModel(r.id)}>Register</button>
                                ) : '-'}
                              </td>
                              <td className="px-3 py-2 font-mono text-xs">{formatMetrics(r.eval_metrics)}</td>
                              <td className="px-3 py-2 text-xs space-x-2">
                                {r.databricks_run_url && <a href={r.databricks_run_url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">Job</a>}
                                {r.mlflow_experiment_id && <a href={`${host}/ml/experiments/${r.mlflow_experiment_id}`} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">MLflow</a>}
                                <button onClick={() => api.deleteRun(r.id).then(reload)} className="text-red-500 hover:underline">Delete</button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        })}
        {specs.length === 0 && <div className="text-center py-8 text-gray-400">No training specs yet</div>}
      </div>
    </div>
  );
}

// ════════════════════════════════════════════
//  TEST ENDPOINT
// ════════════════════════════════════════════
function TestEndpoint({ dep, entityColumns }: { dep: Deployment; entityColumns: string[] }) {
  const [samples, setSamples] = useState<Record<string, any>[]>([]);
  const [inputs, setInputs] = useState<Record<string, string>>({});
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    api.getDeploymentSamples(dep.id).then(setSamples).catch(() => setSamples([]));
  }, [dep.id]);

  const selectSample = (sample: Record<string, any>) => {
    const newInputs: Record<string, string> = {};
    for (const col of entityColumns) {
      newInputs[col] = String(sample[col] ?? '');
    }
    setInputs(newInputs);
  };

  const send = async () => {
    setResult(null); setError(''); setLoading(true);
    try {
      const record: Record<string, any> = {};
      for (const [k, v] of Object.entries(inputs)) {
        const num = Number(v);
        record[k] = v !== '' && !isNaN(num) ? num : v;
      }
      const res = await api.testDeploymentEndpoint(dep.id, { dataframe_records: [record] });
      setResult(res);
    } catch (e: any) {
      setError(e.message);
    }
    setLoading(false);
  };

  return (
    <div className="mt-4 border-t pt-3">
      <div className="text-sm font-medium mb-2">Test Endpoint</div>
      {samples.length > 0 && (
        <div className="mb-2">
          <label className="block text-xs text-gray-500 mb-1">Sample records (click to use)</label>
          <div className="flex gap-2 flex-wrap">
            {samples.map((s, i) => (
              <button
                key={i}
                onClick={() => selectSample(s)}
                className="px-2 py-1 bg-gray-100 text-xs font-mono rounded hover:bg-gray-200 border"
              >{entityColumns.map(c => `${c}=${s[c]}`).join(', ')}</button>
            ))}
          </div>
        </div>
      )}
      <div className="flex gap-3 items-end flex-wrap mb-2">
        {entityColumns.map(col => (
          <div key={col}>
            <label className="block text-xs text-gray-500 mb-1">{col}</label>
            <input
              className="px-3 py-1.5 border rounded font-mono text-sm w-40"
              value={inputs[col] || ''}
              onChange={e => setInputs({ ...inputs, [col]: e.target.value })}
              placeholder={col}
            />
          </div>
        ))}
        <button
          onClick={send}
          disabled={loading || entityColumns.length === 0 || entityColumns.some(c => !inputs[c])}
          className="px-4 py-1.5 bg-purple-600 text-white text-sm rounded hover:bg-purple-700 disabled:bg-gray-300"
        >{loading ? 'Sending...' : 'Send'}</button>
      </div>
      {(result || error) && (
        <pre className="px-3 py-2 border rounded font-mono text-xs overflow-auto bg-gray-50 whitespace-pre-wrap max-h-32">
          {error ? <span className="text-red-500">{error}</span> : JSON.stringify(result, null, 2)}
        </pre>
      )}
    </div>
  );
}

// ════════════════════════════════════════════
//  DEPLOYMENT TAB
// ════════════════════════════════════════════
function DeploymentTab({ projectId, eols, trainingSpecs, runs }: {
  projectId: number; eols: EOL[]; trainingSpecs: TrainingSpec[]; runs: Run[];
}) {
  const [onlineTables, setOnlineTables] = useState<OnlineTable[]>([]);
  const [deployments, setDeployments] = useState<Deployment[]>([]);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({ name: '', run_id: '', endpoint_name: '' });
  const [selectedDepId, setSelectedDepId] = useState<number | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const load = useCallback(async () => {
    const [ot, dep] = await Promise.all([
      api.getOnlineTables(projectId),
      api.getDeployments(projectId),
    ]);
    // Refresh status for any synced table missing a pipeline URL
    const refreshed = await Promise.all(ot.map(async (t) => {
      if (t.status !== 'NOT_PUBLISHED' && (!t.pipeline_id || !t.pipeline_id.startsWith('http'))) {
        try { return await api.checkOnlineTableStatus(t.id); } catch { return t; }
      }
      return t;
    }));
    setOnlineTables(refreshed);
    setDeployments(dep);
  }, [projectId]);

  useEffect(() => { load(); }, [load]);

  // Poll for PROVISIONING online tables and CREATING endpoints
  useEffect(() => {
    const provisioningOt = onlineTables.filter(ot => ot.status === 'PROVISIONING');
    const creatingDep = deployments.filter(d => d.endpoint_status === 'CREATING');
    if (provisioningOt.length === 0 && creatingDep.length === 0) {
      if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
      return;
    }
    if (pollRef.current) return;
    pollRef.current = setInterval(async () => {
      let changed = false;
      for (const ot of provisioningOt) {
        try {
          const updated = await api.checkOnlineTableStatus(ot.id);
          if (updated.status !== 'PROVISIONING') changed = true;
        } catch { /* ignore */ }
      }
      for (const d of creatingDep) {
        try {
          const updated = await api.checkDeploymentStatus(d.id);
          if (updated.endpoint_status !== 'CREATING') changed = true;
        } catch { /* ignore */ }
      }
      if (changed) load();
    }, 15000);
    return () => { if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; } };
  }, [onlineTables, deployments, load]);

  // Derive all distinct source tables from all feature entries across all training specs
  const allFeatureTables = (() => {
    const tableMap = new Map<string, { table: string; lookupKey: string[]; timestampKey: string | null; specNames: string[] }>();
    for (const spec of trainingSpecs) {
      for (const entry of spec.entries) {
        if (!entry.table_name) continue;
        const existing = tableMap.get(entry.table_name);
        if (existing) {
          if (!existing.specNames.includes(spec.name)) existing.specNames.push(spec.name);
        } else {
          tableMap.set(entry.table_name, {
            table: entry.table_name,
            lookupKey: entry.lookup_key || [],
            timestampKey: entry.timestamp_lookup_key,
            specNames: [spec.name],
          });
        }
      }
    }
    return Array.from(tableMap.values());
  })();

  // Map source_table → online table record
  const onlineTableMap = new Map(onlineTables.map(ot => [ot.source_table, ot]));

  // For a selected deployment, compute its required tables
  const selectedDep = deployments.find(d => d.id === selectedDepId);
  const requiredTables = (() => {
    if (!selectedDep) return new Set<string>();
    const run = runs.find(r => r.id === selectedDep.run_id);
    if (!run) return new Set<string>();
    const spec = trainingSpecs.find(s => s.id === run.training_spec_id);
    if (!spec) return new Set<string>();
    return new Set(spec.entries.map(e => e.table_name).filter(Boolean) as string[]);
  })();

  const registeredRuns = runs.filter(r => r.model_name && r.model_version);

  const publishTable = async (table: string, lookupKey: string[], timestampKey: string | null) => {
    try {
      await api.publishOnlineTable(projectId, {
        source_table: table,
        primary_key_columns: lookupKey,
        timeseries_key: timestampKey,
        sync_mode: 'triggered',
      });
      load();
    } catch (e: any) { alert(`Publish failed: ${e.message}`); }
  };

  const removeOnlineTable = async (ot: OnlineTable) => {
    if (!confirm(`Remove synced table for ${ot.source_table}?`)) return;
    try {
      await api.deleteOnlineTable(ot.id);
      load();
    } catch (e: any) { alert(`Delete failed: ${e.message}`); }
  };

  const submitDeployment = async () => {
    if (!form.name || !form.run_id || !form.endpoint_name) return;
    await api.createDeployment(projectId, {
      name: form.name,
      run_id: parseInt(form.run_id),
      endpoint_name: form.endpoint_name,
    });
    setForm({ name: '', run_id: '', endpoint_name: '' });
    setShowForm(false);
    load();
  };

  const publishAllRequired = async (depId: number) => {
    try {
      const result = await api.publishDeploymentTables(depId);
      if (result.published > 0) load();
    } catch (e: any) { alert(`Publish failed: ${e.message}`); }
  };

  const createEndpoint = async (depId: number) => {
    try {
      await api.createDeploymentEndpoint(depId);
      load();
    } catch (e: any) { alert(`Endpoint creation failed: ${e.message}`); }
  };

  // Get entity columns for a deployment (EOL entity columns = inference input keys)
  const getEntityColumns = (dep: Deployment): string[] => {
    const run = runs.find(r => r.id === dep.run_id);
    if (!run) return [];
    const spec = trainingSpecs.find(s => s.id === run.training_spec_id);
    if (!spec) return [];
    const eol = eols.find(e => e.id === spec.eol_id);
    if (!eol) return [];
    const cols = eol.entity_columns;
    if (Array.isArray(cols)) return cols;
    if (typeof cols === 'string') return (cols as string).replace(/^\{|\}$/g, '').split(',').filter(Boolean);
    return [];
  };

  // Check if all required tables for a deployment are ONLINE
  const allTablesOnline = (dep: Deployment) => {
    const run = runs.find(r => r.id === dep.run_id);
    if (!run) return false;
    const spec = trainingSpecs.find(s => s.id === run.training_spec_id);
    if (!spec) return true;
    const tables = spec.entries.map(e => e.table_name).filter(Boolean);
    return tables.every(t => onlineTableMap.get(t!)?.status === 'ONLINE');
  };

  const host = runs[0]?.databricks_run_url?.match(/^https?:\/\/[^/]+/)?.[0] || '';

  return (
    <div>
      {/* ── Online Feature Tables ── */}
      <div className="mb-8">
        <h2 className="text-lg font-medium mb-4">Feature Tables (Online Store)</h2>
        {selectedDep && (
          <div className="mb-3 p-2 bg-blue-50 rounded text-sm text-blue-700">
            Highlighting tables required by deployment <span className="font-medium">{selectedDep.name}</span>
            <button className="ml-2 text-blue-500 hover:underline text-xs" onClick={() => setSelectedDepId(null)}>Clear</button>
          </div>
        )}
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <table className="w-full text-left text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 font-medium text-gray-500">Source Table</th>
                <th className="px-4 py-3 font-medium text-gray-500">Sync Status</th>
                <th className="px-4 py-3 font-medium text-gray-500">Datasets</th>
                <th className="px-4 py-3 font-medium text-gray-500">Pipeline</th>
                <th className="px-4 py-3 font-medium text-gray-500"></th>
              </tr>
            </thead>
            <tbody className="divide-y">
              {allFeatureTables.map(ft => {
                const ot = onlineTableMap.get(ft.table);
                const isRequired = selectedDep ? requiredTables.has(ft.table) : false;
                const rowClass = selectedDep ? (isRequired ? 'bg-blue-50' : 'opacity-40') : '';
                return (
                  <tr key={ft.table} className={`hover:bg-gray-50 ${rowClass}`}>
                    <td className="px-4 py-3 font-mono text-xs">{ft.table}</td>
                    <td className="px-4 py-3">
                      {ot ? (
                        <StatusBadge status={ot.status} />
                      ) : (
                        <span className="text-xs text-gray-400">Not synced</span>
                      )}
                    </td>
                    <td className="px-4 py-3 text-xs text-gray-500">{ft.specNames.join(', ')}</td>
                    <td className="px-4 py-3 text-xs">
                      {ot?.pipeline_id?.startsWith('http') ? (
                        <a href={ot.pipeline_id} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">View</a>
                      ) : (
                        <span className="text-gray-400">-</span>
                      )}
                    </td>
                    <td className="px-4 py-3 text-xs space-x-2">
                      {!ot && (
                        <button
                          onClick={() => publishTable(ft.table, ft.lookupKey, ft.timestampKey)}
                          className="px-2 py-1 bg-blue-600 text-white rounded hover:bg-blue-700"
                        >Sync</button>
                      )}
                      {ot && (
                        <button onClick={() => removeOnlineTable(ot)} className="text-red-500 text-xs hover:underline">Remove</button>
                      )}
                    </td>
                  </tr>
                );
              })}
              {allFeatureTables.length === 0 && (
                <tr><td colSpan={5} className="px-4 py-8 text-center text-gray-400">No feature tables yet — create training specs with feature entries first</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* ── Deployments ── */}
      <div>
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-lg font-medium">Deployments</h2>
          <button onClick={() => setShowForm(!showForm)} className="px-3 py-1.5 bg-blue-600 text-white text-sm rounded hover:bg-blue-700">
            {showForm ? 'Cancel' : 'New Deployment'}
          </button>
        </div>

        {showForm && (
          <div className="mb-4 p-4 border rounded-lg bg-white shadow">
            <div className="grid grid-cols-3 gap-3">
              <div>
                <label className="block text-sm font-medium mb-1">Name</label>
                <input className="w-full px-3 py-2 border rounded" value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} placeholder="e.g. production" />
              </div>
              <div>
                <label className="block text-sm font-medium mb-1">Model (registered run)</label>
                <select className="w-full px-3 py-2 border rounded" value={form.run_id} onChange={e => {
                  const runId = e.target.value;
                  const run = registeredRuns.find(r => r.id === parseInt(runId));
                  setForm({
                    ...form,
                    run_id: runId,
                    endpoint_name: form.endpoint_name || (run ? `mlops-${run.model_name?.split('.').pop() || ''}` : ''),
                  });
                }}>
                  <option value="">Select model...</option>
                  {registeredRuns.map(r => (
                    <option key={r.id} value={r.id}>
                      {r.model_name?.split('.').pop()} v{r.model_version} (run #{r.id})
                    </option>
                  ))}
                </select>
                {registeredRuns.length === 0 && <div className="text-xs text-gray-400 mt-1">No registered models — register a model from the Training tab first</div>}
              </div>
              <div>
                <label className="block text-sm font-medium mb-1">Endpoint Name</label>
                <input className="w-full px-3 py-2 border rounded font-mono text-sm" value={form.endpoint_name} onChange={e => setForm({ ...form, endpoint_name: e.target.value })} placeholder="mlops-model-name" />
              </div>
            </div>
            <div className="mt-3 flex gap-2">
              <button onClick={submitDeployment} className="px-4 py-2 bg-green-600 text-white rounded text-sm" disabled={!form.name || !form.run_id || !form.endpoint_name}>Create</button>
              <button onClick={() => setShowForm(false)} className="px-4 py-2 bg-gray-200 rounded text-sm">Cancel</button>
            </div>
          </div>
        )}

        <div className="space-y-4">
          {deployments.map(dep => {
            const run = runs.find(r => r.id === dep.run_id);
            const spec = run ? trainingSpecs.find(s => s.id === run.training_spec_id) : null;
            const isSelected = selectedDepId === dep.id;
            const tablesReady = allTablesOnline(dep);

            return (
              <div key={dep.id} className={`p-4 bg-white rounded-lg shadow ${isSelected ? 'ring-2 ring-blue-400' : ''}`}>
                <div className="flex justify-between items-start">
                  <div>
                    <h3 className="font-medium">
                      {dep.name} <StatusBadge status={dep.endpoint_status} />
                    </h3>
                    <div className="text-sm text-gray-500 mt-1">
                      Model: {run?.model_name ? (
                        <a
                          href={`${host}/explore/data/models/${run.model_name.replace(/\./g, '/')}${run.model_version ? `/version/${run.model_version}` : ''}`}
                          target="_blank" rel="noopener noreferrer"
                          className="text-blue-600 hover:underline font-mono"
                        >{run.model_name.split('.').pop()} v{run.model_version}</a>
                      ) : 'unknown'}
                      {spec && <> | Spec: <span className="font-mono">{spec.name}</span></>}
                    </div>
                    <div className="text-sm text-gray-500">
                      Endpoint: <span className="font-mono">{dep.endpoint_name}</span>
                    </div>
                  </div>
                  <div className="flex gap-2 items-start">
                    <button
                      onClick={() => setSelectedDepId(isSelected ? null : dep.id)}
                      className={`px-2 py-1 text-xs rounded ${isSelected ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}
                    >{isSelected ? 'Hide Tables' : 'Show Tables'}</button>

                    {dep.endpoint_status === 'NOT_CREATED' && !tablesReady && (
                      <button onClick={() => publishAllRequired(dep.id)} className="px-2 py-1 bg-blue-600 text-white text-xs rounded hover:bg-blue-700">
                        Sync All Tables
                      </button>
                    )}
                    {dep.endpoint_status === 'NOT_CREATED' && tablesReady && (
                      <button onClick={() => createEndpoint(dep.id)} className="px-2 py-1 bg-green-600 text-white text-xs rounded hover:bg-green-700">
                        Create Endpoint
                      </button>
                    )}
                    {dep.endpoint_status === 'NOT_CREATED' && !tablesReady && (
                      <span className="text-xs text-yellow-600 py-1">Tables not synced</span>
                    )}
                    {dep.endpoint_status === 'CREATING' && (
                      <button onClick={() => api.checkDeploymentStatus(dep.id).then(load)} className="px-2 py-1 bg-gray-100 text-gray-600 text-xs rounded hover:bg-gray-200">
                        Check Status
                      </button>
                    )}
                    {(dep.endpoint_status === 'READY' || dep.endpoint_status === 'CREATING') && registeredRuns.length > 0 && (
                      <select
                        className="px-2 py-1 border rounded text-xs"
                        value=""
                        onChange={e => {
                          const newRunId = parseInt(e.target.value);
                          if (newRunId && newRunId !== dep.run_id) {
                            api.updateDeploymentEndpoint(dep.id, newRunId).then(load).catch(err => alert(err.message));
                          }
                        }}
                      >
                        <option value="">Update model...</option>
                        {registeredRuns.filter(r => r.id !== dep.run_id).map(r => (
                          <option key={r.id} value={r.id}>
                            {r.model_name?.split('.').pop()} v{r.model_version} (run #{r.id})
                          </option>
                        ))}
                      </select>
                    )}
                    {dep.endpoint_status !== 'NOT_CREATED' && host && (
                      <a href={`${host}/ml/endpoints/${dep.endpoint_name}`} target="_blank" rel="noopener noreferrer" className="text-blue-600 text-xs hover:underline">Endpoint UI</a>
                    )}
                    <button onClick={() => { api.deleteDeployment(dep.id).then(load); }} className="text-red-500 text-sm">Delete</button>
                  </div>
                </div>

                {/* Test interface for READY endpoints */}
                {dep.endpoint_status === 'READY' && <TestEndpoint dep={dep} entityColumns={getEntityColumns(dep)} />}
              </div>
            );
          })}
          {deployments.length === 0 && (
            <div className="text-center py-8 text-gray-400">No deployments yet</div>
          )}
        </div>
      </div>
    </div>
  );
}

// ════════════════════════════════════════════
//  APP ROOT
// ════════════════════════════════════════════
function App() {
  const [page, navigate] = useHashRoute();

  return (
    <div className="min-h-screen bg-gray-100">
      <header className="bg-white border-b px-6 py-3 flex items-center gap-3">
        <h1 className="text-lg font-bold cursor-pointer" onClick={() => navigate({ view: 'projects' })}>MLOps Manager</h1>
      </header>
      <main className="max-w-7xl mx-auto p-6">
        {page.view === 'projects' && <ProjectsList navigate={navigate} />}
        {page.view === 'project' && <ProjectDetail projectId={page.id} tab={page.tab} navigate={navigate} />}
      </main>
    </div>
  );
}

// Keep legacy tab components compiled (replaced by TrainingSpecTab but not yet deleted).
void FeaturesTab;
void DatasetsTab;
void RunsTab;

export default App;
