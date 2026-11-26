import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import type { Project, EOL, FeatureEntry, Run, Deployment, TrainingSpec, DeploymentFeature, FeatureSchedule, BatchDeployment, ModelAliases } from './types';
import * as api from './api';

// ── Simple hash-based routing ──
type Page =
  | { view: 'projects' }
  | { view: 'project'; id: number; tab: string };

function useHashRoute(): [Page, (p: Page) => void] {
  const parse = (): Page => {
    const hash = window.location.hash.slice(1);
    const m = hash.match(/^\/projects\/(\d+)(?:\/([\w-]+))?/);
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
    ACTIVE: 'bg-green-100 text-green-800',
    PAUSED: 'bg-amber-100 text-amber-800',
    NOT_MATERIALIZED: 'bg-gray-200 text-gray-700',
    SERVING: 'bg-indigo-100 text-indigo-800',
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
  const [form, setForm] = useState({ name: '', description: '', catalog: '', schema: '', model_name: '' });

  const load = useCallback(() => { api.getProjects().then(setProjects); }, []);
  useEffect(() => { load(); }, [load]);

  const submit = async () => {
    await api.createProject(form as Omit<Project, 'id'>);
    setForm({ name: '', description: '', catalog: '', schema: '', model_name: '' });
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
            <div><label className="block text-sm font-medium text-gray-700 mb-1">catalog</label><input className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500 focus:outline-none" value={form.catalog} onChange={(e) => setForm({ ...form, catalog: e.target.value })} placeholder="catalog name" /></div>
            <div><label className="block text-sm font-medium text-gray-700 mb-1">schema</label><input className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500 focus:outline-none" value={form.schema} onChange={(e) => setForm({ ...form, schema: e.target.value })} /></div>
            <div className="col-span-2"><label className="block text-sm font-medium text-gray-700 mb-1">model name <span className="text-xs text-gray-400">(for UC model registry — defaults to project name)</span></label><input className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500 focus:outline-none" value={form.model_name} onChange={(e) => setForm({ ...form, model_name: e.target.value })} placeholder={(form.name || 'project name').trim().toLowerCase().replace(/[^a-z0-9_-]+/g, '_').replace(/^_+|_+$/g, '')} /></div>
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
  const [trainingSpecs, setTrainingSpecs] = useState<(TrainingSpec & { run_count: number; locked?: boolean })[]>([]);
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

  const tabs = ['overview', 'eols', 'training', 'deployment', 'batch'];
  const tabLabels: Record<string, string> = { deployment: 'Online Deployment', batch: 'Batch Deployment' };

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
          >{tabLabels[t] || t}</button>
        ))}
      </div>

      {tab === 'overview' && <OverviewTab project={project} eols={eols} trainingSpecs={trainingSpecs} specRuns={specRuns} />}
      {tab === 'eols' && <EOLsTab projectId={projectId} eols={eols} reload={load} />}
      {tab === 'training' && <TrainingSpecTab projectId={projectId} eols={eols} specs={trainingSpecs} runs={specRuns} reload={load} />}
      {tab === 'deployment' && <DeploymentTab projectId={projectId} eols={eols} trainingSpecs={trainingSpecs} runs={specRuns} />}
      {tab === 'batch' && <BatchTab projectId={projectId} project={project} runs={specRuns} />}
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
          <div><dt className="text-gray-500">Model Name</dt><dd className="font-mono">{(project.model_name || project.name).trim().toLowerCase().replace(/[^a-z0-9_-]+/g, '_').replace(/^_+|_+$/g, '')}</dd></div>
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
  // Columns the SQL produces; null until detected. The SQL comes first — label, timestamp and entity
  // columns are picked from its output (auto-suggested), and every other column is a request field.
  const [cols, setCols] = useState<{ name: string; type: string }[] | null>(null);
  const [detectedSql, setDetectedSql] = useState('');
  const [detecting, setDetecting] = useState(false);
  const [detectError, setDetectError] = useState('');

  const entityList = form.entity_columns.split(',').map((s) => s.trim()).filter(Boolean);

  const detect = async (sql: string, applySuggestions: boolean) => {
    if (!sql.trim()) return;
    setDetecting(true); setDetectError('');
    try {
      const r = await api.detectEolColumns(sql);
      setCols(r.columns); setDetectedSql(sql);
      const names = new Set(r.columns.map((c) => c.name));
      setForm((f) => {
        const keep = (v: string) => (v && names.has(v) ? v : '');
        const keptEntities = f.entity_columns.split(',').map((s) => s.trim()).filter((c) => names.has(c));
        return applySuggestions ? {
          ...f,
          label_column: r.suggested.label_column || keep(f.label_column),
          timestamp_column: r.suggested.timestamp_column || keep(f.timestamp_column),
          entity_columns: (r.suggested.entity_columns.length ? r.suggested.entity_columns : keptEntities).join(', '),
        } : { ...f, label_column: keep(f.label_column), timestamp_column: keep(f.timestamp_column), entity_columns: keptEntities.join(', ') };
      });
    } catch (e: any) { setDetectError(e.message); setCols(null); }
    setDetecting(false);
  };

  // Copying an EOL: load its columns but keep its choices.
  useEffect(() => { if (initial?.sql_definition) detect(initial.sql_definition, false); }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const runPreview = async (sql: string) => {
    setPreviewing(true); setPreviewError(''); setPreview(null);
    try { setPreview(await api.previewSql(sql)); } catch (e: any) { setPreviewError(e.message); }
    setPreviewing(false);
  };

  const stale = cols !== null && detectedSql !== form.sql_definition;
  const toggleEntity = (c: string) => setForm((f) => {
    const cur = f.entity_columns.split(',').map((s) => s.trim()).filter(Boolean);
    return { ...f, entity_columns: (cur.includes(c) ? cur.filter((x) => x !== c) : [...cur, c]).join(', ') };
  });
  const requestFields = (cols || []).filter((c) => c.name !== form.label_column && c.name !== form.timestamp_column && !entityList.includes(c.name));
  const missing = !form.name.trim() ? 'a name' : !cols || stale ? 'detected columns (click Detect columns)'
    : !form.label_column ? 'a label column' : !form.timestamp_column ? 'a timestamp column' : entityList.length === 0 ? 'at least one entity column' : '';

  const submit = async () => {
    try { await api.createEOL(projectId, { ...form, entity_columns: entityList } as any); }
    catch (e: any) { setDetectError(e.message); return; }
    onSaved();
  };

  const colLabel = (c: { name: string; type: string }) => `${c.name} (${c.type})`;

  return (
    <div className="mb-4 p-4 border rounded-lg bg-white shadow">
      <div className="grid grid-cols-2 gap-3">
        <div className="col-span-2"><label className="block text-sm font-medium mb-1">Name</label><input className="w-full px-3 py-2 border rounded" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} /></div>
        <div className="col-span-2">
          <label className="block text-sm font-medium mb-1">SQL Definition</label>
          <textarea className="w-full px-3 py-2 border rounded font-mono text-sm h-32" value={form.sql_definition}
            onChange={(e) => setForm({ ...form, sql_definition: e.target.value })}
            onBlur={() => { if (form.sql_definition.trim() && form.sql_definition !== detectedSql) detect(form.sql_definition, true); }}
            placeholder="SELECT entity keys, observation timestamp, label, and any request fields FROM ..." />
          <div className="mt-1 flex gap-2">
            <button onClick={() => detect(form.sql_definition, true)} disabled={detecting || !form.sql_definition.trim()}
              className="px-3 py-1 bg-blue-600 text-white text-xs rounded hover:bg-blue-700 disabled:bg-gray-300"
            >{detecting ? 'Detecting...' : 'Detect columns'}</button>
            <button onClick={() => runPreview(form.sql_definition)} disabled={previewing || !form.sql_definition}
              className="px-3 py-1 bg-gray-600 text-white text-xs rounded hover:bg-gray-700 disabled:bg-gray-300"
            >{previewing ? 'Running...' : 'Preview SQL'}</button>
            {stale && <span className="text-xs text-amber-600 self-center">SQL changed — detect columns again</span>}
          </div>
        </div>
        {detectError && <div className="col-span-2 text-sm text-red-500 bg-red-50 p-2 rounded">{detectError}</div>}
        {cols && (
          <>
            <div>
              <label className="block text-sm font-medium mb-1">Label Column</label>
              <select className="w-full px-3 py-2 border rounded text-sm" value={form.label_column} onChange={(e) => setForm({ ...form, label_column: e.target.value })}>
                <option value="">Select...</option>
                {cols.map((c) => <option key={c.name} value={c.name}>{colLabel(c)}</option>)}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Timestamp Column</label>
              <select className="w-full px-3 py-2 border rounded text-sm" value={form.timestamp_column} onChange={(e) => setForm({ ...form, timestamp_column: e.target.value })}>
                <option value="">Select...</option>
                {cols.filter((c) => c.name !== form.label_column).map((c) => <option key={c.name} value={c.name}>{colLabel(c)}</option>)}
              </select>
            </div>
            <div className="col-span-2">
              <label className="block text-sm font-medium mb-1">Entity Columns <span className="text-gray-400 font-normal">(lookup / group-by keys)</span></label>
              <div className="flex flex-wrap gap-x-4 gap-y-1 border rounded p-2">
                {cols.filter((c) => c.name !== form.label_column && c.name !== form.timestamp_column).map((c) => (
                  <label key={c.name} className="flex items-center gap-1.5 text-sm cursor-pointer">
                    <input type="checkbox" checked={entityList.includes(c.name)} onChange={() => toggleEntity(c.name)} />
                    <span className="font-mono text-xs">{c.name}</span>
                  </label>
                ))}
              </div>
            </div>
            <div className="col-span-2 text-xs text-gray-600 bg-gray-50 p-2 rounded">
              <span className="font-medium">Request fields</span> (sent by the requesting system at scoring time; model features unless excluded in the training spec):{' '}
              {requestFields.length ? <span className="font-mono">{requestFields.map((c) => c.name).join(', ')}</span> : <span className="text-gray-400">none</span>}
            </div>
          </>
        )}
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
        <button onClick={submit} disabled={!!missing} className="px-4 py-2 bg-green-600 text-white rounded text-sm disabled:bg-gray-300">Save</button>
        <button onClick={onCancel} className="px-4 py-2 bg-gray-200 rounded text-sm">Cancel</button>
        {missing && <span className="text-xs text-gray-500 self-center">Needs {missing}</span>}
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
  spec: TrainingSpec & { run_count: number; locked?: boolean };
  onChanged: () => void;
  locked: boolean;
}) {
  type Output = { name: string; source: string; sourceType: 'lookup' | 'on_demand' | 'declarative' };
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
    } else if (e.feature_type === 'declarative' && e.declarative_spec) {
      const ds = e.declarative_spec as any;
      if (ds.name) outputs.push({ name: ds.name, source: `${ds.function}(${ds.input})`, sourceType: 'declarative' });
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
                    : o.sourceType === 'on_demand'
                      ? <span className="text-xs bg-blue-100 text-blue-700 px-1.5 py-0.5 rounded">On-Demand</span>
                      : <span className="text-xs bg-purple-100 text-purple-700 px-1.5 py-0.5 rounded">Declarative</span>}
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
    return (e.declarative_spec as any)?.name || 'declarative';
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

  // EOL entity/timestamp defaults — a declarative aggregation groups by the EOL entity
  // key(s) and needs a timeseries column; prefill from the EOL but let the user override.
  const initEol = eols.find((e) => e.id === eolId);
  const initRawCols: any = initEol?.entity_columns;
  const initEolColumns: string[] = Array.isArray(initRawCols)
    ? initRawCols
    : typeof initRawCols === 'string'
      ? initRawCols.replace(/^\{|\}$/g, '').split(',').filter(Boolean)
      : [];
  const dspec: any = entry?.declarative_spec || {};

  const [form, setForm] = useState({
    catalog: initCatalog, schema: initSchema, table: initTable,
    selectedFeatures: (entry?.feature_names || []) as string[],
    selectedLookupKeys: (featureType === 'declarative'
      ? (dspec.lookup_key || initEolColumns)
      : (entry?.lookup_key || [])) as string[],
    timestamp_lookup_key: (featureType === 'declarative'
      ? (dspec.timestamp_lookup_key || initEol?.timestamp_column || '')
      : (entry?.timestamp_lookup_key || '')),
    default_values: entry?.default_values ? JSON.stringify(entry.default_values) : '',
    // On-demand fields
    selectedFunction: initFunction,
    outputName: entry?.output_name || '',
    bindings: (entry?.input_bindings || {}) as Record<string, string>,
    // Declarative fields
    declarative_name: dspec.name || '',
    declarative_input: (entry?.declarative_spec as any)?.input || '',
    declarative_function: (entry?.declarative_spec as any)?.function || '',
    declarative_window_type: (entry?.declarative_spec as any)?.time_window?.type || '',
    declarative_window_duration: (entry?.declarative_spec as any)?.time_window?.window_duration || '30d',
    declarative_slide_duration: (entry?.declarative_spec as any)?.time_window?.slide_duration || '',
    declarative_filter: (entry?.declarative_spec as any)?.filter_condition || '',
  });

  // Auto-suggest a feature name from function/input/window until the user edits it.
  const [nameTouched, setNameTouched] = useState<boolean>(!!dspec.name);
  useEffect(() => {
    if (featureType !== 'declarative' || nameTouched) return;
    // Include the window type: UC feature names are shared across specs, so sum_amount_30d (tumbling)
    // and its rolling variant must not collide.
    const windowKind = ({ continuous: 'rolling', tumbling: 'tumbling', sliding: 'sliding' } as Record<string, string>)[form.declarative_window_type] || '';
    const suggested = `${form.declarative_function || ''}_${form.declarative_input || ''}_${form.declarative_window_duration || ''}_${windowKind}`
      .toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
    setForm(f => (f.declarative_name === suggested ? f : { ...f, declarative_name: suggested }));
  }, [form.declarative_function, form.declarative_input, form.declarative_window_duration, form.declarative_window_type, nameTouched, featureType]);

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
      if (form.selectedLookupKeys.length === 0) { alert('Select at least one entity (group-by) key'); return; }
      if (!form.timestamp_lookup_key) { alert('Select a timestamp column — aggregations require a timeseries column'); return; }
      if (form.declarative_window_type === 'sliding' && !form.declarative_slide_duration.trim()) { alert('Enter a slide duration for the sliding window (e.g. 1d)'); return; }
      const safeName = form.declarative_name.trim().toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
      if (!safeName) { alert('Enter a feature name'); return; }
      const windowSpec: Record<string, any> = { type: form.declarative_window_type, window_duration: form.declarative_window_duration };
      if (form.declarative_window_type === 'sliding') windowSpec.slide_duration = form.declarative_slide_duration.trim();
      onSave({
        feature_type: 'declarative',
        table_name: `${fullName}.${form.table}`,
        declarative_spec: {
          name: safeName,
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
          <div className="col-span-2">
            <label className="block text-xs font-medium mb-1">Feature Name <span className="text-gray-400">(UC feature + training-table column)</span></label>
            <input className="w-full px-2 py-1.5 border rounded font-mono text-sm" value={form.declarative_name}
              onChange={e => { setForm({ ...form, declarative_name: e.target.value }); setNameTouched(true); }}
              placeholder="e.g. sum_amount_30d" disabled={dis} />
          </div>
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
              <option value="continuous">Rolling (continuous)</option>
              <option value="tumbling">Tumbling</option>
              <option value="sliding">Sliding</option>
            </select>
            {/* The online store only materializes Last over a rolling window from a batch (Delta) source. */}
            {form.declarative_window_type === 'continuous' && form.declarative_function && form.declarative_function !== 'last' && (
              <p className="text-xs text-amber-600 mt-1">Training only — a rolling {form.declarative_function} can't be served online. Use Sliding (e.g. 1d slide) to deploy.</p>
            )}
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
          <div>
            <label className="block text-xs font-medium mb-1">Entity / group-by key(s) <span className="text-gray-400">(EOL entity columns)</span></label>
            <div className="border rounded p-2 space-y-1">
              {eolColumns.length === 0 && <div className="text-xs text-gray-400">No EOL entity columns</div>}
              {eolColumns.map(col => (
                <label key={col} className="flex items-center gap-1.5 text-sm cursor-pointer hover:bg-gray-50 px-1 rounded">
                  <input type="checkbox" checked={form.selectedLookupKeys.includes(col)} onChange={() => toggleLookupKey(col)} disabled={dis} />
                  <span className="font-mono text-xs">{col}</span>
                </label>
              ))}
            </div>
          </div>
          <div>
            <label className="block text-xs font-medium mb-1">Timestamp Column <span className="text-gray-400">(required)</span></label>
            <select className="w-full px-2 py-1.5 border rounded text-sm" value={form.timestamp_lookup_key} onChange={e => setForm({ ...form, timestamp_lookup_key: e.target.value })} disabled={dis || columns.length === 0}>
              <option value="">Select...</option>
              {columns.map(col => <option key={col.name} value={col.name}>{col.name} ({col.type})</option>)}
              {form.timestamp_lookup_key && !columns.some(c => c.name === form.timestamp_lookup_key) && (
                <option value={form.timestamp_lookup_key}>{form.timestamp_lookup_key}</option>
              )}
            </select>
          </div>
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

// ════════════════════════════════════════════
//  TRAINING SPEC TAB
// ════════════════════════════════════════════
function TrainingSpecTab({ projectId, eols, specs, runs, reload }: {
  projectId: number; eols: EOL[]; specs: (TrainingSpec & { run_count: number; locked?: boolean })[]; runs: Run[]; reload: () => void;
}) {
  const [showForm, setShowForm] = useState(false);
  const [copyingFromId, setCopyingFromId] = useState<number | null>(null);
  const emptyForm = { name: '', eol_id: '', task_type: 'classification', split_strategy: 'none', split_method: 'random', eval_pct: '20', seed: '42', parameters: '' };
  const [form, setForm] = useState(emptyForm);
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [eolExpandedFor, setEolExpandedFor] = useState<number | null>(null);
  const [registerError, setRegisterError] = useState('');
  const [registeringId, setRegisteringId] = useState<number | null>(null);
  const [deletingRunId, setDeletingRunId] = useState<number | null>(null);
  const [renamingId, setRenamingId] = useState<number | null>(null);
  const [renameValue, setRenameValue] = useState('');
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const parseEntityCols = (cols: any): string => {
    if (Array.isArray(cols)) return cols.join(', ');
    if (typeof cols === 'string') return cols.replace(/^\{|\}$/g, '');
    return '';
  };

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
    // Parse inside a guard: a bad JSON string used to throw here, outside the try, so the
    // click silently did nothing.
    let parameters: any = null;
    if (form.parameters.trim()) {
      try { parameters = JSON.parse(form.parameters); }
      catch (e: any) { alert(`Parameters must be valid JSON (e.g. {"iterations": 100}): ${e.message}`); return; }
    }
    const splitConfig = form.split_strategy !== 'none' ? { eval_pct: parseFloat(form.eval_pct), seed: parseInt(form.seed) } : null;
    const payload = {
      name: form.name,
      eol_id: form.eol_id ? parseInt(form.eol_id) : null,
      task_type: form.task_type as any,
      split_strategy: form.split_strategy as any,
      split_method: form.split_strategy !== 'none' ? form.split_method as any : null,
      split_config: splitConfig,
      parameters,
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

  const startCopy = (spec: TrainingSpec & { run_count: number; locked?: boolean }) => {
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
    setRegisteringId(runId);
    try {
      await api.registerModel(runId);
      await reload();
    } catch (e: any) { setRegisterError(e.message); }
    finally { setRegisteringId(null); }
  };

  const assetLabel: Record<api.RunAsset['kind'], string> = {
    alias: 'Model alias', model_version: 'Model version', mlflow_run: 'MLflow run', table: 'Table', job: 'Training job',
  };
  const describeAssets = (assets: api.RunAsset[], kept: string[]) =>
    (assets.length ? assets.map(a => `• ${assetLabel[a.kind]}: ${a.name}`).join('\n') : '• (no Databricks assets — app records only)')
    + (kept.length ? `\n\nKept (shared with other runs):\n${kept.map(k => `• ${k}`).join('\n')}` : '');

  // Delete a training spec, all of its runs, and everything those runs produced.
  const [deletingSpecId, setDeletingSpecId] = useState<number | null>(null);
  const deleteSpec = async (spec: TrainingSpec) => {
    setDeletingSpecId(spec.id);
    try {
      const { runs: runIds, assets, blockers, kept, features } = await api.getTrainingSpecDeletePlan(spec.id);
      if (blockers.length) { alert(`Can't delete spec "${spec.name}":\n\n• ${blockers.join('\n• ')}`); return; }
      const runsLine = runIds.length ? `${runIds.length} run${runIds.length === 1 ? '' : 's'} (${runIds.join(', ')}) and their assets:` : 'No runs.';
      if (!confirm(`Delete training spec "${spec.name}"?\n\n${runsLine}\n${runIds.length ? describeAssets(assets, kept) : ''}${features.delete.length ? `\n\nUC features (used by nothing else):\n${features.delete.map(f => `• ${f}`).join('\n')}` : ''}${features.keep.length ? `\n\nUC features kept:\n${features.keep.map(f => `• ${f}`).join('\n')}` : ''}\n\nThe EOL is shared and kept. This can't be undone (MLflow runs are soft-deleted for 30 days).`)) return;
      await api.deleteTrainingSpec(spec.id);
    } catch (e: any) { alert(`Delete failed: ${e.message}`); }
    finally { setDeletingSpecId(null); await reload(); }
  };

  // Delete a run and everything it owns. Blocked (server-side too) while it's deployed or training.
  const deleteRun = async (runId: number) => {
    setDeletingRunId(runId);
    try {
      const { assets, blockers, kept } = await api.getRunDeletePlan(runId);
      if (blockers.length) { alert(`Can't delete run ${runId}:\n\n• ${blockers.join('\n• ')}`); return; }
      if (!confirm(`Delete run ${runId} and all of its assets?\n\n${describeAssets(assets, kept)}\n\nThis can't be undone (MLflow runs are soft-deleted for 30 days).`)) return;
      await api.deleteRun(runId);
    } catch (e: any) {
      // A proxy error (e.g. 502 during a session refresh) can come back while the server is still deleting —
      // give it a moment, then reload so the list reflects what actually happened.
      alert(`Delete may not have completed: ${e.message}\n\nThe list will refresh in a few seconds.`);
      setTimeout(() => { reload(); }, 4000);
    }
    finally { setDeletingRunId(null); await reload(); }
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
                <option value="train_eval_test" disabled>Train / Eval / Test (coming soon)</option>
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
          <div className="mt-3 flex items-center gap-3">
            <button onClick={submitSpec} disabled={!form.name.trim() || !form.eol_id}
              className="px-4 py-2 bg-green-600 text-white rounded text-sm disabled:opacity-40 disabled:cursor-not-allowed">
              {copyingFromId != null ? 'Save Copy' : 'Create'}
            </button>
            {(!form.name.trim() || !form.eol_id) && (
              <span className="text-xs text-gray-500">
                Required: {[!form.name.trim() && 'Name', !form.eol_id && 'EOL'].filter(Boolean).join(' and ')}
              </span>
            )}
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
          const isLocked = spec.locked ?? spec.run_count > 0;
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
                  <button onClick={() => deleteSpec(spec)} disabled={deletingSpecId === spec.id} className="text-red-500 text-sm disabled:text-gray-400">{deletingSpecId === spec.id ? 'Deleting…' : 'Delete'}</button>
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

                  {/* EOL detail — expandable */}
                  {eol && (
                    <div className="mb-3 border rounded">
                      <div
                        className="px-3 py-2 cursor-pointer flex items-center bg-gray-50 hover:bg-gray-100 text-sm"
                        onClick={() => setEolExpandedFor(eolExpandedFor === spec.id ? null : spec.id)}
                      >
                        <span className="text-gray-400 mr-1">{eolExpandedFor === spec.id ? '▾' : '▸'}</span>
                        <span className="font-medium">EOL:</span>&nbsp;<span className="font-mono">{eol.name}</span>
                        <span className="text-gray-500 ml-3">
                          Label: <span className="font-mono">{eol.label_column}</span>
                          <span className="mx-2">|</span>Entity: <span className="font-mono">{parseEntityCols(eol.entity_columns)}</span>
                          {eol.timestamp_column && <><span className="mx-2">|</span>Timestamp: <span className="font-mono">{eol.timestamp_column}</span></>}
                        </span>
                      </div>
                      {eolExpandedFor === spec.id && (
                        <div className="px-3 py-3 border-t">
                          <div className="text-xs font-medium text-gray-500 mb-1">SQL Definition</div>
                          <pre className="p-2 bg-gray-50 rounded text-xs font-mono overflow-x-auto whitespace-pre-wrap">{eol.sql_definition}</pre>
                        </div>
                      )}
                    </div>
                  )}

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
                                {r.model_name && r.model_version ? (
                                  <a href={`${host}/explore/data/models/${r.model_name.replace(/\./g, '/')}/version/${r.model_version}`} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">
                                    {r.model_name.split('.').pop()} v{r.model_version}
                                  </a>
                                ) : r.status === 'SUCCESS' ? (
                                  <button
                                    className="px-2 py-1 bg-purple-600 text-white text-xs rounded hover:bg-purple-700 disabled:bg-gray-300 disabled:cursor-not-allowed"
                                    disabled={registeringId === r.id}
                                    onClick={() => registerModel(r.id)}
                                  >{registeringId === r.id ? 'Registering…' : 'Register'}</button>
                                ) : '-'}
                              </td>
                              <td className="px-3 py-2 font-mono text-xs">{formatMetrics(r.eval_metrics)}</td>
                              <td className="px-3 py-2 text-xs space-x-2">
                                {r.databricks_run_url && <a href={r.databricks_run_url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">Job</a>}
                                {r.mlflow_experiment_id && <a href={r.mlflow_run_id ? `${host}/ml/experiments/${r.mlflow_experiment_id}/runs/${r.mlflow_run_id}` : `${host}/ml/experiments/${r.mlflow_experiment_id}`} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">MLflow</a>}
                                {/* Per-run persisted data: one table for CV, train + eval (+ test) for split runs */}
                                {([
                                  [r.training_table, r.eval_table || r.test_table ? 'Train' : 'Training set'],
                                  [r.eval_table, 'Eval'],
                                  [r.test_table, 'Test'],
                                ] as [string | null, string][]).filter(([t]) => t).map(([t, label]) => (
                                  <a key={label} href={`${host}/explore/data/${t!.split('.').join('/')}`} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline" title={t!}>{label}</a>
                                ))}
                                <button onClick={() => deleteRun(r.id)} disabled={deletingRunId === r.id} className="text-red-500 hover:underline disabled:text-gray-400">{deletingRunId === r.id ? 'Deleting…' : 'Delete'}</button>
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
  const [schema, setSchema] = useState<{ required: string[]; optional: string[]; types: Record<string, string> } | null>(null);
  const [inputs, setInputs] = useState<Record<string, string>>({});
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    api.getDeploymentSamples(dep.id).then(setSamples).catch(() => setSamples([]));
    api.getDeploymentSchema(dep.id).then(setSchema).catch(() => setSchema(null));
  }, [dep.id]);

  // Every property in the endpoint's request schema is a CALLER-supplied input (the spine:
  // entity/lookup keys + request-time pass-through columns like amount, pos_entry_mode,
  // security_code, transaction_ts). FeatureLookup / aggregation / on-demand features are
  // resolved server-side from the online store and do NOT appear in the request schema at
  // all — so there is nothing in `optional` that comes "from the online store". `required`
  // must be filled by the caller; `optional` columns are also caller-supplied but may be
  // omitted. Fall back to sample / entity shape only if the schema endpoint is unavailable.
  const requiredCols = schema?.required ?? [];
  const optionalCols = schema?.optional ?? [];
  const inputCols = (requiredCols.length || optionalCols.length)
    ? [...requiredCols, ...optionalCols]
    : (samples.length > 0 ? Object.keys(samples[0]) : entityColumns);
  // When the schema is unavailable, require everything we render; otherwise only `required`.
  const mustFill = requiredCols.length ? requiredCols : inputCols;

  const selectSample = (sample: Record<string, any>) => {
    const newInputs: Record<string, string> = {};
    for (const col of inputCols) newInputs[col] = String(sample[col] ?? '');
    setInputs(newInputs);
  };

  const send = async () => {
    setResult(null); setError(''); setLoading(true);
    try {
      const record: Record<string, any> = {};
      for (const col of inputCols) {
        const v = inputs[col] ?? '';
        // Omit empty optional inputs (don't send "" for a pass-through the caller left blank).
        if (!mustFill.includes(col) && v === '') continue;
        // Coerce by the declared schema type; leave as string when type unknown.
        const t = schema?.types?.[col];
        record[col] = (t === 'number' || t === 'integer') && v !== '' && !isNaN(Number(v)) ? Number(v) : v;
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
      <div className="text-sm font-medium mb-1">Test Endpoint</div>
      <p className="text-xs text-gray-500 mb-2">
        Enter the request-time inputs the calling system provides. Looked-up features are
        resolved automatically from the online store.
      </p>
      {samples.length > 0 && (
        <div className="mb-2">
          <label className="block text-xs text-gray-500 mb-1">Sample records (click to fill)</label>
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
        {inputCols.map(col => (
          <div key={col}>
            <label className="block text-xs text-gray-500 mb-1">
              {col}{schema?.types?.[col] ? <span className="text-gray-400"> ({schema.types[col]})</span> : null}
              {!mustFill.includes(col) ? <span className="text-gray-400"> · optional</span> : null}
            </label>
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
          disabled={loading || inputCols.length === 0 || mustFill.some(c => !inputs[c])}
          className="px-4 py-1.5 bg-purple-600 text-white text-sm rounded hover:bg-purple-700 disabled:bg-gray-300"
        >{loading ? 'Sending...' : 'Send'}</button>
      </div>
      <p className="text-xs text-gray-400 mb-2">
        All fields above are supplied by the calling system. FeatureLookup and aggregation
        features are resolved automatically from the online store and are not request inputs.
      </p>
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
const FEATURE_TYPE_LABEL: Record<string, string> = {
  column_selection: 'lookup',
  aggregation: 'aggregation',
  custom_udf: 'on-demand',
};

const DOW: Record<string, string> = { SUN: 'Sun', MON: 'Mon', TUE: 'Tue', WED: 'Wed', THU: 'Thu', FRI: 'Fri', SAT: 'Sat',
  '1': 'Sun', '2': 'Mon', '3': 'Tue', '4': 'Wed', '5': 'Thu', '6': 'Fri', '7': 'Sat' };
const pad2 = (n: string | number) => String(n).padStart(2, '0');
const isNum = (x: string) => /^\d+$/.test(x);

// Plain-English Quartz cron ("sec min hour dom month dow [year]"); falls back to the raw expression.
function describeCron(expr: string, tz: string): string {
  const [sec, min, hour, dom, month, dow] = (expr || '').trim().split(/\s+/);
  const anyDom = dom === '*' || dom === '?';
  const anyDow = dow === '*' || dow === '?';
  if (sec === '0' && month === '*') {
    if (anyDom && anyDow) {
      if (isNum(min) && isNum(hour)) return `daily at ${pad2(hour)}:${pad2(min)} ${tz}`;
      if (isNum(min) && hour === '*') return `hourly at :${pad2(min)}`;
      if (isNum(min) && /^(0|\*)\/\d+$/.test(hour)) return `every ${hour.split('/')[1]}h at :${pad2(min)}`;
      if (/^(0|\*)\/\d+$/.test(min) && hour === '*') return `every ${min.split('/')[1]} min`;
    }
    if (dom === '?' && dow && !anyDow && isNum(min) && isNum(hour)) {
      const days = dow.split(',').map(d => DOW[d.toUpperCase()] || d).join(', ');
      return `weekly on ${days} at ${pad2(hour)}:${pad2(min)} ${tz}`;
    }
  }
  return `cron ${expr} (${tz})`;
}

// Next fire time for the common UTC daily/hourly shapes (null when we can't tell cheaply).
function nextCronRun(expr: string, tz: string): Date | null {
  if (tz !== 'UTC') return null;
  const [sec, min, hour, dom, month, dow] = (expr || '').trim().split(/\s+/);
  if (sec !== '0' || month !== '*' || !(dom === '*' || dom === '?') || !(dow === '*' || dow === '?') || !isNum(min)) return null;
  const now = new Date();
  const t = new Date(now);
  t.setUTCSeconds(0, 0);
  t.setUTCMinutes(Number(min));
  if (isNum(hour)) {
    t.setUTCHours(Number(hour));
    if (t <= now) t.setUTCDate(t.getUTCDate() + 1);
    return t;
  }
  if (hour === '*') {
    if (t <= now) t.setUTCHours(t.getUTCHours() + 1);
    return t;
  }
  return null;
}

function timeUntil(d: Date): string {
  const mins = Math.round((d.getTime() - Date.now()) / 60000);
  if (mins < 60) return `in ${Math.max(mins, 0)}m`;
  return `in ${Math.round(mins / 60)}h`;
}

function describeSchedule(s: FeatureSchedule | null | undefined): string {
  if (!s) return 'unknown';
  if (s.kind === 'on_request') return 'computed per request';
  if (s.kind === 'table_trigger') return 'when the source table changes';
  const auto = s.mode === 'DERIVED' ? ' (auto)' : '';
  return `${describeCron(s.cron_expression, s.timezone_id)}${auto}`;
}

function timeAgo(iso: string | null): string {
  if (!iso) return '—';
  const mins = Math.round((Date.now() - new Date(iso).getTime()) / 60000);
  if (mins < 60) return `${Math.max(mins, 0)}m ago`;
  if (mins < 60 * 48) return `${Math.round(mins / 60)}h ago`;
  return `${Math.round(mins / 1440)}d ago`;
}

// The features an online deployment serves from. Read-only: Deploy / Update model / Delete own the
// lifecycle (features go online on deploy, and are torn down when no deployment uses them).
function DeploymentFeatures({ dep }: { dep: Deployment }) {
  const [features, setFeatures] = useState<DeploymentFeature[]>([]);
  const [open, setOpen] = useState(false);
  const [refreshing, setRefreshing] = useState(false);

  const load = useCallback(async () => {
    try { setFeatures(await api.getDeploymentFeatures(dep.id)); } catch { /* ignore */ }
  }, [dep.id]);
  useEffect(() => { load(); }, [load, dep.deploy_stage]);

  const refresh = async () => {
    setRefreshing(true);
    try { setFeatures(await api.refreshDeploymentFeatures(dep.id)); }
    catch (e: any) { alert(`Refresh failed: ${e.message}`); }
    finally { setRefreshing(false); }
  };

  if (features.length === 0) {
    if (dep.deploy_stage == null && dep.endpoint_status !== 'NOT_CREATED') {
      return <div className="mt-3 text-xs text-amber-700">Online features not tracked yet — click <b>Deploy</b> to sync (nothing is re-materialized).</div>;
    }
    return null;
  }
  const tables = new Set(features.map(f => f.online_table).filter(Boolean));
  const times = features.map(f => f.last_materialization_time).filter(Boolean) as string[];
  const freshest = times.sort().pop() || null;
  const paused = features.filter(f => f.pipeline_state === 'PAUSED').length;
  const missing = features.filter(f => f.pipeline_state === 'MISSING').length;

  // One line per feature type: "Aggregations: daily at 00:00 UTC (auto), next in 5h".
  const groups: { label: string; type: string }[] = [
    { label: 'Aggregations', type: 'aggregation' },
    { label: 'Lookups', type: 'column_selection' },
    { label: 'On-demand', type: 'custom_udf' },
  ];
  const summary = groups.map(g => {
    const fs = features.filter(f => f.feature_type === g.type);
    if (fs.length === 0) return null;
    const descs = [...new Set(fs.map(f => describeSchedule(f.schedule)))];
    const crons = fs.map(f => f.schedule).filter((x): x is Extract<FeatureSchedule, { kind: 'cron' }> => x?.kind === 'cron');
    const next = crons.map(c => nextCronRun(c.cron_expression, c.timezone_id)).filter(Boolean).sort((a, b) => a!.getTime() - b!.getTime())[0];
    return { label: g.label, text: descs.join(' / '), next };
  }).filter(Boolean) as { label: string; text: string; next?: Date | null }[];

  return (
    <div className="mt-3 border-t pt-3">
      <div className="flex items-center gap-3 text-sm">
        <button onClick={() => setOpen(!open)} className="text-gray-700 hover:text-gray-900">
          {open ? '▾' : '▸'} Online features: <span className="font-medium">{features.length}</span>
          <span className="text-gray-500"> in {tables.size} online table{tables.size === 1 ? '' : 's'}</span>
          {freshest && <span className="text-gray-500"> · last materialized {timeAgo(freshest)}</span>}
        </button>
        <button onClick={refresh} disabled={refreshing} className="text-xs text-blue-600 hover:underline disabled:opacity-50">
          {refreshing ? 'Refreshing…' : 'Refresh'}
        </button>
      </div>
      <div className="mt-1 ml-4 text-xs text-gray-600 space-y-0.5">
        {summary.map(g => (
          <div key={g.label}>
            <span className="font-medium text-gray-700">{g.label}:</span> {g.text}
            {g.next && <span className="text-gray-500"> · next run {timeUntil(g.next)}</span>}
          </div>
        ))}
        {paused > 0 && <div className="text-amber-700">{paused} feature pipeline(s) paused</div>}
        {missing > 0 && <div className="text-red-600">{missing} feature(s) no longer materialized online — click Deploy to restore</div>}
      </div>
      {open && (
        <table className="w-full text-left text-xs mt-2">
          <thead className="text-gray-500">
            <tr><th className="py-1 font-medium">Feature</th><th className="py-1 font-medium">Type</th><th className="py-1 font-medium">Online table</th><th className="py-1 font-medium">Refresh schedule</th><th className="py-1 font-medium">Last materialized</th></tr>
          </thead>
          <tbody className="divide-y">
            {features.map(f => (
              <tr key={f.feature_full_name}>
                <td className="py-1 font-mono" title={f.feature_full_name}>{f.feature_full_name.split('.').pop()}</td>
                <td className="py-1 text-gray-600">{FEATURE_TYPE_LABEL[f.feature_type] || f.feature_type}</td>
                <td className="py-1 font-mono text-gray-600" title={f.online_table || ''}>
                  {f.online_table ? f.online_table.split('.').pop() : <span className="text-gray-400">computed at serving</span>}
                </td>
                <td className="py-1 text-gray-600" title={f.schedule?.kind === 'cron' ? `${f.schedule.cron_expression} (${f.schedule.timezone_id}, ${f.schedule.mode || 'MANUAL'})` : ''}>
                  {describeSchedule(f.schedule)}{f.pipeline_state === 'PAUSED' && <span className="text-amber-700"> · paused</span>}
                </td>
                <td className="py-1 text-gray-600" title={f.last_materialization_time || ''}>
                  {f.online_table ? timeAgo(f.last_materialization_time) : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {open && (
        <div className="mt-2 text-xs text-gray-500">
          <b>Auto</b> = Databricks derives each aggregation's schedule from its time window (e.g. a 1-day slide → daily).
          Lookups re-sync whenever their source table changes; on-demand features are computed on every request.
        </div>
      )}
    </div>
  );
}

function DeployProgress({ dep }: { dep: Deployment }) {
  const pending = Object.keys(dep.pending_release || {}).length;
  let msg: string | null = null;
  if (dep.deploy_stage === 'FEATURES') msg = 'Putting the model\'s features online — new pipelines take several minutes to load…';
  else if (dep.deploy_stage === 'ENDPOINT') msg = 'Configuring the serving endpoint…';
  else if (dep.endpoint_status === 'CREATING') msg = 'Endpoint provisioning — first deploy can take 20–25 min (feature-serving sidecar)…';
  return (
    <>
      {msg && <div className="mt-2 text-xs text-blue-700 flex items-center gap-2"><span className="animate-pulse">●</span>{msg}</div>}
      {dep.deploy_stage === 'FAILED' && (
        <div className="mt-2 text-xs text-red-600 whitespace-pre-wrap">Deploy failed: {dep.deploy_error}</div>
      )}
      {dep.deploy_stage !== 'FAILED' && dep.deploy_error && (
        <div className="mt-2 text-xs text-amber-700 whitespace-pre-wrap">{dep.deploy_error}</div>
      )}
      {pending > 0 && (
        <div className="mt-1 text-xs text-gray-500">{pending} feature(s) only the previous model used will be removed once the endpoint is READY.</div>
      )}
    </>
  );
}

function DeploymentTab({ projectId, eols, trainingSpecs, runs }: {
  projectId: number; eols: EOL[]; trainingSpecs: TrainingSpec[]; runs: Run[];
}) {
  const [deployments, setDeployments] = useState<Deployment[]>([]);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({ name: '', run_id: '', endpoint_name: '' });

  const load = useCallback(async () => {
    setDeployments(await api.getDeployments(projectId));
  }, [projectId]);
  useEffect(() => { load(); }, [load]);

  // Poll while a deploy runs in the background or an endpoint is provisioning.
  const busy = deployments.some(d => d.deploy_stage === 'FEATURES' || d.deploy_stage === 'ENDPOINT' || d.endpoint_status === 'CREATING');
  useEffect(() => {
    if (!busy) return;
    const t = setInterval(async () => {
      for (const d of deployments.filter(x => x.endpoint_status === 'CREATING' && x.deploy_stage !== 'FEATURES' && x.deploy_stage !== 'ENDPOINT')) {
        try { await api.checkDeploymentStatus(d.id); } catch { /* ignore */ }
      }
      load();
    }, 10000);
    return () => clearInterval(t);
  }, [busy, deployments, load]);

  const registeredRuns = runs.filter(r => r.model_name && r.model_version);

  const submitDeployment = async () => {
    if (!form.name || !form.run_id || !form.endpoint_name) return;
    await api.createDeployment(projectId, { name: form.name, run_id: parseInt(form.run_id), endpoint_name: form.endpoint_name });
    setForm({ name: '', run_id: '', endpoint_name: '' });
    setShowForm(false);
    load();
  };

  const deploy = async (dep: Deployment) => {
    try { await api.deployDeployment(dep.id); load(); }
    catch (e: any) { alert(`Deploy failed: ${e.message}`); }
  };

  const updateModel = async (dep: Deployment, runId: number) => {
    try { await api.updateDeploymentEndpoint(dep.id, runId); load(); }
    catch (e: any) { alert(`Update failed: ${e.message}`); }
  };

  const remove = async (dep: Deployment) => {
    if (!confirm(`Delete deployment ${dep.name}? This deletes endpoint ${dep.endpoint_name} and removes online features no other deployment uses.`)) return;
    try { await api.deleteDeployment(dep.id); load(); }
    catch (e: any) { alert(`Delete failed: ${e.message}`); }
  };

  // EOL entity columns = inference input keys (for the test form)
  const getEntityColumns = (dep: Deployment): string[] => {
    const run = runs.find(r => r.id === dep.run_id);
    const spec = run ? trainingSpecs.find(s => s.id === run.training_spec_id) : null;
    const eol = spec ? eols.find(e => e.id === spec.eol_id) : null;
    const cols = eol?.entity_columns;
    if (Array.isArray(cols)) return cols;
    if (typeof cols === 'string') return (cols as string).replace(/^\{|\}$/g, '').split(',').filter(Boolean);
    return [];
  };

  const host = runs[0]?.databricks_run_url?.match(/^https?:\/\/[^/]+/)?.[0] || '';

  return (
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
                setForm({ ...form, run_id: runId, endpoint_name: form.endpoint_name || (run ? `mlops-${run.model_name?.split('.').pop() || ''}` : '') });
              }}>
                <option value="">Select model...</option>
                {registeredRuns.map(r => (
                  <option key={r.id} value={r.id}>{r.model_name?.split('.').pop()} v{r.model_version} (run #{r.id})</option>
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
          const inProgress = dep.deploy_stage === 'FEATURES' || dep.deploy_stage === 'ENDPOINT';
          const untracked = dep.deploy_stage == null && dep.endpoint_status !== 'NOT_CREATED';
          const canDeploy = !inProgress && (dep.endpoint_status === 'NOT_CREATED' || dep.endpoint_status === 'FAILED' || dep.deploy_stage === 'FAILED' || untracked);
          const canUpdate = !inProgress && (dep.endpoint_status === 'READY' || dep.endpoint_status === 'CREATING') && registeredRuns.length > 1;

          return (
            <div key={dep.id} className="p-4 bg-white rounded-lg shadow">
              <div className="flex justify-between items-start">
                <div>
                  <h3 className="font-medium">
                    {dep.name} <StatusBadge status={inProgress ? 'RUNNING' : dep.endpoint_status} />
                  </h3>
                  <div className="text-sm text-gray-500 mt-1">
                    Model: {run?.model_name ? (
                      <a href={`${host}/explore/data/models/${run.model_name.replace(/\./g, '/')}${run.model_version ? `/version/${run.model_version}` : ''}`}
                        target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline font-mono"
                      >{run.model_name.split('.').pop()} v{run.model_version}</a>
                    ) : 'unknown'}
                    {spec && <> | Spec: <span className="font-mono">{spec.name}</span></>}
                  </div>
                  <div className="text-sm text-gray-500">Endpoint: <span className="font-mono">{dep.endpoint_name}</span></div>
                </div>
                <div className="flex gap-2 items-start">
                  {canDeploy && (
                    <button onClick={() => deploy(dep)} className="px-3 py-1 bg-green-600 text-white text-xs rounded hover:bg-green-700"
                      title="Put the model's features online (only what's missing), then create or update the endpoint">
                      {untracked ? 'Deploy (sync)' : dep.deploy_stage === 'FAILED' ? 'Retry deploy' : 'Deploy'}
                    </button>
                  )}
                  {canUpdate && (
                    <select className="px-2 py-1 border rounded text-xs" value="" onChange={e => {
                      const newRunId = parseInt(e.target.value);
                      if (newRunId && newRunId !== dep.run_id) updateModel(dep, newRunId);
                    }}>
                      <option value="">Update model...</option>
                      {registeredRuns.filter(r => r.id !== dep.run_id).map(r => (
                        <option key={r.id} value={r.id}>{r.model_name?.split('.').pop()} v{r.model_version} (run #{r.id})</option>
                      ))}
                    </select>
                  )}
                  {dep.endpoint_status !== 'NOT_CREATED' && host && (
                    <a href={`${host}/ml/endpoints/${dep.endpoint_name}`} target="_blank" rel="noopener noreferrer" className="text-blue-600 text-xs hover:underline py-1">Endpoint UI</a>
                  )}
                  <button onClick={() => remove(dep)} disabled={inProgress} className="text-red-500 text-sm disabled:opacity-40">Delete</button>
                </div>
              </div>

              <DeployProgress dep={dep} />
              <DeploymentFeatures dep={dep} />

              {dep.endpoint_status === 'READY' && !inProgress && <TestEndpoint dep={dep} entityColumns={getEntityColumns(dep)} />}
            </div>
          );
        })}
        {deployments.length === 0 && (
          <div className="text-center py-8 text-gray-400">No deployments yet — create one, then <b>Deploy</b> puts its model's features online and creates the endpoint.</div>
        )}
      </div>
    </div>
  );
}

// ════════════════════════════════════════════
//  BATCH DEPLOYMENT TAB
// ════════════════════════════════════════════
// Scheduled batch inference: score a table of request fields with model@alias; the job adds the model's
// timestamp column = run time, resolves features point-in-time and appends input + features + prediction
// to the output table. The job runs without the app (all settings live in the Databricks job).

type ScheduleMode = 'none' | 'daily' | 'cron';
const emptyBatchForm = {
  name: '', model_name: '', model_alias: '', input_mode: 'source' as 'source' | 'sql', input_table: '', input_sql: '',
  output_table: '', schedule: 'daily' as ScheduleMode, daily_time: '06:00', cron_expression: '', timezone_id: 'UTC',
};

function scheduleToCron(f: typeof emptyBatchForm): string | null {
  if (f.schedule === 'none') return null;
  if (f.schedule === 'daily') {
    const [h, m] = (f.daily_time || '06:00').split(':');
    return `0 ${Number(m) || 0} ${Number(h) || 0} * * ?`;
  }
  return f.cron_expression.trim() || null;
}

function ModelAliasPicker({ modelName, value, onChange }: { modelName: string; value: string; onChange: (a: string) => void }) {
  const [info, setInfo] = useState<ModelAliases | null>(null);
  const [setting, setSetting] = useState({ open: false, alias: 'champion', version: '' });
  const load = useCallback(async () => {
    if (!modelName) { setInfo(null); return; }
    try { setInfo(await api.getModelAliases(modelName)); } catch { setInfo({ aliases: [], versions: [] }); }
  }, [modelName]);
  useEffect(() => { load(); }, [load]);

  const save = async () => {
    try {
      await api.setModelAlias(modelName, setting.alias, Number(setting.version));
      setSetting({ ...setting, open: false });
      await load();
      onChange(setting.alias);
    } catch (e: any) { alert(`Set alias failed: ${e.message}`); }
  };

  if (!modelName) return <select className="w-full px-3 py-2 border rounded text-sm" disabled><option>Pick a model first</option></select>;
  return (
    <div>
      <select className="w-full px-3 py-2 border rounded text-sm" value={value} onChange={e => onChange(e.target.value)}>
        <option value="">Select alias...</option>
        {(info?.aliases || []).map(a => <option key={a.alias} value={a.alias}>@{a.alias} → v{a.version}</option>)}
      </select>
      {!setting.open ? (
        <button className="text-xs text-blue-600 hover:underline mt-1" onClick={() => setSetting({ ...setting, open: true, version: String(info?.versions?.[0] || '') })}>
          {info && info.aliases.length === 0 ? 'No aliases yet — set one' : 'Set / move an alias'}
        </button>
      ) : (
        <div className="flex gap-1 mt-1 items-center text-xs">
          @<input className="px-1 py-0.5 border rounded w-24 font-mono" value={setting.alias} onChange={e => setSetting({ ...setting, alias: e.target.value })} />
          →
          <select className="px-1 py-0.5 border rounded" value={setting.version} onChange={e => setSetting({ ...setting, version: e.target.value })}>
            {(info?.versions || []).map(v => <option key={v} value={v}>v{v}</option>)}
          </select>
          <button className="px-2 py-0.5 bg-blue-600 text-white rounded" onClick={save} disabled={!setting.alias || !setting.version}>Set</button>
          <button className="px-2 py-0.5 border rounded" onClick={() => setSetting({ ...setting, open: false })}>Cancel</button>
        </div>
      )}
    </div>
  );
}

function BatchPreview({ id }: { id: number }) {
  const [rows, setRows] = useState<Record<string, any>[] | null>(null);
  useEffect(() => { api.previewBatch(id).then(setRows).catch(() => setRows([])); }, [id]);
  if (rows === null) return <div className="text-xs text-gray-400 mt-2">Loading preview…</div>;
  if (rows.length === 0) return <div className="text-xs text-gray-400 mt-2">No predictions yet — run it once.</div>;
  const cols = Object.keys(rows[0]);
  return (
    <div className="mt-2 overflow-x-auto border rounded">
      <table className="text-xs">
        <thead className="bg-gray-50"><tr>{cols.map(c => (
          <th key={c} className={`px-2 py-1 text-left font-medium whitespace-nowrap ${c === 'prediction' ? 'text-blue-700' : 'text-gray-500'}`}>{c}</th>
        ))}</tr></thead>
        <tbody className="divide-y">{rows.map((r, i) => (
          <tr key={i}>{cols.map(c => (
            <td key={c} className={`px-2 py-1 font-mono whitespace-nowrap ${c === 'prediction' ? 'font-semibold' : ''}`}>{r[c] == null ? '∅' : String(r[c])}</td>
          ))}</tr>
        ))}</tbody>
      </table>
    </div>
  );
}

// Builds the batch input SQL around a source table: each column the model needs (entity keys + request fields)
// is mapped to a source column (same name by default); extra columns can be passed through to the output
// (e.g. an email_id to join predictions back); the model's timestamp column is never selected.
const qid = (c: string) => `\`${c.replace(/`/g, '``')}\``;
function BatchSourceBuilder({ project, reqs, onSql }: {
  project: Project; reqs: api.BatchRequirements; onSql: (sql: string, table: string) => void;
}) {
  const [catalogs, setCatalogs] = useState<string[]>([]);
  const [schemas, setSchemas] = useState<string[]>([]);
  const [tables, setTables] = useState<string[]>([]);
  const [cat, setCat] = useState(project.catalog);
  const [sch, setSch] = useState(project.schema);
  const [tbl, setTbl] = useState('');
  const [cols, setCols] = useState<{ name: string; type: string }[]>([]);
  const [mapping, setMapping] = useState<Record<string, string>>({});
  const [extras, setExtras] = useState<string[]>([]);
  const [filter, setFilter] = useState('');
  const needed = [...reqs.entity_columns, ...reqs.request_fields];

  useEffect(() => { api.getCatalogs().then(setCatalogs).catch(() => setCatalogs([])); }, []);
  useEffect(() => { if (cat) api.getSchemas(cat).then(setSchemas).catch(() => setSchemas([])); }, [cat]);
  useEffect(() => { setTables([]); if (cat && sch) api.getTables(cat, sch).then(setTables).catch(() => setTables([])); }, [cat, sch]);
  useEffect(() => {
    setCols([]); setMapping({}); setExtras([]);
    if (!cat || !sch || !tbl) return;
    api.getColumns(cat, sch, tbl).then((c) => {
      setCols(c);
      const names = new Set(c.map((x) => x.name));
      setMapping(Object.fromEntries(needed.map((n) => [n, names.has(n) ? n : ''])));
      // Pass through other id-like columns by default (handy to join predictions back to the source row).
      setExtras(c.map((x) => x.name).filter((n) => !needed.includes(n) && n !== reqs.ts_col && n !== reqs.label && /(^id$|_id$)/i.test(n)));
    }).catch(() => setCols([]));
  }, [cat, sch, tbl]); // eslint-disable-line react-hooks/exhaustive-deps

  const fqn = tbl ? `${cat}.${sch}.${tbl}` : '';
  const missing = needed.filter((n) => !mapping[n]);
  const sql = !fqn || missing.length ? '' : [
    'SELECT ' + [
      ...needed.map((n) => (mapping[n] === n ? qid(n) : `${qid(mapping[n])} AS ${qid(n)}`)),
      ...extras.filter((e) => !needed.includes(e)).map(qid),
    ].join(', '),
    `FROM ${fqn}`,
    ...(filter.trim() ? [`WHERE ${filter.trim()}`] : []),
  ].join('\n');
  useEffect(() => { onSql(sql, fqn); }, [sql, fqn]); // eslint-disable-line react-hooks/exhaustive-deps

  const extraCandidates = cols.filter((c) => !needed.includes(c.name) && c.name !== reqs.ts_col && c.name !== reqs.label);
  const sel = 'w-full px-2 py-1.5 border rounded text-sm';
  return (
    <div className="space-y-2">
      <div className="grid grid-cols-3 gap-2">
        <select className={sel} value={cat} onChange={(e) => { setCat(e.target.value); setSch(''); setTbl(''); }}>
          <option value="">Catalog...</option>{catalogs.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
        <select className={sel} value={sch} onChange={(e) => { setSch(e.target.value); setTbl(''); }} disabled={!cat}>
          <option value="">Schema...</option>{schemas.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
        <select className={sel} value={tbl} onChange={(e) => setTbl(e.target.value)} disabled={!sch}>
          <option value="">Source table...</option>{tables.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
      </div>
      {tbl && cols.length > 0 && (
        <>
          <table className="w-full text-xs border rounded">
            <thead className="bg-gray-50"><tr>
              <th className="px-2 py-1 text-left font-medium">Model needs</th><th className="px-2 py-1 text-left font-medium">Kind</th>
              <th className="px-2 py-1 text-left font-medium">← Source column</th></tr></thead>
            <tbody className="divide-y">{needed.map((n) => (
              <tr key={n}>
                <td className="px-2 py-1 font-mono">{n}</td>
                <td className="px-2 py-1 text-gray-500">{reqs.entity_columns.includes(n) ? 'entity key' : 'request field'}</td>
                <td className="px-2 py-1">
                  <select className={`px-1 py-0.5 border rounded font-mono ${mapping[n] ? '' : 'border-red-400 bg-red-50'}`} value={mapping[n] || ''}
                    onChange={(e) => setMapping({ ...mapping, [n]: e.target.value })}>
                    <option value="">— missing —</option>
                    {cols.filter((c) => c.name !== reqs.ts_col).map((c) => <option key={c.name} value={c.name}>{c.name} ({c.type})</option>)}
                  </select>
                </td>
              </tr>
            ))}</tbody>
          </table>
          {extraCandidates.length > 0 && (
            <div className="text-xs">
              <span className="font-medium">Pass through to the output</span> <span className="text-gray-400">(not used by the model)</span>:
              <div className="flex flex-wrap gap-x-3 gap-y-1 mt-1">{extraCandidates.map((c) => (
                <label key={c.name} className="flex items-center gap-1 cursor-pointer">
                  <input type="checkbox" checked={extras.includes(c.name)}
                    onChange={() => setExtras(extras.includes(c.name) ? extras.filter((x) => x !== c.name) : [...extras, c.name])} />
                  <span className="font-mono">{c.name}</span>
                </label>
              ))}</div>
            </div>
          )}
          {cols.some((c) => c.name === reqs.ts_col) && (
            <div className="text-xs text-gray-500">The source's <span className="font-mono">{reqs.ts_col}</span> is left out — the job sets it to the run time.</div>
          )}
          <div>
            <label className="block text-xs font-medium mb-1">Filter (optional WHERE clause)</label>
            <input className="w-full px-2 py-1.5 border rounded font-mono text-xs" value={filter} onChange={(e) => setFilter(e.target.value)}
              placeholder={`e.g. ${reqs.ts_col} >= current_date() - INTERVAL 1 DAY`} />
          </div>
          {missing.length > 0
            ? <div className="text-xs text-red-600">Map every column the model needs ({missing.join(', ')} missing).</div>
            : <pre className="p-2 bg-gray-50 rounded text-xs font-mono whitespace-pre-wrap">{sql}</pre>}
        </>
      )}
    </div>
  );
}

function BatchTab({ projectId, project, runs }: { projectId: number; project: Project; runs: Run[] }) {
  const [items, setItems] = useState<BatchDeployment[]>([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState(emptyBatchForm);
  const [saving, setSaving] = useState(false);
  const [previewId, setPreviewId] = useState<number | null>(null);

  const load = useCallback(async () => {
    try { setItems(await api.getBatchDeployments(projectId)); }
    finally { setLoading(false); }
  }, [projectId]);
  useEffect(() => { load(); }, [load]);

  // Poll while any run is in flight.
  const active = items.some(b => b.runs.some(r => ['RUNNING', 'PENDING', 'QUEUED', 'BLOCKED', 'TERMINATING'].includes(r.state)));
  useEffect(() => {
    if (!active) return;
    const t = setInterval(load, 15000);
    return () => clearInterval(t);
  }, [active, load]);

  const models = [...new Set(runs.filter(r => r.model_name && r.model_version).map(r => r.model_name as string))];
  const [reqs, setReqs] = useState<api.BatchRequirements | null>(null);
  const [reqsError, setReqsError] = useState('');
  useEffect(() => {
    setReqs(null); setReqsError('');
    if (!showForm || !form.model_name) return;
    api.getBatchRequirements(projectId, form.model_name).then(setReqs).catch((e) => setReqsError(e.message));
  }, [showForm, form.model_name, projectId]);

  const openForm = () => {
    setForm({ ...emptyBatchForm, model_name: models[0] || '' });
    setShowForm(true);
  };

  const submit = async () => {
    setSaving(true);
    try {
      await api.createBatchDeployment(projectId, {
        name: form.name, model_name: form.model_name, model_alias: form.model_alias,
        input_table: null,
        input_sql: form.input_sql,
        output_table: form.output_table || `${project.catalog}.${project.schema}.${form.name}_predictions`,
        cron_expression: scheduleToCron(form), timezone_id: form.timezone_id || 'UTC',
      });
      setShowForm(false);
      await load();
    } catch (e: any) { alert(`Create failed: ${e.message}`); }
    finally { setSaving(false); }
  };

  const act = async (fn: () => Promise<any>, label: string) => {
    try { await fn(); await load(); } catch (e: any) { alert(`${label} failed: ${e.message}`); }
  };

  const host = runs[0]?.databricks_run_url?.match(/^https?:\/\/[^/]+/)?.[0] || '';
  const cronPreview = scheduleToCron(form);
  const outPlaceholder = `${project.catalog}.${project.schema}.${form.name || '<name>'}_predictions`;

  if (loading) return <div className="text-center py-8">Loading...</div>;

  return (
    <div>
      <div className="bg-blue-50 border border-blue-200 rounded p-3 text-sm text-gray-700 mb-4">
        A batch deployment scores every row of an <b>input table</b> — the fields a requesting system would send
        (entity keys + request-time fields) — with <span className="font-mono">model@alias</span>. Each run sets the
        model's timestamp column to the <b>run time</b>, computes features point-in-time, and appends input + features +
        prediction to the <b>output table</b>. It's a Databricks job: it runs on schedule without this app.
      </div>

      <div className="flex justify-between items-center mb-4">
        <h2 className="text-lg font-medium">Batch Deployments</h2>
        <button onClick={() => (showForm ? setShowForm(false) : openForm())} className="px-3 py-1.5 bg-blue-600 text-white text-sm rounded hover:bg-blue-700">
          {showForm ? 'Cancel' : 'New Batch Deployment'}
        </button>
      </div>

      {showForm && (
        <div className="mb-4 p-4 border rounded-lg bg-white shadow space-y-3">
          <div className="grid grid-cols-3 gap-3">
            <div>
              <label className="block text-sm font-medium mb-1">Name</label>
              <input className="w-full px-3 py-2 border rounded text-sm" value={form.name} placeholder="e.g. daily_scoring"
                onChange={e => setForm({ ...form, name: e.target.value.replace(/[^A-Za-z0-9_]/g, '_') })} />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Model</label>
              <select className="w-full px-3 py-2 border rounded text-sm" value={form.model_name} onChange={e => setForm({ ...form, model_name: e.target.value, model_alias: '' })}>
                <option value="">Select model...</option>
                {models.map(m => <option key={m} value={m}>{m.split('.').pop()}</option>)}
              </select>
              {models.length === 0 && <div className="text-xs text-gray-400 mt-1">Register a model from the Training tab first</div>}
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Alias (followed each run)</label>
              <ModelAliasPicker modelName={form.model_name} value={form.model_alias} onChange={a => setForm(f => ({ ...f, model_alias: a }))} />
            </div>
          </div>

          {reqsError && <div className="text-sm text-red-600 bg-red-50 p-2 rounded">{reqsError}</div>}
          {reqs && (
            <div className="text-sm bg-gray-50 border rounded p-3 space-y-1">
              <div className="font-medium">What the input must provide <span className="font-normal text-xs text-gray-500">
                (from EOL “{reqs.eol_name}”, spec “{reqs.spec_name}”{reqs.model_version ? `, v${reqs.model_version}` : ''})</span></div>
              <div><span className="text-gray-500">Entity keys</span> (features are looked up / aggregated by these):{' '}
                <span className="font-mono">{reqs.entity_columns.join(', ') || '—'}</span></div>
              <div><span className="text-gray-500">Request fields</span> (values the requesting system sends):{' '}
                <span className="font-mono">{reqs.request_fields.join(', ') || 'none'}</span></div>
              <div className="text-xs text-gray-500">One row per thing to score. Don't include <span className="font-mono">{reqs.ts_col}</span> or
                {' '}<span className="font-mono">{reqs.label}</span>: features are computed as of the run time. Any other columns pass through to the output.</div>
            </div>
          )}

          <div>
            <label className="block text-sm font-medium mb-1">
              Input
              <span className="ml-3 font-normal text-xs">
                <label className="mr-3"><input type="radio" checked={form.input_mode === 'source'} onChange={() => setForm({ ...form, input_mode: 'source' })} /> build from a source table</label>
                <label><input type="radio" checked={form.input_mode === 'sql'} onChange={() => setForm({ ...form, input_mode: 'sql' })} /> write SQL</label>
              </span>
            </label>
            {form.input_mode === 'source' ? (
              reqs ? <BatchSourceBuilder project={project} reqs={reqs} onSql={(sql, table) => setForm(f => ({ ...f, input_sql: sql, input_table: table }))} />
                : <div className="text-xs text-gray-400">{form.model_name ? 'Loading model requirements…' : 'Pick a model first'}</div>
            ) : (
              <textarea className="w-full px-3 py-2 border rounded font-mono text-sm" rows={4} value={form.input_sql}
                placeholder={reqs ? `SELECT ${[...reqs.entity_columns, ...reqs.request_fields].join(', ')} FROM ...` : 'SELECT ... FROM ...'}
                onChange={e => setForm({ ...form, input_sql: e.target.value })} />
            )}
          </div>

          <div className="grid grid-cols-3 gap-3">
            <div className="col-span-1">
              <label className="block text-sm font-medium mb-1">Output table (appended)</label>
              <input className="w-full px-3 py-2 border rounded font-mono text-sm" value={form.output_table}
                placeholder={outPlaceholder} onChange={e => setForm({ ...form, output_table: e.target.value.trim() })} />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Schedule</label>
              <div className="flex gap-2">
                <select className="px-2 py-2 border rounded text-sm" value={form.schedule} onChange={e => setForm({ ...form, schedule: e.target.value as ScheduleMode })}>
                  <option value="daily">Daily at</option>
                  <option value="cron">Custom cron</option>
                  <option value="none">Manual only</option>
                </select>
                {form.schedule === 'daily' && (
                  <input type="time" className="px-2 py-2 border rounded text-sm" value={form.daily_time} onChange={e => setForm({ ...form, daily_time: e.target.value })} />
                )}
                {form.schedule === 'cron' && (
                  <input className="flex-1 px-2 py-2 border rounded font-mono text-sm" placeholder="0 0 6 * * ?" value={form.cron_expression}
                    onChange={e => setForm({ ...form, cron_expression: e.target.value })} />
                )}
              </div>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Timezone</label>
              <input className="w-full px-3 py-2 border rounded font-mono text-sm" value={form.timezone_id} disabled={form.schedule === 'none'}
                onChange={e => setForm({ ...form, timezone_id: e.target.value })} />
            </div>
          </div>
          <div className="text-xs text-gray-500">
            {cronPreview ? <>Runs {describeCron(cronPreview, form.timezone_id || 'UTC')}.</> : 'Runs only when you click Run now (or from the Jobs UI).'}
          </div>

          <div className="flex gap-2">
            <button onClick={submit} disabled={saving || !form.name || !form.model_name || !form.model_alias || !form.input_sql.trim()}
              className="px-4 py-2 bg-green-600 text-white rounded text-sm disabled:opacity-50">{saving ? 'Creating job…' : 'Create'}</button>
            <button onClick={() => setShowForm(false)} className="px-4 py-2 bg-gray-200 rounded text-sm">Cancel</button>
          </div>
        </div>
      )}

      <div className="space-y-4">
        {items.map(b => {
          const last = b.runs[0];
          const lastOk = b.runs.find(r => r.state === 'SUCCESS');
          const next = b.cron_expression && !b.paused ? nextCronRun(b.cron_expression, b.timezone_id) : null;
          return (
            <div key={b.id} className="p-4 bg-white rounded-lg shadow">
              <div className="flex justify-between items-start">
                <div className="text-sm">
                  <h3 className="font-medium text-base">
                    {b.name} <StatusBadge status={b.job_error ? 'FAILED' : b.paused ? 'PAUSED' : b.cron_expression ? 'ACTIVE' : 'NOT_STARTED'} />
                  </h3>
                  <div className="text-gray-500 mt-1">
                    Model: <span className="font-mono">{b.model_name.split('.').pop()}@{b.model_alias}</span>
                    {lastOk?.model_version && <span> (last run used v{lastOk.model_version})</span>}
                  </div>
                  <div className="text-gray-500">Input: <span className="font-mono">{b.input_table || (b.input_sql?.match(/\bFROM\s+([\w`.]+)/i)?.[1]?.replace(/`/g, '') ?? 'SQL query')}</span>
                    <span className="text-gray-400"> · {b.ts_col} = run time</span></div>
                  <div className="text-gray-500">Output: <span className="font-mono">{b.output_table}</span></div>
                  <div className="text-gray-500">
                    Schedule: {b.cron_expression ? describeCron(b.cron_expression, b.timezone_id) : 'manual only'}
                    {b.paused && <span className="text-amber-700"> · paused</span>}
                    {next && <span> · next run {timeUntil(next)}</span>}
                  </div>
                  {b.job_error && <div className="text-red-600 text-xs mt-1">Job: {b.job_error}</div>}
                </div>
                <div className="flex gap-2 items-start">
                  <button onClick={() => act(() => api.runBatchNow(b.id), 'Run')} className="px-3 py-1 bg-green-600 text-white text-xs rounded hover:bg-green-700">Run now</button>
                  {b.cron_expression && (
                    <button onClick={() => act(() => (b.paused ? api.resumeBatch(b.id) : api.pauseBatch(b.id)), b.paused ? 'Resume' : 'Pause')}
                      className="px-3 py-1 border rounded text-xs">{b.paused ? 'Resume' : 'Pause'}</button>
                  )}
                  {host && b.job_id && <a href={`${host}/#job/${b.job_id}`} target="_blank" rel="noopener noreferrer" className="text-blue-600 text-xs hover:underline py-1">Job</a>}
                  <button onClick={() => { if (confirm(`Delete batch deployment ${b.name}? Deletes its job; ${b.output_table} is kept.`)) act(() => api.deleteBatchDeployment(b.id), 'Delete'); }}
                    className="text-red-500 text-sm">Delete</button>
                </div>
              </div>

              <div className="mt-3 border-t pt-2">
                <div className="text-xs font-medium text-gray-600 mb-1">Recent runs</div>
                {b.runs.length === 0 ? <div className="text-xs text-gray-400">No runs yet</div> : (
                  <table className="text-xs w-full">
                    <tbody className="divide-y">
                      {b.runs.map(r => (
                        <tr key={r.run_id}>
                          <td className="py-1 pr-3 text-gray-600">{r.start_time ? new Date(r.start_time).toLocaleString() : '—'}</td>
                          <td className="py-1 pr-3"><StatusBadge status={r.state} /></td>
                          <td className="py-1 pr-3 text-gray-600">{r.trigger === 'PERIODIC' ? 'scheduled' : r.trigger === 'ONE_TIME' ? 'manual' : r.trigger || ''}</td>
                          <td className="py-1 pr-3 text-gray-600">{r.rows != null ? `${r.rows} rows` : ''}{r.model_version ? ` · v${r.model_version}` : ''}</td>
                          <td className="py-1 pr-3 text-red-600 truncate max-w-md" title={r.error}>{r.error ? r.error.split('\n')[0].slice(0, 120) : ''}</td>
                          <td className="py-1"><a href={r.url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">view</a></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
                {last && (
                  <button className="text-xs text-blue-600 hover:underline mt-2" onClick={() => setPreviewId(previewId === b.id ? null : b.id)}>
                    {previewId === b.id ? 'Hide predictions' : 'Show latest predictions'}
                  </button>
                )}
                {previewId === b.id && <BatchPreview id={b.id} />}
              </div>
            </div>
          );
        })}
        {items.length === 0 && !showForm && <div className="text-center py-8 text-gray-400">No batch deployments yet</div>}
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

export default App;
