import { createApp, analytics, server, lakebase, type CacheConfig } from '@databricks/appkit';
import fs from 'fs';
import path from 'path';
import { execSync } from 'child_process';

// The Apps platform injects DATABRICKS_HOST without a scheme (e.g. "workspace.cloud.databricks.com"),
// which breaks `fetch(\`${host}/api/...\`)`. Normalize once so all REST calls have https://.
if (process.env.DATABRICKS_HOST && !/^https?:\/\//.test(process.env.DATABRICKS_HOST)) {
  process.env.DATABRICKS_HOST = `https://${process.env.DATABRICKS_HOST}`;
}

// Use PGPASSWORD for native auth (local dev), or LAKEBASE_ENDPOINT for OAuth (deployed)
const lakebaseConfig = process.env.PGPASSWORD
  ? { password: process.env.PGPASSWORD }
  : {};

// AppKit's cache otherwise tries to create persistent tables in the Lakebase `public` schema, which
// the deployed app SP can't (logs "permission denied for schema public", then falls back to memory).
// Only the analytics plugin uses this cache and the app runs its own SQL, so pass a plain in-memory
// store up front.
const cacheEntries = new Map<string, any>();
const memoryCache: NonNullable<CacheConfig['storage']> = {
  get: async (k) => cacheEntries.get(k) ?? null,
  set: async (k, e) => { cacheEntries.set(k, e); },
  delete: async (k) => { cacheEntries.delete(k); },
  clear: async () => { cacheEntries.clear(); },
  has: async (k) => cacheEntries.has(k),
  size: async () => cacheEntries.size,
  isPersistent: () => false,
  healthCheck: async () => true,
  close: async () => {},
};

const appkit = await createApp({
  cache: { storage: memoryCache },
  plugins: [
    analytics(),
    lakebase(lakebaseConfig),
    server({ autoStart: false }),
  ],
});

const db = appkit.lakebase;

// Lakebase project config for synced tables (feature store online serving).
// The online store name must match the Lakebase project name (fe online store).
const LAKEBASE_BRANCH = process.env.LAKEBASE_BRANCH || 'projects/mlops/branches/production';
const ONLINE_STORE_NAME = (LAKEBASE_BRANCH.match(/^projects\/([^/]+)/)?.[1]) || 'mlops';

// Helper: call Databricks REST API using the user's OBO token (or app token for local dev).
// In dev mode, refreshes the CLI-issued token before it expires (~1 hr) so the server
// doesn't need a manual restart every hour.
let _devToken: { token: string; expiresAt: number } | null = null;
function refreshDevToken(): string {
  const profile = process.env.DATABRICKS_CONFIG_PROFILE;
  if (!profile) return process.env.DATABRICKS_TOKEN || '';
  try {
    const out = execSync(`databricks auth token -p ${profile}`, {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    });
    const parsed = JSON.parse(out);
    _devToken = {
      token: parsed.access_token,
      expiresAt: new Date(parsed.expiry).getTime(),
    };
    return _devToken.token;
  } catch {
    return process.env.DATABRICKS_TOKEN || '';
  }
}
// Deployed apps authenticate to the Databricks REST APIs as the app's SERVICE PRINCIPAL
// (client-credentials OAuth using the auto-injected DATABRICKS_CLIENT_ID/SECRET). The user OBO
// token only carries the app's `user_api_scopes` (sql) — it can't drive jobs, serving, MLflow, or
// workspace APIs. The SP is granted the needed permissions instead. Token is cached until expiry.
let _spToken: { token: string; expiresAt: number } | null = null;
async function getServicePrincipalToken(): Promise<string> {
  const clientId = process.env.DATABRICKS_CLIENT_ID;
  const clientSecret = process.env.DATABRICKS_CLIENT_SECRET;
  if (!clientId || !clientSecret) return process.env.DATABRICKS_TOKEN || '';
  if (_spToken && Date.now() < _spToken.expiresAt - 60_000) return _spToken.token;
  const host = process.env.DATABRICKS_HOST || '';
  const resp = await fetch(`${host}/oidc/v1/token`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString('base64')}`,
    },
    body: 'grant_type=client_credentials&scope=all-apis',
  });
  const data: any = await resp.json();
  if (!data.access_token) throw new Error(`SP token request failed: ${JSON.stringify(data)}`);
  _spToken = { token: data.access_token, expiresAt: Date.now() + (data.expires_in ?? 3600) * 1000 };
  return _spToken.token;
}

async function getToken(req: any): Promise<string> {
  if (process.env.NODE_ENV === 'development') {
    // Local dev: CLI token (full access), refreshed 60s before expiry.
    if (!_devToken || Date.now() >= _devToken.expiresAt - 60_000) return refreshDevToken();
    return _devToken.token;
  }
  // Deployed: use the app service principal for management APIs.
  if (process.env.DATABRICKS_CLIENT_ID) return getServicePrincipalToken();
  // Fallback: OBO user token (only useful for scopes in user_api_scopes).
  return (req.headers['x-forwarded-access-token'] as string) || process.env.DATABRICKS_TOKEN || '';
}

// Helper: get the current user's email (cached)
let _cachedUser: string | null = null;
async function getUsername(req: any): Promise<string> {
  if (_cachedUser) return _cachedUser;
  if (process.env.DATABRICKS_USER || process.env.PGUSER) {
    _cachedUser = process.env.DATABRICKS_USER || process.env.PGUSER || '';
    return _cachedUser;
  }
  // Deployed: resolve from Databricks current-user API
  try {
    const result = await databricksApi(req, 'GET', 'preview/scim/v2/Me');
    _cachedUser = result.userName || '';
  } catch {
    _cachedUser = '';
  }
  return _cachedUser ?? '';
}

async function databricksApi(req: any, method: string, apiPath: string, body?: any, apiPrefix = 'api/2.1'): Promise<any> {
  const host = process.env.DATABRICKS_HOST || '';
  const token = await getToken(req);
  const resp = await fetch(`${host}/${apiPrefix}/${apiPath}`, {
    method,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await resp.text();
  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`Databricks API error (${resp.status}): ${text.slice(0, 200)}`);
  }
}

// Helper: upload a local notebook to the Databricks workspace
// workspacePath should use /Workspace/Users/... format (for Jobs API);
// this function strips the /Workspace prefix for the Workspace API calls.
async function uploadNotebook(req: any, localPath: string, workspacePath: string): Promise<void> {
  const content = fs.readFileSync(localPath, 'utf-8');
  const host = process.env.DATABRICKS_HOST || '';
  const token = await getToken(req);
  // Workspace API paths don't use /Workspace prefix
  const apiPath = workspacePath.replace(/^\/Workspace/, '');
  // Ensure parent directory exists
  const parentDir = apiPath.substring(0, apiPath.lastIndexOf('/'));
  const mkdirResp = await fetch(`${host}/api/2.0/workspace/mkdirs`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: parentDir }),
  });
  const mkdirResult: any = await mkdirResp.json();
  if (mkdirResult.error_code) throw new Error(`Workspace mkdirs failed: ${mkdirResult.message}`);
  const resp = await fetch(`${host}/api/2.0/workspace/import`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      path: apiPath,
      format: 'SOURCE',
      language: 'PYTHON',
      overwrite: true,
      content: Buffer.from(content).toString('base64'),
    }),
  });
  const result: any = await resp.json();
  if (result.error_code) throw new Error(`Notebook upload failed: ${result.message}`);
  console.log(`[notebook] Uploaded ${localPath} → ${apiPath}`);
}

// Helper: execute SQL statement on the warehouse and return rows
async function executeSql(req: any, sql: string): Promise<any[]> {
  const host = process.env.DATABRICKS_HOST || '';
  const token = await getToken(req);
  const warehouseId = process.env.DATABRICKS_WAREHOUSE_ID || '';
  const resp = await fetch(`${host}/api/2.0/sql/statements`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ warehouse_id: warehouseId, statement: sql, wait_timeout: '30s' }),
  });
  const data: any = await resp.json();
  if (data.status?.state === 'FAILED') throw new Error(data.status.error?.message || 'SQL execution failed');
  const columns = data.manifest?.schema?.columns?.map((c: any) => c.name) || [];
  const rows = (data.result?.data_array || []).map((row: any[]) => {
    const obj: any = {};
    columns.forEach((col: string, i: number) => { obj[col] = row[i]; });
    return obj;
  });
  return rows;
}

// UC feature name for a spec entry / column (the Feature View bridge's naming rule).
const fvName = (s: string) => (s || '').toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '').slice(0, 120) || 'feature';

// Helper: strip .py extension from notebook path
function stripPyExt(p: string): string {
  return p.endsWith('.py') ? p.slice(0, -3) : p;
}

appkit.server.extend((app) => {

  // ════════════════════════════════════════════
  //  PROJECTS
  // ════════════════════════════════════════════
  app.get('/api/projects', async (_req, res) => {
    try {
      const result = await db.query('SELECT * FROM app.project ORDER BY id DESC');
      res.json(result.rows);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/api/projects/:id', async (req, res) => {
    try {
      const result = await db.query('SELECT * FROM app.project WHERE id = $1', [req.params.id]);
      if (result.rows.length === 0) { res.status(404).json({ error: 'Not found' }); return; }
      res.json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/projects', async (req, res) => {
    try {
      const { name, description, catalog, schema, model_name, git_url, notebook_path, training_notebook, evaluation_notebook } = req.body;
      // git_url/notebook_path/training_notebook/evaluation_notebook are legacy columns
      // (NOT NULL DEFAULT '') from the old user-notebooks design; the app now uses
      // app-managed notebooks and no longer collects them — coalesce to '' so the
      // insert still satisfies the NOT NULL constraints.
      const result = await db.query(
        `INSERT INTO app.project (name, description, catalog, schema, model_name, git_url, notebook_path, training_notebook, evaluation_notebook)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *`,
        [name, description, catalog, schema, model_name || name, git_url || '', notebook_path || '', training_notebook || '', evaluation_notebook || '']
      );
      res.status(201).json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.put('/api/projects/:id', async (req, res) => {
    try {
      const { name, description, catalog, schema, model_name, git_url, notebook_path, training_notebook, evaluation_notebook } = req.body;
      const result = await db.query(
        `UPDATE app.project SET name=$1, description=$2, catalog=$3, schema=$4, model_name=$5, git_url=$6, notebook_path=$7, training_notebook=$8, evaluation_notebook=$9
         WHERE id=$10 RETURNING *`,
        [name, description, catalog, schema, model_name || name, git_url || '', notebook_path || '', training_notebook || '', evaluation_notebook || '', req.params.id]
      );
      if (result.rows.length === 0) { res.status(404).json({ error: 'Not found' }); return; }
      res.json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.delete('/api/projects/:id', async (req, res) => {
    try {
      await db.query('DELETE FROM app.project WHERE id = $1', [req.params.id]);
      res.json({ ok: true });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ════════════════════════════════════════════
  //  GITHUB NOTEBOOK LISTING
  // ════════════════════════════════════════════
  app.get('/api/github/notebooks', async (req, res) => {
    try {
      const repoUrl = req.query.repo_url as string;
      const notebookPath = (req.query.notebook_path as string) || '';
      console.log(`[github/notebooks] repo_url=${repoUrl} notebook_path=${notebookPath}`);
      if (!repoUrl) { res.status(400).json({ error: 'repo_url required' }); return; }

      // Parse GitHub URL: handles both
      //   https://github.com/owner/repo/tree/main/notebooks (web UI URL)
      //   https://github.com/owner/repo (plain repo URL)
      const match = repoUrl.match(/github\.com\/([^/]+)\/([^/]+)/);
      if (!match) {
        console.log(`[github/notebooks] Invalid GitHub URL: ${repoUrl}`);
        res.status(400).json({ error: 'Invalid GitHub URL' });
        return;
      }
      const [, owner, repo] = match;
      const cleanRepo = repo.replace(/\.git$/, '');

      // Strip tree/main/ or tree/branch/ from notebook path (GitHub web UI artifact)
      let cleanPath = notebookPath.replace(/^tree\/[^/]+\//, '');
      console.log(`[github/notebooks] Cleaned path: "${notebookPath}" -> "${cleanPath}"`);

      // Fetch directory contents from GitHub API
      const apiUrl = `https://api.github.com/repos/${owner}/${cleanRepo}/contents/${cleanPath}`;
      console.log(`[github/notebooks] Fetching: ${apiUrl}`);
      const resp = await fetch(apiUrl, {
        headers: { 'Accept': 'application/vnd.github.v3+json', 'User-Agent': 'mlops-app' },
      });
      if (!resp.ok) {
        const err = await resp.text();
        console.log(`[github/notebooks] GitHub API error ${resp.status}: ${err.slice(0, 200)}`);
        res.status(resp.status).json({ error: `GitHub API: ${err}` });
        return;
      }
      const contents = await resp.json() as any[];
      const notebooks = contents
        .filter((f: any) => f.type === 'file' && (f.name.endsWith('.py') || f.name.endsWith('.ipynb')))
        .map((f: any) => ({ name: f.name, path: f.path }));
      console.log(`[github/notebooks] Found ${notebooks.length} notebooks: ${notebooks.map(n => n.name).join(', ')}`);
      res.json(notebooks);
    } catch (e: any) {
      console.error(`[github/notebooks] Error: ${e.message}`);
      res.status(500).json({ error: e.message });
    }
  });

  // ════════════════════════════════════════════
  //  UNITY CATALOG METADATA (via SQL warehouse)
  // ════════════════════════════════════════════
  app.get('/api/uc/catalogs', async (req, res) => {
    try {
      const rows = await executeSql(req, 'SHOW CATALOGS');
      console.log('[uc/catalogs] raw rows:', JSON.stringify(rows.slice(0, 3)));
      // Column name varies by runtime: 'catalog' or 'catalog_name'
      res.json(rows.map((r: any) => r.catalog || r.catalog_name || Object.values(r)[0]));
    } catch (e: any) {
      console.error('[uc/catalogs] error:', e.message);
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/api/uc/schemas', async (req, res) => {
    try {
      const catalog = req.query.catalog as string;
      if (!catalog) { res.status(400).json({ error: 'catalog required' }); return; }
      const rows = await executeSql(req, `SHOW SCHEMAS IN ${catalog}`);
      res.json(rows.map((r: any) => r.databaseName || r.schema_name || r.namespace));
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/api/uc/tables', async (req, res) => {
    try {
      const catalog = req.query.catalog as string;
      const schema = req.query.schema as string;
      if (!catalog || !schema) { res.status(400).json({ error: 'catalog and schema required' }); return; }
      const rows = await executeSql(req, `SHOW TABLES IN ${catalog}.${schema}`);
      res.json(rows.map((r: any) => r.tableName || r.table_name));
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/api/uc/columns', async (req, res) => {
    try {
      const catalog = req.query.catalog as string;
      const schema = req.query.schema as string;
      const table = req.query.table as string;
      if (!catalog || !schema || !table) { res.status(400).json({ error: 'catalog, schema, and table required' }); return; }
      const rows = await executeSql(req, `DESCRIBE TABLE ${catalog}.${schema}.${table}`);
      // DESCRIBE TABLE appends metadata sections ("# Clustering Information", "# Partition Information",
      // "# col_name") after a blank row, repeating the clustering/partition columns — keep only the leading
      // block of real columns.
      const end = rows.findIndex((r: any) => !r.col_name || String(r.col_name).startsWith('#'));
      res.json((end === -1 ? rows : rows.slice(0, end)).map((r: any) => ({ name: r.col_name, type: r.data_type })));
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/api/uc/functions', async (req, res) => {
    try {
      const catalog = req.query.catalog as string;
      const schema = req.query.schema as string;
      if (!catalog || !schema) { res.status(400).json({ error: 'catalog and schema required' }); return; }
      const rows = await executeSql(req, `SELECT routine_name FROM ${catalog}.information_schema.routines WHERE routine_schema = '${schema}' AND routine_type = 'FUNCTION' ORDER BY routine_name`);
      res.json(rows.map((r: any) => r.routine_name || r.ROUTINE_NAME || Object.values(r)[0]));
    } catch (e: any) {
      console.error('[uc/functions] error:', e.message);
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/api/uc/function-params', async (req, res) => {
    try {
      const catalog = req.query.catalog as string;
      const schema = req.query.schema as string;
      const func = req.query.function as string;
      if (!catalog || !schema || !func) { res.status(400).json({ error: 'catalog, schema, and function required' }); return; }
      const rows = await executeSql(req, `SELECT parameter_name, data_type FROM ${catalog}.information_schema.parameters WHERE specific_schema = '${schema}' AND specific_name = '${func}' AND parameter_name IS NOT NULL ORDER BY ordinal_position`);
      res.json(rows.map((r: any) => ({ name: r.parameter_name || r.PARAMETER_NAME, type: r.data_type || r.DATA_TYPE })));
    } catch (e: any) {
      console.error('[uc/function-params] error:', e.message);
      res.status(500).json({ error: e.message });
    }
  });

  // SQL preview — execute arbitrary SQL and return results (for EOL validation)
  app.post('/api/sql/preview', async (req, res) => {
    try {
      const { sql } = req.body;
      if (!sql) { res.status(400).json({ error: 'sql required' }); return; }
      // Add LIMIT to prevent huge result sets
      const safeSql = sql.trim().replace(/;$/, '');
      const rows = await executeSql(req, `${safeSql} LIMIT 100`);
      res.json({ rows, count: rows.length });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // EOL column detection — the columns an EOL query produces, plus a suggested label, timestamp and
  // entity columns. Everything else in an EOL is a request field (sent by the caller at scoring time).
  //   timestamp: the only TIMESTAMP/DATE column (or the one named *_ts/*_time/*timestamp/*_at)
  //   label:     a column named label/target/y/is_* (the last one, if several)
  //   entity:    *_id / id columns, or a column that names a table in a source schema
  //              (sender_domain ↔ sender_domain_features, customer_id ↔ customer_features)
  app.post('/api/sql/columns', async (req, res) => {
    try {
      const sql = String(req.body.sql || '').trim().replace(/;$/, '');
      if (!sql) { res.status(400).json({ error: 'sql required' }); return; }
      const described = await executeSql(req, `DESCRIBE QUERY SELECT * FROM (${sql}) AS _eol`);
      const columns: { name: string; type: string }[] = described
        .filter((r: any) => r.col_name && !String(r.col_name).startsWith('#'))
        .map((r: any) => ({ name: r.col_name, type: String(r.data_type || '').toLowerCase() }));

      const tsCands = columns.filter(c => /^(timestamp|timestamp_ntz|date)$/.test(c.type));
      const tsNamed = tsCands.filter(c => /(_ts|_time|timestamp|_at|_date)$/i.test(c.name));
      const timestamp_column = tsCands.length === 1 ? tsCands[0].name : tsNamed.length === 1 ? tsNamed[0].name : '';

      const labelCands = columns.filter(c => c.name !== timestamp_column && /^(label|target|y|is_.+)$/i.test(c.name));
      // Several candidates (e.g. is_vip, is_fraud) → the last one selected; labels conventionally come last.
      const label_column = labelCands.length ? labelCands[labelCands.length - 1].name : '';

      // Table names in the schemas the query reads from (FQN references only).
      const schemas = new Set<string>();
      for (const m of sql.replace(/`/g, '').matchAll(/\b(\w+)\.(\w+)\.(\w+)\b/g)) schemas.add(`${m[1]}.${m[2]}`);
      const tableNames: string[] = [];
      for (const s of schemas) {
        const [cat, sch] = s.split('.');
        try {
          const rows = await executeSql(req, `SELECT table_name FROM ${cat}.information_schema.tables WHERE table_schema = '${sch}'`);
          tableNames.push(...rows.map((r: any) => String(r.table_name).toLowerCase()));
        } catch { /* no access to information_schema — fall back to the name rule */ }
      }
      const namesTable = (col: string) => {
        const c = col.toLowerCase(), stem = c.replace(/_id$/, '');
        return tableNames.some(t => t.startsWith(`${c}_`) || (stem !== c && t.startsWith(`${stem}_`)));
      };
      const entity_columns = columns
        .filter(c => c.name !== timestamp_column && c.name !== label_column)
        .filter(c => /(^id$|_id$)/i.test(c.name) || namesTable(c.name))
        .map(c => c.name);

      res.json({ columns, suggested: { label_column, timestamp_column, entity_columns } });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ════════════════════════════════════════════
  //  ENTITY OBSERVATION LABELS (EOLs)
  // ════════════════════════════════════════════

  // An EOL needs a label, a timestamp and at least one entity column, all produced by its SQL.
  async function validateEol(req: any, b: any): Promise<string | null> {
    const ents = Array.isArray(b.entity_columns) ? b.entity_columns.filter(Boolean) : [];
    if (!String(b.name || '').trim()) return 'name is required';
    if (!String(b.sql_definition || '').trim()) return 'sql_definition is required';
    if (!String(b.label_column || '').trim()) return 'An EOL needs a label column';
    if (!String(b.timestamp_column || '').trim()) return 'An EOL needs a timestamp column';
    if (ents.length === 0) return 'An EOL needs at least one entity column';
    const cols = await describeColumns(req, `SELECT * FROM (${String(b.sql_definition).trim().replace(/;$/, '')}) AS _eol`);
    const absent = [b.label_column, b.timestamp_column, ...ents].filter((c: string) => !cols.includes(c));
    if (absent.length) return `Not in the EOL SQL output: ${absent.join(', ')}`;
    return null;
  }
  app.get('/api/projects/:projectId/eols', async (req, res) => {
    try {
      const result = await db.query(
        'SELECT * FROM app.entity_observation_label WHERE project_id = $1 ORDER BY id',
        [req.params.projectId]
      );
      res.json(result.rows);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/projects/:projectId/eols', async (req, res) => {
    try {
      const invalid = await validateEol(req, req.body);
      if (invalid) { res.status(400).json({ error: invalid }); return; }
      const { name, sql_definition, label_column, entity_columns, timestamp_column } = req.body;
      const result = await db.query(
        `INSERT INTO app.entity_observation_label (project_id, name, sql_definition, label_column, entity_columns, timestamp_column)
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING *`,
        [req.params.projectId, name, sql_definition, label_column, entity_columns, timestamp_column]
      );
      res.status(201).json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.put('/api/eols/:id', async (req, res) => {
    try {
      const invalid = await validateEol(req, req.body);
      if (invalid) { res.status(400).json({ error: invalid }); return; }
      const { name, sql_definition, label_column, entity_columns, timestamp_column } = req.body;
      const result = await db.query(
        `UPDATE app.entity_observation_label SET name=$1, sql_definition=$2, label_column=$3, entity_columns=$4, timestamp_column=$5
         WHERE id=$6 RETURNING *`,
        [name, sql_definition, label_column, entity_columns, timestamp_column, req.params.id]
      );
      res.json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.delete('/api/eols/:id', async (req, res) => {
    try {
      await db.query('DELETE FROM app.entity_observation_label WHERE id = $1', [req.params.id]);
      res.json({ ok: true });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // Copy an EOL to create a new version
  app.post('/api/eols/:id/copy', async (req, res) => {
    try {
      const src = await db.query('SELECT * FROM app.entity_observation_label WHERE id = $1', [req.params.id]);
      if (src.rows.length === 0) { res.status(404).json({ error: 'EOL not found' }); return; }
      const s = src.rows[0];
      const newName = req.body.name || `${s.name} (copy)`;
      const result = await db.query(
        `INSERT INTO app.entity_observation_label (project_id, name, sql_definition, label_column, entity_columns, timestamp_column)
         VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
        [s.project_id, newName, s.sql_definition, s.label_column, s.entity_columns, s.timestamp_column]
      );
      res.status(201).json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ════════════════════════════════════════════
  //  TRAINING SPECS (consolidated: features + split + params)
  // ════════════════════════════════════════════
  app.get('/api/projects/:projectId/training-specs', async (req, res) => {
    try {
      const result = await db.query(
        'SELECT * FROM app.training_spec WHERE project_id = $1 ORDER BY id',
        [req.params.projectId]
      );
      const specs = [];
      for (const spec of result.rows) {
        const entries = await db.query(
          'SELECT * FROM app.feature_entry WHERE training_spec_id = $1 ORDER BY id',
          [spec.id]
        );
        const runCount = await db.query(
          'SELECT COUNT(*) as count FROM app.run WHERE training_spec_id = $1',
          [spec.id]
        );
        specs.push({ ...spec, entries: entries.rows, run_count: parseInt(runCount.rows[0].count), locked: await specLocked(spec.id) });
      }
      res.json(specs);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/projects/:projectId/training-specs', async (req, res) => {
    try {
      const { eol_id, name, task_type, split_strategy, split_method, split_config, parameters, excluded_features } = req.body;
      const result = await db.query(
        `INSERT INTO app.training_spec (project_id, eol_id, name, task_type, split_strategy, split_method, split_config, parameters, excluded_features)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
        [req.params.projectId, eol_id, name, task_type || 'classification',
         split_strategy || 'none', split_method || null,
         split_config ? JSON.stringify(split_config) : null,
         parameters ? JSON.stringify(parameters) : null,
         excluded_features || null]
      );
      res.status(201).json({ ...result.rows[0], entries: [], run_count: 0 });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // A spec is locked once a run has succeeded (its model/tables must stay traceable to it) or while one is
  // in flight (the run would otherwise succeed against a spec that has since changed). Failed runs don't lock.
  async function specLocked(specId: number | string): Promise<boolean> {
    const r = await db.query(`SELECT 1 FROM app.run WHERE training_spec_id = $1 AND status <> 'FAILED' LIMIT 1`, [specId]);
    return r.rows.length > 0;
  }

  app.put('/api/training-specs/:id', async (req, res) => {
    try {
      // Lock check: reject edits once a run succeeded / is in flight
      if (await specLocked(req.params.id)) {
        res.status(400).json({ error: 'Training spec is locked — it has a successful or in-progress run. Copy it to make changes.' }); return;
      }
      const jsonCols = new Set(['split_config', 'parameters']);
      const allowed = ['eol_id', 'name', 'task_type', 'split_strategy', 'split_method', 'split_config', 'parameters', 'excluded_features'];
      const sets: string[] = [];
      const values: any[] = [];
      for (const key of allowed) {
        if (Object.prototype.hasOwnProperty.call(req.body, key)) {
          const v = req.body[key];
          sets.push(`${key}=$${values.length + 1}`);
          values.push(jsonCols.has(key) ? (v ? JSON.stringify(v) : null) : v);
        }
      }
      if (sets.length === 0) {
        const current = await db.query('SELECT * FROM app.training_spec WHERE id = $1', [req.params.id]);
        res.json(current.rows[0]); return;
      }
      values.push(req.params.id);
      const result = await db.query(
        `UPDATE app.training_spec SET ${sets.join(', ')} WHERE id=$${values.length} RETURNING *`,
        values
      );
      res.json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // DELETE /api/training-specs/:id (+ delete-plan) lives with RUN DELETION below — it deletes every run's assets.

  // Copy a training spec with all feature entries.
  // Body can override any of: name, eol_id, task_type, split_strategy, split_method, split_config, parameters, excluded_features
  app.post('/api/training-specs/:id/copy', async (req, res) => {
    try {
      const src = await db.query('SELECT * FROM app.training_spec WHERE id = $1', [req.params.id]);
      if (src.rows.length === 0) { res.status(404).json({ error: 'Not found' }); return; }
      const s = src.rows[0];
      const b = req.body || {};
      const pick = <K extends keyof typeof s>(k: K) => (Object.prototype.hasOwnProperty.call(b, k) ? b[k] : s[k]);
      const newName = b.name || `${s.name} (copy)`;
      const newSplitConfig = Object.prototype.hasOwnProperty.call(b, 'split_config')
        ? (b.split_config ? JSON.stringify(b.split_config) : null) : s.split_config;
      const newParameters = Object.prototype.hasOwnProperty.call(b, 'parameters')
        ? (b.parameters ? JSON.stringify(b.parameters) : null) : s.parameters;
      const specResult = await db.query(
        `INSERT INTO app.training_spec (project_id, eol_id, name, task_type, split_strategy, split_method, split_config, parameters, excluded_features)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
        [s.project_id, pick('eol_id'), newName, pick('task_type'), pick('split_strategy'), pick('split_method'),
         newSplitConfig, newParameters, pick('excluded_features')]
      );
      const newSpec = specResult.rows[0];
      const entries = await db.query('SELECT * FROM app.feature_entry WHERE training_spec_id = $1', [req.params.id]);
      const copiedEntries = [];
      for (const e of entries.rows) {
        const entryResult = await db.query(
          `INSERT INTO app.feature_entry
           (training_spec_id, feature_type, table_name, feature_names, lookup_key,
            timestamp_lookup_key, output_name, default_values,
            function_name, input_bindings, declarative_spec)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
          [newSpec.id, e.feature_type, e.table_name, e.feature_names, e.lookup_key,
           e.timestamp_lookup_key, e.output_name, e.default_values,
           e.function_name, e.input_bindings, e.declarative_spec]
        );
        copiedEntries.push(entryResult.rows[0]);
      }
      res.status(201).json({ ...newSpec, entries: copiedEntries, run_count: 0 });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // Feature entries for training specs
  app.post('/api/training-specs/:specId/entries', async (req, res) => {
    try {
      // Lock check
      if (await specLocked(req.params.specId)) {
        res.status(400).json({ error: 'Training spec is locked — it has a successful or in-progress run. Copy it to make changes.' }); return;
      }
      const {
        feature_type, table_name, feature_names, lookup_key,
        timestamp_lookup_key, output_name, default_values,
        function_name, input_bindings, declarative_spec
      } = req.body;
      const result = await db.query(
        `INSERT INTO app.feature_entry
         (training_spec_id, feature_type, table_name, feature_names, lookup_key,
          timestamp_lookup_key, output_name, default_values,
          function_name, input_bindings, declarative_spec)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
        [req.params.specId, feature_type, table_name, feature_names, lookup_key,
         timestamp_lookup_key, output_name, default_values ? JSON.stringify(default_values) : null,
         function_name || null, input_bindings ? JSON.stringify(input_bindings) : null,
         declarative_spec ? JSON.stringify(declarative_spec) : null]
      );
      res.status(201).json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.put('/api/training-spec-entries/:id', async (req, res) => {
    try {
      const entry = await db.query('SELECT training_spec_id FROM app.feature_entry WHERE id = $1', [req.params.id]);
      if (entry.rows.length === 0) { res.status(404).json({ error: 'Entry not found' }); return; }
      if (entry.rows[0].training_spec_id) {
        if (await specLocked(entry.rows[0].training_spec_id)) {
          res.status(400).json({ error: 'Training spec is locked — it has a successful or in-progress run. Copy it to make changes.' }); return;
        }
      }
      const {
        feature_type, table_name, feature_names, lookup_key,
        timestamp_lookup_key, output_name, default_values,
        function_name, input_bindings, declarative_spec
      } = req.body;
      const result = await db.query(
        `UPDATE app.feature_entry SET
          feature_type=$1, table_name=$2, feature_names=$3, lookup_key=$4,
          timestamp_lookup_key=$5, output_name=$6, default_values=$7,
          function_name=$8, input_bindings=$9, declarative_spec=$10
         WHERE id=$11 RETURNING *`,
        [feature_type, table_name, feature_names, lookup_key,
         timestamp_lookup_key, output_name, default_values ? JSON.stringify(default_values) : null,
         function_name || null, input_bindings ? JSON.stringify(input_bindings) : null,
         declarative_spec ? JSON.stringify(declarative_spec) : null,
         req.params.id]
      );
      res.json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.delete('/api/training-spec-entries/:id', async (req, res) => {
    try {
      // Check lock via the entry's parent spec
      const entry = await db.query('SELECT training_spec_id FROM app.feature_entry WHERE id = $1', [req.params.id]);
      if (entry.rows.length > 0 && entry.rows[0].training_spec_id) {
        if (await specLocked(entry.rows[0].training_spec_id)) {
          res.status(400).json({ error: 'Training spec is locked — it has a successful or in-progress run. Copy it to make changes.' }); return;
        }
      }
      await db.query('DELETE FROM app.feature_entry WHERE id = $1', [req.params.id]);
      res.json({ ok: true });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ── Runs for training specs ──
  app.get('/api/projects/:projectId/spec-runs', async (req, res) => {
    try {
      const result = await db.query(
        'SELECT * FROM app.run WHERE project_id = $1 AND training_spec_id IS NOT NULL ORDER BY id DESC',
        [req.params.projectId]
      );
      res.json(result.rows);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/training-specs/:specId/runs', async (req, res) => {
    try {
      const spec = (await db.query('SELECT * FROM app.training_spec WHERE id = $1', [req.params.specId])).rows[0];
      if (!spec) { res.status(404).json({ error: 'Training spec not found' }); return; }
      const result = await db.query(
        `INSERT INTO app.run (project_id, training_spec_id, status)
         VALUES ($1,$2,'PENDING') RETURNING *`,
        [spec.project_id, req.params.specId]
      );
      res.status(201).json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // UC feature names the FV bridge gives a spec's entries (must match buildFvEntries). Includes the hidden
  // column_selection upstreams of custom UDF inputs, so it's a (safe) superset of what a spec creates.
  function specFeatureNames(entries: any[]): Set<string> {
    const names = new Set<string>();
    for (const e of entries) {
      if (e.feature_type === 'lookup' && e.table_name) for (const col of (e.feature_names || [])) names.add(fvName(col));
      else if (e.feature_type === 'declarative' && e.declarative_spec) {
        const ds = e.declarative_spec;
        names.add(ds.name || fvName(`${ds.function}_${ds.input}_${ds.time_window?.window_duration || ''}`));
      } else if (e.feature_type === 'on_demand' && e.function_name) {
        names.add(fvName(e.output_name || e.function_name.split('.').pop()));
        for (const col of Object.values(e.input_bindings || {})) names.add(fvName(col as string));
      }
    }
    return names;
  }

  // ── Feature View bridge ──────────────────────────────────────────────
  // Translate a spec's feature_entry rows (lookup/on_demand/declarative) into the declarative
  // schema the FV notebooks consume (column_selection / aggregation / custom_udf), so specs built
  // with the old types work through the Feature View API without a UI change. Every feature shares
  // the EOL observation timestamp (tsCol). Used by both training (launch) and serving materialization.
  async function buildFvEntries(req: any, entries: any[], eol: any): Promise<{ featureEntries: any[]; tsCol: string }> {
    const tsCol: string = eol.timestamp_column;
    if (!tsCol) {
      throw new Error(`EOL "${eol.name}" has no timestamp column — the Feature View API needs an observation timestamp (e.g. transaction_ts) selected in the spine.`);
    }
    const fname = fvName;

    // Pre-fetch columns + entity for each distinct lookup table (to resolve custom_udf inputs).
    const lookupTables = new Map<string, { entity: string[]; columns: Set<string> }>();
    for (const e of entries) {
      if (e.feature_type === 'lookup' && e.table_name && !lookupTables.has(e.table_name)) {
        const [cat, sch, tbl] = e.table_name.split('.');
        let cols: string[] = [];
        try {
          const rows = await executeSql(req,
            `SELECT column_name FROM ${cat}.information_schema.columns WHERE table_schema='${sch}' AND table_name='${tbl}'`);
          cols = rows.map((r: any) => r.column_name);
        } catch (err: any) { console.warn(`[bridge] column fetch for ${e.table_name}: ${err.message}`); }
        lookupTables.set(e.table_name, { entity: e.lookup_key || [], columns: new Set(cols) });
      }
    }
    const findTable = (col: string): { table: string; entity: string[] } | null => {
      for (const [table, info] of lookupTables) if (info.columns.has(col)) return { table, entity: info.entity };
      return null;
    };

    const featureEntries: any[] = [];
    const colSelByColumn = new Map<string, string>(); // source column -> column_selection feature name

    for (const e of entries) {
      if (e.feature_type !== 'lookup' || !e.table_name) continue;
      for (const col of (e.feature_names || [])) {
        const nm = fname(col);
        featureEntries.push({ type: 'column_selection', source_table: e.table_name, column: col, entity: e.lookup_key || [], name: nm, include: true });
        colSelByColumn.set(col, nm);
      }
    }
    for (const e of entries) {
      if (e.feature_type !== 'declarative' || !e.declarative_spec) continue;
      const ds = e.declarative_spec;
      const nm = ds.name || fname(`${ds.function}_${ds.input}_${ds.time_window?.window_duration || ''}`);
      featureEntries.push({ type: 'aggregation', source_table: ds.source_table, input: ds.input, function: ds.function,
        window: ds.time_window, filter_condition: ds.filter_condition || null,
        entity: ds.lookup_key || eol.entity_columns || [], name: nm, include: true });
    }
    for (const e of entries) {
      if (e.feature_type !== 'on_demand' || !e.function_name) continue;
      const bindings = e.input_bindings || {};
      const fvBindings: Record<string, string> = {};
      for (const [param, col] of Object.entries(bindings)) {
        let up = colSelByColumn.get(col as string);
        if (!up) {
          const src = findTable(col as string);
          if (!src) throw new Error(`custom_udf '${e.function_name}' input column '${col}' is not in any lookup table of this spec`);
          up = fname(col as string);
          featureEntries.push({ type: 'column_selection', source_table: src.table, column: col, entity: src.entity, name: up, include: false });
          colSelByColumn.set(col as string, up);
        }
        fvBindings[param] = up;
      }
      featureEntries.push({ type: 'custom_udf', function_name: e.function_name, bindings: fvBindings, name: fname(e.output_name || e.function_name.split('.').pop()), include: true });
    }
    return { featureEntries, tsCol };
  }

  // Launch a training spec run — single job: materialize + train + fe.log_model()
  app.post('/api/spec-runs/:runId/launch', async (req, res) => {
    try {
      const runResult = await db.query('SELECT * FROM app.run WHERE id = $1', [req.params.runId]);
      if (runResult.rows.length === 0) { res.status(404).json({ error: 'Run not found' }); return; }
      const run = runResult.rows[0];

      const spec = (await db.query('SELECT * FROM app.training_spec WHERE id = $1', [run.training_spec_id])).rows[0];
      const project = (await db.query('SELECT * FROM app.project WHERE id = $1', [run.project_id])).rows[0];
      const eol = (await db.query('SELECT * FROM app.entity_observation_label WHERE id = $1', [spec.eol_id])).rows[0];
      const entries = (await db.query('SELECT * FROM app.feature_entry WHERE training_spec_id = $1 ORDER BY id', [spec.id])).rows;

      // Translate the spec's feature_entry rows into the Feature View schema (shared by
      // training and serving-materialization). See buildFvEntries.
      const { featureEntries, tsCol } = await buildFvEntries(req, entries, eol);

      const entityColumns = Array.isArray(eol.entity_columns)
        ? eol.entity_columns
        : (eol.entity_columns || '').replace(/^\{|\}$/g, '').split(',').filter(Boolean);

      const experimentName = project.name;
      const userName = await getUsername(req);

      // Select notebook based on split_strategy
      let notebookFile: string;
      if (spec.split_strategy === 'train_eval') {
        notebookFile = 'train_standard.py';
      } else if (spec.split_strategy === 'train_eval_test') {
        // train_hpsearch.py (eval for tuning, test for the final estimate) isn't written yet.
        const msg = 'Train / Eval / Test (hyperparameter search) is not supported yet — use Train / Eval or None (CV).';
        await db.query(`UPDATE app.run SET status='FAILED', error_message=$1, ended_at=NOW() WHERE id=$2`, [msg, run.id]);
        res.status(400).json({ error: msg }); return;
      } else {
        notebookFile = 'train_cv.py';
      }

      const notebookPath = `/Workspace/Users/${userName}/.mlops/${stripPyExt(notebookFile)}`;
      const localNotebook = path.resolve(import.meta.dirname || '.', '..', 'notebooks', notebookFile);
      await uploadNotebook(req, localNotebook, notebookPath);

      // Build UNIQUE, timestamped table names so each run's resolved training set is preserved
      // (never overwritten) and the exact model can be reproduced later. Applies to every split
      // strategy, including 'none' (CV) — which previously materialized nothing.
      const specSlug = spec.name.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
      const tableTs = new Date().toISOString().replace(/[-:T]/g, '').slice(0, 14); // YYYYMMDDHHMMSS
      const trainingTableName = `${project.catalog}.${project.schema}.${specSlug}_train_${tableTs}`;
      const evalTableName = spec.split_strategy !== 'none' ? `${project.catalog}.${project.schema}.${specSlug}_eval_${tableTs}` : '';
      const testTableName = spec.split_strategy === 'train_eval_test' ? `${project.catalog}.${project.schema}.${specSlug}_test_${tableTs}` : '';

      const params: Record<string, string> = {
        eol_sql: eol.sql_definition,
        label_column: eol.label_column || '',
        entity_columns_json: JSON.stringify(entityColumns),
        feature_entries_json: JSON.stringify(featureEntries),
        excluded_features_json: JSON.stringify(spec.excluded_features || []),
        task_type: spec.task_type || 'classification',
        parameters_json: JSON.stringify(spec.parameters || {}),
        catalog: project.catalog,
        schema: project.schema,
        experiment_name: experimentName,
        timeseries_column: tsCol,
        // Materialize the resolved training set on every run (all notebooks write this table).
        training_table_name: trainingTableName,
      };

      // Add split params for standard/hpsearch
      if (spec.split_strategy !== 'none') {
        params.split_method = spec.split_method || 'random';
        params.split_config_json = JSON.stringify(spec.split_config || { eval_pct: 20, seed: 42 });
        params.eval_table_name = evalTableName;
        if (testTableName) params.test_table_name = testTableName;
      }

      console.log(`[spec-run] Launching ${notebookFile} for spec "${spec.name}" (${spec.task_type}, ${spec.split_strategy})`);

      const { job_id, run_id: jobRunId, run_url } = await createOrRunJob(
        req, `mlops-${project.name}-${specSlug}`, notebookPath, params
      );

      // Look up MLflow experiment ID
      const host = process.env.DATABRICKS_HOST || '';
      const token = await getToken(req);
      let mlflowExperimentId: string | null = null;
      try {
        const experimentFullPath = `/Users/${userName}/${experimentName}`;
        const expResp = await fetch(`${host}/api/2.0/mlflow/experiments/get-by-name?experiment_name=${encodeURIComponent(experimentFullPath)}`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        const expData: any = await expResp.json();
        if (expData.experiment?.experiment_id) mlflowExperimentId = expData.experiment.experiment_id;
      } catch { /* ignore */ }

      await db.query(
        `UPDATE app.run SET status='RUNNING', job_id=$1, run_id=$2, mlflow_experiment_id=$3,
         databricks_run_url=$4, training_table=$5, eval_table=$6, test_table=$7, started_at=NOW()
         WHERE id=$8`,
        [job_id, jobRunId, mlflowExperimentId, run_url,
         trainingTableName, evalTableName || null, testTableName || null, run.id]
      );
      res.json({ job_id, run_id: jobRunId, run_url });
    } catch (e: any) {
      console.error(`[spec-run] Error: ${e.message}`);
      res.status(500).json({ error: e.message });
    }
  });

  // Check status for a spec run — parse notebook output for metrics + mlflow_run_id
  app.post('/api/spec-runs/:id/check-status', async (req, res) => {
    try {
      const result = await db.query('SELECT * FROM app.run WHERE id = $1', [req.params.id]);
      if (result.rows.length === 0) { res.status(404).json({ error: 'Run not found' }); return; }
      const run = result.rows[0];

      if (run.status === 'SUCCESS' || run.status === 'FAILED') { res.json(run); return; }
      if (!run.run_id) { res.json(run); return; }

      const jobRun = await databricksApi(req, 'GET', `jobs/runs/get?run_id=${run.run_id}`);
      const state = jobRun.state;
      const lifeCycleState = state?.life_cycle_state;
      const resultState = state?.result_state;

      if (lifeCycleState === 'TERMINATED' && resultState === 'SUCCESS') {
        // Parse notebook output from the task run
        let mlflowRunId: string | null = null;
        let evalMetrics: Record<string, any> | null = null;
        let trainingTable: string | null = run.training_table;
        let evalTable: string | null = run.eval_table;
        let testTable: string | null = run.test_table;
        try {
          const taskRunId = jobRun.tasks?.[0]?.run_id || run.run_id;
          const output = await databricksApi(req, 'GET', `jobs/runs/get-output?run_id=${taskRunId}`);
          const nbResult = JSON.parse(output.notebook_output?.result || '{}');
          mlflowRunId = nbResult.mlflow_run_id || null;
          evalMetrics = nbResult.metrics || null;
          if (nbResult.training_table) trainingTable = nbResult.training_table;
          if (nbResult.eval_table) evalTable = nbResult.eval_table;
          if (nbResult.test_table) testTable = nbResult.test_table;
        } catch (e: any) {
          console.error(`[spec-run] Output parse error: ${e.message}`);
        }

        await db.query(
          `UPDATE app.run SET status='SUCCESS', mlflow_run_id=$1, eval_metrics=$2,
           training_table=$3, eval_table=$4, test_table=$5, ended_at=NOW() WHERE id=$6`,
          [mlflowRunId, evalMetrics ? JSON.stringify(evalMetrics) : null,
           trainingTable, evalTable, testTable, run.id]
        );
      } else if (['TERMINATED', 'INTERNAL_ERROR', 'SKIPPED'].includes(lifeCycleState)) {
        // A notebook exception ends the run as INTERNAL_ERROR (not TERMINATED) — without this the run
        // stayed RUNNING forever. Prefer the notebook's own error over the generic state message.
        let message = state?.state_message || resultState || 'Unknown error';
        try {
          const taskRunId = jobRun.tasks?.[0]?.run_id || run.run_id;
          const output = await databricksApi(req, 'GET', `jobs/runs/get-output?run_id=${taskRunId}`);
          if (output?.error) message = String(output.error).slice(0, 2000);
        } catch { /* keep the state message */ }
        await db.query(
          `UPDATE app.run SET status='FAILED', error_message=$1, ended_at=NOW() WHERE id=$2`,
          [message, run.id]
        );
      }
      const updated = await db.query('SELECT * FROM app.run WHERE id = $1', [run.id]);
      res.json(updated.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ════════════════════════════════════════════
  //  RUNS (shared by training-spec runs: delete / unregister)
  // ════════════════════════════════════════════
  // DELETE /api/runs/:id + GET /api/runs/:id/delete-plan live after the deployment helpers (RUN DELETION).

  // Clear model registration from a run (so it can be re-registered)
  app.post('/api/runs/:id/unregister', async (req, res) => {
    try {
      await db.query(
        'UPDATE app.run SET model_name=NULL, model_version=NULL, model_uri=NULL WHERE id=$1',
        [req.params.id]
      );
      const updated = await db.query('SELECT * FROM app.run WHERE id = $1', [req.params.id]);
      res.json(updated.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ════════════════════════════════════════════
  //  JOB LAUNCHER — Materialize, Train, Evaluate
  //  Pattern: jobs/create → jobs/run-now → poll
  // ════════════════════════════════════════════

  // Debug: check env vars
  app.get('/api/debug/env', (_req, res) => {
    res.json({
      DATABRICKS_HOST: process.env.DATABRICKS_HOST || '(not set)',
      DATABRICKS_TOKEN: process.env.DATABRICKS_TOKEN ? `${process.env.DATABRICKS_TOKEN.slice(0, 20)}...` : '(not set)',
      DATABRICKS_WAREHOUSE_ID: process.env.DATABRICKS_WAREHOUSE_ID || '(not set)',
    });
  });

  // Helper: create a job (or reuse existing), run it, return job_id + run_id
  async function createOrRunJob(
    req: any, jobName: string, notebookPath: string, params: Record<string, string>,
    existingJobId?: number | null, gitSource?: { git_url: string; git_branch: string }
  ) {
    let jobId = existingJobId;

    if (!jobId) {
      // Create a new job definition
      const taskDef: any = {
        task_key: 'main',
        notebook_task: {
          notebook_path: stripPyExt(notebookPath),
          base_parameters: params,
          ...(gitSource ? { source: 'GIT' } : {}),
        },
        ...(gitSource ? {
          git_source: { git_url: gitSource.git_url, git_provider: 'gitHub', git_branch: gitSource.git_branch },
        } : {}),
        environment_key: 'Default',
      };

      const createPayload = {
        name: jobName,
        tasks: [taskDef],
        environments: [{
          environment_key: 'Default',
          spec: {
            client: '1',
            dependencies: ['databricks-feature-engineering>=0.18.0'],
          },
        }],
      };
      console.log(`[job] Creating job: ${jobName}`);
      const createResult = await databricksApi(req, 'POST', 'jobs/create', createPayload);
      console.log(`[job] Create result:`, JSON.stringify(createResult).slice(0, 500));
      if (createResult.error_code) throw new Error(`Job create failed: ${createResult.message}`);
      jobId = createResult.job_id;
      console.log(`[job] Created job ${jobId}: ${jobName}`);
    } else {
      console.log(`[job] Reusing existing job ${jobId}: ${jobName}`);
    }

    // Trigger a new run with (potentially updated) params
    const runResult = await databricksApi(req, 'POST', 'jobs/run-now', {
      job_id: jobId,
      notebook_params: params,
    });
    console.log(`[job] Run result:`, JSON.stringify(runResult).slice(0, 500));
    if (runResult.error_code) throw new Error(`Job run failed: ${runResult.message}`);
    const runId = runResult.run_id;
    const host = process.env.DATABRICKS_HOST || '';
    const runUrl = `${host}/#job/${jobId}/run/${runId}`;
    console.log(`[job] Started run ${runId} for job ${jobId}: ${runUrl}`);

    return { job_id: jobId!, run_id: runId, run_url: runUrl };
  }

  // ── Register a model from a run into Unity Catalog ──
  // Uses an app-managed notebook job because mlflow.register_model() requires
  // Python SDK access to resolve artifact paths (REST API fails when DBFS root is disabled).
  app.post('/api/runs/:id/register-model', async (req, res) => {
    try {
      const result = await db.query('SELECT * FROM app.run WHERE id = $1', [req.params.id]);
      if (result.rows.length === 0) { res.status(404).json({ error: 'Run not found' }); return; }
      const run = result.rows[0];
      if (run.status !== 'SUCCESS') { res.status(400).json({ error: 'Run must be SUCCESS to register' }); return; }
      if (!run.mlflow_run_id) { res.status(400).json({ error: 'Run has no MLflow run ID' }); return; }

      const project = (await db.query('SELECT * FROM app.project WHERE id = $1', [run.project_id])).rows[0];
      const modelName = project.model_name || project.name;
      const fullModelName = `${project.catalog}.${project.schema}.${modelName}`;

      // Upload the register_model notebook to the workspace
      const userName = await getUsername(req);
      const notebookPath = `/Workspace/Users/${userName}/.mlops/register_model`;
      const localNotebook = path.resolve(import.meta.dirname || '.', '..', 'notebooks', 'register_model.py');
      await uploadNotebook(req, localNotebook, notebookPath);

      const params = {
        run_id: run.mlflow_run_id,
        model_name: fullModelName,
      };

      console.log(`[register] Launching register job: ${fullModelName} from run ${run.mlflow_run_id}`);
      const { run_id: jobRunId } = await createOrRunJob(
        req, `mlops-register-${modelName}`, notebookPath, params
      );

      // Update run to indicate registration is in progress
      await db.query(
        `UPDATE app.run SET model_name=$1, model_uri=$2 WHERE id=$3`,
        [fullModelName, `runs:/${run.mlflow_run_id}/model`, run.id]
      );

      // Poll for completion (registration jobs are fast — usually <30s)
      let attempts = 0;
      const maxAttempts = 30;
      while (attempts < maxAttempts) {
        await new Promise(r => setTimeout(r, 5000));
        attempts++;
        const jobRun = await databricksApi(req, 'GET', `jobs/runs/get?run_id=${jobRunId}`);
        const lifeCycleState = jobRun.state?.life_cycle_state;
        const resultState = jobRun.state?.result_state;

        if (lifeCycleState === 'TERMINATED' && resultState === 'SUCCESS') {
          // Get the output from the task run (not the parent job run)
          let version: string | null = null;
          try {
            // Find the task run_id
            const taskRunId = jobRun.tasks?.[0]?.run_id || jobRunId;
            const output = await databricksApi(req, 'GET', `jobs/runs/get-output?run_id=${taskRunId}`);
            const nbResult = JSON.parse(output.notebook_output?.result || '{}');
            version = nbResult.version;
            console.log(`[register] Registered ${fullModelName} v${version}`);
          } catch (e: any) {
            console.log(`[register] Registered ${fullModelName} (could not parse version: ${e.message})`);
          }

          await db.query(
            `UPDATE app.run SET model_version=$1 WHERE id=$2`,
            [version ? parseInt(version) : null, run.id]
          );
          const updated = await db.query('SELECT * FROM app.run WHERE id = $1', [run.id]);
          res.json(updated.rows[0]);
          return;
        } else if (['TERMINATED', 'INTERNAL_ERROR', 'SKIPPED'].includes(lifeCycleState)) {
          const errMsg = jobRun.state?.state_message || resultState || 'Unknown error';
          // Clear partial model info
          await db.query(
            `UPDATE app.run SET model_name=NULL, model_uri=NULL WHERE id=$1`,
            [run.id]
          );
          throw new Error(`Registration job failed: ${errMsg}`);
        }
        // Still running — continue polling
      }
      throw new Error('Registration timed out after 150s');
    } catch (e: any) {
      console.error(`[register] Error: ${e.message}`);
      res.status(500).json({ error: e.message });
    }
  });

  // ════════════════════════════════════════════
  //  FEATURE MATERIALIZATION JOBS
  // ════════════════════════════════════════════

  // Run an app-managed materialization notebook as a job, wait for it, and return the
  // parsed dbutils.notebook.exit payload ({ status, operation, results }). Throws on failure.
  async function runMaterializeOnline(
    req: any, jobName: string, params: Record<string, string>, notebookFile = 'materialize_serving.py',
    maxMinutes = 10,
  ): Promise<{ status: string; operation: string; results: any[] }> {
    const userName = await getUsername(req);
    const notebookPath = `/Workspace/Users/${userName}/.mlops/${notebookFile.replace(/\.py$/, '')}`;
    const localNotebook = path.resolve(import.meta.dirname || '.', '..', 'notebooks', notebookFile);
    await uploadNotebook(req, localNotebook, notebookPath);

    const { job_id, run_id, run_url } = await createOrRunJob(req, jobName, notebookPath, params);

    let attempts = 0;
    while (attempts < maxMinutes * 12) {
      await new Promise(r => setTimeout(r, 5000));
      attempts++;
      const jobRun = await databricksApi(req, 'GET', `jobs/runs/get?run_id=${run_id}`);
      const life = jobRun.state?.life_cycle_state;
      const result = jobRun.state?.result_state;
      if (life === 'TERMINATED' && result === 'SUCCESS') {
        // runs/get-output requires the TASK run id, not the job-level run id (the latter
        // errors for a job with tasks). A single-task job exposes it at tasks[0].run_id.
        const taskRunId = jobRun.tasks?.[0]?.run_id ?? run_id;
        const out = await databricksApi(req, 'GET', `jobs/runs/get-output?run_id=${taskRunId}`);
        const raw = out?.notebook_output?.result;
        let parsed: any = {};
        try { parsed = raw ? JSON.parse(raw) : {}; } catch { parsed = {}; }
        return { status: 'SUCCESS', operation: parsed.operation, results: parsed.results || [], ...parsed, job_id, run_id, run_url } as any;
      }
      if (['TERMINATED', 'INTERNAL_ERROR', 'SKIPPED'].includes(life)) {
        // Prefer the notebook's own error (e.g. a feature-store BadRequest) over the generic state message.
        let message = jobRun.state?.state_message || result || `${notebookFile} job failed`;
        try {
          const out = await databricksApi(req, 'GET', `jobs/runs/get-output?run_id=${jobRun.tasks?.[0]?.run_id ?? run_id}`);
          if (out?.error) message = String(out.error).slice(0, 2000);
        } catch { /* keep the state message */ }
        throw new Error(message);
      }
    }
    throw new Error(`${notebookFile} job timed out after ${maxMinutes} minutes`);
  }

  // ════════════════════════════════════════════
  //  DEPLOYMENTS
  // ════════════════════════════════════════════
  app.get('/api/projects/:projectId/deployments', async (req, res) => {
    try {
      const result = await db.query(
        'SELECT * FROM app.deployment WHERE project_id = $1 ORDER BY id DESC',
        [req.params.projectId]
      );
      res.json(result.rows);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/projects/:projectId/deployments', async (req, res) => {
    try {
      const { name, run_id, endpoint_name, endpoint_config } = req.body;
      if (!name || !run_id || !endpoint_name) {
        res.status(400).json({ error: 'name, run_id, and endpoint_name required' }); return;
      }
      const result = await db.query(
        `INSERT INTO app.deployment (project_id, name, run_id, endpoint_name, endpoint_config)
         VALUES ($1,$2,$3,$4,$5) RETURNING *`,
        [req.params.projectId, name, run_id, endpoint_name, endpoint_config ? JSON.stringify(endpoint_config) : null]
      );
      res.status(201).json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ── Online features are owned by ONLINE deployments ──────────────────────────────────────
  // Deploy reads the run's training-spec feature set, materializes ONLY what isn't online yet
  // (materialize_serving.py `ensure`), grants the serving role, and records which features + online
  // tables the deployment uses in app.deployment_feature. An online table is torn down (`release`)
  // only once NO deployment uses any feature in it, and only after the endpoint has stopped serving
  // the old model (pending_release is processed by check-status once the endpoint is READY).
  // Training/registering a model and batch deployments never put anything online.

  async function specFeatureParams(req: any, run: any, project: any) {
    const specId = run.training_spec_id;
    if (!specId) throw new Error('Run has no training spec');
    const spec = (await db.query('SELECT * FROM app.training_spec WHERE id = $1', [specId])).rows[0];
    const eol = (await db.query('SELECT * FROM app.entity_observation_label WHERE id = $1', [spec.eol_id])).rows[0];
    const entries = (await db.query('SELECT * FROM app.feature_entry WHERE training_spec_id = $1 ORDER BY id', [specId])).rows;
    const { featureEntries, tsCol } = await buildFvEntries(req, entries, eol);
    return {
      feature_entries_json: JSON.stringify(featureEntries),
      catalog: project.catalog,
      schema: project.schema,
      timeseries_column: tsCol,
      online_store_name: ONLINE_STORE_NAME,
      table_prefix: 'mlops_srv_',
      online_tables_json: '[]',
      feature_tables_json: '{}',
    };
  }

  const projectOf = async (projectId: number) =>
    (await db.query('SELECT * FROM app.project WHERE id = $1', [projectId])).rows[0];

  // Materialize whatever the run's features still need online; returns the per-feature report.
  async function ensureOnlineFeatures(req: any, dep: any, run: any): Promise<any[]> {
    const project = await projectOf(dep.project_id);
    const params = { ...(await specFeatureParams(req, run, project)), operation: 'ensure' };
    const out: any = await runMaterializeOnline(req, `mlops-matserve-${project.name}-${run.training_spec_id}`, params, 'materialize_serving.py', 30);
    return out.features || [];
  }

  // {feature_full_name: online_table} for the tables among `rows` that no deployment references any more.
  async function unusedTables(rows: { feature_full_name: string; online_table: string | null }[]) {
    const tables = [...new Set(rows.map(r => r.online_table).filter(Boolean))] as string[];
    if (tables.length === 0) return {} as Record<string, string>;
    const used = new Set((await db.query(
      'SELECT DISTINCT online_table FROM app.deployment_feature WHERE online_table = ANY($1)', [tables]
    )).rows.map((r: any) => r.online_table));
    const out: Record<string, string> = {};
    for (const r of rows) if (r.online_table && !used.has(r.online_table)) out[r.feature_full_name] = r.online_table;
    return out;
  }

  // Replace a deployment's feature registry; returns tables that became unused.
  async function setDeploymentFeatures(depId: number, features: any[]) {
    const before = (await db.query(
      'SELECT feature_full_name, online_table FROM app.deployment_feature WHERE deployment_id = $1', [depId]
    )).rows;
    await db.query('DELETE FROM app.deployment_feature WHERE deployment_id = $1', [depId]);
    for (const f of features) {
      await db.query(
        `INSERT INTO app.deployment_feature (deployment_id, feature_full_name, feature_type, online_table, last_materialization_time)
         VALUES ($1,$2,$3,$4,$5)`,
        [depId, f.feature_full_name, f.type, f.online_table || null, f.last_materialization_time || null]
      );
    }
    const tables = [...new Set(features.map(f => f.online_table).filter(Boolean))];
    await db.query('UPDATE app.deployment SET serving_tables = $1 WHERE id = $2', [tables, depId]);
    return unusedTables(before);
  }

  // Tear down online tables nobody uses. Re-checks usage first (a later deploy may have re-claimed them).
  async function releaseTables(req: any, projectId: number, candidates: Record<string, string>) {
    const rows = Object.entries(candidates || {}).map(([feature_full_name, online_table]) => ({ feature_full_name, online_table }));
    const featureTables = await unusedTables(rows);
    const tables = [...new Set(Object.values(featureTables))];
    if (tables.length === 0) return null;
    const project = await projectOf(projectId);
    console.log(`[deploy] Releasing unused online tables: ${tables.join(', ')}`);
    return runMaterializeOnline(req, `mlops-matserve-release-${project.name}`, {
      operation: 'release',
      online_tables_json: JSON.stringify(tables),
      feature_tables_json: JSON.stringify(featureTables),
      feature_entries_json: '[]',
      catalog: project.catalog,
      schema: project.schema,
      timeseries_column: '',
      online_store_name: ONLINE_STORE_NAME,
      table_prefix: 'mlops_srv_',
    }, 'materialize_serving.py', 20);
  }

  // Flatten per-table errors from a `release` run (null if none).
  const releaseErrors = (out: any): string | null => {
    const errs = (out?.released || []).flatMap((r: any) => (r.errors || []).map((e: string) => `${String(r.online_table).split('.').pop()}: ${e}`));
    return errs.length ? errs.join('; ').slice(0, 1500) : null;
  };

  async function assertModelReady(req: any, run: any) {
    if (!run?.model_name || !run?.model_version) throw Object.assign(new Error('Run has no registered model'), { status: 400 });
    const host = process.env.DATABRICKS_HOST || '';
    const token = await getToken(req);
    const mvResp = await fetch(
      `${host}/api/2.0/mlflow/unity-catalog/model-versions/get?name=${encodeURIComponent(run.model_name)}&version=${run.model_version}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const status = ((await mvResp.json()) as any).model_version?.status;
    if (status && status !== 'READY') {
      throw Object.assign(new Error(`Model version status is ${status}. Wait for it to become READY before deploying.`), { status: 400 });
    }
  }

  // Create the endpoint, or point an existing one at this model version (no-op if it already serves it).
  async function upsertEndpoint(req: any, dep: any, run: any) {
    const config = dep.endpoint_config || {};
    const served = {
      entity_name: run.model_name,
      entity_version: String(run.model_version),
      workload_size: config.workload_size || 'Small',
      scale_to_zero_enabled: config.scale_to_zero_enabled !== false,
    };
    const existing = await databricksApi(req, 'GET', `serving-endpoints/${encodeURIComponent(dep.endpoint_name)}`, undefined, 'api/2.0');
    if (existing && !existing.error_code) {
      const cur = existing.config?.served_entities?.[0];
      if (cur?.entity_name === served.entity_name && String(cur?.entity_version) === served.entity_version) return 'unchanged';
      const r = await databricksApi(req, 'PUT', `serving-endpoints/${encodeURIComponent(dep.endpoint_name)}/config`, { served_entities: [served] }, 'api/2.0');
      if (r?.error_code) throw new Error(`Endpoint update failed: ${r.message}`);
      return 'updated';
    }
    const r = await databricksApi(req, 'POST', 'serving-endpoints', { name: dep.endpoint_name, config: { served_entities: [served] } }, 'api/2.0');
    if (r?.error_code) throw new Error(`Endpoint create failed: ${r.message}`);
    return 'created';
  }

  const setStage = (id: number, stage: string, error: string | null = null) =>
    db.query('UPDATE app.deployment SET deploy_stage = $1, deploy_error = $2 WHERE id = $3', [stage, error, id]);
  const mergeRelease = async (id: number, add: Record<string, string>) => {
    if (Object.keys(add).length === 0) return;
    await db.query(
      `UPDATE app.deployment SET pending_release = COALESCE(pending_release, '{}'::jsonb) || $1::jsonb WHERE id = $2`,
      [JSON.stringify(add), id]
    );
  };
  const deploying = new Set<number>();
  const releasing = new Set<number>();

  // Deploy (or switch the model of) an online deployment, in the background:
  //   FEATURES (ensure online + grant) → ENDPOINT (create / update config) → DONE  |  FAILED (+ deploy_error)
  // The endpoint then reports CREATING → READY via check-status. Idempotent: re-running on a healthy
  // deployment materializes nothing new and leaves an endpoint that already serves the version alone.
  async function startDeploy(req: any, res: any, depId: number, runId?: number) {
    const dep = (await db.query('SELECT * FROM app.deployment WHERE id = $1', [depId])).rows[0];
    if (!dep) { res.status(404).json({ error: 'Deployment not found' }); return; }
    if (deploying.has(dep.id)) { res.status(409).json({ error: 'A deploy is already in progress for this deployment' }); return; }
    const run = (await db.query('SELECT * FROM app.run WHERE id = $1', [runId || dep.run_id])).rows[0];
    if (!run) { res.status(404).json({ error: 'Run not found' }); return; }
    try { await assertModelReady(req, run); } catch (e: any) { res.status(e.status || 500).json({ error: e.message }); return; }

    deploying.add(dep.id);
    await setStage(dep.id, 'FEATURES');
    res.json((await db.query('SELECT * FROM app.deployment WHERE id = $1', [dep.id])).rows[0]);

    (async () => {
      try {
        const features = await ensureOnlineFeatures(req, dep, run);
        // Tables the previous model used but this one doesn't — released only once the endpoint is
        // READY on the new version (check-status), never while it might still be serving the old one.
        await mergeRelease(dep.id, await setDeploymentFeatures(dep.id, features));
        await setStage(dep.id, 'ENDPOINT');
        const action = await upsertEndpoint(req, dep, run);
        await db.query(
          `UPDATE app.deployment SET run_id = $1, endpoint_status = $2 WHERE id = $3`,
          [run.id, action === 'unchanged' && dep.endpoint_status === 'READY' ? 'READY' : 'CREATING', dep.id]
        );
        await setStage(dep.id, 'DONE');
        console.log(`[deploy] ${dep.name}: ${features.length} features online, endpoint ${action}`);
      } catch (e: any) {
        console.error(`[deploy] ${dep.name} failed: ${e.message}`);
        await setStage(dep.id, 'FAILED', String(e.message).slice(0, 4000));
      } finally {
        deploying.delete(dep.id);
      }
    })();
  }

  app.post('/api/deployments/:id/deploy', async (req, res) => {
    try { await startDeploy(req, res, Number(req.params.id)); }
    catch (e: any) { if (!res.headersSent) res.status(500).json({ error: e.message }); }
  });

  // The features a deployment serves from (registry rows), enriched LIVE from the feature-engineering API
  // with each online materialization's actual schedule — so "Auto" can be shown as what it resolved to
  // (e.g. DERIVED cron "0 0 0 * * ?" = daily 00:00 UTC) — plus pipeline state and last materialization.
  async function deploymentFeaturesLive(req: any, depId: number | string) {
    const rows = (await db.query(
      'SELECT * FROM app.deployment_feature WHERE deployment_id = $1 ORDER BY feature_type, feature_full_name', [depId]
    )).rows;
    return Promise.all(rows.map(async (r: any) => {
      if (!r.online_table) return { ...r, schedule: { kind: 'on_request' }, pipeline_state: null };
      try {
        const out = await databricksApi(req, 'GET',
          `feature-engineering/materialized-features?feature_name=${encodeURIComponent(r.feature_full_name)}`, undefined, 'api/2.0');
        const mf = (out.materialized_features || []).find((m: any) => m.table_name === r.online_table);
        if (!mf) return { ...r, schedule: null, pipeline_state: 'MISSING' };
        const cron = mf.cron_schedule_trigger;
        const schedule = cron
          ? { kind: 'cron', cron_expression: cron.cron_expression, timezone_id: cron.timezone_id || 'UTC', mode: cron.mode }
          : mf.table_trigger ? { kind: 'table_trigger' } : null;
        const last = mf.last_materialization_time || r.last_materialization_time;
        if (mf.last_materialization_time && mf.last_materialization_time !== r.last_materialization_time) {
          await db.query('UPDATE app.deployment_feature SET last_materialization_time = $1 WHERE deployment_id = $2 AND feature_full_name = $3',
            [mf.last_materialization_time, depId, r.feature_full_name]);
        }
        return { ...r, last_materialization_time: last, schedule, pipeline_state: mf.pipeline_schedule_state || null };
      } catch (e: any) {
        return { ...r, schedule: null, pipeline_state: null, live_error: String(e.message).slice(0, 200) };
      }
    }));
  }

  app.get('/api/deployments/:id/features', async (req, res) => {
    try { res.json(await deploymentFeaturesLive(req, req.params.id)); }
    catch (e: any) { res.status(500).json({ error: e.message }); }
  });
  // Kept for the Refresh button: the read is live now, so refresh = re-read.
  app.post('/api/deployments/:id/features/refresh', async (req, res) => {
    try { res.json(await deploymentFeaturesLive(req, req.params.id)); }
    catch (e: any) { res.status(500).json({ error: e.message }); }
  });

  // Check serving endpoint status
  app.post('/api/deployments/:id/check-status', async (req, res) => {
    try {
      const depResult = await db.query('SELECT * FROM app.deployment WHERE id = $1', [req.params.id]);
      if (depResult.rows.length === 0) { res.status(404).json({ error: 'Deployment not found' }); return; }
      const dep = depResult.rows[0];

      if (dep.endpoint_status === 'NOT_CREATED') { res.json(dep); return; }

      const apiResult = await databricksApi(req, 'GET', `serving-endpoints/${encodeURIComponent(dep.endpoint_name)}`, undefined, 'api/2.0');
      const readyState = apiResult.state?.ready;

      // A config update in progress wins: during "Update model" the endpoint stays ready=READY on the
      // OLD version until the new one is live, so checking ready first reported the switch done early.
      let newStatus = dep.endpoint_status;
      if (apiResult.state?.config_update === 'IN_PROGRESS') {
        newStatus = 'CREATING';
      } else if (readyState === 'READY') {
        newStatus = 'READY';
      }
      // Check for failure
      if (apiResult.error_code) {
        newStatus = 'FAILED';
      }

      if (newStatus !== dep.endpoint_status) {
        await db.query('UPDATE app.deployment SET endpoint_status=$1 WHERE id=$2', [newStatus, dep.id]);
      }
      // Tables the previous model used are torn down only now that the endpoint serves the new one.
      if (newStatus === 'READY' && dep.pending_release && Object.keys(dep.pending_release).length > 0 && !releasing.has(dep.id)) {
        releasing.add(dep.id);
        const pending = dep.pending_release;
        // Cleared either way (no retry loop on every status check); failures are surfaced on the card.
        // Typical failure: "Only the creator can delete" — the pipeline was created by another identity
        // (e.g. a user via local dev), so the app SP can't remove it.
        releaseTables(req, dep.project_id, pending)
          .then((out: any) => {
            const errs = releaseErrors(out);
            return db.query('UPDATE app.deployment SET pending_release = NULL, deploy_error = $1 WHERE id = $2',
              [errs ? `Previous model's online tables not fully removed: ${errs}` : null, dep.id]);
          })
          .catch((e: any) => db.query('UPDATE app.deployment SET pending_release = NULL, deploy_error = $1 WHERE id = $2',
            [`Removing the previous model's online tables failed: ${String(e.message).slice(0, 1500)}`, dep.id]))
          .finally(() => releasing.delete(dep.id));
      }
      const updated = await db.query('SELECT * FROM app.deployment WHERE id = $1', [dep.id]);
      res.json(updated.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // Test inference on a deployment's endpoint
  // Get sample entity key values from a deployment's training table
  app.get('/api/deployments/:id/samples', async (req, res) => {
    try {
      const depResult = await db.query('SELECT * FROM app.deployment WHERE id = $1', [req.params.id]);
      if (depResult.rows.length === 0) { res.status(404).json({ error: 'Deployment not found' }); return; }
      const dep = depResult.rows[0];

      // Trace: deployment → run → training_spec → EOL
      const run = (await db.query('SELECT * FROM app.run WHERE id = $1', [dep.run_id])).rows[0];
      let eol: any;
      if (run.training_spec_id) {
        const spec = (await db.query('SELECT * FROM app.training_spec WHERE id = $1', [run.training_spec_id])).rows[0];
        eol = (await db.query('SELECT * FROM app.entity_observation_label WHERE id = $1', [spec.eol_id])).rows[0];
      }
      if (!eol) { res.json([]); return; }

      const entityCols = Array.isArray(eol.entity_columns)
        ? eol.entity_columns
        : (eol.entity_columns || '').replace(/^\{|\}$/g, '').split(',').filter(Boolean);

      if (entityCols.length === 0 || !eol.sql_definition) {
        res.json([]); return;
      }

      // Return full spine rows (every model input = entity keys + direct/pass-through
      // feature columns), minus the label. The model signature marks the direct
      // features (e.g. amount, pos_entry_mode, security_code) as required, not just
      // the entity keys — so the test form must prefill ALL input columns or the
      // endpoint rejects the request with "Model is missing inputs [...]".
      const label = (eol.label_column || '').trim();
      const eolSql = eol.sql_definition.trim().replace(/;$/, '');
      const rows = await executeSql(req, `SELECT * FROM (${eolSql}) AS _eol LIMIT 3`);
      const cleaned = rows.map((r: any) => {
        if (label && label in r) { const { [label]: _drop, ...rest } = r; return rest; }
        return r;
      });
      res.json(cleaned);
    } catch (e: any) {
      console.error(`[samples] Error: ${e.message}`);
      res.status(500).json({ error: e.message });
    }
  });

  // Return the model's serving input schema, split into what the CALLER must provide
  // vs what is looked up automatically. The endpoint OpenAPI marks the spine columns
  // (entity keys + request-time/pass-through features like amount, pos_entry_mode) as
  // `required`; the FeatureLookup-resolved features are optional (served from the
  // online store), so the test form only needs to collect the required ones.
  app.get('/api/deployments/:id/schema', async (req, res) => {
    try {
      const dep = (await db.query('SELECT * FROM app.deployment WHERE id = $1', [req.params.id])).rows[0];
      if (!dep) { res.status(404).json({ error: 'Deployment not found' }); return; }

      const oa = await databricksApi(
        req, 'GET',
        `serving-endpoints/${encodeURIComponent(dep.endpoint_name)}/openapi`,
        undefined, 'api/2.0'
      );

      // Find the invocations request body's dataframe_records variant (name/type map + required list).
      let items: any = null;
      for (const p of Object.values(oa?.paths || {}) as any[]) {
        const oneOf = p?.post?.requestBody?.content?.['application/json']?.schema?.oneOf || [];
        for (const opt of oneOf) {
          if (opt?.properties?.dataframe_records?.items) { items = opt.properties.dataframe_records.items; break; }
        }
        if (items) break;
      }

      const props: Record<string, any> = items?.properties || {};
      const required: string[] = Array.isArray(items?.required) ? items.required : [];
      const types: Record<string, string> = {};
      for (const [k, v] of Object.entries(props)) types[k] = (v as any)?.type || 'string';
      const optional = Object.keys(props).filter((c) => !required.includes(c));

      res.json({ required, optional, types });
    } catch (e: any) {
      console.error(`[schema] Error: ${e.message}`);
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/deployments/:id/test', async (req, res) => {
    try {
      const depResult = await db.query('SELECT * FROM app.deployment WHERE id = $1', [req.params.id]);
      if (depResult.rows.length === 0) { res.status(404).json({ error: 'Deployment not found' }); return; }
      const dep = depResult.rows[0];
      if (dep.endpoint_status !== 'READY') { res.status(400).json({ error: 'Endpoint not ready' }); return; }

      const host = process.env.DATABRICKS_HOST || '';
      const token = await getToken(req);
      const payload = req.body;

      const resp = await fetch(`${host}/serving-endpoints/${encodeURIComponent(dep.endpoint_name)}/invocations`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await resp.json();
      res.status(resp.status).json(data);
    } catch (e: any) {
      console.error(`[test] Error: ${e.message}`);
      res.status(500).json({ error: e.message });
    }
  });

  // Update model: same flow as Deploy with a different run — features for the new model go online
  // BEFORE the endpoint switches; tables only the old model used are released once it's READY.
  app.post('/api/deployments/:id/update-endpoint', async (req, res) => {
    try {
      const { run_id } = req.body;
      if (!run_id) { res.status(400).json({ error: 'run_id required' }); return; }
      await startDeploy(req, res, Number(req.params.id), Number(run_id));
    } catch (e: any) {
      console.error(`[endpoint] Update error: ${e.message}`);
      if (!res.headersSent) res.status(500).json({ error: e.message });
    }
  });

  // Stop: deletes the endpoint but keeps the deployment and its online features (quick restart via Deploy).
  app.post('/api/deployments/:id/stop-endpoint', async (req, res) => {
    try {
      const depResult = await db.query('SELECT * FROM app.deployment WHERE id = $1', [req.params.id]);
      if (depResult.rows.length === 0) { res.status(404).json({ error: 'Deployment not found' }); return; }
      const dep = depResult.rows[0];

      if (dep.endpoint_status === 'NOT_CREATED') { res.json(dep); return; }

      try {
        await databricksApi(req, 'DELETE', `serving-endpoints/${encodeURIComponent(dep.endpoint_name)}`, undefined, 'api/2.0');
        console.log(`[deployment] Stopped endpoint: ${dep.endpoint_name}`);
      } catch (e: any) {
        console.warn(`[deployment] Endpoint stop warning: ${e.message}`);
      }

      await db.query('UPDATE app.deployment SET endpoint_status=$1 WHERE id=$2', ['NOT_CREATED', dep.id]);
      const updated = await db.query('SELECT * FROM app.deployment WHERE id = $1', [dep.id]);
      res.json(updated.rows[0]);
    } catch (e: any) {
      console.error(`[deployment] Stop error: ${e.message}`);
      res.status(500).json({ error: e.message });
    }
  });

  app.delete('/api/deployments/:id', async (req, res) => {
    try {
      const depResult = await db.query('SELECT * FROM app.deployment WHERE id = $1', [req.params.id]);
      if (depResult.rows.length === 0) { res.status(404).json({ error: 'Deployment not found' }); return; }
      const dep = depResult.rows[0];

      // Delete endpoint from Databricks if it exists
      if (dep.endpoint_status !== 'NOT_CREATED') {
        try {
          await databricksApi(req, 'DELETE', `serving-endpoints/${encodeURIComponent(dep.endpoint_name)}`, undefined, 'api/2.0');
          console.log(`[deployment] Deleted endpoint: ${dep.endpoint_name}`);
        } catch (e: any) {
          console.warn(`[deployment] Endpoint delete warning: ${e.message}`);
        }
      }

      const rows = (await db.query(
        'SELECT feature_full_name, online_table FROM app.deployment_feature WHERE deployment_id = $1', [dep.id]
      )).rows;
      await db.query('DELETE FROM app.deployment WHERE id = $1', [req.params.id]);  // cascades registry rows
      const candidates: Record<string, string> = { ...(dep.pending_release || {}) };
      for (const r of rows) if (r.online_table) candidates[r.feature_full_name] = r.online_table;
      // Background: the endpoint delete is already requested; nothing else waits on the teardown.
      releaseTables(req, dep.project_id, candidates)
        .then((out: any) => { const errs = releaseErrors(out); if (errs) console.error(`[deploy] release after delete incomplete: ${errs}`); })
        .catch((e: any) => console.error(`[deploy] release after delete failed: ${e.message}`));
      res.json({ ok: true, releasing: Object.values(candidates) });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ════════════════════════════════════════════
  //  BATCH DEPLOYMENTS
  // ════════════════════════════════════════════
  // A batch deployment = a persistent, scheduled Databricks job running notebooks/score_batch.py with every
  // parameter baked in (model + alias, input, timestamp column, output) — it runs without the app. The job is
  // created by (and runs as) the app SP. Pause state and run history are read live from the Jobs API.
  const FQN = /^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$/;
  const ucPath = (fullName: string) => fullName.split('.').map(encodeURIComponent).join('.');

  async function batchJobSettings(req: any, b: any, project: any) {
    const userName = await getUsername(req);
    const notebookPath = `/Workspace/Users/${userName}/.mlops/score_batch`;
    await uploadNotebook(req, path.resolve(import.meta.dirname || '.', '..', 'notebooks', 'score_batch.py'), notebookPath);
    return {
      name: `mlops-batch-${project.name}-${b.name}`,
      max_concurrent_runs: 1,
      tasks: [{
        task_key: 'score',
        notebook_task: {
          notebook_path: notebookPath,
          base_parameters: {
            model_name: b.model_name,
            model_alias: b.model_alias,
            input_table: b.input_table || '',
            input_sql: b.input_sql || '',
            ts_col: b.ts_col,
            output_table: b.output_table,
            batch_deployment_id: String(b.id),
          },
        },
        environment_key: 'Default',
      }],
      environments: [{ environment_key: 'Default', spec: { client: '1', dependencies: ['databricks-feature-engineering>=0.18.0'] } }],
      ...(b.cron_expression ? {
        schedule: { quartz_cron_expression: b.cron_expression, timezone_id: b.timezone_id || 'UTC', pause_status: b._pause || 'UNPAUSED' },
      } : {}),
    };
  }

  // Columns a query/table produces (works on empty tables).
  async function describeColumns(req: any, sql: string): Promise<string[]> {
    const rows = await executeSql(req, `DESCRIBE QUERY ${sql}`);
    return rows.map((r: any) => r.col_name).filter((c: string) => c && !c.startsWith('#'));
  }

  // The model's observation-timestamp column and the request fields it expects, from the training spine
  // of the newest app run registered under this model (EOL columns minus label minus timestamp).
  async function modelSpine(req: any, projectId: number, modelName: string) {
    const run = (await db.query(
      `SELECT * FROM app.run WHERE project_id = $1 AND model_name = $2 AND training_spec_id IS NOT NULL
       ORDER BY model_version DESC NULLS LAST LIMIT 1`, [projectId, modelName]
    )).rows[0];
    if (!run) throw Object.assign(new Error(`No registered run for ${modelName} in this project`), { status: 400 });
    const spec = (await db.query('SELECT * FROM app.training_spec WHERE id = $1', [run.training_spec_id])).rows[0];
    const eol = (await db.query('SELECT * FROM app.entity_observation_label WHERE id = $1', [spec.eol_id])).rows[0];
    const tsCol = (eol.timestamp_column || '').trim();
    if (!tsCol) throw Object.assign(new Error(`EOL "${eol.name}" has no timestamp column`), { status: 400 });
    const cols = await describeColumns(req, `SELECT * FROM (${eol.sql_definition.trim().replace(/;$/, '')}) AS _eol`);
    const label = (eol.label_column || '').trim();
    return { tsCol, required: cols.filter(c => c !== label && c !== tsCol) };
  }

  function validateBatchBody(b: any) {
    if (!b.name || !b.model_name || !b.model_alias || !b.output_table) return 'name, model_name, model_alias and output_table are required';
    if (!b.input_table && !b.input_sql) return 'input_table or input_sql is required';
    if (b.input_table && !FQN.test(b.input_table)) return 'input_table must be catalog.schema.table';
    if (!FQN.test(b.output_table)) return 'output_table must be catalog.schema.table';
    if (b.cron_expression && b.cron_expression.trim().split(/\s+/).length < 6) return 'cron_expression must be a Quartz cron (6-7 fields)';
    return null;
  }

  async function checkBatchInput(req: any, projectId: number, b: any) {
    const { tsCol, required } = await modelSpine(req, projectId, b.model_name);
    const inputSql = b.input_sql ? b.input_sql.trim().replace(/;$/, '') : `SELECT * FROM ${b.input_table}`;
    const cols = await describeColumns(req, `SELECT * FROM (${inputSql}) AS _in`);
    const missing = required.filter(c => !cols.includes(c));
    if (missing.length) throw Object.assign(new Error(`Input is missing request fields the model needs: ${missing.join(', ')}`), { status: 400 });
    if (cols.includes(tsCol)) throw Object.assign(new Error(`Input must not contain ${tsCol} — the job sets it to the run time`), { status: 400 });
    return tsCol;
  }

  async function batchLive(req: any, b: any) {
    const live: any = { ...b, paused: null, runs: [], job_error: null };
    if (!b.job_id) return live;
    const job = await databricksApi(req, 'GET', `jobs/get?job_id=${b.job_id}`);
    if (job.error_code) { live.job_error = job.message; return live; }
    live.paused = job.settings?.schedule ? job.settings.schedule.pause_status === 'PAUSED' : null;
    const runs = (await databricksApi(req, 'GET', `jobs/runs/list?job_id=${b.job_id}&limit=5&expand_tasks=true`)).runs || [];
    const host = process.env.DATABRICKS_HOST || '';
    live.runs = await Promise.all(runs.map(async (r: any) => {
      const row: any = {
        run_id: r.run_id, start_time: r.start_time ? new Date(r.start_time).toISOString() : null,
        state: r.state?.result_state || r.state?.life_cycle_state, trigger: r.trigger,
        url: `${host}/#job/${b.job_id}/run/${r.run_id}`,
      };
      if (r.state?.life_cycle_state === 'TERMINATED' && r.tasks?.[0]?.run_id) {
        try {
          const out = await databricksApi(req, 'GET', `jobs/runs/get-output?run_id=${r.tasks[0].run_id}`);
          if (out?.notebook_output?.result) {
            const o = JSON.parse(out.notebook_output.result);
            row.rows = o.rows; row.model_version = o.model_version; row.scored_at = o.scored_at;
          }
          if (out?.error) row.error = String(out.error).slice(0, 500);
        } catch { /* keep basic row */ }
      }
      return row;
    }));
    return live;
  }

  app.get('/api/projects/:projectId/batch-deployments', async (req, res) => {
    try {
      const rows = (await db.query('SELECT * FROM app.batch_deployment WHERE project_id = $1 ORDER BY id DESC', [req.params.projectId])).rows;
      res.json(await Promise.all(rows.map((b: any) => batchLive(req, b))));
    } catch (e: any) { res.status(500).json({ error: e.message }); }
  });

  app.post('/api/projects/:projectId/batch-deployments', async (req, res) => {
    try {
      const b = req.body || {};
      const bad = validateBatchBody(b);
      if (bad) { res.status(400).json({ error: bad }); return; }
      const project = await projectOf(Number(req.params.projectId));
      const tsCol = await checkBatchInput(req, project.id, b);
      const row = (await db.query(
        `INSERT INTO app.batch_deployment (project_id, name, model_name, model_alias, input_table, input_sql, ts_col, output_table, cron_expression, timezone_id)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
        [project.id, b.name, b.model_name, b.model_alias, b.input_sql ? null : b.input_table, b.input_sql || null, tsCol,
          b.output_table, b.cron_expression || null, b.timezone_id || 'UTC']
      )).rows[0];
      try {
        const created = await databricksApi(req, 'POST', 'jobs/create', await batchJobSettings(req, row, project));
        if (created.error_code) throw new Error(`Job create failed: ${created.message}`);
        await db.query('UPDATE app.batch_deployment SET job_id = $1 WHERE id = $2', [created.job_id, row.id]);
        row.job_id = created.job_id;
      } catch (e) {
        await db.query('DELETE FROM app.batch_deployment WHERE id = $1', [row.id]);
        throw e;
      }
      res.status(201).json(await batchLive(req, row));
    } catch (e: any) { res.status(e.status || 500).json({ error: e.message }); }
  });

  // Edit alias / input / output / schedule → jobs/reset with the full new settings (keeps pause state).
  app.put('/api/batch-deployments/:id', async (req, res) => {
    try {
      const cur = (await db.query('SELECT * FROM app.batch_deployment WHERE id = $1', [req.params.id])).rows[0];
      if (!cur) { res.status(404).json({ error: 'Not found' }); return; }
      const b = { ...cur, ...req.body, id: cur.id, project_id: cur.project_id, job_id: cur.job_id };
      if ('input_sql' in req.body && req.body.input_sql) b.input_table = null;
      if ('input_table' in req.body && req.body.input_table) b.input_sql = null;
      const bad = validateBatchBody(b);
      if (bad) { res.status(400).json({ error: bad }); return; }
      const project = await projectOf(cur.project_id);
      b.ts_col = await checkBatchInput(req, project.id, b);
      const live = await batchLive(req, cur);
      b._pause = live.paused ? 'PAUSED' : 'UNPAUSED';
      const r = await databricksApi(req, 'POST', 'jobs/reset', { job_id: cur.job_id, new_settings: await batchJobSettings(req, b, project) });
      if (r.error_code) throw new Error(`Job update failed: ${r.message}`);
      await db.query(
        `UPDATE app.batch_deployment SET name=$1, model_alias=$2, input_table=$3, input_sql=$4, ts_col=$5, output_table=$6,
           cron_expression=$7, timezone_id=$8 WHERE id=$9`,
        [b.name, b.model_alias, b.input_table || null, b.input_sql || null, b.ts_col, b.output_table, b.cron_expression || null, b.timezone_id || 'UTC', cur.id]
      );
      res.json(await batchLive(req, (await db.query('SELECT * FROM app.batch_deployment WHERE id = $1', [cur.id])).rows[0]));
    } catch (e: any) { res.status(e.status || 500).json({ error: e.message }); }
  });

  app.post('/api/batch-deployments/:id/run-now', async (req, res) => {
    try {
      const b = (await db.query('SELECT * FROM app.batch_deployment WHERE id = $1', [req.params.id])).rows[0];
      if (!b?.job_id) { res.status(404).json({ error: 'Not found' }); return; }
      const r = await databricksApi(req, 'POST', 'jobs/run-now', { job_id: b.job_id });
      if (r.error_code) throw new Error(r.message);
      res.json({ run_id: r.run_id });
    } catch (e: any) { res.status(500).json({ error: e.message }); }
  });

  async function setBatchPause(req: any, res: any, pause: boolean) {
    try {
      const b = (await db.query('SELECT * FROM app.batch_deployment WHERE id = $1', [req.params.id])).rows[0];
      if (!b?.job_id) { res.status(404).json({ error: 'Not found' }); return; }
      if (!b.cron_expression) { res.status(400).json({ error: 'No schedule to pause' }); return; }
      const r = await databricksApi(req, 'POST', 'jobs/update', {
        job_id: b.job_id,
        new_settings: { schedule: { quartz_cron_expression: b.cron_expression, timezone_id: b.timezone_id || 'UTC', pause_status: pause ? 'PAUSED' : 'UNPAUSED' } },
      });
      if (r.error_code) throw new Error(r.message);
      res.json(await batchLive(req, b));
    } catch (e: any) { res.status(500).json({ error: e.message }); }
  }
  app.post('/api/batch-deployments/:id/pause', (req, res) => setBatchPause(req, res, true));
  app.post('/api/batch-deployments/:id/resume', (req, res) => setBatchPause(req, res, false));

  // Latest run's predictions (the newest scored_at slice of the output table).
  app.get('/api/batch-deployments/:id/preview', async (req, res) => {
    try {
      const b = (await db.query('SELECT * FROM app.batch_deployment WHERE id = $1', [req.params.id])).rows[0];
      if (!b) { res.status(404).json({ error: 'Not found' }); return; }
      if (!FQN.test(b.output_table)) { res.status(400).json({ error: 'bad output table' }); return; }
      try {
        res.json(await executeSql(req,
          `SELECT * FROM ${b.output_table} WHERE batch_deployment_id = ${Number(b.id)}
             AND scored_at = (SELECT max(scored_at) FROM ${b.output_table} WHERE batch_deployment_id = ${Number(b.id)}) LIMIT 50`));
      } catch (e: any) {
        if (/TABLE_OR_VIEW_NOT_FOUND|not found/i.test(e.message)) { res.json([]); return; }
        throw e;
      }
    } catch (e: any) { res.status(500).json({ error: e.message }); }
  });

  app.delete('/api/batch-deployments/:id', async (req, res) => {
    try {
      const b = (await db.query('SELECT * FROM app.batch_deployment WHERE id = $1', [req.params.id])).rows[0];
      if (!b) { res.json({ ok: true }); return; }
      if (b.job_id) {
        const r = await databricksApi(req, 'POST', 'jobs/delete', { job_id: b.job_id });
        if (r?.error_code && r.error_code !== 'RESOURCE_DOES_NOT_EXIST') throw new Error(`Job delete failed: ${r.message}`);
      }
      await db.query('DELETE FROM app.batch_deployment WHERE id = $1', [b.id]);
      res.json({ ok: true, output_table_kept: b.output_table });
    } catch (e: any) { res.status(500).json({ error: e.message }); }
  });

  // UC model aliases + versions (for the batch form), and setting an alias.
  // ════════════════════════════════════════════
  //  RUN DELETION — a run and every asset it owns
  // ════════════════════════════════════════════
  // A run owns: its UC model version (+ any aliases on it), its MLflow run, its persisted
  // train/eval/test tables, and its training job (every launch creates a fresh job). UC features are
  // shared by name across specs, so they're never touched. Deletion is BLOCKED while the run is still
  // training or anything serves it: an online deployment on the run, any endpoint serving the version
  // (covers an in-flight model swap), or a batch deployment whose alias resolves to the version.
  // Partial failures keep the run row (with what's left) so Delete can be retried; "already gone" = done.
  type RunAsset = { kind: 'alias' | 'model_version' | 'mlflow_run' | 'table' | 'job'; name: string };
  const isGone = (r: any) => r?.error_code === 'RESOURCE_DOES_NOT_EXIST' || r?.error_code === 'NOT_FOUND'
    || /does not exist|not found/i.test(r?.message || '');

  // `deleting` = every run being deleted together (a spec delete), so tables shared only among them are dropped.
  async function runDeletePlan(req: any, run: any, deleting: number[] = [run.id]): Promise<{ assets: RunAsset[]; blockers: string[]; kept: string[] }> {
    const blockers: string[] = [];
    const assets: RunAsset[] = [];
    if (run.status === 'RUNNING' || (run.status === 'PENDING' && run.run_id)) {
      blockers.push('Run is still training — wait for it to finish (or cancel the job), then Check status.');
    }
    if ([...deploying].length) {
      const busy = (await db.query('SELECT name FROM app.deployment WHERE id = ANY($1::bigint[])', [[...deploying]])).rows;
      if (busy.length) blockers.push(`A deploy is in progress (${busy.map((b: any) => `"${b.name}"`).join(', ')}) — retry when it finishes.`);
    }
    for (const d of (await db.query('SELECT name, endpoint_name FROM app.deployment WHERE run_id = $1', [run.id])).rows) {
      blockers.push(`Online deployment "${d.name}" (endpoint ${d.endpoint_name}) uses this run.`);
    }

    const version = run.model_name && run.model_version ? Number(run.model_version) : null;
    if (version) {
      // Live endpoint check: catches an endpoint still serving (or switching to) this version.
      const deps = (await db.query(
        `SELECT name, endpoint_name FROM app.deployment WHERE run_id <> $1 AND endpoint_status <> 'NOT_CREATED'`, [run.id]
      )).rows;
      for (const d of deps) {
        const ep = await databricksApi(req, 'GET', `serving-endpoints/${encodeURIComponent(d.endpoint_name)}`, undefined, 'api/2.0').catch(() => null);
        if (!ep || isGone(ep)) continue;
        const ents = [...(ep.config?.served_entities || []), ...(ep.pending_config?.served_entities || [])];
        if (ents.some((s: any) => s.entity_name === run.model_name && String(s.entity_version) === String(version))) {
          blockers.push(`Endpoint ${d.endpoint_name} ("${d.name}") is still serving ${run.model_name} v${version}.`);
        }
      }
      // Batch deployments resolve their alias every run — block if one currently lands on this version.
      const v = await databricksApi(req, 'GET', `unity-catalog/models/${ucPath(run.model_name)}/versions/${version}?include_aliases=true`);
      const versionAliases: string[] = v?.error_code ? [] : (v.aliases || []).map((a: any) => a.alias_name);
      for (const b of (await db.query('SELECT name, model_alias FROM app.batch_deployment WHERE model_name = $1', [run.model_name])).rows) {
        if (versionAliases.includes(b.model_alias)) {
          blockers.push(`Batch deployment "${b.name}" scores ${run.model_name}@${b.model_alias}, which points at v${version} — repoint the alias or delete the batch deployment.`);
        }
      }
      if (v?.error_code && !isGone(v)) blockers.push(`Couldn't read ${run.model_name} v${version}: ${v.message}`);
      for (const a of versionAliases) assets.push({ kind: 'alias', name: `${run.model_name}@${a}` });
      if (!v?.error_code) assets.push({ kind: 'model_version', name: `${run.model_name} v${version}` });
    }
    if (run.mlflow_run_id) assets.push({ kind: 'mlflow_run', name: run.mlflow_run_id });
    // Pre-Oct-14 runs reused fixed table names (e.g. cc_fraud_pos_1_train) — drop a table only with its LAST run.
    const kept: string[] = [];
    for (const t of new Set([run.training_table, run.eval_table, run.test_table].filter(Boolean))) {
      const others = (await db.query(
        `SELECT id FROM app.run WHERE NOT (id = ANY($1::bigint[])) AND $2 IN (training_table, eval_table, test_table) ORDER BY id`, [deleting, t]
      )).rows.map((r: any) => r.id);
      if (others.length) kept.push(`${t} (also used by run ${others.join(', ')})`);
      else assets.push({ kind: 'table', name: t as string });
    }
    if (run.job_id) assets.push({ kind: 'job', name: String(run.job_id) });
    return { assets, blockers, kept };
  }

  // Delete one asset; returns an error message, or null when deleted / already gone.
  async function deleteRunAsset(req: any, run: any, a: RunAsset): Promise<string | null> {
    let r: any;
    if (a.kind === 'alias') {
      const alias = a.name.split('@').pop()!;
      r = await databricksApi(req, 'DELETE', `unity-catalog/models/${ucPath(run.model_name)}/aliases/${encodeURIComponent(alias)}`);
    } else if (a.kind === 'model_version') {
      r = await databricksApi(req, 'DELETE', `unity-catalog/models/${ucPath(run.model_name)}/versions/${run.model_version}`);
    } else if (a.kind === 'mlflow_run') {
      r = await databricksApi(req, 'POST', 'mlflow/runs/delete', { run_id: a.name }, 'api/2.0');
    } else if (a.kind === 'job') {
      r = await databricksApi(req, 'POST', 'jobs/delete', { job_id: Number(a.name) });
    } else {
      if (!FQN.test(a.name)) return `refusing to drop unexpected table name ${a.name}`;
      try { await executeSql(req, `DROP TABLE IF EXISTS ${a.name.split('.').map(p => `\`${p}\``).join('.')}`); return null; }
      catch (e: any) { return e.message; }
    }
    return r?.error_code && !isGone(r) ? (r.message || r.error_code) : null;
  }

  // Delete a run's assets, then its row. On any failure the row is kept (minus what's gone) with an
  // error_message, so Delete can be retried. Returns the failures ([] = fully deleted).
  async function deleteRunWithAssets(req: any, run: any, assets: RunAsset[]): Promise<string[]> {
    // Aliases before the version; the version before the MLflow run it came from.
    const failed: string[] = [];
    const failedKinds = new Set<RunAsset['kind']>();
    for (const a of assets) {
      if (a.kind === 'model_version' && failedKinds.has('alias')) { failed.push(`${a.name}: skipped (alias delete failed)`); failedKinds.add(a.kind); continue; }
      const err = await deleteRunAsset(req, run, a);
      if (err) { failed.push(`${a.kind} ${a.name}: ${err}`); failedKinds.add(a.kind); }
      console.log(`[run-delete] run ${run.id} ${a.kind} ${a.name}: ${err || 'deleted'}`);
    }
    if (!failed.length) {
      await db.query('DELETE FROM app.run WHERE id = $1', [run.id]);
      return failed;
    }
    // Keep the row so Delete can be retried; forget what's gone so the retry skips it.
    const dropped = new Set(assets.filter(a => a.kind === 'table').map(a => a.name));
    const tablesLeft = (t: string | null) => (t && (!dropped.has(t) || failed.some(f => f.startsWith(`table ${t}:`))) ? t : null);
    await db.query(
      `UPDATE app.run SET
         model_name = CASE WHEN $2 THEN model_name ELSE NULL END,
         model_version = CASE WHEN $2 THEN model_version ELSE NULL END,
         model_uri = CASE WHEN $2 THEN model_uri ELSE NULL END,
         mlflow_run_id = CASE WHEN $3 THEN mlflow_run_id ELSE NULL END,
         job_id = CASE WHEN $4 THEN job_id ELSE NULL END,
         training_table = $5, eval_table = $6, test_table = $7,
         error_message = $8
       WHERE id = $1`,
      [run.id, failedKinds.has('model_version') || failedKinds.has('alias'), failedKinds.has('mlflow_run'), failedKinds.has('job'),
       tablesLeft(run.training_table), tablesLeft(run.eval_table), tablesLeft(run.test_table),
       `Delete incomplete — ${failed.join('; ')}`.slice(0, 4000)]
    );
    return failed;
  }

  app.get('/api/runs/:id/delete-plan', async (req, res) => {
    try {
      const run = (await db.query('SELECT * FROM app.run WHERE id = $1', [req.params.id])).rows[0];
      if (!run) { res.status(404).json({ error: 'Run not found' }); return; }
      res.json(await runDeletePlan(req, run));
    } catch (e: any) { res.status(500).json({ error: e.message }); }
  });

  app.delete('/api/runs/:id', async (req, res) => {
    try {
      const run = (await db.query('SELECT * FROM app.run WHERE id = $1', [req.params.id])).rows[0];
      if (!run) { res.json({ ok: true }); return; }
      const { assets, blockers } = await runDeletePlan(req, run);  // shared tables aren't in assets → kept
      if (blockers.length) { res.status(409).json({ error: `Can't delete run ${run.id}:\n• ${blockers.join('\n• ')}`, blockers }); return; }

      const failed = await deleteRunWithAssets(req, run, assets);
      if (!failed.length) { res.json({ ok: true, deleted: assets }); return; }
      res.status(500).json({ error: `Some assets couldn't be deleted (run kept so you can retry):\n• ${failed.join('\n• ')}`, failed });
    } catch (e: any) { res.status(500).json({ error: e.message }); }
  });


  // Delete a training spec and everything it produced: every run's assets (see runDeletePlan), the runs,
  // its UC features nothing else uses (specFeaturePlan), and the spec + its feature entries. Blocked while any run is
  // deployed/serving/training. Tables also used by OTHER specs' runs are kept; EOLs are shared and never touched.
  async function specDeletePlan(req: any, specId: string) {
    const spec = (await db.query('SELECT * FROM app.training_spec WHERE id = $1', [specId])).rows[0];
    if (!spec) return null;
    const runs = (await db.query('SELECT * FROM app.run WHERE training_spec_id = $1 ORDER BY id', [spec.id])).rows;
    const ids = runs.map((r: any) => Number(r.id));
    const blockers = new Set<string>();
    const kept = new Set<string>();
    const plans: { run: any; assets: RunAsset[] }[] = [];
    for (const run of runs) {
      const p = await runDeletePlan(req, run, ids);
      for (const b of p.blockers) blockers.add(b.startsWith('A deploy is in progress') ? b : `Run ${run.id}: ${b}`);
      for (const k of p.kept) kept.add(k);
      plans.push({ run, assets: p.assets });
    }
    // Dedupe for display (shared tables appear under several runs).
    const seen = new Set<string>();
    const assets = plans.flatMap(p => p.assets).filter(a => !seen.has(`${a.kind}:${a.name}`) && !!seen.add(`${a.kind}:${a.name}`));
    const features = await specFeaturePlan(req, spec);
    return { spec, runs: ids, plans, assets, blockers: [...blockers], kept: [...kept], features };
  }

  // Which of the spec's UC features can go: only those no other spec defines (by name, any project in the same
  // catalog.schema), no online deployment registry references, no surviving UC feature depends on, and that have
  // no materializations (those belong to deployments). Ordered dependents-first (custom UDFs before upstreams).
  async function specFeaturePlan(req: any, spec: any): Promise<{ delete: string[]; keep: string[] }> {
    const project = (await db.query('SELECT catalog, schema FROM app.project WHERE id = $1', [spec.project_id])).rows[0];
    const ns = `${project.catalog}.${project.schema}`;
    const entries = (await db.query('SELECT * FROM app.feature_entry WHERE training_spec_id = $1', [spec.id])).rows;
    const mine = [...specFeatureNames(entries)].map(n => `${ns}.${n}`);
    if (!mine.length) return { delete: [], keep: [] };

    // Every UC feature in the namespace (ListFeatures pages ≤ 50).
    const uc = new Map<string, any>();
    let token = '';
    do {
      const r = await databricksApi(req, 'GET', `feature-engineering/features?catalog_name=${encodeURIComponent(project.catalog)}&schema_name=${encodeURIComponent(project.schema)}&page_size=50${token ? `&page_token=${encodeURIComponent(token)}` : ''}`, undefined, 'api/2.0');
      if (r?.error_code) throw new Error(`Listing UC features in ${ns}: ${r.message}`);
      for (const f of r.features || []) uc.set(f.full_name, f);
      token = r.next_page_token || '';
    } while (token);

    const usedByOtherSpecs = new Map<string, string[]>();
    const others = (await db.query(
      `SELECT s.id, s.name, p.catalog, p.schema FROM app.training_spec s JOIN app.project p ON p.id = s.project_id WHERE s.id <> $1`, [spec.id]
    )).rows.filter((o: any) => `${o.catalog}.${o.schema}` === ns);
    for (const o of others) {
      const oe = (await db.query('SELECT * FROM app.feature_entry WHERE training_spec_id = $1', [o.id])).rows;
      for (const n of specFeatureNames(oe)) {
        const full = `${ns}.${n}`;
        usedByOtherSpecs.set(full, [...(usedByOtherSpecs.get(full) || []), `"${o.name}"`]);
      }
    }
    const inRegistry = new Set((await db.query(
      'SELECT DISTINCT feature_full_name FROM app.deployment_feature WHERE feature_full_name = ANY($1)', [mine]
    )).rows.map((r: any) => r.feature_full_name));

    const keep: string[] = [];
    const candidates = new Set<string>();
    for (const full of mine) {
      if (!uc.has(full)) continue;  // never created (or already gone)
      const why = usedByOtherSpecs.get(full)?.length ? `used by spec ${usedByOtherSpecs.get(full)!.join(', ')}`
        : inRegistry.has(full) ? 'served by an online deployment' : null;
      if (why) keep.push(`${full} (${why})`); else candidates.add(full);
    }
    // Materializations belong to deployments — never tear them down from here.
    for (const full of [...candidates]) {
      const m = await databricksApi(req, 'GET', `feature-engineering/materialized-features?feature_name=${encodeURIComponent(full)}`, undefined, 'api/2.0');
      const n = m?.error_code ? -1 : (m.materialized_features || []).length;
      if (n !== 0) { candidates.delete(full); keep.push(`${full} (${n < 0 ? `couldn't check materializations: ${m.message}` : 'has materializations'})`); }
    }
    // Upstreams of a surviving feature stay (e.g. customer_zip under another spec's UDF); repeat to a fixed point.
    const deps = (f: any): string[] => (f?.source?.feature_view_source?.feature_references || []).map((x: any) => x.feature);
    for (let changed = true; changed;) {
      changed = false;
      for (const [full, f] of uc) {
        if (candidates.has(full)) continue;
        for (const d of deps(f)) if (candidates.delete(d)) { keep.push(`${d} (input to ${full})`); changed = true; }
      }
    }
    // Dependents first: a feature that others in the delete set depend on goes last.
    const order: string[] = [];
    const left = new Set(candidates);
    while (left.size) {
      // Emit features nothing still-pending depends on; a cycle (shouldn't happen) is emitted as-is.
      const ready = [...left].filter(x => ![...left].some(y => y !== x && deps(uc.get(y)).includes(x)));
      for (const x of (ready.length ? ready : [...left])) { order.push(x); left.delete(x); }
    }
    return { delete: order, keep };
  }

  app.get('/api/training-specs/:id/delete-plan', async (req, res) => {
    try {
      const p = await specDeletePlan(req, req.params.id);
      if (!p) { res.status(404).json({ error: 'Training spec not found' }); return; }
      res.json({ runs: p.runs, assets: p.assets, blockers: p.blockers, kept: p.kept, features: p.features });
    } catch (e: any) { res.status(500).json({ error: e.message }); }
  });

  app.delete('/api/training-specs/:id', async (req, res) => {
    try {
      const p = await specDeletePlan(req, req.params.id);
      if (!p) { res.json({ ok: true }); return; }
      if (p.blockers.length) {
        res.status(409).json({ error: `Can't delete spec "${p.spec.name}":\n• ${p.blockers.join('\n• ')}`, blockers: p.blockers }); return;
      }
      const failed: string[] = [];
      for (const { run, assets } of p.plans) {
        for (const f of await deleteRunWithAssets(req, run, assets)) failed.push(`run ${run.id} ${f}`);
      }
      // UC features only once every run (and so every model logged against them) is gone.
      if (!failed.length) {
        for (const full of p.features.delete) {
          const r = await databricksApi(req, 'DELETE', `feature-engineering/features/${encodeURIComponent(full)}`, undefined, 'api/2.0');
          const err = r?.error_code && !isGone(r) ? (r.message || r.error_code) : null;
          console.log(`[spec-delete] spec ${p.spec.id} feature ${full}: ${err || 'deleted'}`);
          if (err) failed.push(`feature ${full}: ${err}`);
        }
      }
      if (failed.length) {
        res.status(500).json({ error: `Some assets couldn't be deleted (spec kept so you can retry):\n• ${failed.join('\n• ')}`, failed }); return;
      }
      await db.query('DELETE FROM app.training_spec WHERE id = $1', [p.spec.id]);  // cascades feature entries
      console.log(`[spec-delete] spec ${p.spec.id} "${p.spec.name}": ${p.runs.length} runs, ${p.assets.length} assets deleted`);
      res.json({ ok: true, runs: p.runs, deleted: p.assets, features: p.features.delete });
    } catch (e: any) { res.status(500).json({ error: e.message }); }
  });

  app.get('/api/models/aliases', async (req, res) => {
    try {
      const name = String(req.query.name || '');
      if (!FQN.test(name)) { res.status(400).json({ error: 'name must be catalog.schema.model' }); return; }
      const m = await databricksApi(req, 'GET', `unity-catalog/models/${ucPath(name)}?include_aliases=true`);
      if (m.error_code) throw new Error(m.message);
      const v = await databricksApi(req, 'GET', `unity-catalog/models/${ucPath(name)}/versions?max_results=100`);
      res.json({
        aliases: (m.aliases || []).map((a: any) => ({ alias: a.alias_name, version: a.version_num })),
        versions: (v.model_versions || []).map((x: any) => x.version).sort((a: number, b: number) => b - a),
      });
    } catch (e: any) { res.status(500).json({ error: e.message }); }
  });

  app.put('/api/models/aliases', async (req, res) => {
    try {
      const { name, alias, version } = req.body || {};
      if (!FQN.test(name || '') || !/^[A-Za-z0-9_]+$/.test(alias || '') || !version) {
        res.status(400).json({ error: 'name (catalog.schema.model), alias ([A-Za-z0-9_]+) and version required' }); return;
      }
      const r = await databricksApi(req, 'PUT', `unity-catalog/models/${ucPath(name)}/aliases/${encodeURIComponent(alias)}`, { version_num: Number(version) });
      if (r.error_code) throw new Error(r.message);
      res.json({ alias: r.alias_name, version: r.version_num });
    } catch (e: any) { res.status(500).json({ error: e.message }); }
  });

  // ════════════════════════════════════════════
  //  DATABRICKS PROXY — MLflow & Jobs
  // ════════════════════════════════════════════
  app.all('/api/databricks/*', async (req, res) => {
    try {
      const apiPath = req.url.replace('/api/databricks/', '');
      const host = process.env.DATABRICKS_HOST || '';
      const token = (req.headers['x-forwarded-access-token'] as string) || '';
      const resp = await fetch(`${host}/api/2.0/${apiPath}`, {
        method: req.method,
        headers: {
          Authorization: `Bearer ${token}`,
          ...(req.method !== 'GET' ? { 'Content-Type': 'application/json' } : {}),
        },
        body: req.method !== 'GET' ? JSON.stringify(req.body) : undefined,
      });
      const data = await resp.json();
      res.status(resp.status).json(data);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });
});

// ── Init DB schema ──
// DDL lives in server/schema.sql and must run as the table owner. Locally (PGPASSWORD = the
// deploying user's credential) apply it at startup; when deployed, deploy.sh has already applied
// it as the owner — the app SP lacks CREATE/ownership, so don't try.
if (process.env.PGPASSWORD) {
  const schemaSql = fs.readFileSync(path.resolve(import.meta.dirname || '.', 'schema.sql'), 'utf8');
  await db.query(schemaSql)
    .then(() => console.log('Database schema initialized'))
    .catch((e: Error) => console.error('Schema init error:', e.message));
}

await appkit.server.start();
