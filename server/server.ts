import { createApp, analytics, server, lakebase } from '@databricks/appkit';
import fs from 'fs';
import path from 'path';

// Use PGPASSWORD for native auth (local dev), or LAKEBASE_ENDPOINT for OAuth (deployed)
const lakebaseConfig = process.env.PGPASSWORD
  ? { password: process.env.PGPASSWORD }
  : {};

const appkit = await createApp({
  plugins: [
    analytics(),
    lakebase(lakebaseConfig),
    server({ autoStart: false }),
  ],
});

const db = appkit.lakebase;

// Lakebase project config for synced tables (feature store online serving)
const LAKEBASE_BRANCH = process.env.LAKEBASE_BRANCH || 'projects/mlops/branches/production';
const LAKEBASE_PG_DATABASE = process.env.PGDATABASE || 'databricks_postgres';

// Helper: call Databricks REST API using the user's OBO token (or app token for local dev)
function getToken(req: any): string {
  return (req.headers['x-forwarded-access-token'] as string)
    || process.env.DATABRICKS_TOKEN || '';
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
  return _cachedUser;
}

async function databricksApi(req: any, method: string, apiPath: string, body?: any, apiPrefix = 'api/2.1'): Promise<any> {
  const host = process.env.DATABRICKS_HOST || '';
  const token = getToken(req);
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
  const token = getToken(req);
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
  const token = getToken(req);
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
      const result = await db.query(
        `INSERT INTO app.project (name, description, catalog, schema, model_name, git_url, notebook_path, training_notebook, evaluation_notebook)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *`,
        [name, description, catalog, schema, model_name || name, git_url, notebook_path, training_notebook, evaluation_notebook]
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
        [name, description, catalog, schema, model_name || name, git_url, notebook_path, training_notebook, evaluation_notebook, req.params.id]
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
      res.json(rows.map((r: any) => ({ name: r.col_name, type: r.data_type })));
    } catch (e: any) {
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

  // ════════════════════════════════════════════
  //  ENTITY OBSERVATION LABELS (EOLs)
  // ════════════════════════════════════════════
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
        specs.push({ ...spec, entries: entries.rows, run_count: parseInt(runCount.rows[0].count) });
      }
      res.json(specs);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/projects/:projectId/training-specs', async (req, res) => {
    try {
      const { eol_id, name, task_type, split_strategy, split_method, split_config, parameters } = req.body;
      const result = await db.query(
        `INSERT INTO app.training_spec (project_id, eol_id, name, task_type, split_strategy, split_method, split_config, parameters)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
        [req.params.projectId, eol_id, name, task_type || 'classification',
         split_strategy || 'none', split_method || null,
         split_config ? JSON.stringify(split_config) : null,
         parameters ? JSON.stringify(parameters) : null]
      );
      res.status(201).json({ ...result.rows[0], entries: [], run_count: 0 });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.put('/api/training-specs/:id', async (req, res) => {
    try {
      // Lock check: reject edits if spec has runs
      const runCount = await db.query('SELECT COUNT(*) as count FROM app.run WHERE training_spec_id = $1', [req.params.id]);
      if (parseInt(runCount.rows[0].count) > 0) {
        res.status(400).json({ error: 'Training spec is locked — it has runs. Copy it to make changes.' }); return;
      }
      const { eol_id, name, task_type, split_strategy, split_method, split_config, parameters } = req.body;
      const result = await db.query(
        `UPDATE app.training_spec SET eol_id=$1, name=$2, task_type=$3, split_strategy=$4, split_method=$5, split_config=$6, parameters=$7
         WHERE id=$8 RETURNING *`,
        [eol_id, name, task_type, split_strategy, split_method,
         split_config ? JSON.stringify(split_config) : null,
         parameters ? JSON.stringify(parameters) : null, req.params.id]
      );
      res.json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.delete('/api/training-specs/:id', async (req, res) => {
    try {
      await db.query('DELETE FROM app.training_spec WHERE id = $1', [req.params.id]);
      res.json({ ok: true });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // Copy a training spec with all feature entries
  app.post('/api/training-specs/:id/copy', async (req, res) => {
    try {
      const src = await db.query('SELECT * FROM app.training_spec WHERE id = $1', [req.params.id]);
      if (src.rows.length === 0) { res.status(404).json({ error: 'Not found' }); return; }
      const s = src.rows[0];
      const newName = req.body.name || `${s.name} (copy)`;
      const specResult = await db.query(
        `INSERT INTO app.training_spec (project_id, eol_id, name, task_type, split_strategy, split_method, split_config, parameters)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
        [s.project_id, s.eol_id, newName, s.task_type, s.split_strategy, s.split_method, s.split_config, s.parameters]
      );
      const newSpec = specResult.rows[0];
      const entries = await db.query('SELECT * FROM app.feature_entry WHERE training_spec_id = $1', [req.params.id]);
      const copiedEntries = [];
      for (const e of entries.rows) {
        const entryResult = await db.query(
          `INSERT INTO app.feature_entry
           (training_spec_id, feature_type, table_name, feature_names, lookup_key,
            timestamp_lookup_key, output_name, default_values, declarative_spec)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
          [newSpec.id, e.feature_type, e.table_name, e.feature_names, e.lookup_key,
           e.timestamp_lookup_key, e.output_name, e.default_values, e.declarative_spec]
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
      const runCount = await db.query('SELECT COUNT(*) as count FROM app.run WHERE training_spec_id = $1', [req.params.specId]);
      if (parseInt(runCount.rows[0].count) > 0) {
        res.status(400).json({ error: 'Training spec is locked — it has runs. Copy it to make changes.' }); return;
      }
      const {
        feature_type, table_name, feature_names, lookup_key,
        timestamp_lookup_key, output_name, default_values, declarative_spec
      } = req.body;
      const result = await db.query(
        `INSERT INTO app.feature_entry
         (training_spec_id, feature_type, table_name, feature_names, lookup_key,
          timestamp_lookup_key, output_name, default_values, declarative_spec)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
        [req.params.specId, feature_type, table_name, feature_names, lookup_key,
         timestamp_lookup_key, output_name, default_values ? JSON.stringify(default_values) : null,
         declarative_spec ? JSON.stringify(declarative_spec) : null]
      );
      res.status(201).json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.delete('/api/training-spec-entries/:id', async (req, res) => {
    try {
      // Check lock via the entry's parent spec
      const entry = await db.query('SELECT training_spec_id FROM app.feature_entry WHERE id = $1', [req.params.id]);
      if (entry.rows.length > 0 && entry.rows[0].training_spec_id) {
        const runCount = await db.query('SELECT COUNT(*) as count FROM app.run WHERE training_spec_id = $1', [entry.rows[0].training_spec_id]);
        if (parseInt(runCount.rows[0].count) > 0) {
          res.status(400).json({ error: 'Training spec is locked — it has runs.' }); return;
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

      const featureLookups = entries
        .filter((e: any) => e.feature_type === 'lookup')
        .map((e: any) => ({
          table_name: e.table_name,
          feature_names: e.feature_names,
          lookup_key: e.lookup_key,
          timestamp_lookup_key: e.timestamp_lookup_key || null,
        }));

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
        notebookFile = 'train_hpsearch.py'; // future
      } else {
        notebookFile = 'train_cv.py';
      }

      const notebookPath = `/Workspace/Users/${userName}/.mlops/${stripPyExt(notebookFile)}`;
      const localNotebook = path.resolve(import.meta.dirname || '.', '..', 'notebooks', notebookFile);
      await uploadNotebook(req, localNotebook, notebookPath);

      // Build table names for standard/hpsearch splits
      const specSlug = spec.name.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
      const trainingTableName = `${project.catalog}.${project.schema}.${specSlug}_train`;
      const evalTableName = spec.split_strategy !== 'none' ? `${project.catalog}.${project.schema}.${specSlug}_eval` : '';
      const testTableName = spec.split_strategy === 'train_eval_test' ? `${project.catalog}.${project.schema}.${specSlug}_test` : '';

      const params: Record<string, string> = {
        eol_sql: eol.sql_definition,
        label_column: eol.label_column || '',
        entity_columns_json: JSON.stringify(entityColumns),
        feature_lookups_json: JSON.stringify(featureLookups),
        task_type: spec.task_type || 'classification',
        parameters_json: JSON.stringify(spec.parameters || {}),
        catalog: project.catalog,
        schema: project.schema,
        experiment_name: experimentName,
      };

      // Add split params for standard/hpsearch
      if (spec.split_strategy !== 'none') {
        params.split_method = spec.split_method || 'random';
        params.split_config_json = JSON.stringify(spec.split_config || { eval_pct: 20, seed: 42 });
        params.training_table_name = trainingTableName;
        params.eval_table_name = evalTableName;
        if (testTableName) params.test_table_name = testTableName;
      }

      console.log(`[spec-run] Launching ${notebookFile} for spec "${spec.name}" (${spec.task_type}, ${spec.split_strategy})`);

      const { job_id, run_id: jobRunId, run_url } = await createOrRunJob(
        req, `mlops-${project.name}-${specSlug}`, notebookPath, params
      );

      // Look up MLflow experiment ID
      const host = process.env.DATABRICKS_HOST || '';
      const token = getToken(req);
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
        try {
          const taskRunId = jobRun.tasks?.[0]?.run_id || run.run_id;
          const output = await databricksApi(req, 'GET', `jobs/runs/get-output?run_id=${taskRunId}`);
          const nbResult = JSON.parse(output.notebook_output?.result || '{}');
          mlflowRunId = nbResult.mlflow_run_id || null;
          evalMetrics = nbResult.metrics || null;
          if (nbResult.training_table) trainingTable = nbResult.training_table;
          if (nbResult.eval_table) evalTable = nbResult.eval_table;
        } catch (e: any) {
          console.error(`[spec-run] Output parse error: ${e.message}`);
        }

        await db.query(
          `UPDATE app.run SET status='SUCCESS', mlflow_run_id=$1, eval_metrics=$2,
           training_table=$3, eval_table=$4, ended_at=NOW() WHERE id=$5`,
          [mlflowRunId, evalMetrics ? JSON.stringify(evalMetrics) : null,
           trainingTable, evalTable, run.id]
        );
      } else if (lifeCycleState === 'TERMINATED') {
        await db.query(
          `UPDATE app.run SET status='FAILED', error_message=$1, ended_at=NOW() WHERE id=$2`,
          [state?.state_message || resultState || 'Unknown error', run.id]
        );
      }
      const updated = await db.query('SELECT * FROM app.run WHERE id = $1', [run.id]);
      res.json(updated.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ════════════════════════════════════════════
  //  LEGACY: FEATURE DEFINITIONS (kept for backward compat)
  // ════════════════════════════════════════════
  app.get('/api/projects/:projectId/features', async (req, res) => {
    try {
      const result = await db.query(
        'SELECT * FROM app.feature_definition WHERE project_id = $1 ORDER BY id',
        [req.params.projectId]
      );
      // Attach entries to each feature definition
      const features = [];
      for (const fd of result.rows) {
        const entries = await db.query(
          'SELECT * FROM app.feature_entry WHERE feature_definition_id = $1 ORDER BY id',
          [fd.id]
        );
        features.push({ ...fd, entries: entries.rows });
      }
      res.json(features);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/projects/:projectId/features', async (req, res) => {
    try {
      const { eol_id, name } = req.body;
      const result = await db.query(
        `INSERT INTO app.feature_definition (project_id, eol_id, name) VALUES ($1,$2,$3) RETURNING *`,
        [req.params.projectId, eol_id, name]
      );
      res.status(201).json({ ...result.rows[0], entries: [] });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.delete('/api/features/:id', async (req, res) => {
    try {
      await db.query('DELETE FROM app.feature_definition WHERE id = $1', [req.params.id]);
      res.json({ ok: true });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // Copy a feature definition (with all entries) to create a new version
  app.post('/api/features/:id/copy', async (req, res) => {
    try {
      const src = await db.query('SELECT * FROM app.feature_definition WHERE id = $1', [req.params.id]);
      if (src.rows.length === 0) { res.status(404).json({ error: 'Feature definition not found' }); return; }
      const s = src.rows[0];
      const newName = req.body.name || `${s.name} (copy)`;
      const fdResult = await db.query(
        `INSERT INTO app.feature_definition (project_id, eol_id, name) VALUES ($1,$2,$3) RETURNING *`,
        [s.project_id, s.eol_id, newName]
      );
      const newFd = fdResult.rows[0];
      // Copy all entries
      const entries = await db.query('SELECT * FROM app.feature_entry WHERE feature_definition_id = $1', [req.params.id]);
      const copiedEntries = [];
      for (const e of entries.rows) {
        const entryResult = await db.query(
          `INSERT INTO app.feature_entry
           (feature_definition_id, feature_type, table_name, feature_names, lookup_key,
            timestamp_lookup_key, default_values, declarative_spec)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
          [newFd.id, e.feature_type, e.table_name, e.feature_names, e.lookup_key,
           e.timestamp_lookup_key, e.default_values, e.declarative_spec]
        );
        copiedEntries.push(entryResult.rows[0]);
      }
      res.status(201).json({ ...newFd, entries: copiedEntries });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ════════════════════════════════════════════
  //  FEATURE ENTRIES (individual lookups/declarative within a definition)
  // ════════════════════════════════════════════
  app.post('/api/features/:featureId/entries', async (req, res) => {
    try {
      const {
        feature_type, table_name, feature_names, lookup_key,
        timestamp_lookup_key, output_name, default_values, declarative_spec
      } = req.body;
      const result = await db.query(
        `INSERT INTO app.feature_entry
         (feature_definition_id, feature_type, table_name, feature_names, lookup_key,
          timestamp_lookup_key, output_name, default_values, declarative_spec)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
        [req.params.featureId, feature_type, table_name, feature_names, lookup_key,
         timestamp_lookup_key, output_name, default_values ? JSON.stringify(default_values) : null,
         declarative_spec ? JSON.stringify(declarative_spec) : null]
      );
      res.status(201).json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.delete('/api/feature-entries/:id', async (req, res) => {
    try {
      await db.query('DELETE FROM app.feature_entry WHERE id = $1', [req.params.id]);
      res.json({ ok: true });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ════════════════════════════════════════════
  //  DATASETS
  // ════════════════════════════════════════════
  app.get('/api/projects/:projectId/datasets', async (req, res) => {
    try {
      const result = await db.query(
        'SELECT * FROM app.dataset WHERE project_id = $1 ORDER BY id DESC',
        [req.params.projectId]
      );
      res.json(result.rows);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/projects/:projectId/datasets', async (req, res) => {
    try {
      const { name, feature_definition_id, eval_split_type, eval_split_config } = req.body;
      const result = await db.query(
        `INSERT INTO app.dataset (project_id, name, feature_definition_id, eval_split_type, eval_split_config)
         VALUES ($1,$2,$3,$4,$5) RETURNING *`,
        [req.params.projectId, name, feature_definition_id,
         eval_split_type, eval_split_config ? JSON.stringify(eval_split_config) : null]
      );
      res.status(201).json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.put('/api/datasets/:id', async (req, res) => {
    try {
      const fields = req.body;
      const setClauses: string[] = [];
      const values: any[] = [];
      let idx = 1;
      for (const [key, value] of Object.entries(fields)) {
        setClauses.push(`${key}=$${idx}`);
        values.push(key.endsWith('_config') ? JSON.stringify(value) : value);
        idx++;
      }
      values.push(req.params.id);
      const result = await db.query(
        `UPDATE app.dataset SET ${setClauses.join(', ')} WHERE id=$${idx} RETURNING *`,
        values
      );
      res.json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.delete('/api/datasets/:id', async (req, res) => {
    try {
      await db.query('DELETE FROM app.dataset WHERE id = $1', [req.params.id]);
      res.json({ ok: true });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ════════════════════════════════════════════
  //  RUNS (train + evaluate in one job)
  // ════════════════════════════════════════════
  app.get('/api/projects/:projectId/runs', async (req, res) => {
    try {
      const result = await db.query(
        'SELECT * FROM app.run WHERE project_id = $1 ORDER BY id DESC',
        [req.params.projectId]
      );
      res.json(result.rows);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/projects/:projectId/runs', async (req, res) => {
    try {
      const { dataset_id, parameters } = req.body;
      const result = await db.query(
        `INSERT INTO app.run (project_id, dataset_id, parameters, status)
         VALUES ($1,$2,$3,'PENDING') RETURNING *`,
        [req.params.projectId, dataset_id, parameters ? JSON.stringify(parameters) : null]
      );
      res.status(201).json(result.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.delete('/api/runs/:id', async (req, res) => {
    try {
      await db.query('DELETE FROM app.run WHERE id = $1', [req.params.id]);
      res.json({ ok: true });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

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
            dependencies: ['databricks-feature-engineering'],
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

  // ── Materialize a dataset ──
  app.post('/api/projects/:projectId/datasets/:datasetId/materialize', async (req, res) => {
    try {
      const datasetResult = await db.query('SELECT * FROM app.dataset WHERE id = $1', [req.params.datasetId]);
      if (datasetResult.rows.length === 0) { res.status(404).json({ error: 'Dataset not found' }); return; }
      const dataset = datasetResult.rows[0];

      const projectResult = await db.query('SELECT * FROM app.project WHERE id = $1', [req.params.projectId]);
      const project = projectResult.rows[0];

      // Get feature definition (required) and EOL via feature definition
      if (!dataset.feature_definition_id) { res.status(400).json({ error: 'Dataset has no feature definition' }); return; }
      const fdResult = await db.query('SELECT * FROM app.feature_definition WHERE id = $1', [dataset.feature_definition_id]);
      if (fdResult.rows.length === 0) { res.status(404).json({ error: 'Feature definition not found' }); return; }
      const featureDef = fdResult.rows[0];

      const eolResult = await db.query('SELECT * FROM app.entity_observation_label WHERE id = $1', [featureDef.eol_id]);
      if (eolResult.rows.length === 0) { res.status(404).json({ error: 'EOL not found' }); return; }
      const eol = eolResult.rows[0];

      const trainingTable = `${project.catalog}.${project.schema}.${dataset.name}_train`;
      const evalTable = `${project.catalog}.${project.schema}.${dataset.name}_eval`;
      const evalPct = dataset.eval_split_config?.percentage || 20;

      // Gather all feature entries from this definition
      const entriesResult = await db.query(
        'SELECT * FROM app.feature_entry WHERE feature_definition_id = $1 ORDER BY id',
        [featureDef.id]
      );
      const featureLookups = entriesResult.rows
        .filter((e: any) => e.feature_type === 'lookup')
        .map((e: any) => ({
          table_name: e.table_name,
          feature_names: e.feature_names,
          lookup_key: e.lookup_key,
          timestamp_lookup_key: e.timestamp_lookup_key || null,
        }));
      const declarativeFeatures = entriesResult.rows
        .filter((e: any) => e.feature_type === 'declarative' && e.declarative_spec)
        .map((e: any) => ({
          ...e.declarative_spec,
          // Ensure entity columns and timestamp from EOL are available
          entity_columns: eol.entity_columns || [],
          timeseries_column: eol.timestamp_column || '',
        }));

      const params = {
        eol_sql: eol.sql_definition,
        label_column: eol.label_column || '',
        entity_columns_json: JSON.stringify(eol.entity_columns || []),
        feature_definitions_json: JSON.stringify(featureLookups),
        declarative_features_json: JSON.stringify(declarativeFeatures),
        catalog: project.catalog,
        schema: project.schema,
        training_table_name: trainingTable,
        eval_table_name: evalTable,
        eval_split_percentage: String(evalPct),
      };

      // Upload the app-managed materialize notebook to the workspace
      const userName = await getUsername(req);
      const notebookPath = `/Workspace/Users/${userName}/.mlops/materialize`;
      const localNotebook = path.resolve(import.meta.dirname || '.', '..', 'notebooks', 'materialize.py');
      await uploadNotebook(req, localNotebook, notebookPath);

      const { job_id, run_id, run_url } = await createOrRunJob(
        req, `mlops-materialize-${dataset.name}`, notebookPath, params, dataset.materialize_job_id
      );

      await db.query(
        `UPDATE app.dataset SET status='MATERIALIZING', materialize_job_id=$1, materialize_run_id=$2, materialize_run_url=$3, training_table=$4, eval_table=$5 WHERE id=$6`,
        [job_id, run_id, run_url, trainingTable, evalTable, req.params.datasetId]
      );

      res.json({ job_id, run_id, run_url });
    } catch (e: any) {
      console.error(`[materialize] Error: ${e.message}`);
      res.status(500).json({ error: e.message });
    }
  });

  // ── Check and update status for a dataset's materialize job ──
  app.post('/api/projects/:projectId/datasets/:datasetId/check-status', async (req, res) => {
    try {
      const dsResult = await db.query('SELECT * FROM app.dataset WHERE id = $1', [req.params.datasetId]);
      if (dsResult.rows.length === 0) { res.status(404).json({ error: 'Dataset not found' }); return; }
      const dataset = dsResult.rows[0];

      // Already in a terminal state — return without hitting Databricks API
      if (dataset.status === 'READY' || dataset.status === 'FAILED' || dataset.status === 'NOT_STARTED') {
        res.json(dataset); return;
      }
      if (!dataset.materialize_run_id) { res.json(dataset); return; }

      const runResult = await databricksApi(req, 'GET', `jobs/runs/get?run_id=${dataset.materialize_run_id}`);
      const state = runResult.state;
      const lifeCycleState = state?.life_cycle_state;
      const resultState = state?.result_state;

      if (lifeCycleState === 'TERMINATED' && resultState === 'SUCCESS') {
        await db.query(
          `UPDATE app.dataset SET status='READY' WHERE id=$1`,
          [dataset.id]
        );
        const updated = await db.query('SELECT * FROM app.dataset WHERE id = $1', [dataset.id]);
        res.json(updated.rows[0]);
      } else if (lifeCycleState === 'TERMINATED' && resultState !== 'SUCCESS') {
        const errorMsg = state?.state_message || resultState || 'Unknown error';
        await db.query(
          `UPDATE app.dataset SET status='FAILED' WHERE id=$1`,
          [dataset.id]
        );
        const updated = await db.query('SELECT * FROM app.dataset WHERE id = $1', [dataset.id]);
        res.json({ ...updated.rows[0], error_message: errorMsg });
      } else {
        // Still running
        res.json(dataset);
      }
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ── Launch a run (multi-task job: train → evaluate) ──
  app.post('/api/projects/:projectId/runs/:runId/launch', async (req, res) => {
    try {
      const runId = parseInt(req.params.runId);
      const runResult = await db.query('SELECT * FROM app.run WHERE id = $1', [runId]);
      if (runResult.rows.length === 0) { res.status(404).json({ error: 'Run not found' }); return; }
      const run = runResult.rows[0];

      const projectResult = await db.query('SELECT * FROM app.project WHERE id = $1', [req.params.projectId]);
      const project = projectResult.rows[0];
      const datasetResult = await db.query('SELECT * FROM app.dataset WHERE id = $1', [run.dataset_id]);
      const dataset = datasetResult.rows[0];
      const fdResult = await db.query('SELECT * FROM app.feature_definition WHERE id = $1', [dataset.feature_definition_id]);
      const featureDef = fdResult.rows[0];
      const eolResult = await db.query('SELECT * FROM app.entity_observation_label WHERE id = $1', [featureDef.eol_id]);
      const eol = eolResult.rows[0];

      const experimentName = project.name;
      const userName = await getUsername(req);
      const experimentFullPath = `/Users/${userName}/${experimentName}`;
      const trainNotebookPath = project.notebook_path
        ? `${project.notebook_path}/${project.training_notebook}`
        : project.training_notebook;
      const evalNotebookPath = project.notebook_path
        ? `${project.notebook_path}/${project.evaluation_notebook}`
        : project.evaluation_notebook;

      // Build feature lookups JSON for the training notebook (for fe.log_model)
      const entriesResult = await db.query(
        'SELECT * FROM app.feature_entry WHERE feature_definition_id = $1 ORDER BY id',
        [featureDef.id]
      );
      const featureLookups = entriesResult.rows
        .filter((e: any) => e.feature_type === 'lookup')
        .map((e: any) => ({
          table_name: e.table_name,
          feature_names: e.feature_names,
          lookup_key: e.lookup_key,
          timestamp_lookup_key: e.timestamp_lookup_key || null,
        }));

      const trainParams = {
        target: eol.label_column || '',
        training_table_name: dataset.training_table || '',
        eval_table_name: dataset.eval_table || '',
        experiment_name: experimentName,
        catalog: project.catalog,
        schema: project.schema,
        feature_lookups_json: JSON.stringify(featureLookups),
        ...(run.parameters || {}),
      };

      // Build single-task job: train (includes evaluation via mlflow.evaluate)
      let jobId = run.job_id;
      if (!jobId) {
        const createPayload = {
          name: `mlops-${project.name}`,
          git_source: { git_url: project.git_url, git_provider: 'gitHub', git_branch: 'main' },
          tasks: [
            {
              task_key: 'train',
              notebook_task: {
                notebook_path: stripPyExt(trainNotebookPath),
                base_parameters: trainParams,
                source: 'GIT',
              },
              environment_key: 'Default',
            },
          ],
          environments: [{
            environment_key: 'Default',
            spec: { client: '1', dependencies: ['databricks-feature-engineering'] },
          }],
        };
        console.log(`[job] Creating job: mlops-${project.name}`);
        const createResult = await databricksApi(req, 'POST', 'jobs/create', createPayload);
        if (createResult.error_code) throw new Error(`Job create failed: ${createResult.message}`);
        jobId = createResult.job_id;
        console.log(`[job] Created job ${jobId}`);
      } else {
        console.log(`[job] Reusing existing job ${jobId}`);
      }

      // Trigger run with updated params
      const runNowResult = await databricksApi(req, 'POST', 'jobs/run-now', {
        job_id: jobId,
        notebook_params: trainParams, // passed to first task; eval task uses base_parameters
      });
      if (runNowResult.error_code) throw new Error(`Job run failed: ${runNowResult.message}`);
      const databricksRunId = runNowResult.run_id;
      const host = process.env.DATABRICKS_HOST || '';
      const runUrl = `${host}/#job/${jobId}/run/${databricksRunId}`;

      // Look up the numeric MLflow experiment ID by name (MLflow API is at 2.0, not 2.1)
      let mlflowExperimentId: string | null = null;
      try {
        const token = getToken(req);
        const expUrl = `${host}/api/2.0/mlflow/experiments/get-by-name?experiment_name=${encodeURIComponent(experimentFullPath)}`;
        console.log(`[mlflow] Looking up experiment: ${expUrl}`);
        const expResp = await fetch(expUrl, {
          headers: { Authorization: `Bearer ${token}` },
        });
        const expText = await expResp.text();
        console.log(`[mlflow] Response (${expResp.status}): ${expText.slice(0, 300)}`);
        const expData = JSON.parse(expText);
        if (expData.experiment?.experiment_id) {
          mlflowExperimentId = expData.experiment.experiment_id;
          console.log(`[mlflow] Resolved experiment ID: ${mlflowExperimentId}`);
        }
      } catch (e: any) { console.error(`[mlflow] Experiment lookup failed: ${e.message}`); }

      await db.query(
        `UPDATE app.run SET status='RUNNING', job_id=$1, run_id=$2, mlflow_experiment_id=$3, databricks_run_url=$4, started_at=NOW() WHERE id=$5`,
        [jobId, databricksRunId, mlflowExperimentId, runUrl, runId]
      );
      res.json({ job_id: jobId, run_id: databricksRunId, run_url: runUrl });
    } catch (e: any) {
      console.error(`[launch] Error: ${e.message}`);
      res.status(500).json({ error: e.message });
    }
  });

  // ── Check and update status for a run ──
  app.post('/api/runs/:id/check-status', async (req, res) => {
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
        // Fetch MLflow experiment ID and metrics
        const host = process.env.DATABRICKS_HOST || '';
        const token = getToken(req);
        let mlflowExperimentId = run.mlflow_experiment_id;
        let trainingMetrics: Record<string, any> | null = null;
        let evalMetrics: Record<string, any> | null = null;
        let mlflowRunId: string | null = run.mlflow_run_id;

        try {
          // Resolve experiment ID if we don't have it yet
          if (!mlflowExperimentId) {
            const project = (await db.query('SELECT * FROM app.project WHERE id = $1', [run.project_id])).rows[0];
            const userName = await getUsername(req);
            const experimentFullPath = `/Users/${userName}/${project.name}`;
            const expResp = await fetch(`${host}/api/2.0/mlflow/experiments/get-by-name?experiment_name=${encodeURIComponent(experimentFullPath)}`, {
              headers: { Authorization: `Bearer ${token}` },
            });
            const expData = await expResp.json();
            if (expData.experiment?.experiment_id) {
              mlflowExperimentId = expData.experiment.experiment_id;
            }
          }

          // Find the MLflow run associated with this Databricks job run
          if (mlflowExperimentId) {
            const searchResp = await fetch(`${host}/api/2.0/mlflow/runs/search`, {
              method: 'POST',
              headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
              body: JSON.stringify({
                experiment_ids: [mlflowExperimentId],
                filter_string: `tags.mlflow.databricks.jobRunId = '${run.run_id}'`,
                max_results: 1,
              }),
            });
            const searchData = await searchResp.json();
            const mlRun = searchData.runs?.[0];
            if (mlRun) {
              mlflowRunId = mlRun.info?.run_id || null;
              // Extract metrics into training vs test buckets
              // Handles both mlflow.evaluate() prefixes (eval_, evaluation_) and
              // CatBoost CV prefixes (test-, train-)
              const metrics = mlRun.data?.metrics || [];
              const train: Record<string, any> = {};
              const eval_: Record<string, any> = {};
              for (const m of metrics) {
                if (m.key.startsWith('eval_') || m.key.startsWith('evaluation_') || m.key.startsWith('test-')) {
                  eval_[m.key] = m.value;
                } else if (m.key.startsWith('train-')) {
                  train[m.key] = m.value;
                } else {
                  train[m.key] = m.value;
                }
              }
              if (Object.keys(train).length > 0) trainingMetrics = train;
              if (Object.keys(eval_).length > 0) evalMetrics = eval_;
            }
          }
        } catch (e: any) {
          console.error(`[mlflow] Post-run lookup failed: ${e.message}`);
        }

        await db.query(
          `UPDATE app.run SET status='SUCCESS', mlflow_experiment_id=$1, mlflow_run_id=$2, training_metrics=$3, eval_metrics=$4, ended_at=NOW() WHERE id=$5`,
          [mlflowExperimentId, mlflowRunId, trainingMetrics ? JSON.stringify(trainingMetrics) : null, evalMetrics ? JSON.stringify(evalMetrics) : null, run.id]
        );
      } else if (lifeCycleState === 'TERMINATED') {
        await db.query(
          `UPDATE app.run SET status='FAILED', error_message=$1, ended_at=NOW() WHERE id=$2`,
          [state?.state_message || resultState || 'Unknown error', run.id]
        );
      }
      const updated = await db.query('SELECT * FROM app.run WHERE id = $1', [run.id]);
      res.json(updated.rows[0]);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

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
      const { job_id, run_id: jobRunId, run_url } = await createOrRunJob(
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
        } else if (lifeCycleState === 'TERMINATED') {
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
  //  ONLINE TABLES
  // ════════════════════════════════════════════
  app.get('/api/projects/:projectId/online-tables', async (req, res) => {
    try {
      const result = await db.query(
        'SELECT * FROM app.online_table WHERE project_id = $1 ORDER BY source_table',
        [req.params.projectId]
      );
      res.json(result.rows);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/projects/:projectId/online-tables', async (req, res) => {
    try {
      const { source_table, primary_key_columns, timeseries_key, sync_mode } = req.body;
      if (!source_table) {
        res.status(400).json({ error: 'source_table required' }); return;
      }

      const projResult = await db.query('SELECT * FROM app.project WHERE id = $1', [req.params.projectId]);
      if (projResult.rows.length === 0) { res.status(404).json({ error: 'Project not found' }); return; }
      const project = projResult.rows[0];

      const publishMode = (sync_mode || 'triggered').toUpperCase();
      const shortName = source_table.split('.').pop();
      const onlineTableName = `${project.catalog}.${project.schema}.${shortName}_online`;
      const onlineStoreName = project.catalog; // must match UC catalog for serving endpoint lookup

      // Enable CDF on source table (required for TRIGGERED/CONTINUOUS)
      try {
        await executeSql(req, `ALTER TABLE ${source_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`);
        console.log(`[publish] Enabled CDF on ${source_table}`);
      } catch (e: any) {
        console.warn(`[publish] CDF enable warning: ${e.message}`);
      }

      // Ensure PK NOT NULL constraints (required by online store)
      try {
        const parts = source_table.split('.');
        const pkRows = await executeSql(req,
          `SELECT column_name FROM ${parts[0]}.information_schema.constraint_column_usage
           WHERE table_catalog='${parts[0]}' AND table_schema='${parts[1]}' AND table_name='${parts[2]}'`);
        for (const pk of pkRows) {
          await executeSql(req, `ALTER TABLE ${source_table} ALTER COLUMN ${pk.column_name} SET NOT NULL`);
        }
      } catch (e: any) {
        console.warn(`[publish] PK NOT NULL warning: ${e.message}`);
      }

      // Upload and run the publish_table notebook
      const userName = await getUsername(req);
      const notebookPath = `/Workspace/Users/${userName}/.mlops/publish_table`;
      const localNotebook = path.resolve(import.meta.dirname || '.', '..', 'notebooks', 'publish_table.py');
      await uploadNotebook(req, localNotebook, notebookPath);

      const params = {
        online_store_name: onlineStoreName,
        source_table_name: source_table,
        online_table_name: onlineTableName,
        publish_mode: publishMode,
      };

      console.log(`[publish] Publishing ${source_table} → ${onlineTableName} (store: ${onlineStoreName})`);
      const { job_id, run_id: jobRunId, run_url } = await createOrRunJob(
        req, `mlops-publish-${shortName}`, notebookPath, params
      );

      // Insert tracking record as PROVISIONING
      const result = await db.query(
        `INSERT INTO app.online_table (project_id, source_table, online_table_name, primary_key_columns, timeseries_key, sync_mode, status, pipeline_id)
         VALUES ($1,$2,$3,$4,$5,$6,'PROVISIONING',$7) RETURNING *`,
        [req.params.projectId, source_table, onlineTableName, primary_key_columns || [], timeseries_key || null, sync_mode || 'triggered', run_url]
      );

      // Poll for notebook completion (publish jobs are usually fast)
      let attempts = 0;
      while (attempts < 60) {
        await new Promise(r => setTimeout(r, 5000));
        attempts++;
        const jobRun = await databricksApi(req, 'GET', `jobs/runs/get?run_id=${jobRunId}`);
        const lifeCycleState = jobRun.state?.life_cycle_state;
        const resultState = jobRun.state?.result_state;

        if (lifeCycleState === 'TERMINATED' && resultState === 'SUCCESS') {
          await db.query('UPDATE app.online_table SET status=$1, pipeline_id=$2 WHERE id=$3',
            ['ONLINE', run_url, result.rows[0].id]);
          const updated = await db.query('SELECT * FROM app.online_table WHERE id = $1', [result.rows[0].id]);
          console.log(`[publish] Published: ${onlineTableName}`);
          res.status(201).json(updated.rows[0]);
          return;
        } else if (lifeCycleState === 'TERMINATED') {
          const errMsg = jobRun.state?.state_message || resultState || 'Unknown error';
          await db.query('UPDATE app.online_table SET status=$1, pipeline_id=$2 WHERE id=$3',
            ['FAILED', errMsg, result.rows[0].id]);
          throw new Error(`Publish failed: ${errMsg}`);
        }
      }
      throw new Error('Publish timed out after 5 minutes');
    } catch (e: any) {
      console.error(`[publish] Error: ${e.message}`);
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/online-tables/:id/check-status', async (req, res) => {
    try {
      const result = await db.query('SELECT * FROM app.online_table WHERE id = $1', [req.params.id]);
      if (result.rows.length === 0) { res.status(404).json({ error: 'Not found' }); return; }
      const ot = result.rows[0];

      // Status is set by the publish job — just return current state
      res.json(ot);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.delete('/api/online-tables/:id', async (req, res) => {
    try {
      await db.query('DELETE FROM app.online_table WHERE id = $1', [req.params.id]);
      res.json({ ok: true });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

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

  // Publish all required online tables for a deployment
  app.post('/api/deployments/:id/publish-tables', async (req, res) => {
    try {
      const depResult = await db.query('SELECT * FROM app.deployment WHERE id = $1', [req.params.id]);
      if (depResult.rows.length === 0) { res.status(404).json({ error: 'Deployment not found' }); return; }
      const dep = depResult.rows[0];

      // Trace: deployment → run → training_spec → feature_entries
      const runResult = await db.query('SELECT * FROM app.run WHERE id = $1', [dep.run_id]);
      const run = runResult.rows[0];
      const specId = run.training_spec_id;
      const entriesResult = specId
        ? await db.query('SELECT * FROM app.feature_entry WHERE training_spec_id = $1 AND table_name IS NOT NULL', [specId])
        : { rows: [] };

      // Get existing online tables for this project
      const existingOt = await db.query(
        'SELECT source_table FROM app.online_table WHERE project_id = $1',
        [dep.project_id]
      );
      const existingTables = new Set(existingOt.rows.map((r: any) => r.source_table));

      // Publish missing tables using the publish_table notebook
      const projResult = await db.query('SELECT * FROM app.project WHERE id = $1', [dep.project_id]);
      const project = projResult.rows[0];
      const userName = await getUsername(req);
      const notebookPath = `/Workspace/Users/${userName}/.mlops/publish_table`;
      const localNotebook = path.resolve(import.meta.dirname || '.', '..', 'notebooks', 'publish_table.py');
      await uploadNotebook(req, localNotebook, notebookPath);

      const onlineStoreName = project.catalog; // must match UC catalog for serving endpoint lookup
      let published = 0;
      const seen = new Set<string>();
      for (const entry of entriesResult.rows) {
        const sourceTable = entry.table_name;
        if (seen.has(sourceTable) || existingTables.has(sourceTable)) continue;
        seen.add(sourceTable);

        const shortName = sourceTable.split('.').pop();
        const onlineTableName = `${project.catalog}.${project.schema}.${shortName}_online`;

        // Enable CDF
        try {
          await executeSql(req, `ALTER TABLE ${sourceTable} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`);
        } catch { /* may already be set */ }

        const params = {
          online_store_name: onlineStoreName,
          source_table_name: sourceTable,
          online_table_name: onlineTableName,
          publish_mode: 'TRIGGERED',
        };

        console.log(`[publish-tables] Publishing ${sourceTable} → ${onlineTableName}`);
        const { run_url } = await createOrRunJob(req, `mlops-publish-${shortName}`, notebookPath, params);

        await db.query(
          `INSERT INTO app.online_table (project_id, source_table, online_table_name, sync_mode, status, pipeline_id)
           VALUES ($1,$2,$3,'triggered','PROVISIONING',$4)`,
          [dep.project_id, sourceTable, onlineTableName, run_url]
        );
        published++;
      }
      res.json({ published });
    } catch (e: any) {
      console.error(`[publish-tables] Error: ${e.message}`);
      res.status(500).json({ error: e.message });
    }
  });

  // Create serving endpoint for a deployment
  app.post('/api/deployments/:id/create-endpoint', async (req, res) => {
    try {
      const depResult = await db.query('SELECT * FROM app.deployment WHERE id = $1', [req.params.id]);
      if (depResult.rows.length === 0) { res.status(404).json({ error: 'Deployment not found' }); return; }
      const dep = depResult.rows[0];

      const runResult = await db.query('SELECT * FROM app.run WHERE id = $1', [dep.run_id]);
      const run = runResult.rows[0];
      if (!run.model_name || !run.model_version) {
        res.status(400).json({ error: 'Run has no registered model' }); return;
      }

      // Check model version status — must be READY before creating endpoint
      const host = process.env.DATABRICKS_HOST || '';
      const token = getToken(req);
      const mvResp = await fetch(
        `${host}/api/2.0/mlflow/unity-catalog/model-versions/get?name=${encodeURIComponent(run.model_name)}&version=${run.model_version}`,
        { headers: { Authorization: `Bearer ${token}` } }
      );
      const mvData: any = await mvResp.json();
      const mvStatus = mvData.model_version?.status;
      if (mvStatus && mvStatus !== 'READY') {
        res.status(400).json({ error: `Model version status is ${mvStatus}. Wait for it to become READY before deploying.` });
        return;
      }

      const config = dep.endpoint_config || {};
      const payload = {
        name: dep.endpoint_name,
        config: {
          served_entities: [{
            entity_name: run.model_name,
            entity_version: String(run.model_version),
            workload_size: config.workload_size || 'Small',
            scale_to_zero_enabled: config.scale_to_zero_enabled !== false,
          }],
        },
      };

      console.log(`[endpoint] Creating: ${dep.endpoint_name} with model ${run.model_name} v${run.model_version}`);
      const apiResult = await databricksApi(req, 'POST', 'serving-endpoints', payload, 'api/2.0');
      if (apiResult.error_code) throw new Error(`Endpoint create failed: ${apiResult.message}`);

      await db.query(
        `UPDATE app.deployment SET endpoint_status='CREATING' WHERE id=$1`,
        [dep.id]
      );
      const updated = await db.query('SELECT * FROM app.deployment WHERE id = $1', [dep.id]);
      res.json(updated.rows[0]);
    } catch (e: any) {
      console.error(`[endpoint] Error: ${e.message}`);
      res.status(500).json({ error: e.message });
    }
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

      let newStatus = dep.endpoint_status;
      if (readyState === 'READY') {
        newStatus = 'READY';
      } else if (apiResult.state?.config_update === 'IN_PROGRESS') {
        newStatus = 'CREATING';
      }
      // Check for failure
      if (apiResult.error_code) {
        newStatus = 'FAILED';
      }

      if (newStatus !== dep.endpoint_status) {
        await db.query('UPDATE app.deployment SET endpoint_status=$1 WHERE id=$2', [newStatus, dep.id]);
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
      } else if (run.dataset_id) {
        // Legacy path
        const dataset = (await db.query('SELECT * FROM app.dataset WHERE id = $1', [run.dataset_id])).rows[0];
        const fd = (await db.query('SELECT * FROM app.feature_definition WHERE id = $1', [dataset.feature_definition_id])).rows[0];
        eol = (await db.query('SELECT * FROM app.entity_observation_label WHERE id = $1', [fd.eol_id])).rows[0];
      }
      if (!eol) { res.json([]); return; }

      const entityCols = Array.isArray(eol.entity_columns)
        ? eol.entity_columns
        : (eol.entity_columns || '').replace(/^\{|\}$/g, '').split(',').filter(Boolean);

      if (entityCols.length === 0 || !eol.sql_definition) {
        res.json([]); return;
      }

      // Query the EOL SQL definition (the spine) which has the entity columns
      const colList = entityCols.join(', ');
      const eolSql = eol.sql_definition.trim().replace(/;$/, '');
      const rows = await executeSql(req, `SELECT DISTINCT ${colList} FROM (${eolSql}) AS _eol LIMIT 3`);
      res.json(rows);
    } catch (e: any) {
      console.error(`[samples] Error: ${e.message}`);
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
      const token = getToken(req);
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

  // Stop a deployment's serving endpoint (deletes the endpoint, keeps the deployment record)
  // Update a deployment's serving endpoint to a different model version
  app.post('/api/deployments/:id/update-endpoint', async (req, res) => {
    try {
      const { run_id } = req.body;
      if (!run_id) { res.status(400).json({ error: 'run_id required' }); return; }

      const depResult = await db.query('SELECT * FROM app.deployment WHERE id = $1', [req.params.id]);
      if (depResult.rows.length === 0) { res.status(404).json({ error: 'Deployment not found' }); return; }
      const dep = depResult.rows[0];

      const runResult = await db.query('SELECT * FROM app.run WHERE id = $1', [run_id]);
      if (runResult.rows.length === 0) { res.status(404).json({ error: 'Run not found' }); return; }
      const run = runResult.rows[0];
      if (!run.model_name || !run.model_version) {
        res.status(400).json({ error: 'Run has no registered model' }); return;
      }

      // Check model version is READY
      const host = process.env.DATABRICKS_HOST || '';
      const token = getToken(req);
      const mvResp = await fetch(
        `${host}/api/2.0/mlflow/unity-catalog/model-versions/get?name=${encodeURIComponent(run.model_name)}&version=${run.model_version}`,
        { headers: { Authorization: `Bearer ${token}` } }
      );
      const mvData: any = await mvResp.json();
      if (mvData.model_version?.status && mvData.model_version.status !== 'READY') {
        res.status(400).json({ error: `Model version status is ${mvData.model_version.status}. Wait for READY.` }); return;
      }

      // Update the endpoint config with the new model version
      const config = dep.endpoint_config || {};
      await databricksApi(req, 'PUT', `serving-endpoints/${encodeURIComponent(dep.endpoint_name)}/config`, {
        served_entities: [{
          entity_name: run.model_name,
          entity_version: String(run.model_version),
          workload_size: config.workload_size || 'Small',
          scale_to_zero_enabled: config.scale_to_zero_enabled !== false,
        }],
      }, 'api/2.0');

      console.log(`[endpoint] Updated ${dep.endpoint_name} → ${run.model_name} v${run.model_version}`);

      await db.query(
        `UPDATE app.deployment SET run_id=$1, endpoint_status='CREATING' WHERE id=$2`,
        [run_id, dep.id]
      );
      const updated = await db.query('SELECT * FROM app.deployment WHERE id = $1', [dep.id]);
      res.json(updated.rows[0]);
    } catch (e: any) {
      console.error(`[endpoint] Update error: ${e.message}`);
      res.status(500).json({ error: e.message });
    }
  });

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

      await db.query('DELETE FROM app.deployment WHERE id = $1', [req.params.id]);
      res.json({ ok: true });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
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
db.query(`
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
    parameters JSONB
  );
  CREATE TABLE IF NOT EXISTS app.feature_entry (
    id BIGSERIAL PRIMARY KEY, feature_definition_id BIGINT REFERENCES app.feature_definition(id) ON DELETE CASCADE,
    training_spec_id BIGINT REFERENCES app.training_spec(id) ON DELETE CASCADE,
    feature_type VARCHAR(50) NOT NULL DEFAULT 'lookup',
    table_name VARCHAR(255), feature_names TEXT[], lookup_key TEXT[],
    timestamp_lookup_key VARCHAR(255), output_name VARCHAR(255), default_values JSONB, declarative_spec JSONB
  );
  ALTER TABLE app.feature_entry ADD COLUMN IF NOT EXISTS training_spec_id BIGINT REFERENCES app.training_spec(id) ON DELETE CASCADE;
  ALTER TABLE app.feature_entry ALTER COLUMN feature_definition_id DROP NOT NULL;
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
`).then(() => console.log('Database schema initialized'))
  .catch((e: Error) => console.error('Schema init error:', e.message));

await appkit.server.start();
