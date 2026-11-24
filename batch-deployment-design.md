# Batch Deployment — Design Draft v3 (2026-11-20)

Status: **BUILT + DEPLOYED 2026-11-23** (see spec.md §6). Deviation from this draft: the input lives on the batch
deployment row itself (no separate `app.batch_input`), and the UI is its own *Batch Deployment* tab. Spike basis: `~/Documents/Notes/notes/scratch/2026-11-20/spike-batch-features-findings.md`.

**Decisions (Ben, 2026-11-20):**
- Batch deployments get their **own table**.
- The batch input is a **table with every field the requesting system supplies** (the same fields an online
  request carries). The **observation time is the run time**, filled in by the job.
- Output table = **input fields + features + prediction**, **history kept** (append, one slice per run).
- The batch job must run **without the app**.
- Model follows a **UC alias**.
- Offline feature precompute deferred to phase 2.
- No "as-of" override — dropped.

## Core approach

A batch deployment is a **scheduled Databricks job** (created by the app, run as the app SP) running
`notebooks/score_batch.py`. Every parameter is baked into the job definition, so it runs on schedule (or from
the Jobs UI) whether or not the app is up.

```
batch input (table/SQL: request fields)                 run time T
        │                                                   │
        └──► spine = SELECT <request fields>, T AS <ts_col> ◄┘
                 │
                 ▼
     fe.score_batch("models:/<model>@<alias>", spine)    ← alias resolved each run
                 │     features computed point-in-time as of T
                 ▼
     output table (append): request fields · scored_at=T · features… · prediction · model_version
```

The spine has exactly the shape of an online request, so **any model servable online is batch-scorable** —
including per-transaction models like `cc_fraud` (verified in the spike: `score_batch` resolves lookups,
sliding aggregations and the `distance` CustomUDF for v10, and returns the feature columns).

## Data model (Lakebase)

The input definition is its own table, owned by batch inference (not part of EOLs):

```sql
CREATE TABLE IF NOT EXISTS app.batch_input (
  id BIGSERIAL PRIMARY KEY,
  project_id BIGINT NOT NULL REFERENCES app.project(id) ON DELETE CASCADE,
  name VARCHAR(255) NOT NULL,
  sql_definition TEXT NOT NULL,     -- e.g. SELECT * FROM ...credit_card_fraud_demo.transactions_to_score
  columns TEXT[]                    -- captured on save (LIMIT 0 dry run) for validation + display
);

CREATE TABLE IF NOT EXISTS app.batch_deployment (
  id BIGSERIAL PRIMARY KEY,
  project_id BIGINT NOT NULL REFERENCES app.project(id) ON DELETE CASCADE,
  name VARCHAR(255) NOT NULL,
  batch_input_id BIGINT NOT NULL REFERENCES app.batch_input(id),
  model_name VARCHAR(512) NOT NULL,             -- UC full name
  model_alias VARCHAR(255) NOT NULL,            -- e.g. champion; resolved by the job each run
  output_table VARCHAR(512) NOT NULL,           -- default {catalog}.{schema}.{name}_predictions
  cron_expression VARCHAR(255),                 -- Quartz; NULL = manual / Jobs-UI only
  timezone_id VARCHAR(64) DEFAULT 'UTC',
  job_id BIGINT,
  status VARCHAR(30) DEFAULT 'NOT_CREATED',     -- display cache: NOT_CREATED|ACTIVE|PAUSED
  created_at TIMESTAMP DEFAULT NOW()
);
```

Run history (last run, rows, version scored, errors) is **read live from the Jobs API**, not stored — the job is
the source of truth. Applied by `deploy.sh` like the other tables.

**Validation (on create / alias change):** the input's columns must include every caller-supplied input of the
model's spine **except** the timeseries column (read from the model's feature spec — the same source the online
test UI uses). The timeseries column must *not* be in the input (the job supplies it). Extra columns are passed
through to the output (e.g. `transaction_id`).

## Notebook: `notebooks/score_batch.py`

Job parameters (fixed in the job definition): `model_name`, `model_alias`, `input_sql`, `ts_col`,
`output_table`, `batch_deployment_id`.

1. Resolve `models:/<model_name>@<model_alias>` → concrete version. Install that version's requirements
   (`mlflow.pyfunc.get_model_dependencies` → `%pip install -r`; restart python) — generalises past catboost.
2. `T = current_timestamp()` captured **once** at start (all rows share one as-of time).
3. Spine = `SELECT *, T AS <ts_col> FROM (<input_sql>)`. Empty ⇒ exit cleanly, no write.
4. `fe.score_batch(model_uri, spine)` → input fields + features + `prediction`.
5. Add `scored_at = T`, `model_version`, `batch_deployment_id`; **append** to `output_table`
   (first run creates it; schema evolution on if a new model version adds features).
6. Exit JSON: `rows`, `model_version`, `scored_at`, features read from offline tables vs recomputed.

Uploaded to the SP workspace (`.mlops/score_batch`) at create time, re-uploaded on update; the job never calls
the app. Shipped as a plain file (no notebook header).

## Server routes

| Route | Does |
|---|---|
| `GET/POST /api/projects/:projectId/batch-inputs`, `PUT/DELETE /api/batch-inputs/:id`, `GET .../preview` | input CRUD + preview |
| `GET  /api/projects/:projectId/batch-deployments` | list (+ live last-run summary from Jobs API) |
| `POST /api/projects/:projectId/batch-deployments` | validate input vs model spine, insert, **create job** |
| `POST /api/batch-deployments/:id/run-now` | `jobs/run-now` |
| `POST /api/batch-deployments/:id/pause` / `resume` | `jobs/update` schedule `pause_status` |
| `PUT  /api/batch-deployments/:id` | change alias / schedule / output → `jobs/reset` |
| `GET  /api/batch-deployments/:id/runs` | recent runs (task-run output: rows, version) |
| `GET  /api/batch-deployments/:id/preview` | latest `scored_at` slice of the output table |
| `DELETE /api/batch-deployments/:id` | delete job + row (output table kept) |
| `POST /api/models/:name/aliases` | set `@alias` → version (`cc_fraud` has none today) |

## UI (Deployment tab)

- Deployments listed with a type badge `ONLINE` / `BATCH`; "New deployment" asks for the type.
- **Batch inputs** section (batch-owned): name + SQL editor + column/row preview.
- **Batch form:** name · model + alias (existing aliases, with "set alias" shortcut) · batch input ·
  output table (prefilled) · schedule (none / Quartz cron + timezone).
- **Batch detail:** version the alias resolves to now, schedule + state, recent runs (link, rows, version,
  scored_at), *Run now · Pause/Resume · Edit · Delete*, and a **Preview** of the latest predictions.

## Demo data (BUILT 2026-11-20)

- `examples/credit_card_fraud/generate_daily_transactions.py`, bundle job **`mlops-demo-daily-transactions`** (daily 05:00 UTC,
  job 912042967128619). Appends each day's synthetic transactions to `transactions`, catching up any missed
  days (idempotent), and overwrites **`transactions_to_score`** with the newest day's transactions — request
  fields only (`transaction_id, customer_id, merchant_id, amount, pos_entry_mode, security_code`).
- Labels sampled from the seed data's empirical fraud rates per (amount quartile × pos_entry_mode='4' ×
  merchant-risk tercile) — fit on seed data only. Verified: pos4 fraud 55.5% vs seed 55.4%; amount gradient matches.
- Catch-up run done: 2026-09-30 → 2026-11-20, 2,910 rows; `transactions_to_score` = 60 rows.
- Demo batch deployment: input `SELECT * FROM transactions_to_score`, `cc_fraud@champion`, daily 06:00 UTC
  (after the generator).

## Phase 2 (deferred)

Offline-only precompute toggle (`materialize_features` with offline_config only — verified to work), prefix
`mlops_bat_`. `score_batch` reuses it automatically **only if** grid-aligned and covering the spine's time range
(the spike's existing offline table was *not* reused) — ship with the notebook's reuse-vs-recompute report.

## Out of scope

Streaming scoring; monitoring / drift (Lakehouse Monitoring on the output table is a natural follow-on);
writing batch predictions to Lakebase.
