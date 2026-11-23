# MAGIC %pip install databricks-feature-engineering>=0.18.0 'psycopg[binary]'
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC # Materialize a model's Feature View features to the ONLINE store (for serving)
# MAGIC App-managed notebook. Given the same feature_entries_json a spec trains on, (idempotently)
# MAGIC registers the declarative features and materializes the resolvable ones to the online store so
# MAGIC a FeatureView-trained model's serving endpoint can look them up:
# MAGIC   - ColumnSelection features (incl. custom_udf inputs) → online only, TableTrigger
# MAGIC   - Aggregation features → offline + online, CronSchedule (different trigger → separate call)
# MAGIC   - CustomUDF features are computed at serving from their upstreams — not materialized.
# MAGIC Then grants SELECT on each online table to PUBLIC (serving role). Mirrors notebooks/train_cv.py
# MAGIC feature-building, then grants via psycopg to the online store.
# MAGIC
# MAGIC Operations (`operation` widget) — the app's online DEPLOYMENT owns this lifecycle:
# MAGIC   - `ensure`  — register features + materialize ONLY the ones not already online (a batch that mixes
# MAGIC                 new + existing features would otherwise fail "already exists" and skip the new ones),
# MAGIC                 grant, and report per-feature {type, online_table, last_materialization_time}
# MAGIC   - `release` — delete the online materializations of the given online TABLES (offline sibling first —
# MAGIC                 that cascades). UC features are KEPT (training + batch scoring still use them).
# MAGIC   - `status`  — per-feature online table + last materialization time, no mutation

# COMMAND ----------

dbutils.widgets.text("feature_entries_json", "[]")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("timeseries_column", "")
dbutils.widgets.text("online_store_name", "")
dbutils.widgets.text("table_prefix", "mlops_srv_")
dbutils.widgets.text("operation", "ensure")
dbutils.widgets.text("online_tables_json", "[]")      # release: online tables to tear down
dbutils.widgets.text("feature_tables_json", "{}")     # release: {feature_full_name: online_table}

# COMMAND ----------

import json
import time
from datetime import timedelta
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities import (
    DeltaTableSource, ColumnSelection, AggregationFunction,
    TumblingWindow, SlidingWindow, RollingWindow, CronSchedule, TableTrigger,
)
from databricks.ml_features.entities.data_source import FeatureViewSource
from databricks.ml_features.entities.transformation import CustomUDF
from databricks.ml_features.entities.function import AGGREGATION_FUNCTION_BY_SHORTHAND
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.ml import OfflineStoreConfig, OnlineStoreConfig

entries = json.loads(dbutils.widgets.get("feature_entries_json"))
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
timeseries_column = dbutils.widgets.get("timeseries_column").strip()
online_store_name = dbutils.widgets.get("online_store_name").strip()
prefix = dbutils.widgets.get("table_prefix").strip() or "mlops_srv_"

operation = dbutils.widgets.get("operation").strip() or "ensure"
release_tables = json.loads(dbutils.widgets.get("online_tables_json") or "[]")

fe = FeatureEngineeringClient(); w = WorkspaceClient()
if operation in ("ensure", "status") and not timeseries_column:
    raise ValueError("timeseries_column is required")

def _dur(s):
    s = (s or "").strip()
    if s[-1] == "d": return timedelta(days=int(s[:-1]))
    if s[-1] == "h": return timedelta(hours=int(s[:-1]))
    if s[-1] == "m": return timedelta(minutes=int(s[:-1]))
    return timedelta(days=int(s))

def _window(w_):
    if w_["type"] == "tumbling": return TumblingWindow(window_duration=_dur(w_["window_duration"]))
    if w_["type"] == "sliding": return SlidingWindow(window_duration=_dur(w_["window_duration"]), slide_duration=_dur(w_["slide_duration"]))
    if w_["type"] == "continuous": return RollingWindow(window_duration=_dur(w_["window_duration"]))
    raise ValueError(f"bad window {w_['type']}")

def _full(n): return f"{catalog}.{schema}.{n}"
def _delta(table, **extra):
    p = table.split(".")
    return DeltaTableSource(catalog_name=p[0] if len(p) == 3 else catalog, schema_name=p[1] if len(p) == 3 else schema, table_name=p[-1], **extra)
def _fn_sig(fn):
    """Comparable definition of a feature function. The window TYPE is added explicitly — a time
    window's yaml dict holds only its duration, so tumbling vs rolling would otherwise compare equal."""
    try:
        d = fn._to_yaml_dict()
    except Exception:  # noqa: BLE001
        d = repr(fn)
    tw = getattr(fn, "time_window", None)
    return (type(fn).__name__, json.dumps(d, sort_keys=True, default=str), type(tw).__name__ if tw is not None else None)

def _create(name, **kw):
    try:
        return fe.create_feature(catalog_name=catalog, schema_name=schema, name=name, **kw)
    except Exception as e:  # noqa: BLE001
        if "ALREADY" in str(e).upper() or "EXIST" in str(e).upper():
            # Reuse only if the existing UC feature has the SAME definition — names are shared across
            # specs, so e.g. a rolling entry named like an existing tumbling feature must not silently
            # train on the tumbling one.
            existing = fe.get_feature(full_name=_full(name))
            if "function" in kw:
                want, have = _fn_sig(kw["function"]), _fn_sig(existing.function)
                if want != have:
                    raise ValueError(f"Feature {_full(name)} already exists with a different definition "
                                     f"(existing {have}, requested {want}). Rename this feature entry "
                                     f"(e.g. include the window type).")
            return existing
        raise

# COMMAND ----------

def _is_dup(e):
    m = str(e).upper()
    return "ALREADY" in m or "EXIST" in m

def _mfs(full_name):
    try:
        return fe.list_materialized_features(feature_name=full_name) or []
    except Exception as e:  # noqa: BLE001
        print(f"[list] {full_name}: {e}")
        return []

def _online_mf(full_name):
    return next((m for m in _mfs(full_name) if getattr(m, "is_online", False)), None)

def _ts(v):
    return v.isoformat() if hasattr(v, "isoformat") else (str(v) if v else None)

def _feature_report():
    """Per spec entry: type, online table (None for CustomUDF — computed at serving) and freshness."""
    out = []
    for e in entries:
        full = _full(e["name"])
        row = {"feature_full_name": full, "type": e["type"], "online_table": None, "last_materialization_time": None}
        if e["type"] != "custom_udf":
            mf = _online_mf(full)
            if mf is not None:
                row["online_table"] = getattr(mf, "table_name", None)
                row["last_materialization_time"] = _ts(getattr(mf, "last_materialization_time", None))
        out.append(row)
    return out

def _build_features():
    """Rebuild the feature objects (idempotent) — same as train_cv.py."""
    by_name, colsels, aggs = {}, [], []
    for e in entries:
        if e["type"] == "column_selection":
            f = _create(e["name"], source=_delta(e["source_table"]), function=ColumnSelection(column=e["column"]),
                        entity=e["entity"], timeseries_column=timeseries_column)
            by_name[e["name"]] = f; colsels.append(f)
        elif e["type"] == "aggregation":
            op = AGGREGATION_FUNCTION_BY_SHORTHAND.get(e["function"])
            if op is None: raise ValueError(f"bad function {e['function']}")
            extra = {"filter_condition": e["filter_condition"]} if e.get("filter_condition") else {}
            f = _create(e["name"], source=_delta(e["source_table"], **extra),
                        function=AggregationFunction(operator=op(input=e["input"]), time_window=_window(e["window"])),
                        entity=e["entity"], timeseries_column=timeseries_column)
            by_name[e["name"]] = f; aggs.append(f)
    for e in entries:
        if e["type"] != "custom_udf": continue
        ups = [by_name[v] for v in e["bindings"].values()]
        _create(e["name"], source=FeatureViewSource(features=ups),
                function=CustomUDF(e["function_name"], input_bindings={p: _full(v) for p, v in e["bindings"].items()}))
    return colsels, aggs

def _materialize_missing(label, feats, **cfg):
    """Materialize only features with no online materialization yet. Missing ones go in ONE call so
    features sharing an entity co-locate in one table; if that still hits "already exists" (a race or a
    sibling materialized elsewhere), fall back to one call per feature so no feature is silently skipped."""
    missing = [f for f in feats if _online_mf(f.full_name) is None]
    if not missing:
        return f"{label}: all {len(feats)} already online"
    try:
        fe.materialize_features(features=missing, **cfg)
        return f"{label}: materialized {len(missing)}"
    except Exception as e:  # noqa: BLE001
        if not _is_dup(e):
            raise
    done = []
    for f in missing:
        try:
            fe.materialize_features(features=[f], **cfg); done.append(f.name)
        except Exception as e:  # noqa: BLE001
            if not _is_dup(e):
                raise
    return f"{label}: materialized {len(done)} individually ({len(missing) - len(done)} already online)"

# COMMAND ----------

import psycopg
endpoint_path = f"projects/{online_store_name}/branches/production/endpoints/primary"
me = w.current_user.me().user_name

def _pg_connect(db_):
    pg_host = w.api_client.do("GET", f"/api/2.0/postgres/{endpoint_path}")["status"]["hosts"]["host"]
    tok = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint_path})["token"]
    return psycopg.connect(host=pg_host, port=5432, dbname=db_, user=me, password=tok, sslmode="require")

def _grant(online_tables):
    """The pipelines create the online Postgres tables asynchronously (a few minutes). Serving can't read a
    table until it exists AND the serving role has SELECT, so wait for each, then grant to PUBLIC."""
    grants = []
    pending = {t: t.split(".") for t in online_tables}  # name -> [db, schema, table]
    deadline = time.time() + 540  # ~9 min
    while pending and time.time() < deadline:
        try:
            with _pg_connect(next(iter(pending.values()))[0]) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    for name in list(pending):
                        _, sch_, tbl_ = pending[name]
                        cur.execute("SELECT to_regclass(%s)", (f'"{sch_}"."{tbl_}"',))
                        if cur.fetchone()[0] is None:
                            continue  # not created yet
                        # Already readable by PUBLIC (grantee oid 0)? Then nothing to do — and the caller may
                        # not own it (e.g. the app SP re-syncing tables a user materialized), so don't try.
                        cur.execute("""SELECT
                              EXISTS (SELECT 1 FROM pg_namespace n, aclexplode(n.nspacl) a
                                      WHERE n.nspname = %s AND a.grantee = 0 AND a.privilege_type = 'USAGE'),
                              EXISTS (SELECT 1 FROM aclexplode((SELECT relacl FROM pg_class WHERE oid = %s::regclass)) a
                                      WHERE a.grantee = 0 AND a.privilege_type = 'SELECT')""",
                                    (sch_, f'"{sch_}"."{tbl_}"'))
                        has_usage, has_select = cur.fetchone()
                        try:
                            if not has_usage:
                                cur.execute(f'GRANT USAGE ON SCHEMA "{sch_}" TO PUBLIC')
                            if not has_select:
                                cur.execute(f'GRANT SELECT ON "{sch_}"."{tbl_}" TO PUBLIC')
                            grants.append(f"{'granted' if not (has_usage and has_select) else 'already granted'} {sch_}.{tbl_}")
                        except psycopg.errors.InsufficientPrivilege as ge:
                            grants.append(f"cannot grant {sch_}.{tbl_} (not owner): {str(ge)[:120]}")
                        del pending[name]
        except Exception as e:  # noqa: BLE001
            grants.append(f"poll error: {str(e)[:120]}")
        if pending:
            time.sleep(20)
    for name in pending:
        grants.append(f"timed out waiting for {name}")
    return grants

# COMMAND ----------

def op_ensure():
    colsels, aggs = _build_features()
    results = []
    if colsels:
        results.append(_materialize_missing("colsels", colsels,
            online_config=OnlineStoreConfig(catalog_name=catalog, schema_name=schema, table_name_prefix=f"{prefix}cs_", online_store_name=online_store_name),
            trigger=TableTrigger()))
    if aggs:
        results.append(_materialize_missing("aggs", aggs,
            offline_config=OfflineStoreConfig(catalog_name=catalog, schema_name=schema, table_name_prefix=f"{prefix}aggoff_"),
            online_config=OnlineStoreConfig(catalog_name=catalog, schema_name=schema, table_name_prefix=f"{prefix}aggon_", online_store_name=online_store_name),
            trigger=CronSchedule()))
    features = _feature_report()
    missing = [f["feature_full_name"] for f in features if f["type"] != "custom_udf" and not f["online_table"]]
    if missing:
        raise RuntimeError(f"Features still not online after materialize: {missing}")
    online_tables = sorted({f["online_table"] for f in features if f["online_table"]})
    return {"results": results, "features": features, "online_tables": online_tables, "grants": _grant(online_tables)}

def op_release():
    """Tear down whole online TABLES (the server only asks for tables no deployment uses). Every feature
    materialized into the table is found from its source features' MFs; offline siblings are deleted
    first (cascades to online), then any remaining online-only MFs. Only this notebook's own prefix."""
    report = []
    for table in release_tables:
        row = {"online_table": table, "deleted": [], "errors": []}
        if not table.split(".")[-1].startswith(prefix):
            row["errors"].append(f"refusing: not a {prefix}* table"); report.append(row); continue
        # Features that live in this table: listed via the deployment registry's feature names.
        names = [n for n, t in release_feature_tables.items() if t == table]
        for full in names:
            mfs = _mfs(full)
            online = [m for m in mfs if getattr(m, "is_online", False) and getattr(m, "table_name", None) == table]
            if not online:
                continue
            offline = [m for m in mfs if not getattr(m, "is_online", False)
                       and (getattr(m, "table_name", None) or "").split(".")[-1].startswith(prefix)]
            for m in offline + online:
                try:
                    fe.delete_materialized_feature(materialized_feature=m)
                    row["deleted"].append(f"{full} → {getattr(m, 'table_name', None)}")
                except Exception as e:  # noqa: BLE001 — online sibling already gone via cascade
                    msg = str(e).upper()
                    if "NOT FOUND" not in msg and "DOES NOT EXIST" not in msg:
                        row["errors"].append(f"{full}: {str(e)[:200]}")
        report.append(row)
    return {"released": report}

# `release` needs feature→table pairs (one table holds several co-located features).
release_feature_tables = json.loads(dbutils.widgets.get("feature_tables_json") or "{}") if operation == "release" else {}

if operation == "ensure":
    out = op_ensure()
elif operation == "release":
    out = op_release()
elif operation == "status":
    out = {"features": _feature_report()}
else:
    raise ValueError(f"unknown operation {operation}")

print(json.dumps(out, default=str)[:4000])
dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "operation": operation, **out}, default=str))
