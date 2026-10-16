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

# COMMAND ----------

dbutils.widgets.text("feature_entries_json", "[]")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("timeseries_column", "")
dbutils.widgets.text("online_store_name", "")
dbutils.widgets.text("table_prefix", "mlops_srv_")

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

fe = FeatureEngineeringClient(); w = WorkspaceClient()
if not timeseries_column:
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
def _create(name, **kw):
    try:
        return fe.create_feature(catalog_name=catalog, schema_name=schema, name=name, **kw)
    except Exception as e:  # noqa: BLE001
        if "ALREADY" in str(e).upper() or "EXIST" in str(e).upper():
            return fe.get_feature(full_name=_full(name))
        raise

# COMMAND ----------

# Rebuild the feature objects (idempotent) — same as train_cv.py
by_name = {}
colsels = []
aggs = []
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

# COMMAND ----------

# Materialize to the online store (separate calls: TableTrigger for colsels, CronSchedule for aggs).
# Collect ONLY the online (is_online) table names — the offline agg Delta table lives in UC, not the
# online store, so it is neither tracked here nor granted.
online_tables = []

def _collect(res):
    for mf in (res or []):
        t = getattr(mf, "table_name", None)
        if t and getattr(mf, "is_online", False) and t not in online_tables:
            online_tables.append(t)

def _already_online():
    for e2 in entries:
        nm = e2.get("name")
        if not nm:
            continue
        try:
            for mf in (fe.list_materialized_features(feature_name=_full(nm)) or []):
                if getattr(mf, "is_online", False):
                    t = getattr(mf, "table_name", None)
                    if t and t not in online_tables:
                        online_tables.append(t)
        except Exception:
            pass

def _materialize(label, fn):
    try:
        _collect(fn()); return f"{label}: ok"
    except Exception as e:  # noqa: BLE001
        if "ALREADY" in str(e).upper() or "EXIST" in str(e).upper():
            _already_online(); return f"{label}: already materialized"
        raise

results = []
if colsels:
    results.append(_materialize("colsels", lambda: fe.materialize_features(
        features=colsels,
        online_config=OnlineStoreConfig(catalog_name=catalog, schema_name=schema, table_name_prefix=f"{prefix}cs_", online_store_name=online_store_name),
        trigger=TableTrigger())))
if aggs:
    results.append(_materialize("aggs", lambda: fe.materialize_features(
        features=aggs,
        offline_config=OfflineStoreConfig(catalog_name=catalog, schema_name=schema, table_name_prefix=f"{prefix}aggoff_"),
        online_config=OnlineStoreConfig(catalog_name=catalog, schema_name=schema, table_name_prefix=f"{prefix}aggon_", online_store_name=online_store_name),
        trigger=CronSchedule())))

# COMMAND ----------

# The materialization pipelines create the online Postgres tables asynchronously (a few minutes).
# Serving can't read a table until it exists AND the serving role has SELECT, so wait for each
# online table to appear, then grant USAGE+SELECT to PUBLIC. Mirrors notebooks/publish_table.py.
import psycopg
endpoint_path = f"projects/{online_store_name}/branches/production/endpoints/primary"
me = w.current_user.me().user_name

def _pg_connect():
    pg_host = w.api_client.do("GET", f"/api/2.0/postgres/{endpoint_path}")["status"]["hosts"]["host"]
    tok = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint_path})["token"]
    db_ = online_tables[0].split(".")[0] if online_tables else catalog
    return psycopg.connect(host=pg_host, port=5432, dbname=db_, user=me, password=tok, sslmode="require")

grants = []
pending = {t: t.split(".") for t in online_tables}  # name -> [db, schema, table]
deadline = time.time() + 540  # ~9 min
while pending and time.time() < deadline:
    try:
        with _pg_connect() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                for name in list(pending):
                    _, sch_, tbl_ = pending[name]
                    cur.execute("SELECT to_regclass(%s)", (f'"{sch_}"."{tbl_}"',))
                    if cur.fetchone()[0] is None:
                        continue  # not created yet
                    cur.execute(f'GRANT USAGE ON SCHEMA "{sch_}" TO PUBLIC')
                    cur.execute(f'GRANT SELECT ON "{sch_}"."{tbl_}" TO PUBLIC')
                    grants.append(f"granted {sch_}.{tbl_}")
                    del pending[name]
    except Exception as e:  # noqa: BLE001
        grants.append(f"poll error: {str(e)[:120]}")
    if pending:
        time.sleep(20)
for name in pending:
    grants.append(f"timed out waiting for {name}")

dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "results": results, "online_tables": online_tables, "grants": grants}))
