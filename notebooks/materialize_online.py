# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering>=0.18.0 'psycopg[binary]'
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC # Materialize Declarative Features to the Online Store (on a schedule)
# MAGIC App-managed notebook. Registers declarative aggregation features in Unity Catalog
# MAGIC (`fe.create_feature`) and starts a managed materialization pipeline that pushes them to
# MAGIC the offline + online stores on a chosen schedule (`fe.materialize_features`).
# MAGIC
# MAGIC This is the "feature views" path (https://docs.databricks.com/.../feature-store/feature-views):
# MAGIC Databricks owns both the recompute of the window aggregation AND the push to Lakebase.
# MAGIC Contrast with `publish_table.py`, which only mirrors a precomputed Delta table and does not
# MAGIC recompute aggregations.
# MAGIC
# MAGIC Operations (via the `operation` widget):
# MAGIC   - `create`  — create_feature(s) + materialize_features with the chosen trigger
# MAGIC   - `pause`   — pause the materialization pipeline (schedule state → PAUSED)
# MAGIC   - `resume`  — resume (schedule state → ACTIVE)
# MAGIC   - `delete`  — delete the materialized feature(s) (stops the pipeline) and the UC feature
# MAGIC   - `status`  — report current state (schedule, last run) without mutating anything

# COMMAND ----------

dbutils.widgets.text("operation", "create")
dbutils.widgets.text("declarative_features_json", "[]")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("online_store_name", "")
dbutils.widgets.text("table_name_prefix", "mlops_feat_")
# Trigger config (create only): trigger_type ∈ {derived, manual_cron, table_trigger}
dbutils.widgets.text("trigger_type", "derived")
dbutils.widgets.text("cron_expression", "")
dbutils.widgets.text("timezone_id", "")
# For pause/resume/delete/status: the UC feature full names to act on
dbutils.widgets.text("feature_full_names_json", "[]")

# COMMAND ----------

import json
from datetime import timedelta

operation = dbutils.widgets.get("operation").strip()
declarative_defs = json.loads(dbutils.widgets.get("declarative_features_json"))
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
online_store_name = dbutils.widgets.get("online_store_name").strip()
table_name_prefix = dbutils.widgets.get("table_name_prefix").strip() or "mlops_feat_"
trigger_type = dbutils.widgets.get("trigger_type").strip() or "derived"
cron_expression = dbutils.widgets.get("cron_expression").strip()
timezone_id = dbutils.widgets.get("timezone_id").strip()
feature_full_names = json.loads(dbutils.widgets.get("feature_full_names_json"))

print(f"operation={operation} catalog={catalog} schema={schema} store={online_store_name}")
print(f"trigger_type={trigger_type} cron='{cron_expression}' tz='{timezone_id}'")

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities import (
    DeltaTableSource,
    AggregationFunction,
    TumblingWindow,
    SlidingWindow,
    RollingWindow,
    CronSchedule,
    TableTrigger,
)
from databricks.ml_features.entities.function import AGGREGATION_FUNCTION_BY_SHORTHAND
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.ml import (
    OfflineStoreConfig,
    OnlineStoreConfig,
    MaterializedFeature,
    MaterializedFeaturePipelineScheduleState,
)

fe = FeatureEngineeringClient()
w = WorkspaceClient()


def parse_duration(s):
    """'30d' | '12h' | '60m' -> timedelta. Bare number = days."""
    s = (s or "").strip()
    if not s:
        raise ValueError("Window duration is empty — check the declarative feature definition")
    unit = s[-1]
    if unit == "d":
        return timedelta(days=int(s[:-1]))
    if unit == "h":
        return timedelta(hours=int(s[:-1]))
    if unit == "m":
        return timedelta(minutes=int(s[:-1]))
    return timedelta(days=int(s))


def build_window(tw):
    """Map the app's window spec to a 0.18 TimeWindow. 'continuous' -> RollingWindow
    (ContinuousWindow was removed in 0.18)."""
    kind = tw["type"]
    dur = parse_duration(tw["window_duration"])
    if kind == "tumbling":
        return TumblingWindow(window_duration=dur)
    if kind == "sliding":
        return SlidingWindow(window_duration=dur, slide_duration=parse_duration(tw["slide_duration"]))
    if kind == "continuous":
        return RollingWindow(window_duration=dur)
    raise ValueError(f"Unknown window type: {kind}")


def feature_full_name(name):
    return f"{catalog}.{schema}.{name}"


def build_trigger():
    if trigger_type == "table_trigger":
        return TableTrigger()
    if trigger_type == "manual_cron":
        if not cron_expression:
            raise ValueError("manual_cron trigger requires a quartz cron expression")
        return CronSchedule(quartz_cron_expression=cron_expression, timezone_id=timezone_id or "UTC")
    # 'derived': let the service compute the cadence from the window/delay.
    # timezone_id is incompatible with DERIVED mode, so don't pass it.
    return CronSchedule()


def mf_to_dict(mf):
    """Serialize an SDK MaterializedFeature for the app, tolerating missing fields."""
    state = getattr(mf, "pipeline_schedule_state", None)
    return {
        "feature_name": getattr(mf, "feature_name", None),
        "materialized_feature_id": getattr(mf, "materialized_feature_id", None),
        "online_table_name": getattr(mf, "table_name", None),
        "pipeline_id": getattr(mf, "pipeline_id", None),
        "job_id": getattr(mf, "job_id", None),
        "schedule_state": state.value if hasattr(state, "value") else (str(state) if state else None),
        "resolved_cron": getattr(mf, "cron_schedule", None),
        "is_online": getattr(mf, "is_online", None),
        "last_materialization_time": getattr(mf, "last_materialization_time", None),
    }

# COMMAND ----------


def op_create():
    """Register each declarative feature in UC, then materialize to offline + online stores."""
    # Offline and online destinations must differ (same catalog.schema is fine, but the
    # table_name_prefix must not collide), so derive distinct prefixes from the base.
    offline_config = OfflineStoreConfig(
        catalog_name=catalog, schema_name=schema, table_name_prefix=f"{table_name_prefix}off_",
    )
    online_config = OnlineStoreConfig(
        catalog_name=catalog, schema_name=schema, table_name_prefix=f"{table_name_prefix}on_",
        online_store_name=online_store_name,
    )
    trigger = build_trigger()

    results = []
    for dd in declarative_defs:
        name = dd["name"]
        full_name = feature_full_name(name)

        # Build the aggregation function: operator (e.g. Sum(input="amount")) + time window.
        fn_shorthand = dd["function"]
        operator_cls = AGGREGATION_FUNCTION_BY_SHORTHAND.get(fn_shorthand)
        if operator_cls is None:
            raise ValueError(f"Unsupported aggregation function '{fn_shorthand}'")
        operator = operator_cls(input=dd["input"])
        agg = AggregationFunction(operator=operator, time_window=build_window(dd["time_window"]))

        # Source Delta table; filter_condition now lives on the source (0.18 API change).
        src = dd["source_table"].split(".")
        src_catalog = src[0] if len(src) == 3 else catalog
        src_schema = src[1] if len(src) == 3 else schema
        src_table = src[-1]
        source_kwargs = dict(catalog_name=src_catalog, schema_name=src_schema, table_name=src_table)
        if dd.get("filter_condition"):
            source_kwargs["filter_condition"] = dd["filter_condition"]
        source = DeltaTableSource(**source_kwargs)

        entity = dd.get("lookup_key") or dd.get("entity_columns") or []
        timeseries_column = dd.get("timestamp_lookup_key") or dd.get("timeseries_column") or None

        # Register the feature in UC (idempotent: reuse if it already exists).
        try:
            feat = fe.create_feature(
                source=source, function=agg, catalog_name=catalog, schema_name=schema,
                entity=entity, timeseries_column=timeseries_column, name=name,
            )
            print(f"Created feature {full_name}")
        except Exception as e:  # noqa: BLE001 — already-exists is the expected re-run case
            if "ALREADY" in str(e).upper() or "EXIST" in str(e).upper():
                feat = fe.get_feature(full_name=full_name)
                print(f"Feature {full_name} already exists — reusing")
            else:
                raise

        # Materialize to offline + online (aggregation features REQUIRE offline_config).
        try:
            materialized = fe.materialize_features(
                features=[feat], offline_config=offline_config,
                online_config=online_config, trigger=trigger,
            )
        except Exception as e:  # noqa: BLE001
            if "ALREADY" in str(e).upper() or "EXIST" in str(e).upper():
                materialized = fe.list_materialized_features(feature_name=full_name)
                print(f"{full_name} already materialized — returning existing state")
            else:
                raise

        online_mf = None
        for mf in (materialized or []):
            if getattr(mf, "online_store_config", None) is not None or getattr(mf, "is_online", False):
                online_mf = mf
                break
        online_mf = online_mf or (materialized[0] if materialized else None)

        row = {"feature_name": name, "feature_full_name": full_name}
        if online_mf is not None:
            row.update(mf_to_dict(online_mf))
            _grant_online_select(online_mf)
        results.append(row)

    return results


def _grant_online_select(mf):
    """Grant SELECT on the online table to PUBLIC so the serving endpoint's Postgres role
    can read it. Mirrors publish_table.py — materialize_features does not grant it either.
    Best-effort: a failure here must not fail the whole materialization."""
    online_table_name = getattr(mf, "table_name", None)
    if not online_table_name or online_table_name.count(".") != 2:
        print(f"[grant] skip: no 3-part online table name ({online_table_name})")
        return
    try:
        import psycopg
        pg_database, pg_schema, pg_table = online_table_name.split(".")
        me = w.current_user.me().user_name
        endpoint_path = f"projects/{online_store_name}/branches/production/endpoints/primary"
        endpoint_info = w.api_client.do("GET", f"/api/2.0/postgres/{endpoint_path}")
        pg_host = endpoint_info["status"]["hosts"]["host"]
        cred = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint_path})
        with psycopg.connect(
            host=pg_host, port=5432, dbname=pg_database, user=me,
            password=cred["token"], sslmode="require",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f'GRANT USAGE ON SCHEMA "{pg_schema}" TO PUBLIC')
                cur.execute(f'GRANT SELECT ON "{pg_schema}"."{pg_table}" TO PUBLIC')
            conn.commit()
        print(f"[grant] USAGE+SELECT on {pg_schema}.{pg_table} to PUBLIC")
    except Exception as e:  # noqa: BLE001
        print(f"[grant] WARNING (serving may need a manual grant): {e}")


def _materialized_for(full_name):
    """Return the list of SDK MaterializedFeature objects for a UC feature."""
    try:
        return fe.list_materialized_features(feature_name=full_name) or []
    except Exception as e:  # noqa: BLE001
        print(f"[list] {full_name}: {e}")
        return []


def op_set_state(target_state):
    """Pause/resume the ONLINE materialization's pipeline schedule state. A feature has both
    an offline and an online materialized feature; only the online one drives serving freshness
    and is individually updatable here. Only pipeline_schedule_state is live-updatable; changing
    the cron itself requires delete + re-create."""
    results = []
    for full_name in feature_full_names:
        mfs = _materialized_for(full_name)
        targets = [m for m in mfs if getattr(m, "is_online", False)] or mfs
        updated_rows = []
        for mf in targets:
            mfid = getattr(mf, "materialized_feature_id", None)
            if not mfid:
                continue
            try:
                # The PATCH returns the authoritative updated object (no list-read lag).
                updated = w.feature_engineering.update_materialized_feature(
                    materialized_feature_id=mfid,
                    materialized_feature=MaterializedFeature(
                        feature_name=getattr(mf, "feature_name", full_name),
                        pipeline_schedule_state=target_state,
                    ),
                    update_mask="pipeline_schedule_state",
                )
                updated_rows.append({"feature_full_name": full_name, **mf_to_dict(updated)})
            except Exception as e:  # noqa: BLE001 — tolerate a sibling that can't be updated
                print(f"[set_state] {mfid}: {e}")
        results.extend(updated_rows)
    return results


def op_delete():
    results = []
    for full_name in feature_full_names:
        for mf in _materialized_for(full_name):
            try:
                fe.delete_materialized_feature(materialized_feature=mf)
            except Exception as e:  # noqa: BLE001
                print(f"[delete] materialized {full_name}: {e}")
        try:
            fe.delete_feature(full_name=full_name)
        except Exception as e:  # noqa: BLE001
            print(f"[delete] feature {full_name}: {e}")
        results.append({"feature_full_name": full_name, "deleted": True})
    return results


def op_status():
    results = []
    for full_name in feature_full_names:
        mfs = _materialized_for(full_name)
        online_mf = next((m for m in mfs if getattr(m, "is_online", False)), (mfs[0] if mfs else None))
        row = {"feature_full_name": full_name}
        if online_mf is not None:
            row.update(mf_to_dict(online_mf))
        results.append(row)
    return results

# COMMAND ----------

if operation == "create":
    out = op_create()
elif operation == "pause":
    out = op_set_state(MaterializedFeaturePipelineScheduleState.PAUSED)
elif operation == "resume":
    out = op_set_state(MaterializedFeaturePipelineScheduleState.ACTIVE)
elif operation == "delete":
    out = op_delete()
elif operation == "status":
    out = op_status()
else:
    raise ValueError(f"Unknown operation: {operation}")

dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "operation": operation, "results": out}))
