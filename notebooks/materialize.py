# Databricks notebook source
# MAGIC %md
# MAGIC # Materialize Dataset
# MAGIC App-managed notebook: materializes a training set from an EOL SQL + feature lookups,
# MAGIC splits into train/eval tables, and writes to Unity Catalog.

# COMMAND ----------

dbutils.widgets.text("eol_sql", "")
dbutils.widgets.text("label_column", "")
dbutils.widgets.text("entity_columns_json", "[]")
dbutils.widgets.text("feature_definitions_json", "[]")
dbutils.widgets.text("declarative_features_json", "[]")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("training_table_name", "")
dbutils.widgets.text("eval_table_name", "")
dbutils.widgets.text("eval_split_percentage", "20")

# COMMAND ----------

import json

eol_sql = dbutils.widgets.get("eol_sql")
label_column = dbutils.widgets.get("label_column")
entity_columns = json.loads(dbutils.widgets.get("entity_columns_json"))
feature_defs = json.loads(dbutils.widgets.get("feature_definitions_json"))
declarative_defs = json.loads(dbutils.widgets.get("declarative_features_json"))
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
training_table_name = dbutils.widgets.get("training_table_name")
eval_table_name = dbutils.widgets.get("eval_table_name")
eval_split_pct = float(dbutils.widgets.get("eval_split_percentage")) / 100.0

print(f"EOL SQL: {eol_sql[:200]}...")
print(f"Label: {label_column}, Entity columns: {entity_columns}")
print(f"Feature lookups: {len(feature_defs)}, Declarative features: {len(declarative_defs)}")
print(f"Output: {training_table_name} / {eval_table_name}")
print(f"Eval split: {eval_split_pct*100}%")

# COMMAND ----------

# Step 1: Execute EOL SQL to get the base DataFrame
eol_df = spark.sql(eol_sql)
print(f"EOL rows: {eol_df.count()}, columns: {eol_df.columns}")

# COMMAND ----------

# Step 2: Build FeatureLookups and declarative Features
full_df = eol_df

if feature_defs or declarative_defs:
    from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
    from datetime import timedelta

    fe = FeatureEngineeringClient()
    feature_lookups = []

    # Standard FeatureLookups
    for fd in feature_defs:
        fl_kwargs = {
            "table_name": fd["table_name"],
            "lookup_key": fd["lookup_key"],
        }
        if fd.get("feature_names"):
            fl_kwargs["feature_names"] = fd["feature_names"]
        if fd.get("timestamp_lookup_key"):
            fl_kwargs["timestamp_lookup_key"] = fd["timestamp_lookup_key"]

        feature_lookups.append(FeatureLookup(**fl_kwargs))

    print(f"Built {len(feature_lookups)} FeatureLookups")

    # Declarative Features (databricks-feature-engineering 0.18 API):
    #   function=AggregationFunction(operator, time_window); filter_condition lives on the
    #   DeltaTableSource; entity/timeseries/name go on create_feature. ContinuousWindow was
    #   removed → use RollingWindow. Mirrors notebooks/materialize_online.py.
    import re
    from databricks.feature_engineering.entities import (
        DeltaTableSource, AggregationFunction,
        TumblingWindow, SlidingWindow, RollingWindow,
    )
    from databricks.ml_features.entities.function import AGGREGATION_FUNCTION_BY_SHORTHAND

    def parse_duration(s):
        s = (s or "").strip()
        if not s:
            raise ValueError("Window duration is empty — check the declarative feature definition")
        if s.endswith('d'):
            return timedelta(days=int(s[:-1]))
        if s.endswith('h'):
            return timedelta(hours=int(s[:-1]))
        if s.endswith('m'):
            return timedelta(minutes=int(s[:-1]))
        return timedelta(days=int(s))

    def feature_name_for(dd):
        # Prefer the user-supplied name; fall back to a generated one (kept in sync with
        # makeFeatureName() in server.ts so offline + online share a feature).
        if dd.get("name"):
            return dd["name"]
        tw = dd.get("time_window", {})
        base = f"{dd.get('function', 'agg')}_{dd.get('input', 'x')}_{tw.get('window_duration', '')}".lower()
        return re.sub(r'[^a-z0-9]+', '_', base).strip('_')[:120] or "feature"

    features = []
    for dd in declarative_defs:
        tw = dd["time_window"]
        window_duration = parse_duration(tw["window_duration"])
        if tw["type"] == "continuous":
            time_window = RollingWindow(window_duration=window_duration)
        elif tw["type"] == "tumbling":
            time_window = TumblingWindow(window_duration=window_duration)
        elif tw["type"] == "sliding":
            time_window = SlidingWindow(window_duration=window_duration, slide_duration=parse_duration(tw["slide_duration"]))
        else:
            raise ValueError(f"Unknown window type: {tw['type']}")

        operator_cls = AGGREGATION_FUNCTION_BY_SHORTHAND.get(dd["function"])
        if operator_cls is None:
            raise ValueError(f"Unsupported aggregation function '{dd['function']}'")
        agg = AggregationFunction(operator=operator_cls(input=dd["input"]), time_window=time_window)

        src_parts = dd["source_table"].split(".")
        src_catalog = src_parts[0] if len(src_parts) == 3 else catalog
        src_schema = src_parts[1] if len(src_parts) == 3 else schema
        src_table = src_parts[-1]
        source_kwargs = dict(catalog_name=src_catalog, schema_name=src_schema, table_name=src_table)
        if dd.get("filter_condition"):
            source_kwargs["filter_condition"] = dd["filter_condition"]
        source = DeltaTableSource(**source_kwargs)

        name = feature_name_for(dd)
        entity = dd.get("lookup_key") or dd.get("entity_columns") or entity_columns
        timeseries_column = dd.get("timestamp_lookup_key") or dd.get("timeseries_column") or None

        try:
            feat = fe.create_feature(
                source=source, function=agg, catalog_name=catalog, schema_name=schema,
                entity=entity, timeseries_column=timeseries_column, name=name,
            )
        except Exception as e:  # noqa: BLE001 — idempotent re-run
            if "ALREADY" in str(e).upper() or "EXIST" in str(e).upper():
                feat = fe.get_feature(full_name=f"{catalog}.{schema}.{name}")
            else:
                raise
        features.append(feat)

    print(f"Built {len(features)} declarative Features")

    # Exclude entity columns + any timestamp lookup keys (observation dates)
    exclude = list(entity_columns) if entity_columns else []
    for fd in feature_defs:
        ts_key = fd.get("timestamp_lookup_key")
        if ts_key and ts_key not in exclude:
            exclude.append(ts_key)
    for dd in declarative_defs:
        ts_key = dd.get("timestamp_lookup_key")
        if ts_key and ts_key not in exclude:
            exclude.append(ts_key)

    # 0.18 constraint: create_training_set accepts feature_lookups OR features, not both.
    if features and feature_lookups:
        raise ValueError(
            "This training spec mixes declarative (aggregation) features with FeatureLookups. "
            "databricks-feature-engineering 0.18 does not allow both in one create_training_set — "
            "use one kind per spec, or materialize the aggregation features online (Online Features "
            "tab) and reference their tables as FeatureLookups instead."
        )
    ts_kwargs = dict(
        df=eol_df,
        label=label_column if label_column else None,
        exclude_columns=exclude,
    )
    if features:
        ts_kwargs["features"] = features
    else:
        ts_kwargs["feature_lookups"] = feature_lookups
    training_set = fe.create_training_set(**ts_kwargs)
    full_df = training_set.load_df()

print(f"Full dataset: {full_df.count()} rows, {len(full_df.columns)} columns")

# COMMAND ----------

# Step 3: Split into train/eval
train_df, eval_df = full_df.randomSplit([1 - eval_split_pct, eval_split_pct], seed=42)
train_count = train_df.count()
eval_count = eval_df.count()
print(f"Train: {train_count} rows, Eval: {eval_count} rows")

# COMMAND ----------

# Step 4: Write to Unity Catalog
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

train_df.write.mode("overwrite").saveAsTable(training_table_name)
eval_df.write.mode("overwrite").saveAsTable(eval_table_name)

print(f"Written: {training_table_name} ({train_count} rows)")
print(f"Written: {eval_table_name} ({eval_count} rows)")

# COMMAND ----------

# Return results
dbutils.notebook.exit(json.dumps({
    "status": "READY",
    "training_table": training_table_name,
    "eval_table": eval_table_name,
    "row_count": train_count + eval_count,
}))
