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

    # Declarative Features
    features = []
    for dd in declarative_defs:
        from databricks.feature_engineering.entities import (
            Feature, DeltaTableSource,
            ContinuousWindow, TumblingWindow, SlidingWindow,
        )

        # Parse duration string (e.g. "30d", "12h", "60m") to timedelta
        def parse_duration(s):
            if not s or not s.strip():
                raise ValueError(f"Window duration is empty — check the declarative feature definition")
            s = s.strip()
            if s.endswith('d'):
                return timedelta(days=int(s[:-1]))
            elif s.endswith('h'):
                return timedelta(hours=int(s[:-1]))
            elif s.endswith('m'):
                return timedelta(minutes=int(s[:-1]))
            else:
                return timedelta(days=int(s))

        tw = dd["time_window"]
        window_duration = parse_duration(tw["window_duration"])
        if tw["type"] == "continuous":
            time_window = ContinuousWindow(window_duration=window_duration)
        elif tw["type"] == "tumbling":
            time_window = TumblingWindow(window_duration=window_duration)
        elif tw["type"] == "sliding":
            slide_duration = parse_duration(tw["slide_duration"])
            time_window = SlidingWindow(window_duration=window_duration, slide_duration=slide_duration)

        # Parse the three-level source table name
        src_parts = dd["source_table"].split(".")
        src_catalog = src_parts[0] if len(src_parts) == 3 else catalog
        src_schema = src_parts[1] if len(src_parts) == 3 else schema
        src_table = src_parts[-1]

        # Determine entity and timeseries columns from EOL
        ts_col = dd.get("timestamp_lookup_key") or (entity_columns[0] if not entity_columns else None)
        source = DeltaTableSource(
            table_name=src_table,
            catalog_name=src_catalog,
            schema_name=src_schema,
            entity_columns=dd.get("lookup_key") or dd.get("entity_columns") or entity_columns,
            timeseries_column=dd.get("timestamp_lookup_key") or dd.get("timeseries_column") or "",
        )

        feat = fe.create_feature(
            source=source,
            inputs=[dd["input"]],
            function=dd["function"],
            time_window=time_window,
            catalog_name=catalog,
            schema_name=schema,
            **({"filter_condition": dd["filter_condition"]} if dd.get("filter_condition") else {}),
        )
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

    training_set = fe.create_training_set(
        df=eol_df,
        feature_lookups=feature_lookups,
        features=features if features else None,
        label=label_column if label_column else None,
        exclude_columns=exclude,
    )
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
