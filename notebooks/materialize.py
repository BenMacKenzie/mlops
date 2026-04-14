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
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
training_table_name = dbutils.widgets.get("training_table_name")
eval_table_name = dbutils.widgets.get("eval_table_name")
eval_split_pct = float(dbutils.widgets.get("eval_split_percentage")) / 100.0

print(f"EOL SQL: {eol_sql[:200]}...")
print(f"Label: {label_column}, Entity columns: {entity_columns}")
print(f"Feature definitions: {len(feature_defs)}")
print(f"Output: {training_table_name} / {eval_table_name}")
print(f"Eval split: {eval_split_pct*100}%")

# COMMAND ----------

# Step 1: Execute EOL SQL to get the base DataFrame
eol_df = spark.sql(eol_sql)
print(f"EOL rows: {eol_df.count()}, columns: {eol_df.columns}")

# COMMAND ----------

# Step 2: Build FeatureLookups if any feature definitions are provided
full_df = eol_df

if feature_defs:
    from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

    fe = FeatureEngineeringClient()
    feature_lookups = []

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

    training_set = fe.create_training_set(
        df=eol_df,
        feature_lookups=feature_lookups,
        label=label_column if label_column else None,
        exclude_columns=entity_columns if entity_columns else [],
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
