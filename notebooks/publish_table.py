# Databricks notebook source
# MAGIC %md
# MAGIC # Publish Feature Table to Online Store
# MAGIC App-managed notebook: publishes a UC feature table to a Databricks Online Feature Store
# MAGIC (Lakebase) for model serving with auto feature lookup.

# COMMAND ----------

dbutils.widgets.text("online_store_name", "")
dbutils.widgets.text("source_table_name", "")
dbutils.widgets.text("online_table_name", "")
dbutils.widgets.text("publish_mode", "TRIGGERED")

# COMMAND ----------

import json

online_store_name = dbutils.widgets.get("online_store_name")
source_table_name = dbutils.widgets.get("source_table_name")
online_table_name = dbutils.widgets.get("online_table_name")
publish_mode = dbutils.widgets.get("publish_mode")

print(f"Online store: {online_store_name}")
print(f"Source: {source_table_name}")
print(f"Destination: {online_table_name}")
print(f"Mode: {publish_mode}")

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Get or create the online store
# Per docs: for Lakebase projects, the online store name = project name
try:
    online_store = fe.get_online_store(name=online_store_name)
    print(f"Using existing online store: {online_store}")
except Exception:
    print(f"Creating online store: {online_store_name}")
    fe.create_online_store(name=online_store_name, capacity="CU_1")
    online_store = fe.get_online_store(name=online_store_name)
    print(f"Created online store: {online_store}")

# COMMAND ----------

# Publish the feature table
fe.publish_table(
    online_store=online_store,
    source_table_name=source_table_name,
    online_table_name=online_table_name,
    publish_mode=publish_mode,
)

print(f"Published: {source_table_name} → {online_table_name} ({publish_mode})")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "source_table": source_table_name,
    "online_table": online_table_name,
    "publish_mode": publish_mode,
}))
