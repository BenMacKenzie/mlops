# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering>=0.15.0 'psycopg[binary]'
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC # Publish Feature Table to Online Store
# MAGIC App-managed notebook: publishes a UC feature table to a Databricks Online Feature Store
# MAGIC (Lakebase) for model serving with auto feature lookup.
# MAGIC
# MAGIC Follows: /Users/ben.mackenzie@databricks.com/feature-store-online-example-lakebase

# COMMAND ----------

dbutils.widgets.text("online_store_name", "")
dbutils.widgets.text("source_table_name", "")
dbutils.widgets.text("online_table_name", "")
dbutils.widgets.text("publish_mode", "TRIGGERED")

# COMMAND ----------

import json
import time

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

# Look up the online store. Per the reference notebook, get_online_store returns
# None (not an exception) when the store does not exist.
online_store = fe.get_online_store(name=online_store_name)

if online_store is None:
    print(f"Creating online store: {online_store_name}")
    fe.create_online_store(name=online_store_name, capacity="CU_1")
    # Poll until AVAILABLE
    for attempt in range(60):
        online_store = fe.get_online_store(name=online_store_name)
        state = online_store.state if online_store else "NOT_FOUND"
        print(f"  attempt {attempt}: state={state}")
        if online_store and online_store.state == "AVAILABLE":
            break
        time.sleep(5)
    if online_store is None or online_store.state != "AVAILABLE":
        raise RuntimeError(
            f"Online store '{online_store_name}' not available after wait: {online_store}"
        )

print(f"Using online store: {online_store.name} (state={online_store.state}, capacity={online_store.capacity})")

# COMMAND ----------

# Enable CDF on the source table (required for TRIGGERED / CONTINUOUS publish).
spark.sql(
    f"ALTER TABLE {source_table_name} "
    "SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)

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

# fe.publish_table() does not configure Postgres-level grants for the model serving
# endpoint's role. Without this, serving requests fail with
# `psycopg.errors.InsufficientPrivilege: permission denied for table <name>`.
# Grant SELECT on the published table to PUBLIC so any role the serving endpoint
# uses can read it. In production, tighten to the specific service principal role.

import psycopg
from databricks.sdk import WorkspaceClient

pg_database, pg_schema, pg_table = online_table_name.split(".")

w = WorkspaceClient()
me = w.current_user.me().user_name

# Lakebase project endpoint path. Online stores created by FeatureEngineeringClient
# are Lakebase projects (newer API), accessed via /api/2.0/postgres/projects/*.
endpoint_path = f"projects/{online_store_name}/branches/production/endpoints/primary"

endpoint_info = w.api_client.do("GET", f"/api/2.0/postgres/{endpoint_path}")
pg_host = endpoint_info["status"]["hosts"]["host"]

cred = w.api_client.do(
    "POST", "/api/2.0/postgres/credentials",
    body={"endpoint": endpoint_path},
)
pg_token = cred["token"]

with psycopg.connect(
    host=pg_host, port=5432, dbname=pg_database,
    user=me, password=pg_token, sslmode="require",
) as conn:
    with conn.cursor() as cur:
        cur.execute(f'GRANT USAGE ON SCHEMA "{pg_schema}" TO PUBLIC')
        cur.execute(f'GRANT SELECT ON "{pg_schema}"."{pg_table}" TO PUBLIC')
    conn.commit()

print(f"Granted USAGE on {pg_schema} + SELECT on {pg_schema}.{pg_table} to PUBLIC")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "source_table": source_table_name,
    "online_table": online_table_name,
    "publish_mode": publish_mode,
}))
