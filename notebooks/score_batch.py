# MAGIC %md
# MAGIC # Batch inference — score a table of requests with a UC model alias
# MAGIC App-managed notebook, run by a **scheduled job that needs nothing from the app**: every input is a job
# MAGIC parameter. Each run:
# MAGIC 1. resolves `models:/<model_name>@<model_alias>` to a concrete version (so moving the alias moves the job)
# MAGIC 2. installs that version's own requirements (e.g. catboost) — minus the feature-store / mlflow / pyspark
# MAGIC    pins, which the job environment already provides
# MAGIC 3. builds the spine = every row of the input (the fields a requesting system supplies) + the model's
# MAGIC    timestamp column set to the RUN TIME (one value for the whole run)
# MAGIC 4. `fe.score_batch` → input fields + resolved features + `prediction`, computed point-in-time as of the run
# MAGIC 5. appends to the output table with `scored_at`, `model_version`, `batch_deployment_id` (history kept)
# MAGIC
# MAGIC Shipped as a plain file (no notebook header) — see the deploy recipe.

# COMMAND ----------

dbutils.widgets.text("model_name", "")
dbutils.widgets.text("model_alias", "")
dbutils.widgets.text("input_table", "")          # either a table …
dbutils.widgets.text("input_sql", "")            # … or a SQL query (wins if set)
dbutils.widgets.text("ts_col", "")               # model's observation-timestamp column (filled with run time)
dbutils.widgets.text("output_table", "")
dbutils.widgets.text("batch_deployment_id", "")

import mlflow
from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")
_name = dbutils.widgets.get("model_name").strip()
_alias = dbutils.widgets.get("model_alias").strip()
if not _name or not _alias:
    raise ValueError("model_name and model_alias are required")
_version = MlflowClient().get_model_version_by_alias(_name, _alias).version
_uri = f"models:/{_name}/{_version}"
print(f"{_name}@{_alias} → version {_version}")

# The model's own requirements — from EVERY requirements.txt in its artifacts: an fe.log_model model wraps
# the raw model (e.g. catboost lives in the inner raw_model's requirements, not the outer one) — minus what
# the job env already pins (the serving-side feature-lookup client / an older mlflow would break score_batch).
# Written to a file and installed with %pip in the next cell: %pip (unlike a pip subprocess) also makes the
# packages available on the workers where the model's predict UDF runs.
import os
_SKIP = ("databricks-feature", "mlflow", "pyspark", "databricks-connect")
_dir = mlflow.artifacts.download_artifacts(artifact_uri=_uri)
_reqs = []
for _root, _, _files in os.walk(_dir):
    if "requirements.txt" in _files:
        for _l in open(os.path.join(_root, "requirements.txt")):
            _l = _l.strip()
            if _l and not _l.startswith(("#", "-")) and not _l.lower().startswith(_SKIP) and _l not in _reqs:
                _reqs.append(_l)
print("model requirements to install:", _reqs)
with open("/tmp/mlops_model_requirements.txt", "w") as _f:
    _f.write("# model requirements (filtered)\n" + "\n".join(_reqs) + "\n")

# COMMAND ----------

# MAGIC %pip install -q -r /tmp/mlops_model_requirements.txt

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import json
from datetime import datetime, timezone
import mlflow
from mlflow import MlflowClient
from pyspark.sql import functions as F
from databricks.feature_engineering import FeatureEngineeringClient

mlflow.set_registry_uri("databricks-uc")
model_name = dbutils.widgets.get("model_name").strip()
model_alias = dbutils.widgets.get("model_alias").strip()
input_table = dbutils.widgets.get("input_table").strip()
input_sql = dbutils.widgets.get("input_sql").strip().rstrip(";")
ts_col = dbutils.widgets.get("ts_col").strip()
output_table = dbutils.widgets.get("output_table").strip()
batch_deployment_id = dbutils.widgets.get("batch_deployment_id").strip()
if not ts_col or not output_table or not (input_table or input_sql):
    raise ValueError("ts_col, output_table and input_table/input_sql are required")

version = MlflowClient().get_model_version_by_alias(model_name, model_alias).version
model_uri = f"models:/{model_name}/{version}"

# One as-of time for the whole run.
T = datetime.now(timezone.utc).replace(microsecond=0)
src = spark.sql(input_sql) if input_sql else spark.table(input_table)
if ts_col in src.columns:
    raise ValueError(f"Input must not contain {ts_col} — the job sets it to the run time")
spine = src.withColumn(ts_col, F.lit(T).cast("timestamp"))

n = spine.count()
if n == 0:
    dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "rows": 0, "model_version": version, "scored_at": T.isoformat()}))

preds = FeatureEngineeringClient().score_batch(model_uri=model_uri, df=spine)

# Observability: which feature sources the plan read (offline-materialized tables vs recompute from source).
import contextlib, io, re
_buf = io.StringIO()
with contextlib.redirect_stdout(_buf):
    preds.explain(mode="formatted")
scanned = sorted(set(re.findall(r"Scan \w+ ([\w.]+)", _buf.getvalue())))

out = (preds
       .withColumn("scored_at", F.lit(T).cast("timestamp"))
       .withColumn("model_name", F.lit(model_name))
       .withColumn("model_version", F.lit(int(version)))
       .withColumn("batch_deployment_id", F.lit(int(batch_deployment_id) if batch_deployment_id else None).cast("bigint")))
out.write.mode("append").option("mergeSchema", "true").saveAsTable(output_table)

summary = {"status": "SUCCESS", "rows": n, "model_version": version, "scored_at": T.isoformat(),
           "output_table": output_table, "tables_scanned": scanned}
print(summary)
dbutils.notebook.exit(json.dumps(summary))
