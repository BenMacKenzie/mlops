# Databricks notebook source
# MAGIC %pip install mlflow catboost
# MAGIC dbutils.library.restartPython()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Training Notebook — Standard Train/Eval
# MAGIC App-managed notebook: EOL → features → create_training_set → split → train → eval → fe.log_model()
# MAGIC Split strategy: train_eval (train on one split, evaluate on the other)

# COMMAND ----------

dbutils.widgets.text("eol_sql", "")
dbutils.widgets.text("label_column", "")
dbutils.widgets.text("entity_columns_json", "[]")
dbutils.widgets.text("feature_lookups_json", "[]")
dbutils.widgets.text("task_type", "classification")
dbutils.widgets.text("split_method", "random")
dbutils.widgets.text("split_config_json", '{"eval_pct": 20, "seed": 42}')
dbutils.widgets.text("parameters_json", "{}")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("experiment_name", "")
dbutils.widgets.text("training_table_name", "")
dbutils.widgets.text("eval_table_name", "")

# COMMAND ----------

import json
import os
import tempfile

eol_sql = dbutils.widgets.get("eol_sql")
label_column = dbutils.widgets.get("label_column")
entity_columns = json.loads(dbutils.widgets.get("entity_columns_json"))
feature_lookups_raw = json.loads(dbutils.widgets.get("feature_lookups_json"))
task_type = dbutils.widgets.get("task_type")
split_method = dbutils.widgets.get("split_method")
split_config = json.loads(dbutils.widgets.get("split_config_json"))
user_params = json.loads(dbutils.widgets.get("parameters_json"))
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
experiment_name = dbutils.widgets.get("experiment_name")
training_table_name = dbutils.widgets.get("training_table_name")
eval_table_name = dbutils.widgets.get("eval_table_name")

eval_pct = split_config.get("eval_pct", 20) / 100.0
seed = split_config.get("seed", 42)

print(f"Task type: {task_type}, Split: {split_method} (eval {eval_pct*100}%)")

# COMMAND ----------

import mlflow
from mlflow.models import infer_signature

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
EXP_NAME = f"/Users/{user}/{experiment_name}"

if mlflow.get_experiment_by_name(EXP_NAME) is None:
    mlflow.create_experiment(name=EXP_NAME)
mlflow.set_experiment(EXP_NAME)

tmpdir = tempfile.mkdtemp()
os.environ["TMPDIR"] = tmpdir

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Execute EOL SQL → create_training_set → load

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()

feature_lookups = []
for fd in feature_lookups_raw:
    fl_kwargs = {"table_name": fd["table_name"], "lookup_key": fd["lookup_key"]}
    if fd.get("feature_names"):
        fl_kwargs["feature_names"] = fd["feature_names"]
    if fd.get("timestamp_lookup_key"):
        fl_kwargs["timestamp_lookup_key"] = fd["timestamp_lookup_key"]
    feature_lookups.append(FeatureLookup(**fl_kwargs))

exclude = list(entity_columns) if entity_columns else []
for fd in feature_lookups_raw:
    ts_key = fd.get("timestamp_lookup_key")
    if ts_key and ts_key not in exclude:
        exclude.append(ts_key)

eol_df = spark.sql(eol_sql)

training_set = fe.create_training_set(
    df=eol_df,
    feature_lookups=feature_lookups,
    label=label_column,
    exclude_columns=exclude,
)

full_df = training_set.load_df()
print(f"Full dataset: {full_df.count()} rows, {len(full_df.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Split into train/eval

# COMMAND ----------

if split_method == "temporal":
    # Sort by timestamp, latest rows to eval
    from pyspark.sql.window import Window
    from pyspark.sql import functions as F
    timestamp_col = [c for c in full_df.columns if c not in exclude and "date" in c.lower() or "time" in c.lower()]
    # Use the EOL's timestamp column if available, otherwise fall back to row ordering
    ts_col = timestamp_col[0] if timestamp_col else None
    if ts_col:
        ordered = full_df.orderBy(F.col(ts_col))
        total = ordered.count()
        eval_count = int(total * eval_pct)
        train_count = total - eval_count
        train_df = ordered.limit(train_count)
        eval_df = ordered.subtract(train_df)
    else:
        # Fallback to random if no timestamp found
        train_df, eval_df = full_df.randomSplit([1 - eval_pct, eval_pct], seed=seed)
else:
    # Random stratified split on label column
    from pyspark.sql import functions as F
    label_counts = full_df.groupBy(label_column).count().collect()
    fractions = {row[label_column]: 1 - eval_pct for row in label_counts}
    train_df = full_df.stat.sampleBy(label_column, fractions, seed=seed)
    eval_df = full_df.subtract(train_df)

train_count = train_df.count()
eval_count = eval_df.count()
print(f"Train: {train_count}, Eval: {eval_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Write tables to UC

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

train_df.write.mode("overwrite").saveAsTable(training_table_name)
eval_df.write.mode("overwrite").saveAsTable(eval_table_name)
print(f"Written: {training_table_name} ({train_count}), {eval_table_name} ({eval_count})")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Train model

# COMMAND ----------

import catboost
from catboost import Pool, CatBoostClassifier, CatBoostRegressor

pdf_train = train_df.toPandas().dropna()
y_train = pdf_train.pop(label_column)
X_train = pdf_train

pdf_eval = eval_df.toPandas().dropna()
y_eval = pdf_eval.pop(label_column)
X_eval = pdf_eval

cat_features_by_type = [col for col in X_train.columns if X_train.dtypes[col] == 'object']
N = 10
cat_features_by_distribution = []
for col in X_train.columns:
    if col in cat_features_by_type:
        continue
    if train_df.select(col).distinct().count() <= N:
        cat_features_by_distribution.append(col)
cat_features = list(set(cat_features_by_type + cat_features_by_distribution))

if task_type == "regression":
    base_params = {'loss_function': 'RMSE', 'eval_metric': 'RMSE'}
else:
    base_params = {'loss_function': 'Logloss', 'eval_metric': 'AUC'}

params = {
    **base_params,
    'cat_features': cat_features,
    'early_stopping_rounds': 10,
    'random_seed': 42,
    'verbose': False,
    'train_dir': tmpdir,
    **user_params,
}

eval_pool = Pool(X_eval, y_eval, cat_features=cat_features)

if task_type == "regression":
    model = CatBoostRegressor(**params)
else:
    model = CatBoostClassifier(**params)

model.fit(X_train, y_train, cat_features=cat_features, eval_set=eval_pool, use_best_model=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Evaluate + log with feature specs

# COMMAND ----------

eval_metrics = {}
if task_type == "regression":
    from sklearn.metrics import mean_squared_error, r2_score
    preds = model.predict(X_eval)
    eval_metrics["eval_rmse"] = float(mean_squared_error(y_eval, preds) ** 0.5)
    eval_metrics["eval_r2"] = float(r2_score(y_eval, preds))
else:
    from sklearn.metrics import roc_auc_score, accuracy_score, f1_score
    preds_proba = model.predict_proba(X_eval)[:, 1]
    preds = model.predict(X_eval)
    eval_metrics["eval_auc"] = float(roc_auc_score(y_eval, preds_proba))
    eval_metrics["eval_accuracy"] = float(accuracy_score(y_eval, preds))
    eval_metrics["eval_f1"] = float(f1_score(y_eval, preds))

print(f"Eval metrics: {eval_metrics}")

with mlflow.start_run() as run:
    mlflow.log_metrics(eval_metrics)
    mlflow.log_params({k: str(v) for k, v in params.items()})

    fe.log_model(
        model=model,
        artifact_path="model",
        flavor=mlflow.catboost,
        training_set=training_set,
        input_example=X_train.head(2),
    )
    print(f"Model logged. Run ID: {run.info.run_id}")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "mlflow_run_id": run.info.run_id,
    "metrics": eval_metrics,
    "training_table": training_table_name,
    "eval_table": eval_table_name,
    "train_count": train_count,
    "eval_count": eval_count,
}))
