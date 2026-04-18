# Databricks notebook source
# MAGIC %pip install mlflow catboost
# MAGIC dbutils.library.restartPython()
# MAGIC

# COMMAND ----------

dbutils.widgets.text("target", "", "Target")
dbutils.widgets.text("training_table_name", "", "Training Data Table")
dbutils.widgets.text("eval_table_name", "", "Eval Data Table")
dbutils.widgets.text("experiment_name", "", "Experiment Name")
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "", "Schema")
dbutils.widgets.text("feature_lookups_json", "[]", "Feature Lookups JSON")

# COMMAND ----------

import json
import catboost
from catboost import *
from mlflow.models import infer_signature
import mlflow

# COMMAND ----------

training_table_name = dbutils.widgets.get("training_table_name")
eval_table_name = dbutils.widgets.get("eval_table_name")
target = dbutils.widgets.get("target")
experiment_name = dbutils.widgets.get("experiment_name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
feature_lookups_raw = json.loads(dbutils.widgets.get("feature_lookups_json"))

# COMMAND ----------

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

EXP_NAME = f"/Users/{user}/{experiment_name}"

if mlflow.get_experiment_by_name(EXP_NAME) is None:
    mlflow.create_experiment(name=EXP_NAME)
mlflow.set_experiment(EXP_NAME)

# COMMAND ----------

import os
import tempfile

tmpdir = tempfile.mkdtemp()
os.environ["TMPDIR"] = tmpdir

# COMMAND ----------

spark_df = spark.table(training_table_name)
df = spark_df.toPandas().dropna()
y = df.pop(target)
X = df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find Categorical columns

# COMMAND ----------

cat_features_by_type = [col for col in df.columns if df.dtypes[col] == 'object']

N = 10
cat_features_by_distribution = []
for col in df.columns:
    if col in cat_features_by_type:
        continue
    if spark_df.select(col).distinct().count() <= N:
        cat_features_by_distribution.append(col)

cat_features = list(set(cat_features_by_type + cat_features_by_distribution))

# COMMAND ----------

params = {
    'loss_function': 'Logloss',
    'eval_metric': 'AUC',
    'cat_features': cat_features,
    'early_stopping_rounds': 10,
    'random_seed': 42,
    'verbose': False,
    'train_dir': tmpdir
}

cv_dataset = Pool(X, y, cat_features=cat_features)
df_sample = X.head(2)
signature = infer_signature(X, y)

scores = cv(
    cv_dataset,
    params,
    fold_count=5,
    seed=42,
    plot=False
)
best_iteration = scores['test-AUC-mean'].idxmax()
params["iterations"] = best_iteration + 1
model = CatBoostClassifier(**params)
model.fit(X, y, cat_features=cat_features)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log model with Feature Engineering client for auto feature lookup at serving time

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()

# Rebuild FeatureLookup objects from the JSON passed by the app
feature_lookups = []
for fd in feature_lookups_raw:
    fl_kwargs = {
        "table_name": fd["table_name"],
        "lookup_key": fd["lookup_key"],
    }
    if fd.get("feature_names"):
        fl_kwargs["feature_names"] = fd["feature_names"]
    if fd.get("timestamp_lookup_key"):
        fl_kwargs["timestamp_lookup_key"] = fd["timestamp_lookup_key"]
    feature_lookups.append(FeatureLookup(**fl_kwargs))

print(f"Feature lookups for model logging: {len(feature_lookups)}")

# COMMAND ----------

with mlflow.start_run() as run:
    # Log CV metrics
    metrics = scores.iloc[best_iteration].to_dict()
    mlflow.log_metrics(metrics)
    mlflow.log_params(params)

    # Log model with feature specs embedded — enables auto feature lookup at serving time
    fe.log_model(
        model=model,
        artifact_path="model",
        flavor=mlflow.catboost,
        training_set=fe.create_training_set(
            df=spark.table(training_table_name),
            feature_lookups=feature_lookups,
            label=target,
            exclude_columns=[],
        ),
        input_example=df_sample,
    )
    print(f"Model logged with feature specs. Run ID: {run.info.run_id}")
