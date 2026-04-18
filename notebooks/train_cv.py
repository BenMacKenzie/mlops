# Databricks notebook source
# MAGIC %pip install mlflow catboost
# MAGIC dbutils.library.restartPython()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Training Notebook — Cross-Validation
# MAGIC App-managed notebook: EOL → features → create_training_set → CV → fe.log_model()
# MAGIC Split strategy: none (CV handles train/val splitting internally)

# COMMAND ----------

dbutils.widgets.text("eol_sql", "")
dbutils.widgets.text("label_column", "")
dbutils.widgets.text("entity_columns_json", "[]")
dbutils.widgets.text("feature_lookups_json", "[]")
dbutils.widgets.text("task_type", "classification")
dbutils.widgets.text("parameters_json", "{}")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("experiment_name", "")

# COMMAND ----------

import json
import os
import tempfile

eol_sql = dbutils.widgets.get("eol_sql")
label_column = dbutils.widgets.get("label_column")
entity_columns = json.loads(dbutils.widgets.get("entity_columns_json"))
feature_lookups_raw = json.loads(dbutils.widgets.get("feature_lookups_json"))
task_type = dbutils.widgets.get("task_type")
user_params = json.loads(dbutils.widgets.get("parameters_json"))
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
experiment_name = dbutils.widgets.get("experiment_name")

print(f"Task type: {task_type}")
print(f"EOL SQL: {eol_sql[:200]}...")
print(f"Label: {label_column}, Entity columns: {entity_columns}")
print(f"Feature lookups: {len(feature_lookups_raw)}")
print(f"User params: {user_params}")

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
# MAGIC #### Step 1: Execute EOL SQL → spine DataFrame

# COMMAND ----------

eol_df = spark.sql(eol_sql)
print(f"EOL rows: {eol_df.count()}, columns: {eol_df.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Build FeatureLookups → create_training_set

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()

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

print(f"Built {len(feature_lookups)} FeatureLookups")

# Exclude entity columns + timestamp lookup keys from the training data
exclude = list(entity_columns) if entity_columns else []
for fd in feature_lookups_raw:
    ts_key = fd.get("timestamp_lookup_key")
    if ts_key and ts_key not in exclude:
        exclude.append(ts_key)

training_set = fe.create_training_set(
    df=eol_df,
    feature_lookups=feature_lookups,
    label=label_column,
    exclude_columns=exclude,
)

full_df = training_set.load_df()
print(f"Training set: {full_df.count()} rows, {len(full_df.columns)} columns: {full_df.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Convert to Pandas, identify categorical features

# COMMAND ----------

df = full_df.toPandas().dropna()
y = df.pop(label_column)
X = df

cat_features_by_type = [col for col in X.columns if X.dtypes[col] == 'object']
N = 10
cat_features_by_distribution = []
spark_df = full_df
for col in X.columns:
    if col in cat_features_by_type:
        continue
    if spark_df.select(col).distinct().count() <= N:
        cat_features_by_distribution.append(col)

cat_features = list(set(cat_features_by_type + cat_features_by_distribution))
print(f"Categorical features: {cat_features}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Cross-validation + train final model

# COMMAND ----------

import catboost
from catboost import Pool, cv, CatBoostClassifier, CatBoostRegressor

# Base params — user can override via parameters_json
if task_type == "regression":
    base_params = {
        'loss_function': 'RMSE',
        'eval_metric': 'RMSE',
    }
    cv_metric = 'test-RMSE-mean'
    best_fn = lambda s: s.idxmin()  # minimize RMSE
else:
    base_params = {
        'loss_function': 'Logloss',
        'eval_metric': 'AUC',
    }
    cv_metric = 'test-AUC-mean'
    best_fn = lambda s: s.idxmax()  # maximize AUC

params = {
    **base_params,
    'cat_features': cat_features,
    'early_stopping_rounds': 10,
    'random_seed': 42,
    'verbose': False,
    'train_dir': tmpdir,
    **user_params,
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

best_iteration = best_fn(scores[cv_metric])
params["iterations"] = int(best_iteration) + 1

if task_type == "regression":
    model = CatBoostRegressor(**params)
else:
    model = CatBoostClassifier(**params)

model.fit(X, y, cat_features=cat_features)
print(f"Best iteration: {best_iteration}, Params: {params}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Log model with feature specs

# COMMAND ----------

with mlflow.start_run() as run:
    metrics = scores.iloc[int(best_iteration)].to_dict()
    mlflow.log_metrics(metrics)
    mlflow.log_params({k: str(v) for k, v in params.items()})

    fe.log_model(
        model=model,
        artifact_path="model",
        flavor=mlflow.catboost,
        training_set=training_set,
        input_example=df_sample,
    )
    print(f"Model logged with feature specs. Run ID: {run.info.run_id}")

# COMMAND ----------

# Return results
dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "mlflow_run_id": run.info.run_id,
    "metrics": metrics,
}))
