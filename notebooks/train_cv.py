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
dbutils.widgets.text("feature_entries_json", "[]")
dbutils.widgets.text("excluded_features_json", "[]")
dbutils.widgets.text("task_type", "classification")
dbutils.widgets.text("parameters_json", "{}")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("experiment_name", "")
# Shared point-in-time column (the EOL/spine observation timestamp). Every declarative
# feature uses this as its timeseries_column, and it must be a column in the spine.
dbutils.widgets.text("timeseries_column", "")

# COMMAND ----------

import json
import os
import tempfile

eol_sql = dbutils.widgets.get("eol_sql")
label_column = dbutils.widgets.get("label_column")
entity_columns = json.loads(dbutils.widgets.get("entity_columns_json"))
feature_entries_raw = json.loads(dbutils.widgets.get("feature_entries_json"))
excluded_features = json.loads(dbutils.widgets.get("excluded_features_json"))
task_type = dbutils.widgets.get("task_type")
user_params = json.loads(dbutils.widgets.get("parameters_json"))
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
experiment_name = dbutils.widgets.get("experiment_name")
timeseries_column = dbutils.widgets.get("timeseries_column")

print(f"Task type: {task_type}")
print(f"EOL SQL: {eol_sql[:200]}...")
print(f"Label: {label_column}, Entity columns: {entity_columns}")
print(f"Feature entries: {len(feature_entries_raw)}")
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
# MAGIC #### Step 2: Build declarative Features (Feature View API) → create_training_set
# MAGIC Each entry is one declarative feature:
# MAGIC - `column_selection`: point-in-time attribute from a (Type-2) table — {source_table, column, entity, name, include}
# MAGIC - `aggregation`: windowed agg — {source_table, input, function, window{type,window_duration,slide_duration?}, filter_condition?, entity, name, include}
# MAGIC - `custom_udf`: UC function over upstream features — {function_name, bindings{param: sibling-feature name}, name, include}
# MAGIC All features share `timeseries_column` (the spine's observation timestamp). `include=false`
# MAGIC marks a feature that only feeds a custom_udf (not a model column).

# COMMAND ----------

from datetime import timedelta
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities import (
    DeltaTableSource, ColumnSelection, AggregationFunction,
    TumblingWindow, SlidingWindow, RollingWindow,
)
from databricks.ml_features.entities.data_source import FeatureViewSource
from databricks.ml_features.entities.transformation import CustomUDF
from databricks.ml_features.entities.function import AGGREGATION_FUNCTION_BY_SHORTHAND

fe = FeatureEngineeringClient()

if not timeseries_column:
    raise ValueError("timeseries_column (the EOL observation timestamp) is required for the Feature View API")

def _parse_duration(s):
    s = (s or "").strip()
    if not s:
        raise ValueError("window_duration is empty")
    if s[-1] == "d":
        return timedelta(days=int(s[:-1]))
    if s[-1] == "h":
        return timedelta(hours=int(s[:-1]))
    if s[-1] == "m":
        return timedelta(minutes=int(s[:-1]))
    return timedelta(days=int(s))

def _window(w):
    dur = _parse_duration(w["window_duration"])
    kind = w["type"]
    if kind == "tumbling":
        return TumblingWindow(window_duration=dur)
    if kind == "sliding":
        return SlidingWindow(window_duration=dur, slide_duration=_parse_duration(w["slide_duration"]))
    if kind == "continuous":
        return RollingWindow(window_duration=dur)
    raise ValueError(f"Unknown window type: {kind}")

def _full(name):
    return f"{catalog}.{schema}.{name}"

def _create(name, **kwargs):
    """create_feature, idempotent on re-run."""
    try:
        return fe.create_feature(catalog_name=catalog, schema_name=schema, name=name, **kwargs)
    except Exception as e:  # noqa: BLE001
        if "ALREADY" in str(e).upper() or "EXIST" in str(e).upper():
            return fe.get_feature(full_name=_full(name))
        raise

by_name = {}  # entry name -> Feature

# Pass 1: column_selection + aggregation (independent of each other)
for e in feature_entries_raw:
    t = e["type"]
    if t == "column_selection":
        src_parts = e["source_table"].split(".")
        source = DeltaTableSource(
            catalog_name=src_parts[0] if len(src_parts) == 3 else catalog,
            schema_name=src_parts[1] if len(src_parts) == 3 else schema,
            table_name=src_parts[-1],
        )
        by_name[e["name"]] = _create(
            e["name"], source=source, function=ColumnSelection(column=e["column"]),
            entity=e["entity"], timeseries_column=timeseries_column)
    elif t == "aggregation":
        src_parts = e["source_table"].split(".")
        src_kwargs = dict(catalog_name=src_parts[0] if len(src_parts) == 3 else catalog,
                          schema_name=src_parts[1] if len(src_parts) == 3 else schema,
                          table_name=src_parts[-1])
        if e.get("filter_condition"):
            src_kwargs["filter_condition"] = e["filter_condition"]
        op_cls = AGGREGATION_FUNCTION_BY_SHORTHAND.get(e["function"])
        if op_cls is None:
            raise ValueError(f"Unsupported aggregation function '{e['function']}'")
        by_name[e["name"]] = _create(
            e["name"], source=DeltaTableSource(**src_kwargs),
            function=AggregationFunction(operator=op_cls(input=e["input"]), time_window=_window(e["window"])),
            entity=e["entity"], timeseries_column=timeseries_column)

# Pass 2: custom_udf (binds to upstream features created in pass 1)
for e in feature_entries_raw:
    if e["type"] != "custom_udf":
        continue
    upstreams = [by_name[v] for v in e["bindings"].values()]
    full_bindings = {param: _full(v) for param, v in e["bindings"].items()}
    by_name[e["name"]] = _create(
        e["name"], source=FeatureViewSource(features=upstreams),
        function=CustomUDF(e["function_name"], input_bindings=full_bindings))

# Model features = those flagged include (default true); input-only upstreams are excluded.
features = [by_name[e["name"]] for e in feature_entries_raw if e.get("include", True)]
print(f"Built {len(by_name)} declarative features ({len(features)} included in the model)")

# Exclude entity + the shared timeseries column + user-excluded features from the training frame.
exclude = list(entity_columns) if entity_columns else []
if timeseries_column and timeseries_column not in exclude:
    exclude.append(timeseries_column)
for col in excluded_features:
    if col not in exclude:
        exclude.append(col)

training_set = fe.create_training_set(
    df=eol_df,
    features=features,
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

# Categorical = string/object columns only. (We intentionally do NOT flag low-cardinality
# NUMERIC columns as categorical: the feature store returns a feature's stored type at serving,
# so a numeric column trained as a CatBoost cat_feature fails inference with
# "cat_features must be integer or string" when the online value arrives as a float.)
cat_features = [col for col in X.columns if X.dtypes[col] == 'object']
# Cast categoricals to string so training matches the serving wrapper (which also stringifies).
for c in cat_features:
    X[c] = X[c].astype(str)
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
signature = infer_signature(X, y)

# Serving input example: the spine columns a caller actually sends at inference
# (entity/lookup keys + any direct features), minus the label. Feature lookups and
# on-demand functions are resolved by the endpoint, so they must NOT be in the example.
# This is what pre-fills the "Query endpoint" dialog and the app's test UI.
serving_example = eol_df.drop(label_column).limit(5).toPandas()

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
# MAGIC Wrap the CatBoost model so it reindexes the incoming frame to the training column order
# MAGIC before predict. The feature store reconstructs the serving frame in a different column
# MAGIC order than training, and CatBoost resolves cat_features positionally — without this a
# MAGIC numeric feature lands at a categorical index ("cat_features must be integer or string").

# COMMAND ----------

import mlflow.pyfunc

class ReorderModel(mlflow.pyfunc.PythonModel):
    def __init__(self, model, feature_order, cat_cols):
        self.model = model
        self.feature_order = list(feature_order)
        self.cat_cols = list(cat_cols)
    def predict(self, context, model_input, params=None):
        pdf = model_input.copy()
        for c in self.feature_order:
            if c not in pdf.columns:
                pdf[c] = None
        pdf = pdf[self.feature_order]
        for c in self.cat_cols:
            pdf[c] = pdf[c].astype("string").fillna("")
        return self.model.predict(pdf)

with mlflow.start_run() as run:
    metrics = scores.iloc[int(best_iteration)].to_dict()
    mlflow.log_metrics(metrics)
    mlflow.log_params({k: str(v) for k, v in params.items()})

    fe.log_model(
        model=ReorderModel(model, list(X.columns), cat_features),
        artifact_path="model",
        flavor=mlflow.pyfunc,
        training_set=training_set,
        input_example=serving_example,
    )
    print(f"Model logged with feature specs. Run ID: {run.info.run_id}")

# COMMAND ----------

# Return results
dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "mlflow_run_id": run.info.run_id,
    "metrics": metrics,
}))
