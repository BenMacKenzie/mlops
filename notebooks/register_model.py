# Databricks notebook source
# Register a model version in Unity Catalog from an MLflow run.
# Launched by the MLOps app as a Databricks Job.

import mlflow
import json

run_id = dbutils.widgets.get("run_id")
model_name = dbutils.widgets.get("model_name")

model_uri = f"runs:/{run_id}/model"
print(f"Registering model: {model_uri} → {model_name}")

result = mlflow.register_model(model_uri, model_name)

print(f"Registered: {model_name} version {result.version} (status: {result.status})")

dbutils.notebook.exit(json.dumps({
    "name": result.name,
    "version": str(result.version),
    "status": result.status
}))
