import mlflow
mlflow.set_tracking_uri("databricks")

from mlflow import MlflowClient



client = MlflowClient()
def get_experiment_id(experiment_name):
    return MlflowClient().get_experiment_by_name(experiment_name).experiment_id

experiment_name = '/ML/mlflow_workshop/mlflow3-ml-example_2'
experiment_id = get_experiment_id(experiment_name)


runs = client.search_runs(experiment_id)
for run in runs:
    print(f"Name: {run.info.run_name}, ID: {run.info.run_id}")
    print(run)

