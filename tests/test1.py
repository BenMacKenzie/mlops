from databricks.sdk import WorkspaceClient
from databricks.sdk.service import ml

w = WorkspaceClient()

all_experiments = w.experiments.list_experiments()

for e in all_experiments:
    print(e.name)

e = w.experiments.get_by_name('/ML/mlflow_workshop/mlflow3-ml-example')
print(e.as_dict())

search_runs = w.experiments.search_runs(experiment_ids=[e.experiment.experiment_id], max_results=1000)

for r in search_runs:
    print(r.info.run_name)





