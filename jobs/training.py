from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, Task, GitSource, GitProvider

def create_training_job(job_name, experiment_name, target, training_table_name, eval_table_name, git_url, git_provider, git_branch, notebook_path):
    
    workspace_client = WorkspaceClient()
    print(eval_table_name)
    # print(notebook_path) # Original print statement

    # Remove .py extension if present for the SDK path
    if notebook_path and notebook_path.endswith(".py"):
        notebook_path_for_sdk = notebook_path[:-3]
    else:
        notebook_path_for_sdk = notebook_path
    
    print(f"[DEBUG] Original notebook_path: '{notebook_path}', Path for Databricks SDK: '{notebook_path_for_sdk}'")

    job =  workspace_client.jobs.create(
        name=job_name,
        git_source=GitSource(
                        git_url=git_url,
                        git_provider=GitProvider(git_provider),
                        git_branch=git_branch
                    ),
        tasks=[
            Task(
                task_key="TrainModel",
                notebook_task=NotebookTask(
                    notebook_path=f"notebooks/{notebook_path_for_sdk}", # Use the modified path
                    
                    base_parameters={
                        "experiment_name": experiment_name,
                        "target": target,
                        "training_table_name": training_table_name,
                        "eval_table_name": eval_table_name
                    }
                ),
                description="train model"
                # No cluster configuration is specified, making it serverless by default
            )
        ]
    )
    return job


def run_training_job(job_id):
    workspace_client = WorkspaceClient()
    run = workspace_client.jobs.run_now(job_id=job_id)
    print(f"Job run started with run ID: {run.run_id}")
    return run