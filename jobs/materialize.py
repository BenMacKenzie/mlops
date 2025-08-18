from databricks.sdk import WorkspaceClient
import databricks.sdk.service.jobs as j

def run_materialize_job(job_id, parameters):
    """
    Run the materialize table job with specified parameters.
    
    Args:
        job_id: The Databricks job ID to run
        parameters: Dictionary of parameters to pass to the job
            - app_catalog_name: Application catalog name
            - app_schema_name: Application schema name  
            - project_catalog_name: Project catalog name
            - project_schema_name: Project schema name
            - feature_lookup_id: Feature lookup ID
            - eol_view: End-of-line view name
    
    Returns:
        The run result from Databricks
    """
    # Initialize Databricks client
    client = WorkspaceClient()
    
    print(f"Running materialize job {job_id} with parameters:")
    for key, value in parameters.items():
        print(f"  {key}: {value}")
    print()
    
    # Run the job with notebook_params
    run = client.jobs.run_now(
        job_id=job_id,
        notebook_params=parameters
    )
    
    print(f"Job run initiated successfully!")
    print(f"Run ID: {run.run_id}")
    
    # Wait for run completion using the waiter utility
    result = client.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=run.run_id,
        callback=_print_status
    )
    
    print(f"Job finished: {result.run_page_url}")
    print(result)
    
    return result


def _print_status(run: j.Run):
    """Helper function to print job status during execution."""
    statuses = [f"{t.task_key}: {t.state.life_cycle_state}" for t in run.tasks]
    print(f"Workflow intermediate status: {', '.join(statuses)}")