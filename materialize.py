#!/usr/bin/env python3
"""
Script to run the materialize table job on Databricks.

This script reads the job ID from db_config.yaml and runs it with specified parameters.
"""

from datetime import datetime
import yaml
import sys
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.jobs as j

def load_config():
    """Load database configuration from db_config.yaml."""
    with open('db_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    return config


def print_status(run: j.Run):
    statuses = [f"{t.task_key}: {t.state.life_cycle_state}" for t in run.tasks]
    print(f"workflow intermediate status: {', '.join(statuses)}")



def run_materialize_job():
    """Run the materialize table job with specified parameters."""
    # Load configuration
    config = load_config()
    
    # Get job ID and app catalog/schema from config
    job_id = config['database']['materialize_table_job']
    app_catalog_name = config['database']['catalog']
    app_schema_name = config['database']['schema']
    
    # Set other parameters
    project_catalog_name = "mlops_demo"
    project_schema_name = "credit_card_fraud_demo"
    feature_lookup_id = "16"
    eol_view = "mlops_demo.credit_card_fraud_demo.card_present"
    
    # Initialize Databricks client
    client = WorkspaceClient()
    
    # Define job parameters as a dictionary
    parameters = {
        "app_catalog_name": app_catalog_name,
        "app_schema_name": app_schema_name,
        "project_catalog_name": project_catalog_name,
        "project_schema_name": project_schema_name,
        "feature_lookup_id": feature_lookup_id,
        "eol_view": eol_view
    }
    
    print(f"Running materialize job {job_id} with parameters:")
    print(f"  app_catalog_name: {app_catalog_name}")
    print(f"  app_schema_name: {app_schema_name}")
    print(f"  project_catalog_name: {project_catalog_name}")
    print(f"  project_schema_name: {project_schema_name}")
    print(f"  feature_lookup_id: {feature_lookup_id}")
    print(f"  eol_view: {eol_view}")
    print()
    
    try:
        # Run the job with notebook_params
        run = client.jobs.run_now(
            job_id=job_id,
            notebook_params=parameters
        )
        
        print(f"Job run initiated successfully!")
        print(f"Run ID: {run.run_id}")
        
        # Wait for run completion using the waiter utility:
        result = client.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=run.run_id,    # Optional: custom timeout (default 20m)
        callback=print_status                     # Optional: print status after each poll
)  # returns when run is in TERMINATED or SKIPPED state, or timeout/error

        print(f"job finished: {result.run_page_url}")

        print(result)
        
        
            
    except Exception as e:
        print(f"\nError running job: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_materialize_job()