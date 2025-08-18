#!/usr/bin/env python3
"""
Script to run the materialize table job on Databricks.

This script reads the job ID from db_config.yaml and runs it with specified parameters.
"""

import yaml
import sys
from jobs.materialize import run_materialize_job

def load_config():
    """Load database configuration from db_config.yaml."""
    with open('db_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    return config


def main():
    """Main function to run the materialize table job."""
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
    
    # Define job parameters as a dictionary
    parameters = {
        "app_catalog_name": app_catalog_name,
        "app_schema_name": app_schema_name,
        "project_catalog_name": project_catalog_name,
        "project_schema_name": project_schema_name,
        "feature_lookup_id": feature_lookup_id,
        "eol_view": eol_view
    }
    
    try:
        # Run the job using the function from jobs module
        result = run_materialize_job(job_id, parameters)
        
    except Exception as e:
        print(f"\nError running job: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()