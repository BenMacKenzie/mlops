import json
import os
from datetime import datetime
from dash import Input, Output, State, no_update, ALL
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from dash import html, ctx
import pandas as pd

from utils.db import get_datasets, get_project_by_id
from jobs.training import create_training_job, run_training_job
from databricks.sdk import WorkspaceClient


def register_training_callbacks(app):
    
    @app.callback(
        Output('train-dataset-dropdown', 'options'),
        Output('train-dataset-dropdown', 'value'),
        Input("tabs", "active_tab"),
        Input("list-store", "data"),
        prevent_initial_call=True
    )
    def populate_train_dataset_dropdown(active_tab, list_store):
        if active_tab != 'tab-train' or not list_store:
            return no_update, no_update

        project_id = list_store.get('active_project_id')
        if not project_id:
            return [], None 

        print(f"Populating dataset dropdown for project ID: {project_id}")
        df_datasets = get_datasets(project_id)
        options = []
        value = None
        if not df_datasets.empty:
            # Only include materialized datasets that have training tables
            df_materialized = df_datasets[
                (df_datasets['materialized'] == True) & 
                (df_datasets['training_table_name'].notna())
            ]
            options = [
                {'label': row['name'], 'value': row['id']}
                for index, row in df_materialized.iterrows()
            ]
            if options:
                value = options[0]['value']
        
        return options, value
    
    @app.callback(
        Output("train-status-output", "children"),
        Input("train-run-button", "n_clicks"),
        State("list-store", "data"),
        State("train-dataset-dropdown", "value"),
        State("train-parameters-input", "value"),
        prevent_initial_call=True
    )
    def handle_train_button_click(n_clicks, proj_store, selected_ds_id, params_json_str):
        if n_clicks == 0:
            raise PreventUpdate

        project_id = proj_store.get("active_project_id")
        
        if not project_id:
            return dbc.Alert("Error: Please select a project first.", color="danger")
            
        if not selected_ds_id:
            return dbc.Alert("Error: Please select a dataset from the dropdown.", color="danger")

        # Parse parameters
        try:
            parameters = json.loads(params_json_str or '{}')
        except json.JSONDecodeError as e:
            return dbc.Alert(f"Error parsing parameters JSON: {e}", color="danger")

        # Get dataset details
        df_datasets = get_datasets(project_id)
        dataset_row = df_datasets[df_datasets['id'] == selected_ds_id]
        
        if dataset_row.empty:
            return dbc.Alert("Error: Could not find selected dataset.", color="danger")
            
        dataset = dataset_row.iloc[0]
        dataset_name = dataset['name']
        training_table = dataset['training_table_name']
        eval_table = dataset['eval_table_name']
        feature_lookup_id = dataset['feature_lookup_id']
        
        if not training_table:
            return dbc.Alert(f"Error: Dataset '{dataset_name}' has not been materialized yet.", color="danger")

        # Get project details
        project_details = get_project_by_id(project_id)
        if project_details is None:
            return dbc.Alert("Error: Could not retrieve project details.", color="danger")
            
        project_name = project_details.get('name')
        project_catalog = project_details.get('catalog')
        project_schema = project_details.get('schema')
        git_url = project_details.get('git_url')
        training_notebook = project_details.get('training_notebook')
        
        # Get target variable from EOL definition associated with feature lookup
        from utils.db import sqlQuery, CATALOG_NAME, SCHEMA_NAME
        feature_lookup_query = f"""
            SELECT fl.name as feature_lookup_name, fl.eol_id, eol.label 
            FROM {CATALOG_NAME}.{SCHEMA_NAME}.feature_lookups fl
            LEFT JOIN {CATALOG_NAME}.{SCHEMA_NAME}.eol_definition eol ON fl.eol_id = eol.id
            WHERE fl.id = {feature_lookup_id}
        """
        feature_lookup_df = sqlQuery(feature_lookup_query)
        
        if feature_lookup_df.empty:
            return dbc.Alert("Error: Could not find feature lookup configuration.", color="danger")
            
        # Use label from EOL definition if available, otherwise fall back to feature lookup name
        target_variable = feature_lookup_df.iloc[0]['label']
        if not target_variable or pd.isna(target_variable):
            target_variable = feature_lookup_df.iloc[0]['feature_lookup_name']
        
        # Create job name and experiment name
        job_name = f"{project_name}_{dataset_name}_training"
        experiment_name = f"{project_name}"
        
        # Construct full table paths with catalog.schema.table format
        full_training_table = f"{project_catalog}.{project_schema}.{training_table}"
        full_eval_table = f"{project_catalog}.{project_schema}.{eval_table}" if eval_table else ""
        
        try:
            # Default git settings if not specified
            git_provider = os.getenv("GIT_PROVIDER", "gitHub")
            git_branch = os.getenv("GIT_BRANCH", "main")
            
            print(f"Creating training job: {job_name}")
            print(f"Experiment: {experiment_name}")
            print(f"Target: {target_variable}")
            print(f"Training table: {full_training_table}")
            print(f"Eval table: {full_eval_table}")
            print(f"Git URL: {git_url}")
            print(f"Notebook: {training_notebook}")
            
            # Create and run the training job
            new_job = create_training_job(
                job_name=job_name,
                experiment_name=experiment_name,
                target=target_variable,
                training_table_name=full_training_table,
                eval_table_name=full_eval_table,
                git_url=git_url,
                git_provider=git_provider,
                git_branch=git_branch,
                notebook_path=training_notebook
            )
            
            job_id = new_job.job_id
            print(f"Created Databricks Job ID: {job_id}")
            
            # Run the job
            run_info = run_training_job(job_id)
            
            return dbc.Alert(
                f"Successfully started training job {job_id}. Run ID: {run_info.run_id}",
                color="success"
            )
            
        except Exception as e:
            import traceback
            print(f"Error during training job creation/run: {traceback.format_exc()}")
            return dbc.Alert(f"An error occurred: {str(e)}", color="danger")
    
    @app.callback(
        Output("train-runs-list", "children"),
        Input("tabs", "active_tab"),
        Input("list-store", "data"),
        Input("train-status-output", "children"),  # Trigger on new training runs
        prevent_initial_call=True
    )
    def update_training_runs_list(active_tab, proj_store, status_output):
        if active_tab != 'tab-train' or not proj_store:
            raise PreventUpdate

        project_id = proj_store.get('active_project_id')
        if not project_id:
            return [html.Tr(html.Td("Select a project to view training runs.", colSpan=6))]

        try:
            # Get all datasets for this project
            df_datasets = get_datasets(project_id)
            
            if df_datasets.empty:
                return [html.Tr(html.Td("No datasets found for this project.", colSpan=6))]
            
            # Get project details
            project_details = get_project_by_id(project_id)
            if project_details is None:
                return [html.Tr(html.Td("Error retrieving project details.", colSpan=6))]
            
            # Get recent job runs from Databricks
            workspace_client = WorkspaceClient()
            
            table_rows = []
            
            # Check for jobs related to this project
            project_name = project_details.get('name')
            jobs = workspace_client.jobs.list()
            
            for job in jobs:
                if job.settings and job.settings.name and project_name in job.settings.name:
                    # Get recent runs for this job
                    runs = workspace_client.jobs.list_runs(job_id=job.job_id, limit=5)
                    
                    for run in runs:
                        # Extract dataset name from job name
                        job_name_parts = job.settings.name.split('_')
                        dataset_name = '_'.join(job_name_parts[1:-1]) if len(job_name_parts) > 2 else "Unknown"
                        
                        # Format start time
                        start_time = "N/A"
                        if run.start_time:
                            start_time = datetime.fromtimestamp(run.start_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
                        
                        # Create status badge
                        status = str(run.state.life_cycle_state) if run.state else "UNKNOWN"
                        status_color = {
                            "RUNNING": "primary",
                            "TERMINATED": "success",
                            "FAILED": "danger",
                            "PENDING": "warning",
                            "SKIPPED": "secondary"
                        }.get(status, "secondary")
                        
                        status_badge = dbc.Badge(status, color=status_color)
                        
                        # Create link to Databricks job run
                        host = os.getenv("DATABRICKS_HOST")
                        if host and not host.startswith('https://'):
                            host = 'https://' + host
                        # Remove trailing slash if present
                        if host and host.endswith('/'):
                            host = host.rstrip('/')
                        
                        run_link = "#"
                        if host and run.run_id:
                            run_link = f"{host}/jobs/{job.job_id}/runs/{run.run_id}"
                        
                        view_button = html.A(
                            "View Run",
                            href=run_link,
                            target="_blank",
                            className="btn btn-sm btn-outline-primary"
                        )
                        
                        table_rows.append(html.Tr([
                            html.Td(dataset_name),
                            html.Td(str(job.job_id)),
                            html.Td(str(run.run_id)),
                            html.Td(status_badge),
                            html.Td(start_time),
                            html.Td(view_button)
                        ]))
            
            if not table_rows:
                return [html.Tr(html.Td("No training runs found for this project.", colSpan=6))]
            
            return table_rows
            
        except Exception as e:
            print(f"Error fetching training runs: {e}")
            return [html.Tr(html.Td(f"Error fetching training runs: {str(e)}", colSpan=6))]