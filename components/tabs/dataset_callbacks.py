import dash
from dash import Input, Output, State, callback_context, ALL
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from utils.db_universal import (
    get_datasets,
    get_dataset_by_id,
    create_dataset,
    update_dataset,
    delete_dataset,
    get_feature_lookups,
    get_feature_lookup_by_id,
    get_eol_definition_by_id,
    get_project_by_id,
    update_dataset_run_info,
    update_dataset_materialized_status
)
import yaml
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.jobs as j

def register_dataset_callbacks(app):
    """Register callbacks for the Datasets tab."""
    
    # Refresh dataset list when project changes OR on initial load
    @app.callback(
        Output('dataset-store', 'data', allow_duplicate=True),
        Input('list-store', 'data'),
        State('dataset-store', 'data'),
        prevent_initial_call='initial_duplicate'  # Allow initial call with duplicate output
    )
    def refresh_dataset_store_on_project_change(project_store, current_dataset_store):
        """Refresh dataset store when project selection changes or on initial load."""
        project_id = project_store.get('active_project_id') if isinstance(project_store, dict) else None
        
        # Fetch datasets for the current project
        df = get_datasets(project_id)
        items = []
        if not df.empty:
            records = df.to_dict('records')
            items = [
                {
                    'id': int(rec['id']), 
                    'name': rec.get('name'),
                    'feature_lookup_id': rec.get('feature_lookup_id'),
                    'evaluation_type': rec.get('evaluation_type'),
                    'percentage': rec.get('percentage'),
                    'status': rec.get('status', 'NOT_STARTED'),
                    'materialized': rec.get('materialized'),
                    'training_table_name': rec.get('training_table_name'),
                    'eval_table_name': rec.get('eval_table_name'),
                    'run_id': rec.get('run_id'),
                    'run_url': rec.get('run_url')
                }
                for rec in records
            ]
        
        # Preserve current selection if it exists and is still valid, otherwise auto-select first item
        # But don't auto-select if we're in "create mode"
        current_active_id = current_dataset_store.get('active_id') if current_dataset_store else None
        in_create_mode = current_dataset_store.get('create_mode', False) if current_dataset_store else False
        
        # Check if current selection is still valid in the new items list
        if current_active_id and any(item['id'] == current_active_id for item in items):
            active_id = current_active_id  # Preserve existing selection
            create_mode = False  # Clear create mode when we have a valid selection
        elif in_create_mode:
            active_id = None  # Preserve None selection in create mode
            create_mode = True  # Keep create mode flag
        else:
            active_id = None  # Don't auto-select - let user or other callbacks handle selection
            create_mode = False
        
        print(f"DEBUG: refresh_dataset_store_on_project_change - project_id: {project_id}, found {len(items)} datasets, preserving active_id: {active_id}, create_mode: {create_mode}")
        print(f"DEBUG: refresh_dataset_store_on_project_change - incoming current_active_id: {current_active_id}")
        
        return {'items': items, 'active_id': active_id, 'create_mode': create_mode}
    
    # Populate feature lookup dropdown based on selected project
    @app.callback(
        Output('dataset-feature-lookup-dropdown', 'options'),
        Input('list-store', 'data')
    )
    def update_feature_lookup_dropdown(store_data):
        print(f"DEBUG: dataset update_feature_lookup_dropdown called with store_data: {store_data}")
        project_id = None
        if isinstance(store_data, dict):
            project_id = store_data.get('active_project_id')
        elif isinstance(store_data, list) and len(store_data) > 0:
            # Handle list format (happens on initial load sometimes)
            project_id = store_data[0].get('id') if store_data[0] else None
        
        print(f"DEBUG: dataset update_feature_lookup_dropdown project_id: {project_id}")
        
        # Fetch feature lookups for project
        if not project_id:
            print("DEBUG: dataset - No project_id found, returning empty options")
            return []
        
        try:
            df = get_feature_lookups(project_id)
            print(f"DEBUG: dataset - get_feature_lookups returned {len(df)} rows")
            
            if df.empty:
                print("DEBUG: dataset - No feature lookups found for project")
                return []
            
            # Build dropdown options: label=name, value=id
            opts = []
            for _, row in df.iterrows():
                try:
                    val = int(row['id'])
                    name = row.get('name')
                    print(f"DEBUG: dataset - Adding feature lookup option: {name} (id={val})")
                    opts.append({'label': name, 'value': val})
                except Exception as e:
                    print(f"DEBUG: dataset - Error processing row {row}: {e}")
                    continue
            
            print(f"DEBUG: dataset - Returning {len(opts)} feature lookup dropdown options: {opts}")
            return opts
        except Exception as e:
            print(f"ERROR: dataset - Exception in update_feature_lookup_dropdown: {e}")
            import traceback
            traceback.print_exc()
            return []

    @app.callback(
        Output('dataset-list', 'children'),
        Input('dataset-store', 'data'),
        prevent_initial_call=False  # Allow initial call to show datasets on first load
    )
    def refresh_dataset_list(store_data):
        items = store_data.get('items', []) if isinstance(store_data, dict) else []
        active_id = store_data.get('active_id') if isinstance(store_data, dict) else None
        print(f"DEBUG: refresh_dataset_list - active_id={active_id}, items count={len(items)}")
        list_items = []
        for itm in items:
            list_items.append(
                dbc.ListGroupItem(
                    itm.get('name'),
                    id={'type': 'dataset-item', 'index': itm.get('id')},
                    action=True,
                    active=(itm.get('id') == active_id)
                )
            )
        if not list_items:
            list_items = [
                dbc.ListGroupItem(
                    "No datasets found.",
                    id={'type': 'dataset-item', 'index': -1},
                    disabled=True
                )
            ]
        return list_items

    @app.callback(
        Output('dataset-store', 'data', allow_duplicate=True),
        Input({'type': 'dataset-item', 'index': ALL}, 'n_clicks'),
        State('dataset-store', 'data'),
        prevent_initial_call=True
    )
    def select_dataset(n_clicks, store_data):
        ctx = callback_context
        if not ctx.triggered_id or not isinstance(ctx.triggered_id, dict) or ctx.triggered_id.get('type') != 'dataset-item':
            raise PreventUpdate
        dataset_id = ctx.triggered_id['index']
        if dataset_id == -1:
            raise PreventUpdate
        items = store_data.get('items', []) if isinstance(store_data, dict) else []
        
        print(f"select_dataset - Selected dataset id: {dataset_id}")
        
        # Just update the store - form population will be handled by populate_dataset_form
        # Clear create_mode when selecting an item
        return {'items': items, 'active_id': dataset_id, 'create_mode': False}

    @app.callback(
        [Output('dataset-name', 'value', allow_duplicate=True),
         Output('dataset-feature-lookup-dropdown', 'value', allow_duplicate=True),
         Output('dataset-evaluation-type-dropdown', 'value', allow_duplicate=True),
         Output('dataset-percentage', 'value', allow_duplicate=True),
         Output('dataset-status-display', 'children', allow_duplicate=True),
         Output('dataset-status-display', 'color', allow_duplicate=True),
         Output('dataset-run-id', 'value', allow_duplicate=True),
         Output('dataset-run-url', 'value', allow_duplicate=True),
         Output('dataset-run-url-link', 'href', allow_duplicate=True),
         Output('dataset-run-url-link', 'style', allow_duplicate=True),
         Output('dataset-run-url-placeholder', 'style', allow_duplicate=True),
         Output('dataset-training-table', 'value', allow_duplicate=True),
         Output('dataset-eval-table', 'value', allow_duplicate=True),
         Output('materialize-dataset-button', 'disabled', allow_duplicate=True),
         Output('dataset-store', 'data', allow_duplicate=True)],
        Input('create-dataset-button', 'n_clicks'),
        State('dataset-store', 'data'),
        State('list-store', 'data'),
        prevent_initial_call=True
    )
    def create_dataset_callback(n_clicks, dataset_store, list_store):
        """Create a new dataset with default values when Create button is clicked."""
        if not n_clicks:
            raise PreventUpdate
            
        # Get current project ID
        project_id = list_store.get('active_project_id') if isinstance(list_store, dict) else None
        if not project_id:
            print("No active project for new dataset")
            raise PreventUpdate
            
        # Get available feature lookups for this project
        fl_df = get_feature_lookups(project_id)
        if fl_df.empty:
            # Can't create dataset without a feature lookup - show error alert
            print("Cannot create dataset: No feature lookups available for this project")
            raise PreventUpdate
        
        # Use the first available feature lookup as default
        default_fl_id = int(fl_df.iloc[0]['id'])
        
        # Find a unique name for the new dataset
        df = get_datasets(project_id)
        existing_names = df['name'].tolist() if not df.empty else []
        
        counter = 1
        new_name = "New Dataset"
        while new_name in existing_names:
            counter += 1
            new_name = f"New Dataset {counter}"
        
        success = create_dataset(
            project_id=project_id,
            feature_lookup_id=default_fl_id,  # Use first available feature lookup
            name=new_name,
            evaluation_type="random",
            percentage=80.0,
            materialized=False  # New datasets are not materialized by default
        )
        
        if not success:
            print("Failed to create new dataset")
            raise PreventUpdate
        
        # Refresh the dataset list and find the newly created item
        df = get_datasets(project_id)
        items = []
        new_dataset_id = None
        
        if not df.empty:
            records = df.to_dict('records')
            for rec in records:
                item = {
                    'id': int(rec['id']), 
                    'name': rec.get('name'),
                    'feature_lookup_id': rec.get('feature_lookup_id'),
                    'evaluation_type': rec.get('evaluation_type'),
                    'percentage': rec.get('percentage'),
                    'status': rec.get('status', 'NOT_STARTED'),
                    'materialized': rec.get('materialized', False),
                    'run_id': rec.get('run_id'),
                    'run_url': rec.get('run_url'),
                    'training_table_name': rec.get('training_table_name'),
                    'eval_table_name': rec.get('eval_table_name')
                }
                items.append(item)
                
                # Find the newly created dataset by name
                if rec.get('name') == new_name:
                    new_dataset_id = int(rec['id'])
        
        # Select the newly created dataset and return form values
        if new_dataset_id:
            store_data = {'items': items, 'active_id': new_dataset_id, 'create_mode': False}
        else:
            # Fallback - select the last item if we can't find by name
            new_dataset_id = items[-1]['id'] if items else None
            store_data = {'items': items, 'active_id': new_dataset_id, 'create_mode': False}
        
        # Return form values for the new dataset
        return (new_name, default_fl_id, "random", 80.0, "NOT_STARTED", "secondary", '', '', '#', 
                {'display': 'none'}, {'display': 'inline'}, '', '', True, store_data)

    @app.callback(
        Output('dataset-name', 'value'),
        Output('dataset-feature-lookup-dropdown', 'value'),
        Output('dataset-evaluation-type-dropdown', 'value'),
        Output('dataset-percentage', 'value'),
        Output('dataset-status-display', 'children'),
        Output('dataset-status-display', 'color'),
        Output('dataset-run-id', 'value'),
        Output('dataset-run-url', 'value'),
        Output('dataset-run-url-link', 'href'),
        Output('dataset-run-url-link', 'style'),
        Output('dataset-run-url-placeholder', 'style'),
        Output('dataset-training-table', 'value'),
        Output('dataset-eval-table', 'value'),
        Output('materialize-dataset-button', 'disabled'),
        Input('dataset-store', 'data'),
        prevent_initial_call=False  # Allow initial call to populate form on first load
    )
    def populate_dataset_form(store_data):
        """Populate form inputs when the dataset store updates (like projects tab)."""
        if not isinstance(store_data, dict):
            return '', None, None, None, "NOT_STARTED", "secondary", '', '', '#', {'display': 'none'}, {'display': 'inline'}, '', '', True
            
        active_id = store_data.get('active_id')
        items = store_data.get('items', [])
        
        # If no active selection, clear the form
        if active_id is None or not items:
            return '', None, None, None, "NOT_STARTED", "secondary", '', '', '#', {'display': 'none'}, {'display': 'inline'}, '', '', True
            
        # Find the selected dataset
        for rec in items:
            if rec.get('id') == active_id:
                name = rec.get('name') or ''
                feature_lookup_id = rec.get('feature_lookup_id')
                evaluation_type = rec.get('evaluation_type')
                percentage = rec.get('percentage')
                status = rec.get('status', 'NOT_STARTED')
                materialized = rec.get('materialized', False)
                run_id = rec.get('run_id') or ''
                run_url = rec.get('run_url') or ''
                training_table = rec.get('training_table_name') or ''
                eval_table = rec.get('eval_table_name') or ''
                
                # Control visibility of run URL link vs placeholder
                if run_url:
                    link_style = {'display': 'inline'}
                    placeholder_style = {'display': 'none'}
                else:
                    link_style = {'display': 'none'}
                    placeholder_style = {'display': 'inline'}
                
                # Disable materialize button if dataset is already materialized (status = SUCCESS)
                materialize_disabled = (status == 'SUCCESS')
                
                # Set badge color based on status
                status_colors = {
                    'NOT_STARTED': 'secondary',
                    'PENDING': 'warning',
                    'RUNNING': 'info',
                    'SUCCESS': 'success',
                    'FAILED': 'danger',
                    'TERMINATED': 'dark',
                    'SKIPPED': 'light'
                }
                badge_color = status_colors.get(status, 'secondary')
                
                return name, feature_lookup_id, evaluation_type, percentage, status, badge_color, run_id, run_url, run_url, link_style, placeholder_style, training_table, eval_table, materialize_disabled
        
        # If no matching record found, clear the form
        return '', None, None, None, "NOT_STARTED", "secondary", '', '', '#', {'display': 'none'}, {'display': 'inline'}, '', '', True

    @app.callback(
        Output('dataset-store', 'data', allow_duplicate=True),
        Input('update-dataset-button', 'n_clicks'),
        State('dataset-store', 'data'),
        State('dataset-name', 'value'),
        State('dataset-feature-lookup-dropdown', 'value'),
        State('dataset-evaluation-type-dropdown', 'value'),
        State('dataset-percentage', 'value'),
        State('list-store', 'data'),
        prevent_initial_call=True
    )
    def update_dataset_callback(n_clicks, store_data, name, feature_lookup_id, evaluation_type, percentage, project_store):
        dataset_id = store_data.get('active_id') if isinstance(store_data, dict) else None
        if dataset_id is None or not name:
            return dash.no_update
        
        # Update only user-editable fields - materialized status is controlled by materialize job
        if not update_dataset(dataset_id, name, feature_lookup_id, evaluation_type, percentage):
            return dash.no_update
            
        # Refresh the list from database
        project_id = project_store.get('active_project_id') if isinstance(project_store, dict) else None
        df = get_datasets(project_id)
        records = df.to_dict('records') if not df.empty else []
        items = [
            {
                'id': int(rec['id']), 
                'name': rec.get('name'),
                'feature_lookup_id': rec.get('feature_lookup_id'),
                'evaluation_type': rec.get('evaluation_type'),
                'percentage': rec.get('percentage'),
                'status': rec.get('status', 'NOT_STARTED'),
                'materialized': rec.get('materialized'),
                'training_table_name': rec.get('training_table_name'),
                'eval_table_name': rec.get('eval_table_name')
            }
            for rec in records
        ]
        print(f"DEBUG: update_dataset_callback returning store with active_id={dataset_id}, items={len(items)}")
        return {'items': items, 'active_id': dataset_id, 'create_mode': False}

    @app.callback(
        Output('dataset-store', 'data', allow_duplicate=True),
        Input('delete-dataset-button', 'n_clicks'),
        State('dataset-store', 'data'),
        State('list-store', 'data'),
        prevent_initial_call=True
    )
    def delete_dataset_callback(n_clicks, store_data, project_store):
        dataset_id = store_data.get('active_id') if isinstance(store_data, dict) else None
        if dataset_id is None:
            return dash.no_update
        if not delete_dataset(dataset_id):
            return dash.no_update
            
        # Refresh the list from database
        project_id = project_store.get('active_project_id') if isinstance(project_store, dict) else None
        df = get_datasets(project_id)
        records = df.to_dict('records') if not df.empty else []
        items = [
            {
                'id': int(rec['id']), 
                'name': rec.get('name'),
                'feature_lookup_id': rec.get('feature_lookup_id'),
                'evaluation_type': rec.get('evaluation_type'),
                'percentage': rec.get('percentage'),
                'status': rec.get('status', 'NOT_STARTED'),
                'materialized': rec.get('materialized'),
                'training_table_name': rec.get('training_table_name'),
                'eval_table_name': rec.get('eval_table_name')
            }
            for rec in records
        ]
        # After deletion, don't auto-select anything
        new_active_id = None
        return {'items': items, 'active_id': new_active_id, 'create_mode': False}
    
    @app.callback(
        Output('dataset-store', 'data', allow_duplicate=True),
        Output('dataset-form-alert', 'children'),
        Input('materialize-dataset-button', 'n_clicks'),
        State('dataset-store', 'data'),
        State('list-store', 'data'),
        prevent_initial_call=True
    )
    def materialize_dataset_callback(n_clicks, store_data, project_store):
        """Run the materialize job for the selected dataset."""
        dataset_id = store_data.get('active_id') if isinstance(store_data, dict) else None
        if dataset_id is None:
            return dash.no_update, dbc.Alert("No dataset selected", color="warning", dismissable=True)
        
        # Get dataset info
        dataset = get_dataset_by_id(dataset_id)
        if dataset is None:
            return dash.no_update, dbc.Alert("Dataset not found", color="danger", dismissable=True)
        
        # Check if dataset is already materialized
        if dataset.get('materialized', False):
            dataset_name = dataset.get('name', f'ID {dataset_id}')
            return dash.no_update, dbc.Alert(
                f"Dataset '{dataset_name}' is already materialized.", 
                color="warning", 
                dismissable=True
            )
        
        # Get project info
        project_id = project_store.get('active_project_id') if isinstance(project_store, dict) else None
        if project_id is None:
            return dash.no_update, dbc.Alert("No project selected", color="warning", dismissable=True)
        
        project = get_project_by_id(project_id)
        if project is None:
            return dash.no_update, dbc.Alert("Project not found", color="danger", dismissable=True)
        
        # Get feature lookup info
        feature_lookup_id = dataset.get('feature_lookup_id')
        feature_lookup = get_feature_lookup_by_id(feature_lookup_id)
        if feature_lookup is None:
            return dash.no_update, dbc.Alert("Feature lookup not found", color="danger", dismissable=True)
        
        # Get EOL definition info
        eol_id = feature_lookup.get('eol_id')
        if not eol_id:
            return dash.no_update, dbc.Alert("Feature lookup has no EOL definition", color="danger", dismissable=True)
        
        eol_def = get_eol_definition_by_id(eol_id)
        if eol_def is None:
            return dash.no_update, dbc.Alert("EOL definition not found", color="danger", dismissable=True)
        
        # Load database configuration
        try:
            with open('db_config.yaml', 'r') as f:
                config = yaml.safe_load(f)
            job_id = config['database']['materialize_table_job']
            app_catalog_name = config['database']['catalog']
            app_schema_name = config['database']['schema']
        except Exception as e:
            return dash.no_update, dbc.Alert(f"Error loading config: {e}", color="danger", dismissable=True)
        
        # Prepare job parameters
        project_catalog_name = project.get('catalog')
        project_schema_name = project.get('schema')
        eol_view = f"{project_catalog_name}.{project_schema_name}.{eol_def.get('name')}"
        
        # Initialize Databricks client
        try:
            client = WorkspaceClient()
        except Exception as e:
            return dash.no_update, dbc.Alert(f"Error initializing Databricks client: {e}", color="danger", dismissable=True)
        
        # Get the label from EOL definition
        label_column = eol_def.get('label', '')
        
        # Debug: print the EOL definition and label value
        print(f"DEBUG: EOL definition: {eol_def}")
        print(f"DEBUG: Label column extracted: '{label_column}' (type: {type(label_column)})")
        
        # Get features data from feature lookup and convert to JSON
        features_data = feature_lookup.get('features', [])
        feature_lookup_name = feature_lookup.get('name', '')
        print(f"DEBUG: Features data from lookup: {features_data}")
        print(f"DEBUG: Feature lookup name: {feature_lookup_name}")
        
        # Convert features to JSON string
        import json
        features_json = json.dumps(features_data)
        print(f"DEBUG: Features JSON: {features_json}")
        
        # Define job parameters
        parameters = {
            "app_catalog_name": app_catalog_name,
            "app_schema_name": app_schema_name,
            "project_catalog_name": project_catalog_name,
            "project_schema_name": project_schema_name,
            "features": features_json,
            "name": feature_lookup_name,
            "eol_view": eol_view,
            "dataset_id": str(dataset_id),
            "label": label_column
        }
        
        print(f"DEBUG: Materialize job parameters: {parameters}")
        
        try:
            # Run the job with notebook_params
            run = client.jobs.run_now(
                job_id=job_id,
                notebook_params=parameters
            )
            
            print(f"Materialize job initiated for dataset {dataset_id}")
            print(f"Run ID: {run.run_id}")
            
            # Update dataset record with run information
            import os
            databricks_host = os.getenv("DATABRICKS_HOST", "https://dbc-ce343e89-af12.cloud.databricks.com")
            if databricks_host and not databricks_host.startswith('https://'):
                databricks_host = 'https://' + databricks_host
            # Remove trailing slash if present
            if databricks_host and databricks_host.endswith('/'):
                databricks_host = databricks_host.rstrip('/')
            
            run_url = f"{databricks_host}/jobs/{job_id}/runs/{run.run_id}"
            
            # Update the dataset record immediately with PENDING status
            from utils.db_universal import update_dataset_run_info
            update_success = update_dataset_run_info(
                dataset_id=dataset_id,
                run_id=str(run.run_id),
                run_url=run_url,
                status='PENDING'
            )
            
            if not update_success:
                print(f"Warning: Failed to update dataset record for dataset {dataset_id}")
            
            # Start background thread to poll for job completion and extract table names
            import threading
            def poll_job_completion():
                try:
                    print(f"Starting background polling for job completion: {run.run_id}")
                    
                    # Wait for job completion - handle both success and failure cases
                    try:
                        result = client.jobs.wait_get_run_job_terminated_or_skipped(
                            run_id=run.run_id
                        )
                        print(f"Materialization job completed: {result.run_page_url}")
                        job_succeeded = True
                    except Exception as wait_error:
                        # Job failed - get the run details to understand what happened
                        print(f"Materialization job failed: {wait_error}")
                        try:
                            result = client.jobs.get_run(run_id=run.run_id)
                            print(f"Failed job details: {result.run_page_url if hasattr(result, 'run_page_url') else 'N/A'}")
                            job_succeeded = False
                        except Exception as get_run_error:
                            print(f"Could not get run details: {get_run_error}")
                            return  # Exit early if we can't even get run details
                    
                    # Determine final status based on job result
                    if job_succeeded:
                        final_status = 'SUCCESS'
                    else:
                        final_status = 'FAILED'
                    
                    # Only try to extract table names if job succeeded
                    training_table_name = None
                    eval_table_name = None
                    
                    if job_succeeded:
                        try:
                            # Get the run details to find task outputs
                            if hasattr(result, 'tasks') and result.tasks:
                                for task in result.tasks:
                                    if hasattr(task, 'run_id'):
                                        task_run_id = task.run_id
                                        print(f"Getting output for task run_id: {task_run_id}")
                                    
                                        try:
                                            # Get the task output
                                            task_output = client.jobs.get_run_output(run_id=task_run_id)
                                        
                                            # Check for notebook output with table names
                                            if hasattr(task_output, 'notebook_output') and task_output.notebook_output:
                                                nb_output = task_output.notebook_output
                                            
                                                if hasattr(nb_output, 'result'):
                                                    output_str = nb_output.result
                                                    print(f"Notebook output: {output_str[:500] if output_str else 'None'}")
                                                
                                                    if output_str:
                                                        # Try to parse as JSON first
                                                        try:
                                                            import json
                                                            output_data = json.loads(output_str)
                                                        
                                                            # Look for table names in various formats
                                                            for key in ['train_table_name', 'training_table_name']:
                                                                if key in output_data:
                                                                    training_table_name = output_data[key]
                                                                    print(f"Found {key}: {training_table_name}")
                                                        
                                                            for key in ['eval_table_name', 'evaluation_table_name']:
                                                                if key in output_data:
                                                                    eval_table_name = output_data[key]
                                                                    print(f"Found {key}: {eval_table_name}")
                                                                
                                                        except json.JSONDecodeError:
                                                            # Try regex extraction as fallback
                                                            import re
                                                            train_patterns = [
                                                                r'"train_table_name"\s*:\s*"([^"]+)"',
                                                                r'"training_table_name"\s*:\s*"([^"]+)"',
                                                            ]
                                                            eval_patterns = [
                                                                r'"eval_table_name"\s*:\s*"([^"]+)"',
                                                                r'"evaluation_table_name"\s*:\s*"([^"]+)"',
                                                            ]
                                                        
                                                            for pattern in train_patterns:
                                                                match = re.search(pattern, output_str, re.IGNORECASE)
                                                                if match:
                                                                    training_table_name = match.group(1)
                                                                    print(f"Extracted training table via regex: {training_table_name}")
                                                                    break
                                                        
                                                            for pattern in eval_patterns:
                                                                match = re.search(pattern, output_str, re.IGNORECASE)
                                                                if match:
                                                                    eval_table_name = match.group(1)
                                                                    print(f"Extracted eval table via regex: {eval_table_name}")
                                                                    break
                                        except Exception as e:
                                            print(f"Error getting output for task {task_run_id}: {e}")
                        
                            # Update dataset record with table names and SUCCESS status
                            print(f"Updating dataset {dataset_id} with status=SUCCESS and table names: train={training_table_name}, eval={eval_table_name}")
                            update_dataset_run_info(
                                dataset_id=dataset_id,
                                run_id=str(run.run_id),
                                run_url=run_url,
                                status='SUCCESS',
                                training_table_name=training_table_name,
                                eval_table_name=eval_table_name
                            )
                                
                        except Exception as e:
                            print(f"Error extracting table names from job output: {e}")
                            import traceback
                            traceback.print_exc()
                    else:
                        # Job failed - update status to FAILED
                        print(f"Job failed for dataset {dataset_id}, updating status to FAILED")
                        update_dataset_run_info(
                            dataset_id=dataset_id,
                            run_id=str(run.run_id),
                            run_url=run_url,
                            status='FAILED'
                        )
                        
                except Exception as e:
                    print(f"Error in background job polling: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Start the polling thread
            polling_thread = threading.Thread(target=poll_job_completion, daemon=True)
            polling_thread.start()
            
            # Refresh dataset store to reflect changes
            try:
                from utils.db_universal import get_datasets
                project_id = project_store.get('active_project_id') if isinstance(project_store, dict) else None
                if project_id:
                    records = get_datasets(project_id)
                    items = [
                        {
                            'id': rec.get('id'),
                            'name': rec.get('name'),
                            'feature_lookup_id': rec.get('feature_lookup_id'),
                            'evaluation_type': rec.get('evaluation_type'),
                            'percentage': rec.get('percentage'),
                            'status': rec.get('status', 'NOT_STARTED'),
                            'materialized': rec.get('materialized', False),
                            'run_id': rec.get('run_id'),
                            'run_url': rec.get('run_url'),
                            'training_table_name': rec.get('training_table_name'),
                            'eval_table_name': rec.get('eval_table_name')
                        }
                        for rec in records.to_dict('records') if rec
                    ]
                    updated_store = {'items': items, 'active_id': dataset_id}
                    print(f"DEBUG: Materialize - updating store with active_id={dataset_id}, items count={len(items)}")
                else:
                    updated_store = dash.no_update
            except Exception as e:
                print(f"Warning: Failed to refresh dataset store: {e}")
                updated_store = dash.no_update
            
            # Return success message and updated store
            dataset_name = dataset.get('name', f'ID {dataset_id}')
            return updated_store, dbc.Alert(
                f"Successfully started materialization job for dataset '{dataset_name}'. Job ID: {job_id}, Run ID: {run.run_id}",
                color="success",
                dismissable=True
            )
            
        except Exception as e:
            error_msg = f"Error running materialize job: {e}"
            print(error_msg)
            return dash.no_update, dbc.Alert(error_msg, color="danger", dismissable=True)
    
    # Clear alert when dataset selection changes
    @app.callback(
        Output('dataset-form-alert', 'children', allow_duplicate=True),
        Input('dataset-store', 'data'),
        prevent_initial_call=True
    )
    def clear_alert_on_dataset_change(store_data):
        """Clear the alert when dataset selection changes."""
        return []