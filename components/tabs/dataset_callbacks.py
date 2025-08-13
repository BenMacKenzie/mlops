import dash
from dash import Input, Output, State, callback_context, ALL
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from utils.db import (
    get_datasets,
    get_dataset_by_id,
    create_dataset,
    update_dataset,
    delete_dataset,
    get_feature_lookups,
    get_feature_lookup_by_id,
    get_eol_definition_by_id,
    get_project_by_id,
    update_dataset_run_info
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
        prevent_initial_call='initial_duplicate'  # Allow initial call with duplicate output
    )
    def refresh_dataset_store_on_project_change(project_store):
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
                    'materialized': rec.get('materialized'),
                    'training_table_name': rec.get('training_table_name'),
                    'eval_table_name': rec.get('eval_table_name'),
                    'run_id': rec.get('run_id'),
                    'run_url': rec.get('run_url')
                }
                for rec in records
            ]
        
        # Set first item as active, or None if no items
        active_id = items[0]['id'] if items else None
        print(f"refresh_dataset_store_on_project_change - project_id: {project_id}, found {len(items)} datasets")
        
        return {'items': items, 'active_id': active_id}
    
    # Populate feature lookup dropdown based on selected project
    @app.callback(
        Output('dataset-feature-lookup-dropdown', 'options'),
        Input('list-store', 'data')
    )
    def update_feature_lookup_dropdown(store_data):
        project_id = None
        if isinstance(store_data, dict):
            project_id = store_data.get('active_project_id')
        # Fetch feature lookups for project
        if not project_id:
            return []
        df = get_feature_lookups(project_id)
        if df.empty:
            return []
        # Build dropdown options: label=name, value=id
        opts = []
        for _, row in df.iterrows():
            try:
                val = int(row['id'])
            except Exception:
                continue
            opts.append({'label': row.get('name'), 'value': val})
        return opts

    @app.callback(
        Output('dataset-list', 'children'),
        Input('dataset-store', 'data'),
        prevent_initial_call=False  # Allow initial call to show datasets on first load
    )
    def refresh_dataset_list(store_data):
        items = store_data.get('items', []) if isinstance(store_data, dict) else []
        active_id = store_data.get('active_id') if isinstance(store_data, dict) else None
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
        return {'items': items, 'active_id': dataset_id}

    @app.callback(
        Output('dataset-store', 'data', allow_duplicate=True),
        Input('create-dataset-button', 'n_clicks'),
        State('list-store', 'data'),
        prevent_initial_call=True
    )
    def create_dataset_callback(n_clicks, project_store):
        # Determine current project
        project_id = project_store.get('active_project_id') if isinstance(project_store, dict) else None
        if project_id is None:
            # Nothing to do if no project selected
            return dash.no_update
            
        # Get the first available feature lookup for this project as default
        feature_lookups_df = get_feature_lookups(project_id)
        if feature_lookups_df.empty:
            print("No feature lookups available for this project - cannot create dataset")
            return dash.no_update
        
        default_feature_lookup_id = int(feature_lookups_df.iloc[0]['id'])
        
        # Create dataset with default values (like projects tab)
        default_name = "New Dataset"
        default_evaluation_type = "random"
        default_percentage = 80.0
        default_materialized = False
        
        # Create dataset in DB with defaults
        if not create_dataset(project_id, default_feature_lookup_id, default_name, default_evaluation_type, default_percentage, default_materialized):
            return dash.no_update
            
        # Refresh the list of datasets from database
        df = get_datasets(project_id)
        records = df.to_dict('records') if not df.empty else []
        items = [
            {
                'id': int(rec['id']), 
                'name': rec.get('name'),
                'feature_lookup_id': rec.get('feature_lookup_id'),
                'evaluation_type': rec.get('evaluation_type'),
                'percentage': rec.get('percentage'),
                'materialized': rec.get('materialized'),
                'training_table_name': rec.get('training_table_name'),
                'eval_table_name': rec.get('eval_table_name')
            }
            for rec in records
        ]
        
        # Find the newly created dataset (should be the last one)
        new_dataset_id = items[-1]['id'] if items else None
        
        # Set the newly created dataset as active (like projects tab)
        return {'items': items, 'active_id': new_dataset_id}

    @app.callback(
        Output('dataset-name', 'value'),
        Output('dataset-feature-lookup-dropdown', 'value'),
        Output('dataset-evaluation-type-dropdown', 'value'),
        Output('dataset-percentage', 'value'),
        Output('dataset-materialized', 'value'),
        Output('dataset-run-id', 'value'),
        Output('dataset-run-url', 'value'),
        Output('dataset-run-url-link', 'href'),
        Output('dataset-run-url-link', 'style'),
        Output('dataset-run-url-placeholder', 'style'),
        Output('dataset-training-table', 'value'),
        Output('dataset-eval-table', 'value'),
        Input('dataset-store', 'data'),
        prevent_initial_call=False  # Allow initial call to populate form on first load
    )
    def populate_dataset_form(store_data):
        """Populate form inputs when the dataset store updates (like projects tab)."""
        if not isinstance(store_data, dict):
            return '', None, None, None, False, '', '', '#', {'display': 'none'}, {'display': 'inline'}, '', ''
            
        active_id = store_data.get('active_id')
        items = store_data.get('items', [])
        
        # If no active selection, clear the form
        if active_id is None or not items:
            return '', None, None, None, False, '', '', '#', {'display': 'none'}, {'display': 'inline'}, '', ''
            
        # Find the selected dataset
        for rec in items:
            if rec.get('id') == active_id:
                name = rec.get('name') or ''
                feature_lookup_id = rec.get('feature_lookup_id')
                evaluation_type = rec.get('evaluation_type')
                percentage = rec.get('percentage')
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
                
                return name, feature_lookup_id, evaluation_type, percentage, materialized, run_id, run_url, run_url, link_style, placeholder_style, training_table, eval_table
        
        # If no matching record found, clear the form
        return '', None, None, None, False, '', '', '#', {'display': 'none'}, {'display': 'inline'}, '', ''

    @app.callback(
        Output('dataset-store', 'data', allow_duplicate=True),
        Input('update-dataset-button', 'n_clicks'),
        State('dataset-store', 'data'),
        State('dataset-name', 'value'),
        State('dataset-feature-lookup-dropdown', 'value'),
        State('dataset-evaluation-type-dropdown', 'value'),
        State('dataset-percentage', 'value'),
        State('dataset-materialized', 'value'),
        State('list-store', 'data'),
        prevent_initial_call=True
    )
    def update_dataset_callback(n_clicks, store_data, name, feature_lookup_id, evaluation_type, percentage, materialized, project_store):
        dataset_id = store_data.get('active_id') if isinstance(store_data, dict) else None
        if dataset_id is None or not name:
            return dash.no_update
        
        if not update_dataset(dataset_id, name, feature_lookup_id, evaluation_type, percentage, materialized):
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
                'materialized': rec.get('materialized'),
                'training_table_name': rec.get('training_table_name'),
                'eval_table_name': rec.get('eval_table_name')
            }
            for rec in records
        ]
        return {'items': items, 'active_id': dataset_id}

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
                'materialized': rec.get('materialized'),
                'training_table_name': rec.get('training_table_name'),
                'eval_table_name': rec.get('eval_table_name')
            }
            for rec in records
        ]
        new_active_id = items[0]['id'] if items else None
        return {'items': items, 'active_id': new_active_id}
    
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
        
        # Define job parameters
        parameters = {
            "app_catalog_name": app_catalog_name,
            "app_schema_name": app_schema_name,
            "project_catalog_name": project_catalog_name,
            "project_schema_name": project_schema_name,
            "feature_lookup_id": str(feature_lookup_id),
            "eol_view": eol_view,
            "dataset_id": str(dataset_id)
        }
        
        try:
            # Run the job with notebook_params
            run = client.jobs.run_now(
                job_id=job_id,
                notebook_params=parameters
            )
            
            print(f"Materialize job initiated for dataset {dataset_id}")
            print(f"Run ID: {run.run_id}")
            
            # Wait for run completion
            result = client.jobs.wait_get_run_job_terminated_or_skipped(
                run_id=run.run_id
            )
            
            print(f"Materialize job finished: {result.run_page_url}")
            
            # Extract table names from task outputs
            training_table_name = None
            eval_table_name = None
            
            # Get the detailed run information to find task run IDs
            try:
                print(f"\n=== EXTRACTING TABLE NAMES FROM RUN ===")
                print(f"Result object type: {type(result)}")
                print(f"Result attributes: {[attr for attr in dir(result) if not attr.startswith('_')]}")
                
                # Task values are stored differently - they're in the run's state
                # Try to get task values using the Databricks SDK
                if hasattr(result, 'state') and result.state:
                    print(f"Run state: {result.state}")
                    if hasattr(result.state, 'state_message'):
                        print(f"State message: {result.state.state_message}")
                
                # Get the run details which should include task values
                try:
                    # Get the full run details
                    run_details = client.jobs.get_run(run_id=run.run_id)
                    print(f"Run details type: {type(run_details)}")
                    
                    # Check for task outputs in the run details
                    if hasattr(run_details, 'tasks') and run_details.tasks:
                        print(f"Found {len(run_details.tasks)} tasks in run details")
                        for task in run_details.tasks:
                            print(f"\nTask key: {getattr(task, 'task_key', 'N/A')}")
                            print(f"Task attributes: {[attr for attr in dir(task) if not attr.startswith('_')]}")
                            
                            # Check multiple possible locations for task values
                            # 1. Direct values attribute
                            if hasattr(task, 'values'):
                                task_values = task.values
                                print(f"Task.values found: {task_values}")
                                if isinstance(task_values, dict):
                                    training_table_name = task_values.get('train_table_name', training_table_name)
                                    eval_table_name = task_values.get('eval_table_name', eval_table_name)
                                    print(f"Extracted from task.values - train: {training_table_name}, eval: {eval_table_name}")
                            
                            # 2. Check output attribute
                            if hasattr(task, 'output'):
                                task_output = task.output
                                print(f"Task.output found: {task_output}")
                                if isinstance(task_output, dict):
                                    training_table_name = task_output.get('train_table_name', training_table_name)
                                    eval_table_name = task_output.get('eval_table_name', eval_table_name)
                                    print(f"Extracted from task.output - train: {training_table_name}, eval: {eval_table_name}")
                            
                            # 3. Check outputs attribute (plural)
                            if hasattr(task, 'outputs'):
                                task_outputs = task.outputs
                                print(f"Task.outputs found: {task_outputs}")
                                if isinstance(task_outputs, dict):
                                    training_table_name = task_outputs.get('train_table_name', training_table_name)
                                    eval_table_name = task_outputs.get('eval_table_name', eval_table_name)
                                    print(f"Extracted from task.outputs - train: {training_table_name}, eval: {eval_table_name}")
                            
                            # 4. Check state for values
                            if hasattr(task, 'state') and task.state:
                                print(f"Task.state attributes: {[attr for attr in dir(task.state) if not attr.startswith('_')]}")
                                if hasattr(task.state, 'result_state'):
                                    print(f"Task result state: {task.state.result_state}")
                                
                    # Alternative: Try to get values from the job run output
                    run_output = client.jobs.get_run_output(run_id=run.run_id)
                    
                    # Check if task values are in metadata
                    if hasattr(run_output, 'metadata') and run_output.metadata:
                        print(f"Run output metadata: {run_output.metadata}")
                        if isinstance(run_output.metadata, dict):
                            task_values = run_output.metadata.get('task_values', {})
                            if task_values:
                                training_table_name = task_values.get('train_table_name', training_table_name)
                                eval_table_name = task_values.get('eval_table_name', eval_table_name)
                                print(f"Extracted from metadata task values - train: {training_table_name}, eval: {eval_table_name}")
                    
                    # Try getting task values through task run outputs
                    if hasattr(run_output, 'tasks') and run_output.tasks:
                        print(f"Found tasks in run output")
                        # Task values might be in a specific format when set with dbutils.jobs.taskValues.set
                        
                except Exception as e:
                    print(f"Error getting run details: {e}")
                
                # Also check if the result has tasks with run_ids (keeping existing logic as fallback)
                if hasattr(result, 'tasks') and result.tasks:
                    print(f"Found {len(result.tasks)} tasks in the result")
                    for i, task in enumerate(result.tasks):
                        print(f"\n--- Task {i} ---")
                        print(f"Task object type: {type(task)}")
                        print(f"Task attributes: {[attr for attr in dir(task) if not attr.startswith('_')]}")
                        print(f"Task key: {getattr(task, 'task_key', 'N/A')}")
                        print(f"Task run_id: {getattr(task, 'run_id', 'N/A')}")
                        
                        # Print all task attributes that might contain output
                        for attr in ['state', 'notebook_output', 'outputs', 'run_id']:
                            if hasattr(task, attr):
                                value = getattr(task, attr)
                                print(f"Task.{attr}: {value}")
                        
                        # Get the task-specific output using its run_id
                        if hasattr(task, 'run_id'):
                            task_run_id = task.run_id
                            print(f"\nGetting detailed output for task run_id: {task_run_id}")
                            
                            try:
                                # Get the task output
                                task_output = client.jobs.get_run_output(run_id=task_run_id)
                                print(f"Task output type: {type(task_output)}")
                                print(f"Task output attributes: {[attr for attr in dir(task_output) if not attr.startswith('_')]}")
                                
                                # Print all output attributes
                                for attr in ['notebook_output', 'error', 'error_trace', 'logs', 'metadata']:
                                    if hasattr(task_output, attr):
                                        value = getattr(task_output, attr)
                                        if value:
                                            print(f"Task output.{attr}: {value}")
                                
                                # Check for notebook output with task values
                                if hasattr(task_output, 'notebook_output') and task_output.notebook_output:
                                    nb_output = task_output.notebook_output
                                    print(f"Notebook output type: {type(nb_output)}")
                                    print(f"Notebook output attributes: {[attr for attr in dir(nb_output) if not attr.startswith('_')]}")
                                    
                                    if hasattr(nb_output, 'result'):
                                        output_str = nb_output.result
                                        print(f"Notebook result string length: {len(output_str) if output_str else 0}")
                                        print(f"Notebook result (first 500 chars): {output_str[:500] if output_str else 'None'}")
                                        
                                        # Only try to parse if we have actual content
                                        if output_str:
                                            # Try to parse as JSON
                                            try:
                                                import json
                                                output_data = json.loads(output_str)
                                                print(f"Successfully parsed JSON. Keys: {list(output_data.keys())}")
                                                
                                                # Look for table names in the output
                                                for key in ['train_table_name', 'training_table_name']:
                                                    if key in output_data:
                                                        training_table_name = output_data[key]
                                                        print(f"Found {key}: {training_table_name}")
                                                
                                                for key in ['eval_table_name', 'evaluation_table_name']:
                                                    if key in output_data:
                                                        eval_table_name = output_data[key]
                                                        print(f"Found {key}: {eval_table_name}")
                                                        
                                            except (json.JSONDecodeError, TypeError) as e:
                                                print(f"Could not parse as JSON: {e}")
                                                # Try regex extraction as fallback
                                                import re
                                                # More flexible patterns
                                                train_patterns = [
                                                    r'"train_table_name"\s*:\s*"([^"]+)"',
                                                    r'"training_table_name"\s*:\s*"([^"]+)"',
                                                    r'train_table_name\s*=\s*"([^"]+)"',
                                                    r'training_table_name\s*=\s*"([^"]+)"'
                                                ]
                                                eval_patterns = [
                                                    r'"eval_table_name"\s*:\s*"([^"]+)"',
                                                    r'"evaluation_table_name"\s*:\s*"([^"]+)"',
                                                    r'eval_table_name\s*=\s*"([^"]+)"',
                                                    r'evaluation_table_name\s*=\s*"([^"]+)"'
                                                ]
                                                
                                                for pattern in train_patterns:
                                                    match = re.search(pattern, output_str, re.IGNORECASE)
                                                    if match:
                                                        training_table_name = match.group(1)
                                                        print(f"Extracted training table via regex pattern '{pattern}': {training_table_name}")
                                                        break
                                                
                                                for pattern in eval_patterns:
                                                    match = re.search(pattern, output_str, re.IGNORECASE)
                                                    if match:
                                                        eval_table_name = match.group(1)
                                                        print(f"Extracted eval table via regex pattern '{pattern}': {eval_table_name}")
                                                        break
                                        else:
                                            print("Notebook result is empty - task values may be set but not returned via notebook.exit()")
                                
                            except Exception as e:
                                print(f"Error getting output for task {task_run_id}: {e}")
                                import traceback
                                traceback.print_exc()
                
                # Skip trying to get overall run output - Databricks doesn't support it for multi-task runs
                print("\nNote: Cannot get overall run output for multi-task jobs (Databricks limitation)")
                
            except Exception as e:
                print(f"Error extracting table names: {e}")
                import traceback
                traceback.print_exc()
            
            # DO NOT use defaults - table names must come from the materialization job
            print(f"\n=== FINAL RESULTS ===")
            print(f"Training table: {training_table_name if training_table_name else 'NOT FOUND'}")
            print(f"Eval table: {eval_table_name if eval_table_name else 'NOT FOUND'}")
            
            if not training_table_name or not eval_table_name:
                print("\nERROR: Could not extract table names from job output.")
                print("The notebook is currently using:")
                print("  dbutils.jobs.taskValues.set(key='train_table_name', value=train_table_name)")
                print("  dbutils.jobs.taskValues.set(key='eval_table_name', value=eval_table_name)")
                print("\nHowever, these values are not accessible via the Databricks SDK's get_run_output().")
                print("\nSOLUTION: The notebook should ALSO add at the end:")
                print("  import json")
                print("  dbutils.notebook.exit(json.dumps({")
                print("    'train_table_name': train_table_name,")
                print("    'eval_table_name': eval_table_name")
                print("  }))")
                print("\nThis will make the table names available in the notebook output.")
                print("\nTable names will not be set in the database until this is fixed.")
            
            # Update dataset with run information
            success = update_dataset_run_info(
                dataset_id=dataset_id,
                run_id=str(run.run_id),
                run_url=result.run_page_url,
                training_table_name=training_table_name,
                eval_table_name=eval_table_name
            )
            
            if not success:
                return dash.no_update, dbc.Alert("Failed to update dataset with run info", color="warning", dismissable=True)
            
            # Refresh the dataset store
            df = get_datasets(project_id)
            records = df.to_dict('records') if not df.empty else []
            items = [
                {
                    'id': int(rec['id']), 
                    'name': rec.get('name'),
                    'feature_lookup_id': rec.get('feature_lookup_id'),
                    'evaluation_type': rec.get('evaluation_type'),
                    'percentage': rec.get('percentage'),
                    'materialized': rec.get('materialized'),
                    'training_table_name': rec.get('training_table_name'),
                    'eval_table_name': rec.get('eval_table_name'),
                    'run_id': rec.get('run_id'),
                    'run_url': rec.get('run_url')
                }
                for rec in records
            ]
            
            alert = dbc.Alert(
                f"Materialization completed successfully! Run ID: {run.run_id}",
                color="success",
                dismissable=True
            )
            
            return {'items': items, 'active_id': dataset_id}, alert
            
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