  @app.callback(
        Output("train-status-output", "children"),
        Input("train-run-button", "n_clicks"),
        State("list-store", "data"), # Get project ID
        State("train-dataset-dropdown", "value"), # Get selected dataset ID from dropdown
        State("train-parameters-input", "value"), # Get user-provided params
        prevent_initial_call=True
    )
    def handle_train_button_click(n_clicks, proj_store, selected_ds_id, params_json_str):
        if n_clicks == 0:
            raise PreventUpdate

        # --- 1. Get Selected Project and Dataset IDs --- #
        project_id = proj_store.get("active_project_id")
        # selected_ds_id is now directly from the dropdown state
        
        # --- Remove Debugging related to ds_store and active states --- #
        print("--- Debug: handle_train_button_click ---")
        print(f"Project ID: {project_id}")
        print(f"Selected Dataset ID from Dropdown: {selected_ds_id}")
        # print(f"Dataset Active States: {ds_active_states}") # Removed
        # ds_items_count = len(ds_store.get("items", [])) # Removed
        # print(f"Items in dataset-store: {ds_items_count}") # Removed
        # --- End Debugging Removal --- #

        # --- Add Validation for Dropdown Selection --- #
        if not selected_ds_id:
             return dbc.Alert("Error: Please select a dataset from the dropdown.", color="danger")
        # --- End Validation --- #
        
        print("----------------------------------------")

        if not project_id: # selected_ds_id already checked
            return dbc.Alert("Error: Project not selected properly.", color="danger")

        # --- 2. Parse Parameters --- #
        try:
            parameters = json.loads(params_json_str or '{}') # Default to empty dict if no input
        except json.JSONDecodeError as e:
            return dbc.Alert(f"Error parsing parameters JSON: {e}", color="danger")

        # --- 3. Check Existing Training Job Record --- #
        training_record = get_training_job(project_id, selected_ds_id)
        db_job_id = None
        training_record_id = None

        if training_record:
            training_record_id = training_record.get('id')
            db_job_id = training_record.get('job_id')
            print(f"Found existing training record ID: {training_record_id} with Job ID: {db_job_id}")
        else:
            print("No existing training record found, will create one.")

        # --- 4. Get Job Details (Dataset Target, Git Info, etc.) --- #
        dataset_details = get_dataset_details(selected_ds_id)
        project_details = get_project_from_store(proj_store, project_id)
        
        # Get base git details (provider, branch, notebook_path from env/fallbacks in db.py)
        # and potentially a DB-fetched or default git_url if not in project_details
        raw_git_details = get_project_git_details(project_id)

        if not dataset_details or not project_details:
            return dbc.Alert("Error: Could not retrieve necessary project/dataset details.", color="danger")

        # Prioritize git_url from the project form (via store) if available
        git_url_from_store = project_details.get('git_url')
        final_git_url = git_url_from_store if git_url_from_store else raw_git_details.get('git_url')
        
        # Construct final git_details, prioritizing store's git_url
        final_git_details = {
            "git_url": final_git_url,
            "git_provider": raw_git_details.get('git_provider'),
            "git_branch": raw_git_details.get('git_branch'),
            "notebook_path": raw_git_details.get('notebook_path')
        }

        target_variable = dataset_details.get('target')
        training_table = dataset_details.get('training_table_name')
        # --- Add retrieval for eval table --- #
        eval_table = dataset_details.get('eval_table_name') 
        # --- End retrieval --- #
        dataset_name = dataset_details.get('name')
        project_name = project_details.get('text')

        if not training_table:
             return dbc.Alert(f"Error: Dataset '{dataset_name}' has not been materialized yet (no training table name).", color="danger")
        
        # --- Add check for eval table if needed by the job --- #
        # Depending on your notebook logic, you might need the eval table even if created.
        # Add a check here if the eval table is strictly required.
        if not eval_table:
             print(f"Warning: Dataset '{dataset_name}' does not have a generated evaluation table name.")
             # Optionally return an error if eval_table is mandatory:
             # return dbc.Alert(f"Error: Dataset '{dataset_name}' is missing a generated evaluation table.", color="danger")
        # --- End check --- #

        # --- 5. Logic: Run Existing or Create New Job --- #
        try:
            if db_job_id:
                # --- 5a. Run Existing Job --- #
                print(f"Running existing Databricks Job ID: {db_job_id}")
                run_info = run_training_job(db_job_id)
                return dbc.Alert(f"Started existing job {db_job_id}. Run ID: {run_info.run_id}", color="info")
            else:
                # --- 5b. Create New Job --- # 
                job_name = f"{project_name}_{dataset_name}"
                # --- Use project name for experiment path --- #
                experiment_name = project_name
                # --- End change --- #
                print(f"Creating new Databricks Job: {job_name}")
                
                # Prepare base parameters for the notebook task
                # --- Remove base_params dict, pass directly --- #
                # base_params = {
                #     "experiment_name": experiment_name,
                #     "target": target_variable,
                #     "table_name": training_table # Old parameter
                # }
                # Merge user parameters, user params take precedence
                # base_params.update(parameters) 
                # --- End Removal --- #

                new_job = create_databricks_job(
                    job_name=job_name,
                    experiment_name=experiment_name,
                    target=target_variable,
                    # --- Pass correct table names --- #
                    training_table_name=training_table,
                    eval_table_name=eval_table, # Pass the retrieved eval table name
                    # --- End pass correct table names --- #
                    git_url=final_git_details['git_url'],
                    git_provider=final_git_details['git_provider'],
                    git_branch=final_git_details['git_branch'],
                    notebook_path=final_git_details['notebook_path']
                    # Pass merged parameters to the notebook task
                    # This assumes create_training_job is updated or handles extra kwargs
                    # to pass them to the notebook_task.base_parameters
                    # --- Modification needed in utils/training.py --- #
                    # **base_params # This line needs adjustment based on function signature
                )
                new_job_id = new_job.job_id
                print(f"Created Databricks Job ID: {new_job_id}")

                # --- 5c. Update DB --- #
                if training_record_id:
                    # Update existing record that didn't have a job_id
                    success = update_training_job_id(training_record_id, new_job_id)
                    print(f"Updated existing training record {training_record_id} with job ID {new_job_id}: {success}")
                else:
                    # Create new training record
                    training_record_id = create_training_job_record(project_id, selected_ds_id, json.dumps(parameters))
                    if training_record_id:
                         success = update_training_job_id(training_record_id, new_job_id)
                         print(f"Created new training record {training_record_id}, updated with job ID {new_job_id}: {success}")
                    else:
                         print("Failed to create training record in DB.")
                         # Decide how to handle this - maybe don't run the job?

                if not training_record_id or not success:
                     return dbc.Alert("Error: Failed to update database with new Job ID. Job was created but not run.", color="warning")

                # --- 5d. Run New Job --- #
                print(f"Running newly created Databricks Job ID: {new_job_id}")
                run_info = run_training_job(new_job_id)
                return dbc.Alert(f"Created job {new_job_id} and started run. Run ID: {run_info.run_id}", color="success")

        except Exception as e:
            import traceback
            print(f"Error during training job creation/run: {traceback.format_exc()}")
            return dbc.Alert(f"An error occurred: {e}", color="danger")
    
    @app.callback(
        Output("train-mlflow-runs-list", "children"),
        Input("tabs", "active_tab"),
        Input("list-store", "data"), # Trigger when project list/selection changes
        prevent_initial_call=True
    )
    def update_mlflow_runs_list(active_tab, proj_store):
        if active_tab != 'tab-train' or not proj_store:
            raise PreventUpdate

        project_id = proj_store.get('active_project_id')
        if not project_id:
            return html.Tr(html.Td("Select a project to view MLflow runs.", colSpan=7))

        project_details = get_project_from_store(proj_store, project_id)
        if not project_details:
            print(f"Error: Could not find details for project ID {project_id} in store.")
            return html.Tr(html.Td("Error retrieving project details.", colSpan=7))

        base_experiment_name = project_details.get('text')
        catalog = project_details.get('catalog')
        schema = project_details.get('schema')

        if not base_experiment_name:
            return html.Tr(html.Td("Project name is missing.", colSpan=7))

        # --- Determine target_model_name --- START ---
        # First, get runs without model info to find a dataset name
        pre_runs, _, pre_error_msg = get_experiment_runs(base_experiment_name) # No target_model_name yet
        
        target_model_name = None
        dataset_name_for_model = None

        if pre_error_msg:
            print(f"MLflow fetch error (pre-fetch for dataset name): {pre_error_msg}")
            # Proceed without target_model_name, table will show N/A for model columns
        elif pre_runs:
            for pre_run in pre_runs:
                pre_run_data = pre_run.get('data', {})
                pre_tags = {tag['key']: tag['value'] for tag in pre_run_data.get('tags', [])}
                job_id_str = pre_tags.get('mlflow.databricks.jobID')
                if job_id_str:
                    try:
                        dataset_name_for_model = get_dataset_name_by_job_id(int(job_id_str))
                        if dataset_name_for_model:
                            break # Found a dataset name
                    except ValueError:
                        pass # Invalid job ID format
                    except Exception as e:
                        print(f"Error looking up dataset by job ID {job_id_str} for model naming: {e}")
            
            if dataset_name_for_model and catalog and schema:
                sanitized_dataset_name = dataset_name_for_model.replace(" ", "_").replace("-", "_") # Basic sanitization
                target_model_name = f"{catalog}.{schema}.{sanitized_dataset_name}"
                print(f"Constructed target_model_name: {target_model_name}")
            else:
                print("Could not determine dataset name for model, or catalog/schema missing.")
        # --- Determine target_model_name --- END ---

        print(f"Fetching MLflow runs for base experiment: {base_experiment_name}, target model: {target_model_name}")
        runs, experiment_id, error_msg = get_experiment_runs(base_experiment_name, target_model_name=target_model_name)

        if error_msg:
            print(f"MLflow fetch error: {error_msg}")
            return html.Tr(html.Td(error_msg, colSpan=7))
        
        if runs is None:
            print("MLflow runs list is None unexpectedly after main fetch.")
            return html.Tr(html.Td("Error fetching runs.", colSpan=7))

        if not runs:
            return html.Tr(html.Td("No runs found for this experiment.", colSpan=7))

        table_rows = []
        host = os.getenv("DATABRICKS_HOST") 
        if host and not host.startswith('https://'):
             host = 'https://' + host 
        
        for run in runs: 
            run_info = run.get('info', {})
            run_data = run.get('data', {})
            run_id = run_info.get('run_id', 'N/A')
            
            tags = {tag['key']: tag['value'] for tag in run_data.get('tags', [])}
            job_id_str = tags.get('mlflow.databricks.jobID')
            job_run_id_str = tags.get('mlflow.databricks.jobRunID')

            source_link = "#" 
            link_text = "Source Info Missing"
            if host and job_id_str and job_run_id_str:
                host_cleaned = host.rstrip('/') 
                source_link = f"{host_cleaned}/jobs/{job_id_str}/runs/{job_run_id_str}"
                link_text = f"run {job_run_id_str} of job {job_id_str}"
            elif job_id_str and job_run_id_str: 
                 link_text = f"run {job_run_id_str} of job {job_id_str} (Link Unavailable)"

            mlflow_run_link = "#" 
            if host and experiment_id and run_id != 'N/A':
                 host_cleaned = host.rstrip('/')
                 mlflow_run_link = f"{host_cleaned}/ml/experiments/{experiment_id}/runs/{run_id}"

            source_cell = html.Td(html.A(link_text, href=source_link, target="_blank"))
            run_id_cell = html.Td(html.A(run_id, href=mlflow_run_link, target="_blank")) 

            metrics_str = "N/A"
            metrics_list = run_data.get('metrics', []) 
            if metrics_list:
                metrics_str = " | ".join([
                    f"{metric.get('key')}: {metric.get('value'):.4f}"
                    for metric in metrics_list 
                    if metric.get('key') and metric.get('value') is not None
                ])
            metrics_cell = html.Td(metrics_str)

            dataset_name_display = "N/A" 
            if job_id_str:
                try:
                    dataset_name_from_db = get_dataset_name_by_job_id(int(job_id_str))
                    dataset_name_display = dataset_name_from_db if dataset_name_from_db else "Dataset Not Found"
                except ValueError:
                    dataset_name_display = "Invalid Job ID Tag"
                except Exception as e:
                    print(f"Error looking up dataset by job ID {job_id_str}: {e}")
                    dataset_name_display = "DB Lookup Error"
            else:
                 dataset_name_display = "Job ID Tag Missing"
                 
            dataset_cell = html.Td(dataset_name_display)

            # --- Get Registered Model Info --- 
            reg_model_name = run.get('registered_model_name', 'N/A')
            reg_model_version = run.get('registered_model_version', 'N/A')
            model_name_cell = html.Td(reg_model_name)
            model_version_cell = html.Td(reg_model_version)
            # --- End Registered Model Info --- 

            model_source = f"runs:/{run_id}/model"
            button_dataset_name = dataset_name_display if isinstance(dataset_name_display, str) else "UnknownDataset"

            # Disable button if model name and version are already populated for this run
            button_disabled = bool(reg_model_name and reg_model_name != 'N/A')

            register_button = html.Button(
                "Register Model",
                id={
                    'type': 'register-model-button', 
                    'index': run_id, 
                    'datasetname': button_dataset_name,
                    'modelsource': model_source
                },
                n_clicks=0,
                disabled=button_disabled, # Updated disabled state
                className="btn btn-primary"
            )
            actions_cell = html.Td(register_button)

            # Reorder: Dataset, Metrics, Run ID, Source, Reg. Model, Version, Actions
            table_rows.append(html.Tr([dataset_cell, metrics_cell, run_id_cell, source_cell, model_name_cell, model_version_cell, actions_cell]))

        return table_rows
    
    # --- Callback to populate the dataset dropdown on the Train tab --- #
    @app.callback(
        Output('train-dataset-dropdown', 'options'),
        Output('train-dataset-dropdown', 'value'),
        Input("tabs", "active_tab"),
        Input("list-store", "data"), # Trigger when project changes
        # Also consider Input("dataset-store", "data") if datasets can change while on train tab
        prevent_initial_call=True
    )
    def populate_train_dataset_dropdown(active_tab, list_store):
        if active_tab != 'tab-train' or not list_store:
            # Don't update if not on the right tab or store is empty
            return no_update, no_update

        project_id = list_store.get('active_project_id')
        if not project_id:
            # No project selected, clear dropdown
            return [], None 

        print(f"Populating dataset dropdown for project ID: {project_id}")
        df_datasets = get_datasets(project_id)
        options = []
        value = None
        if not df_datasets.empty:
            # Only include materialized datasets? Or all? Let's include all for now.
            # Might want to filter: df_datasets = df_datasets[df_datasets['materialized'] == True]
            options = [
                {'label': row['name'], 'value': row['id']}
                for index, row in df_datasets.iterrows()
            ]
            if options: # Set default value to the first dataset
                value = options[0]['value']
        
        return options, value
    # --- End Dropdown Population Callback --- #
    
    @app.callback(
        Output("train-status-output", "children", allow_duplicate=True), # Output for feedback
        Input({'type': 'register-model-button', 'index': ALL, 'datasetname': ALL, 'modelsource': ALL}, 'n_clicks'),
        State("list-store", "data"), # For catalog/schema
        prevent_initial_call=True
    )
    def handle_register_model_click(n_clicks_list, proj_store):
        if not ctx.triggered_id:
            # This condition might be met if n_clicks_list is all None or 0, prevent update.
            raise PreventUpdate

        # Check if the sum of n_clicks (for all buttons that could have triggered) is 0
        # This filters out initial calls or callbacks where no button was actually clicked
        # (e.g. if a button was dynamically removed and Dash still tried to process its callback)
        if not any(n_click for n_click in n_clicks_list if n_click is not None):
            raise PreventUpdate

        clicked_button_id_dict = ctx.triggered_id 
        
        run_id = clicked_button_id_dict.get('index') 
        dataset_name = clicked_button_id_dict.get('datasetname')
        model_source = clicked_button_id_dict.get('modelsource')

        if not dataset_name or not model_source or not run_id:
            return dbc.Alert("Error: Missing necessary data from the button (run_id, dataset name, or model source).", color="danger")

        active_project_id = proj_store.get("active_project_id")
        if not active_project_id:
            return dbc.Alert("Error: No active project selected.", color="danger")
        
        project_details = get_project_from_store(proj_store, active_project_id) 
        if not project_details:
            return dbc.Alert(f"Error: Could not retrieve details for project ID {active_project_id}.", color="danger")

        catalog = project_details.get('catalog')
        schema = project_details.get('schema')

        if not catalog or not schema:
            return dbc.Alert("Error: Project catalog or schema is not defined. Please set them in the Project tab.", color="danger")
        
        if dataset_name == "UnknownDataset" or dataset_name == "Dataset Not Found" or dataset_name == "DB Lookup Error" or dataset_name == "Invalid Job ID Tag" or dataset_name == "Job ID Tag Missing":
            return dbc.Alert(f"Error: Cannot register model. The dataset name associated with run ID '{run_id}' is '{dataset_name}'. Please ensure the run is correctly tagged with a job ID and the job ID is linked to a valid dataset.", color="danger")

        # Sanitize dataset_name by replacing spaces with underscores
        sanitized_dataset_name = dataset_name.replace(" ", "_")

        model_name_str = f"{catalog}.{schema}.{sanitized_dataset_name}"
        
        print(f"Attempting to register model: {model_name_str} from source: {model_source} for run: {run_id}")
        
        response_data, error_msg = register_model_version(model_name=model_name_str, model_source=model_source)

        if error_msg:
            return dbc.Alert(f"Error registering model '{model_name_str}': {error_msg}", color="danger")
        
        # Check if response_data itself is the model version information
        if response_data and response_data.get("version"): 
            version = response_data.get("version")
            status_message = response_data.get("status_message", "Status not available.")
            # mlflow_web_url = proj_store.get("mlflow_web_url") # Assuming you might store this
            # model_link = f"{mlflow_web_url}/#/models/{model_name_str}/versions/{version}" # Example link
            
            return dbc.Alert(f"Successfully registered model '{model_name_str}' as version {version}. Source: {model_source}. Status: {status_message}", color="success")
        else:
            # This case should ideally not be hit if error_msg is None and registration was successful
            return dbc.Alert(f"Model registration for '{model_name_str}' returned an unexpected response format. API Response: {response_data}", color="warning")
    
    # --- Callback to update notebook dropdown in Project Tab --- #
    @app.callback(
        Output("project-notebook-dropdown", "options"),
        Input("project-git-url", "value"),
        prevent_initial_call=True
    )
    def update_notebook_dropdown_options(git_url):
        if not git_url:
            return [] # Return empty options, value will be handled by populate_form or remain as is
        
        notebook_options = fetch_notebook_files_from_github(git_url, folder_path="notebooks")
        
        if not notebook_options:
            return [{"label": "No files found in 'notebooks' folder or error", "value": "", "disabled": True}]

        return notebook_options
    # --- End Notebook Dropdown Callback --- #