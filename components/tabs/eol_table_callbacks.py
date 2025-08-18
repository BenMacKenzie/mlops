import dash_bootstrap_components as dbc
from dash import html
import pandas as pd
from dash import html, dcc, Input, Output, State, no_update, ALL, callback_context
import dash_bootstrap_components as dbc
from utils.db import get_eol_definitions, create_eol_definition, delete_eol_definition, get_eol_definition_by_name, update_eol_definition, get_project_by_id, sqlQuery
import yaml, json
import yaml


def register_eol_callbacks(app):
    
    @app.callback(
        [Output('eol-name-input', 'value', allow_duplicate=True),
         Output('eol-sql-definition-input', 'value', allow_duplicate=True),
         Output('eol-label-input', 'value', allow_duplicate=True)],
        [Input('eol-form-store', 'data')],
        [State('list-store', 'data')],
        prevent_initial_call=True
    )
    def auto_populate_first_eol(form_store, store_data):
        """Automatically populate form with first EOL definition when tab loads."""
        if not form_store or not form_store.get('old_name'):
            return no_update, no_update, no_update
            
        # Get current project ID from store
        if isinstance(store_data, dict):
            current_project_id = store_data.get('active_project_id')
        else:
            current_project_id = store_data[0]['id'] if store_data else None
            
        if not current_project_id:
            return no_update, no_update, no_update
            
        eol_name = form_store.get('old_name')
        if eol_name:
            eol_def = get_eol_definition_by_name(eol_name, current_project_id)
            if eol_def is not None:
                name_val = eol_def.get('name', '') if hasattr(eol_def, 'get') else eol_def['name']
                sql_val = eol_def.get('sql_definition', '') if hasattr(eol_def, 'get') else eol_def['sql_definition']
                label_val = eol_def.get('label', '') if hasattr(eol_def, 'get') else eol_def.get('label', '')
                return name_val, sql_val, label_val
                
        return no_update, no_update, no_update

    @app.callback(
        [Output('eol-definitions-list', 'children'),
         Output('eol-form-store', 'data', allow_duplicate=True),
         Output('eol-form-alert', 'children', allow_duplicate=True)],
        [Input('list-store', 'data'),
            Input('save-eol-button', 'n_clicks'),
            Input('delete-eol-button', 'n_clicks'),
            Input('tabs', 'active_tab')],
        [State('eol-name-input', 'value'),
            State('eol-sql-definition-input', 'value'),
            State('eol-label-input', 'value'),
            State('eol-form-store', 'data')],
        prevent_initial_call='initial_duplicate'
    )
    def update_eol_definitions(store_data, save_clicks, delete_clicks, active_tab,
                                name, sql_def, label, form_store):
        """Update the EOL definitions list, dropdown, and form store on save/delete."""
        # Only refresh when EOL Definitions tab is active
        if active_tab != 'tab-eol':
            return no_update, no_update, no_update
        """Update the EOL definitions list, dropdown, and form store on save/delete."""
        global current_project_id
        # Determine current project
        if isinstance(store_data, dict):
            current_project_id = store_data.get('active_project_id')
        else:
            current_project_id = store_data[0]['id'] if store_data else None
        print(f"DEBUG: current_project_id = {current_project_id}")
        # Default: retain existing store state
        new_form_store = form_store or {'old_name': None}
        # If no project, nothing to do
        if not current_project_id:
            return [], new_form_store, None
        
        # Initialize variables for view creation feedback
        view_creation_success = None
        view_error_msg = None
        operation_performed = None
        
        # Handle save or delete triggers
        ctx = callback_context
        if ctx.triggered:
            trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
            # Save: create new or update existing
            if trigger_id == 'save-eol-button' and name and sql_def:
                operation_performed = 'save'
                old_name = new_form_store.get('old_name')
                # Persist to DB
                if old_name:
                    update_eol_definition(old_name, name, sql_def, current_project_id, label)
                else:
                    create_eol_definition(name, sql_def, current_project_id, label)
                # Also create or replace the view in the project schema
                view_creation_success = True
                view_error_msg = None
                try:
                    proj = get_project_by_id(current_project_id)
                    if proj is not None:
                        catalog = proj.get('catalog')
                        schema = proj.get('schema')
                        view_name = name
                        # Build and execute DDL for view
                        ddl = f"CREATE OR REPLACE VIEW {catalog}.{schema}.{view_name} AS {sql_def}"
                        print(f"DEBUG: Creating view with DDL: {ddl}")
                        sqlQuery(ddl)
                        print(f"DEBUG: Successfully created view {catalog}.{schema}.{view_name}")
                    else:
                        view_creation_success = False
                        view_error_msg = "Could not retrieve project details for view creation"
                except Exception as e:
                    view_creation_success = False
                    view_error_msg = str(e)
                    print(f"Error creating view for EOL '{name}': {e}")
                # Reset form store after save
                new_form_store = {'old_name': None}
            # Delete
            elif trigger_id == 'delete-eol-button' and name:
                operation_performed = 'delete'
                delete_eol_definition(name, current_project_id)
                new_form_store = {'old_name': None}
        # Fetch updated EOL definitions
        print(f"DEBUG: Fetching EOL definitions for project_id = {current_project_id}")
        eol_df = get_eol_definitions(current_project_id)
        print(f"DEBUG: eol_df shape = {eol_df.shape}")
        # If none found
        if eol_df.empty:
            return [html.P("No EOL definitions found for this project.")], new_form_store, None
        # Build list items and dropdown options
        eol_items = []
        # Get the currently selected EOL name from form store
        selected_eol = new_form_store.get('old_name', None)
        
        # Only auto-select first item if:
        # 1. No current selection AND
        # 2. This is triggered by a save/delete operation (which clears selection) OR initial load
        ctx = callback_context
        should_auto_select = (
            selected_eol is None and 
            not eol_df.empty and 
            (not ctx.triggered or  # Initial load
             any('save-eol-button' in str(t['prop_id']) or 'delete-eol-button' in str(t['prop_id']) 
                 for t in ctx.triggered))  # After save/delete operations
        )
        
        if should_auto_select:
            selected_eol = eol_df.iloc[0]['name']
            new_form_store = {'old_name': selected_eol}
        
        for idx, (_, row) in enumerate(eol_df.iterrows()):
            # Highlight the selected item or first item by default
            is_active = (selected_eol == row['name'])
            eol_items.append(
                dbc.ListGroupItem(
                    row['name'], id={"type": "eol-list-item", "index": row['name']},
                    action=True, active=is_active
                )
            )
        dropdown_options = [{'label': row['name'], 'value': row['name']} for _, row in eol_df.iterrows()]
        list_group = dbc.ListGroup(eol_items, id="eol-list-group")
        
        # Generate alert based on any operations performed
        alert = None
        if operation_performed == 'save':
            if view_creation_success:
                alert = dbc.Alert("EOL definition and view created successfully!", color="success", dismissable=True)
            elif view_creation_success is False:
                alert = dbc.Alert(f"EOL definition saved, but view creation failed: {view_error_msg}", color="warning", dismissable=True)
            else:
                alert = dbc.Alert("EOL definition saved successfully!", color="success", dismissable=True)
        elif operation_performed == 'delete':
            alert = dbc.Alert("EOL definition deleted successfully!", color="success", dismissable=True)
        
        return [list_group], new_form_store, alert

    # Clear alert when EOL selection changes
    @app.callback(
        Output('eol-form-alert', 'children', allow_duplicate=True),
        Input('eol-form-store', 'data'),
        prevent_initial_call=True
    )
    def clear_eol_alert(form_store):
        return None


    @app.callback(
        [Output('eol-name-input', 'value', allow_duplicate=True),
            Output('eol-sql-definition-input', 'value', allow_duplicate=True),
            Output('eol-label-input', 'value', allow_duplicate=True),
            Output('eol-form-store', 'data', allow_duplicate=True)],
        [Input({'type': 'eol-list-item', 'index': ALL}, 'n_clicks')],
        [State('list-store', 'data'),
            State('eol-form-store', 'data')],
        prevent_initial_call=True
    )
    def populate_eol_form(eol_clicks, store_data, form_store):
        """Populate the EOL form when an EOL definition is selected from the list."""
        ctx = callback_context
        print(f"DEBUG: populate_eol_form triggered")
        print(f"DEBUG: ctx.triggered = {ctx.triggered}")
        
        if not ctx.triggered:
            print("DEBUG: No trigger, returning no_update")
            return no_update, no_update, no_update, no_update
        
        # Get current project ID from store
        if isinstance(store_data, dict):
            current_project_id = store_data.get('active_project_id')
        else:
            current_project_id = store_data[0]['id'] if store_data else None
        
        print(f"DEBUG: current_project_id = {current_project_id}")
        
        if not current_project_id:
            print("DEBUG: No current_project_id, returning no_update")
            return no_update, no_update, no_update, no_update
        
        # Identify which EOL list item was clicked
        trigger = ctx.triggered[0]['prop_id']
        clean_id = trigger.split('.', 1)[0]
        try:
            import json
            trigger_obj = json.loads(clean_id)
        except Exception as e:
            print(f"DEBUG: Could not parse trigger id '{clean_id}' as JSON: {e}")
            return no_update, no_update, no_update, form_store
        # Only handle clicks on eol-list-item entries
        if trigger_obj.get('type') == 'eol-list-item':
            eol_name = trigger_obj.get('index')
            print(f"DEBUG: Selected EOL definition = {eol_name}")
            eol_def = get_eol_definition_by_name(eol_name, current_project_id)
            if eol_def is not None:
                # Populate form fields and update store with old_name
                name_val = eol_def.get('name', '') if hasattr(eol_def, 'get') else eol_def['name']
                sql_val = eol_def.get('sql_definition', '') if hasattr(eol_def, 'get') else eol_def['sql_definition']
                label_val = eol_def.get('label', '') if hasattr(eol_def, 'get') else eol_def.get('label', '')
                print(f"DEBUG: Returning name='{name_val}', sql_definition={sql_val[:50]}..., label='{label_val}'")
                return name_val, sql_val, label_val, {'old_name': eol_name}
        # Fallback: do not update
        print("DEBUG: No valid EOL item selected or definition not found, no_update")
        return no_update, no_update, no_update, form_store

    @app.callback(
        [Output('eol-name-input', 'value', allow_duplicate=True),
            Output('eol-sql-definition-input', 'value', allow_duplicate=True),
            Output('eol-label-input', 'value', allow_duplicate=True)],
        Input('save-eol-button', 'n_clicks'),
        prevent_initial_call=True
    )
    def clear_eol_form_after_save(n_clicks):
        """Reset the EOL form to initial state after saving."""
        if n_clicks:
            # Set name back to 'new' and clear SQL definition and label
            return 'new', '', ''
        return no_update, no_update, no_update

    @app.callback(
        [Output('eol-name-input', 'value', allow_duplicate=True),
            Output('eol-sql-definition-input', 'value', allow_duplicate=True),
            Output('eol-label-input', 'value', allow_duplicate=True),
            Output('eol-form-store', 'data', allow_duplicate=True)],
        Input('new-eol-button', 'n_clicks'),
        prevent_initial_call=True
    )
    def new_eol_definition(n_clicks):
        """Reset form for creating a new EOL definition."""
        if n_clicks:
            # Reset inputs and clear old_name
            return 'new', '', '', {'old_name': None}
        return no_update, no_update, no_update, no_update