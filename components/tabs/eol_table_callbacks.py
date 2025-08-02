import dash_bootstrap_components as dbc
from dash import html
import pandas as pd
from dash import html, dcc, Input, Output, State, no_update, ALL, callback_context
import dash_bootstrap_components as dbc
from utils.db import get_eol_definitions, create_eol_definition, delete_eol_definition, get_eol_definition_by_name, update_eol_definition, get_project_by_id
import yaml, json
import yaml


def register_eol_callbacks(app):

    @app.callback(
        [Output('eol-definitions-list', 'children'),
         Output('eol-form-store', 'data', allow_duplicate=True)],
        [Input('list-store', 'data'),
            Input('save-eol-button', 'n_clicks'),
            Input('delete-eol-button', 'n_clicks'),
            Input('tabs', 'active_tab')],
        [State('eol-name-input', 'value'),
            State('eol-sql-definition-input', 'value'),
            State('eol-form-store', 'data')],
        prevent_initial_call='initial_duplicate'
    )
    def update_eol_definitions(store_data, save_clicks, delete_clicks, active_tab,
                                name, sql_def, form_store):
        """Update the EOL definitions list, dropdown, and form store on save/delete."""
        # Only refresh when EOL Definitions tab is active
        if active_tab != 'tab-eol':
            return no_update, no_update
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
            return [], new_form_store
        # Handle save or delete triggers
        ctx = callback_context
        if ctx.triggered:
            trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
            # Save: create new or update existing
            if trigger_id == 'save-eol-button' and name and sql_def:
                old_name = new_form_store.get('old_name')
                # Persist to DB
                if old_name:
                    update_eol_definition(old_name, name, sql_def, current_project_id)
                else:
                    create_eol_definition(name, sql_def, current_project_id)
                # Also create or replace the view in the project schema
                try:
                    proj = get_project_by_id(current_project_id)
                    if proj is not None:
                        catalog = proj.get('catalog')
                        schema = proj.get('schema')
                        view_name = name
                        # Build and execute DDL for view
                        ddl = f"CREATE OR REPLACE VIEW {catalog}.{schema}.{view_name} AS {sql_def}"
                        sqlQuery(ddl)
                except Exception as e:
                    print(f"Error creating view for EOL '{name}': {e}")
                # Reset form store after save
                new_form_store = {'old_name': None}
            # Delete
            elif trigger_id == 'delete-eol-button' and name:
                delete_eol_definition(name, current_project_id)
                new_form_store = {'old_name': None}
        # Fetch updated EOL definitions
        print(f"DEBUG: Fetching EOL definitions for project_id = {current_project_id}")
        eol_df = get_eol_definitions(current_project_id)
        print(f"DEBUG: eol_df shape = {eol_df.shape}")
        # If none found
        if eol_df.empty:
            return [html.P("No EOL definitions found for this project.")], new_form_store
        # Build list items and dropdown options
        eol_items = []
        for _, row in eol_df.iterrows():
            eol_items.append(
                dbc.ListGroupItem(
                    row['name'], id={"type": "eol-list-item", "index": row['name']},
                    action=True, active=False
                )
            )
        dropdown_options = [{'label': row['name'], 'value': row['name']} for _, row in eol_df.iterrows()]
        list_group = dbc.ListGroup(eol_items, id="eol-list-group")
        return [list_group], new_form_store



    @app.callback(
        [Output('eol-name-input', 'value', allow_duplicate=True),
            Output('eol-sql-definition-input', 'value', allow_duplicate=True),
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
            return no_update, no_update
        
        # Get current project ID from store
        if isinstance(store_data, dict):
            current_project_id = store_data.get('active_project_id')
        else:
            current_project_id = store_data[0]['id'] if store_data else None
        
        print(f"DEBUG: current_project_id = {current_project_id}")
        
        if not current_project_id:
            print("DEBUG: No current_project_id, returning no_update")
            return no_update, no_update
        
        # Identify which EOL list item was clicked
        trigger = ctx.triggered[0]['prop_id']
        clean_id = trigger.split('.', 1)[0]
        try:
            import json
            trigger_obj = json.loads(clean_id)
        except Exception as e:
            print(f"DEBUG: Could not parse trigger id '{clean_id}' as JSON: {e}")
            return no_update, no_update, form_store
        # Only handle clicks on eol-list-item entries
        if trigger_obj.get('type') == 'eol-list-item':
            eol_name = trigger_obj.get('index')
            print(f"DEBUG: Selected EOL definition = {eol_name}")
            eol_def = get_eol_definition_by_name(eol_name, current_project_id)
            if eol_def is not None:
                # Populate form fields and update store with old_name
                name_val = eol_def.get('name', '') if hasattr(eol_def, 'get') else eol_def['name']
                sql_val = eol_def.get('sql_definition', '') if hasattr(eol_def, 'get') else eol_def['sql_definition']
                print(f"DEBUG: Returning name='{name_val}', sql_definition={sql_val[:50]}...")
                return name_val, sql_val, {'old_name': eol_name}
        # Fallback: do not update
        print("DEBUG: No valid EOL item selected or definition not found, no_update")
        return no_update, no_update, form_store

    @app.callback(
        [Output('eol-name-input', 'value', allow_duplicate=True),
            Output('eol-sql-definition-input', 'value', allow_duplicate=True)],
        Input('save-eol-button', 'n_clicks'),
        prevent_initial_call=True
    )
    def clear_eol_form_after_save(n_clicks):
        """Reset the EOL form to initial state after saving."""
        if n_clicks:
            # Set name back to 'new' and clear SQL definition
            return 'new', ''
        return no_update, no_update

    @app.callback(
        [Output('eol-name-input', 'value', allow_duplicate=True),
            Output('eol-sql-definition-input', 'value', allow_duplicate=True),
            Output('eol-form-store', 'data', allow_duplicate=True)],
        Input('new-eol-button', 'n_clicks'),
        prevent_initial_call=True
    )
    def new_eol_definition(n_clicks):
        """Reset form for creating a new EOL definition."""
        if n_clicks:
            # Reset inputs and clear old_name
            return 'new', '', {'old_name': None}
        return no_update, no_update, no_update