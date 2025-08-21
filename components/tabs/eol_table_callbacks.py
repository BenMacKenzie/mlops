import dash_bootstrap_components as dbc
from dash import html, Input, Output, State, no_update, ALL, callback_context
import dash_bootstrap_components as dbc
from utils.db_universal import (
    get_eol_definitions, 
    create_eol_definition, 
    delete_eol_definition, 
    get_eol_definition_by_name, 
    update_eol_definition, 
    get_project_by_id, 
    sqlQuery
)
import yaml
from datetime import datetime


def register_eol_callbacks(app):
    """Clean, simple EOL callbacks without conflicting logic."""
    
    # Single store to manage EOL state
    @app.callback(
        [Output('eol-definitions-list', 'children'),
         Output('eol-form-store', 'data'),
         Output('eol-form-alert', 'children')],
        [Input('list-store', 'data'),  # Project changes
         Input('create-eol-button', 'n_clicks'),
         Input('update-eol-button', 'n_clicks'),
         Input('delete-eol-button', 'n_clicks'),
         Input({'type': 'eol-list-item', 'index': ALL}, 'n_clicks')],
        [State('eol-name-input', 'value'),
         State('eol-sql-definition-input', 'value'),
         State('eol-label-input', 'value'),
         State('eol-form-store', 'data')],
        prevent_initial_call=True
    )
    def manage_eol_state(project_store, create_clicks, update_clicks, delete_clicks, 
                        item_clicks, name_input, sql_input, label_input, current_store):
        """Central callback to manage all EOL state changes."""
        
        ctx = callback_context
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else ''
        
        print(f"=== EOL MANAGE STATE ===")
        print(f"Trigger: {trigger_id}")
        
        # Get current project ID
        project_id = project_store.get('active_project_id') if isinstance(project_store, dict) else None
        if not project_id:
            return [], {'selected_eol': None}, None
            
        # Handle CREATE button
        if trigger_id == 'create-eol-button' and create_clicks:
            print("Handling CREATE")
            
            # Find unique name
            df = get_eol_definitions(project_id)
            existing_names = df['name'].tolist() if not df.empty else []
            
            counter = 1
            new_name = "new"
            while new_name in existing_names:
                counter += 1
                new_name = f"new{counter}"
            
            # Create in database
            success = create_eol_definition(
                name=new_name,
                sql_definition="SELECT 1 as placeholder",
                project_id=project_id,
                label=""
            )
            
            if success:
                # Refresh and select the new item
                df = get_eol_definitions(project_id)
                list_items = build_eol_list(df, new_name)
                store = {'selected_eol': new_name}
                alert = dbc.Alert("EOL definition created successfully!", color="success", dismissable=True)
                return list_items, store, alert
            else:
                return no_update, no_update, dbc.Alert("Failed to create EOL definition", color="danger", dismissable=True)
        
        # Handle UPDATE button
        elif trigger_id == 'update-eol-button' and update_clicks:
            print("Handling UPDATE")
            
            if not current_store or not current_store.get('selected_eol'):
                return no_update, no_update, dbc.Alert("No EOL definition selected", color="warning", dismissable=True)
            
            if not name_input or not sql_input:
                return no_update, no_update, dbc.Alert("Name and SQL definition are required", color="warning", dismissable=True)
            
            old_name = current_store['selected_eol']
            
            # Update in database
            success = update_eol_definition(old_name, name_input, sql_input, project_id, label_input)
            
            if success:
                # If name changed, update selection
                new_selected = name_input
                
                # Also create or replace the view in the project schema
                try:
                    proj = get_project_by_id(project_id)
                    if proj is not None and not (hasattr(proj, 'empty') and proj.empty):
                        # Handle both dict and Series formats
                        if hasattr(proj, 'get'):
                            catalog = proj.get('catalog')
                            schema = proj.get('schema')
                        else:
                            # Series format
                            catalog = proj['catalog'] if 'catalog' in proj else None
                            schema = proj['schema'] if 'schema' in proj else None
                        
                        if catalog and schema:
                            view_name = name_input
                            ddl = f"CREATE OR REPLACE VIEW {catalog}.{schema}.{view_name} AS {sql_input}"
                            from utils.db_metadata import sqlQuery as databricks_sqlQuery
                            databricks_sqlQuery(ddl)
                            alert_msg = "EOL definition and view updated successfully!"
                        else:
                            alert_msg = "EOL definition updated, but project missing catalog/schema info"
                    else:
                        alert_msg = "EOL definition updated, but project not found"
                except Exception as e:
                    print(f"Error creating view: {e}")
                    alert_msg = f"EOL definition updated, but view creation failed: {e}"
                
                # Refresh and maintain selection
                df = get_eol_definitions(project_id)
                list_items = build_eol_list(df, new_selected)
                store = {'selected_eol': new_selected}
                alert = dbc.Alert(alert_msg, color="success", dismissable=True)
                return list_items, store, alert
            else:
                return no_update, no_update, dbc.Alert("Failed to update EOL definition", color="danger", dismissable=True)
        
        # Handle DELETE button
        elif trigger_id == 'delete-eol-button' and delete_clicks:
            print("Handling DELETE")
            
            if not current_store or not current_store.get('selected_eol'):
                return no_update, no_update, dbc.Alert("No EOL definition selected", color="warning", dismissable=True)
            
            eol_name = current_store['selected_eol']
            
            # Delete from database
            success = delete_eol_definition(eol_name, project_id)
            
            if success:
                # Refresh and clear selection
                df = get_eol_definitions(project_id)
                list_items = build_eol_list(df, None)
                store = {'selected_eol': None}
                alert = dbc.Alert("EOL definition deleted successfully!", color="success", dismissable=True)
                return list_items, store, alert
            else:
                return no_update, no_update, dbc.Alert("Failed to delete EOL definition", color="danger", dismissable=True)
        
        # Handle list item selection
        elif 'eol-list-item' in trigger_id:
            print("Handling SELECTION")
            
            try:
                import json
                trigger_obj = json.loads(trigger_id)
                selected_name = trigger_obj.get('index')
                
                if selected_name:
                    df = get_eol_definitions(project_id)
                    list_items = build_eol_list(df, selected_name)
                    store = {'selected_eol': selected_name}
                    return list_items, store, None
            except Exception as e:
                print(f"Error handling selection: {e}")
        
        # Handle project change (refresh list)
        elif trigger_id == 'list-store':
            print("Handling PROJECT CHANGE")
            
            df = get_eol_definitions(project_id)
            if df.empty:
                return [html.P("No EOL definitions found for this project.")], {'selected_eol': None}, None
            
            list_items = build_eol_list(df, None)
            store = {'selected_eol': None}
            return list_items, store, None
        
        # Default: no update
        return no_update, no_update, no_update
    
    # Populate form based on selected EOL
    @app.callback(
        [Output('eol-name-input', 'value'),
         Output('eol-sql-definition-input', 'value'),
         Output('eol-label-input', 'value')],
        [Input('eol-form-store', 'data')],
        [State('list-store', 'data')],
        prevent_initial_call=True
    )
    def populate_eol_form(form_store, project_store):
        """Populate form when selection changes."""
        
        print(f"=== POPULATE FORM ===")
        print(f"form_store: {form_store}")
        
        if not form_store or not form_store.get('selected_eol'):
            print("No selection - clearing form")
            return '', '', ''
        
        # Get current project ID
        project_id = project_store.get('active_project_id') if isinstance(project_store, dict) else None
        if not project_id:
            return '', '', ''
        
        eol_name = form_store['selected_eol']
        eol_def = get_eol_definition_by_name(eol_name, project_id)
        
        if eol_def is not None and not (hasattr(eol_def, 'empty') and eol_def.empty):
            # Handle both dict and Series formats
            if hasattr(eol_def, 'get'):
                name_val = eol_def.get('name', '')
                sql_val = eol_def.get('sql_definition', '')
                label_val = eol_def.get('label', '')
            else:
                # Series format
                name_val = eol_def['name'] if 'name' in eol_def else ''
                sql_val = eol_def['sql_definition'] if 'sql_definition' in eol_def else ''
                label_val = eol_def['label'] if 'label' in eol_def else ''
            
            print(f"Populating form with: {name_val}, {sql_val[:50] if sql_val else ''}..., {label_val}")
            return name_val, sql_val, label_val
        else:
            print("EOL definition not found")
            return '', '', ''


def build_eol_list(eol_df, selected_name):
    """Helper function to build the EOL list items."""
    if eol_df.empty:
        return [html.P("No EOL definitions found for this project.")]
    
    eol_items = []
    for _, row in eol_df.iterrows():
        is_active = (selected_name == row['name'])
        eol_items.append(
            dbc.ListGroupItem(
                row['name'], 
                id={"type": "eol-list-item", "index": row['name']},
                action=True, 
                active=is_active
            )
        )
    
    return [dbc.ListGroup(eol_items, id="eol-list-group")]