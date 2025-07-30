import pandas as pd
from dash import html, dcc, Input, Output, State, no_update, ALL, callback_context
import dash_bootstrap_components as dbc
from feature_lookup import FeatureLookup
from utils.db import get_eol_definitions, create_eol_definition, delete_eol_definition, get_eol_definition_by_name, update_eol_definition
import dash

# Global variables to store current selections and features
current_project_id = None
selected_features = []

def create_feature_lookup_builder_layout():
    """Create the feature lookup builder layout."""
    return dbc.Row([
        dbc.Col([
            html.H4("Build Feature Lookup Configuration"),
            
            # EOL Definitions Section - List on left, form on right
            html.H5("EOL Definitions", className='mt-4'),
            dbc.Row([
                # Left side - EOL definitions list
                dbc.Col([
                    html.Div(id='eol-definitions-list', className='mt-2')
                ], width=8),
                
                # Right side - EOL definition form
        dbc.Col([
            # Store selected EOL old name for edits
            dcc.Store(id='eol-form-store', data={'old_name': None}),
            html.H5("Create EOL Definition", className='mt-4'),
            dbc.Form([
                        html.Div([
                            dbc.Label("Name", html_for="eol-name-input"),
                            dbc.Input(
                                id='eol-name-input',
                                type="text",
                                placeholder="Enter EOL definition name...",
                                value='new'
                            ),
                        ], className="mb-3"),
                        html.Div([
                            dbc.Label("SQL Definition", html_for="eol-sql-definition-input"),
                            dbc.Textarea(
                                id='eol-sql-definition-input',
                                placeholder="Enter SQL definition for the EOL table...",
                                rows=8
                            ),
                        ], className="mb-3"),
                        html.Div([
                            # New EOL definition (reset form)
                            dbc.Button("New EOL Definition", id='new-eol-button', color='secondary', className='me-2'),
                            # Save (create/update) EOL definition
                            dbc.Button("Save EOL Definition", id='save-eol-button', color='success', className='me-2'),
                            # Delete selected EOL definition
                            dbc.Button("Delete EOL Definition", id='delete-eol-button', color='danger'),
                        ], className="mt-3"),
                        html.Div(id='eol-form-alert', className='mt-3')
                    ])
                ], width=4)
            ]),
            
            # Features Section
            html.H5("Feature Tables", className='mt-4'),
            dbc.Row([
                dbc.Col([
                    dbc.Label("EOL Definition"),
                    dcc.Dropdown(
                        id='eol-definition-dropdown',
                        options=[],
                        placeholder="Select EOL definition...",
                        value=None
                    )
                ], width=6),
                dbc.Col([
                    dbc.Label("Feature Columns"),
                    dcc.Dropdown(
                        id='feature-columns-dropdown',
                        options=[],
                        placeholder="Select feature columns...",
                        value=[],
                        multi=True
                    )
                ], width=6)
            ], className='mt-3'),
            dbc.Row([
                dbc.Col([
                    dbc.Label("Lookup Key"),
                    dcc.Dropdown(
                        id='lookup-key-dropdown',
                        options=[],
                        placeholder="Select lookup key...",
                        value=None
                    )
                ], width=6),
                dbc.Col([
                    dbc.Label("Timestamp Key (Optional)"),
                    dcc.Dropdown(
                        id='timestamp-key-dropdown',
                        options=[],
                        placeholder="Select timestamp key...",
                        value=None
                    )
                ], width=6)
            ], className='mt-3'),
            dbc.Row([
                dbc.Col([
                    dbc.Button("Add to Features", id='add-features-button', color='primary', className='mt-3', disabled=True)
                ], width=12)
            ]),
            dbc.Row([
                dbc.Col([
                    html.H5("Selected Features"),
                    html.Div(id='selected-features-list', className='mt-2')
                ], width=12)
            ]),
            dbc.Row([
                dbc.Col([
                    dbc.Button("Generate Python Code", id='generate-code-button', color='success', className='mt-3')
                ], width=12)
            ]),
            html.Div(id='python-code-output', className='mt-3')
        ], width=12)
    ])

def register_feature_lookup_callbacks(app, sqlQuery):
    """Register all the callbacks for the feature lookup builder."""
    
    @app.callback(
        [Output('eol-definitions-list', 'children'),
         Output('eol-definition-dropdown', 'options'),
         Output('eol-form-store', 'data', allow_duplicate=True)],
        [Input('list-store', 'data'),
         Input('save-eol-button', 'n_clicks'),
         Input('delete-eol-button', 'n_clicks')],
        [State('eol-name-input', 'value'),
         State('eol-sql-definition-input', 'value'),
         State('eol-form-store', 'data')],
        prevent_initial_call='initial_duplicate'
    )
    def update_eol_definitions(store_data, save_clicks, delete_clicks,
                               name, sql_def, form_store):
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
            return [], [], new_form_store
        # Handle save or delete triggers
        ctx = callback_context
        if ctx.triggered:
            trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
            # Save: create new or update existing
            if trigger_id == 'save-eol-button' and name and sql_def:
                old_name = new_form_store.get('old_name')
                if old_name:
                    # Update existing definition
                    update_eol_definition(old_name, name, sql_def, current_project_id)
                else:
                    # Create new definition
                    create_eol_definition(name, sql_def, current_project_id)
                # Reset form store after save
                new_form_store = {'old_name': None}
            # Delete
            elif trigger_id == 'delete-eol-button' and name:
                delete_eol_definition(name, current_project_id)
                # Reset form store after delete
                new_form_store = {'old_name': None}
        # Fetch updated EOL definitions
        print(f"DEBUG: Fetching EOL definitions for project_id = {current_project_id}")
        eol_df = get_eol_definitions(current_project_id)
        print(f"DEBUG: eol_df shape = {eol_df.shape}")
        # If none found
        if eol_df.empty:
            return [html.P("No EOL definitions found for this project.")], [], new_form_store
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
        return [list_group], dropdown_options, new_form_store



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

    @app.callback(
        [Output('feature-columns-dropdown', 'options'),
         Output('feature-columns-dropdown', 'value'),
         Output('feature-columns-dropdown', 'disabled')],
        Input('eol-definition-dropdown', 'value'),
        State('list-store', 'data')
    )
    def update_feature_columns_dropdown(eol_name, store_data):
        """Update feature columns dropdown based on selected EOL definition."""
        if not eol_name:
            return [], [], True
        
        # Get current project ID from store
        if isinstance(store_data, dict):
            current_project_id = store_data.get('active_project_id')
        else:
            current_project_id = store_data[0]['id'] if store_data else None
        
        if not current_project_id:
            return [], [], True
        
        eol_def = get_eol_definition_by_name(eol_name, current_project_id)
        if not eol_def:
            return [], [], True
        
        # For now, we'll provide a generic list of common feature columns
        # In a real implementation, you might want to parse the SQL definition
        # or provide a way to specify columns manually
        common_columns = [
            'feature_1', 'feature_2', 'feature_3', 'feature_4', 'feature_5',
            'numeric_feature', 'categorical_feature', 'boolean_feature'
        ]
        
        column_options = [{'label': col, 'value': col} for col in common_columns]
        return column_options, [], False

    @app.callback(
        [Output('lookup-key-dropdown', 'options'),
         Output('lookup-key-dropdown', 'value'),
         Output('lookup-key-dropdown', 'disabled'),
         Output('timestamp-key-dropdown', 'options'),
         Output('timestamp-key-dropdown', 'value'),
         Output('timestamp-key-dropdown', 'disabled')],
        Input('eol-definition-dropdown', 'value'),
        State('list-store', 'data')
    )
    def update_lookup_keys_dropdown(eol_name, store_data):
        """Update lookup key and timestamp key dropdowns based on selected EOL definition."""
        if not eol_name:
            return [], None, True, [], None, True
        
        # Get current project ID from store
        if isinstance(store_data, dict):
            current_project_id = store_data.get('active_project_id')
        else:
            current_project_id = store_data[0]['id'] if store_data else None
        
        if not current_project_id:
            return [], None, True, [], None, True
        
        eol_def = get_eol_definition_by_name(eol_name, current_project_id)
        if not eol_def:
            return [], None, True, [], None, True
        
        # Common lookup keys
        lookup_keys = ['id', 'user_id', 'customer_id', 'order_id', 'transaction_id']
        lookup_options = [{'label': key, 'value': key} for key in lookup_keys]
        
        # Timestamp keys
        timestamp_keys = ['created_at', 'updated_at', 'timestamp', 'date', 'datetime']
        timestamp_options = [{'label': key, 'value': key} for key in timestamp_keys]
        
        return lookup_options, None, False, timestamp_options, None, False

    @app.callback(
        Output('add-features-button', 'disabled'),
        [Input('feature-columns-dropdown', 'value'),
         Input('eol-definition-dropdown', 'value'),
         Input('lookup-key-dropdown', 'value')]
    )
    def update_add_features_button(columns_value, eol_name, lookup_key_value):
        """Enable/disable the 'Add to Features' button based on selections."""
        return not columns_value or len(columns_value) == 0 or not eol_name or not lookup_key_value

    @app.callback(
        [Output('feature-columns-dropdown', 'value', allow_duplicate=True),
         Output('lookup-key-dropdown', 'value', allow_duplicate=True),
         Output('timestamp-key-dropdown', 'value', allow_duplicate=True)],
        Input('add-features-button', 'n_clicks'),
        State('feature-columns-dropdown', 'value'),
        State('lookup-key-dropdown', 'value'),
        State('timestamp-key-dropdown', 'value'),
        prevent_initial_call=True
    )
    def clear_form_after_add(n_clicks, columns_value, lookup_key, timestamp_key):
        """Clear the form after adding features."""
        if n_clicks and columns_value:
            return [], None, None
        return no_update, no_update, no_update

    @app.callback(
        Output('selected-features-list', 'children'),
        Input('add-features-button', 'n_clicks'),
        State('feature-columns-dropdown', 'value'),
        State('lookup-key-dropdown', 'value'),
        State('timestamp-key-dropdown', 'value'),
        State('eol-definition-dropdown', 'value'),
        State('list-store', 'data')
    )
    def add_features_to_list(n_clicks, columns_value, lookup_key, timestamp_key, eol_name, store_data):
        """Add selected features to the features list."""
        global selected_features
        
        if not n_clicks or not columns_value or not eol_name:
            # Return existing features if no new ones to add
            if selected_features:
                # Create table header
                table_header = html.Thead(html.Tr([
                    html.Th("EOL Definition"),
                    html.Th("Features"),
                    html.Th("Lookup Key"),
                    html.Th("Timestamp Key"),
                    html.Th("Actions")
                ]))
                
                # Create table body
                table_rows = []
                for i, feature in enumerate(selected_features):
                    table_rows.append(
                        html.Tr([
                            html.Td(feature['eol_name']),
                            html.Td(', '.join(feature['features'])),
                            html.Td(feature['lookup_key']),
                            html.Td(feature.get('timestamp_key', '')),
                            html.Td(dbc.Button("Remove", id={'type': 'remove-feature', 'index': i}, 
                                             color='danger', size='sm'))
                        ])
                    )
                
                table_body = html.Tbody(table_rows)
                feature_table = dbc.Table([table_header, table_body], striped=True, bordered=True, hover=True)
                
                return [feature_table]
            else:
                return [html.P("No features added yet.")]
        
        # Get current project ID from store
        if isinstance(store_data, dict):
            current_project_id = store_data.get('active_project_id')
        else:
            current_project_id = store_data[0]['id'] if store_data else None
        
        if not current_project_id:
            return [html.P("No project selected.")]
        
        # Add new features if button was clicked
        if n_clicks and columns_value and eol_name:
            eol_def = get_eol_definition_by_name(eol_name, current_project_id)
            if eol_def:
                new_feature = {
                    'eol_name': eol_name,
                    'features': columns_value,
                    'lookup_key': lookup_key or '',
                    'timestamp_key': timestamp_key or None
                }
                selected_features.append(new_feature)
        
        # Update display
        if selected_features:
            # Create table header
            table_header = html.Thead(html.Tr([
                html.Th("EOL Definition"),
                html.Th("Features"),
                html.Th("Lookup Key"),
                html.Th("Timestamp Key"),
                html.Th("Actions")
            ]))
            
            # Create table body
            table_rows = []
            for i, feature in enumerate(selected_features):
                table_rows.append(
                    html.Tr([
                        html.Td(feature['eol_name']),
                        html.Td(', '.join(feature['features'])),
                        html.Td(feature['lookup_key']),
                        html.Td(feature.get('timestamp_key', '')),
                        html.Td(dbc.Button("Remove", id={'type': 'remove-feature', 'index': i}, 
                                         color='danger', size='sm'))
                    ])
                )
            
            table_body = html.Tbody(table_rows)
            feature_table = dbc.Table([table_header, table_body], striped=True, bordered=True, hover=True)
            
            return [feature_table]
        else:
            return [html.P("No features added yet.")]

    @app.callback(
        Output('python-code-output', 'children'),
        Input('generate-code-button', 'n_clicks')
    )
    def generate_python_code(n_clicks):
        """Generate and display the Python code for the current feature lookups."""
        global selected_features
        
        if not n_clicks or not selected_features:
            return ""
        
        python_code = "feature_lookups = [\n"
        for feature in selected_features:
            python_code += f"    FeatureLookup(\n"
            python_code += f"      table_name='{feature['eol_name']}',\n"
            python_code += f"      feature_names={feature['features']},\n"
            python_code += f"      lookup_key='{feature['lookup_key']}'"
            if feature.get('timestamp_key'):
                python_code += f",\n      timestamp_key='{feature['timestamp_key']}'"
            python_code += f"\n    ),\n"
        python_code += "]"
        
        return html.Div([
            html.H5("Generated Python Code:"),
            html.Pre(python_code, style={'background-color': '#f8f9fa', 'padding': '10px', 'border-radius': '5px'})
        ])

def get_selected_features():
    """Get the currently selected features."""
    global selected_features
    return selected_features

def clear_selected_features():
    """Clear all selected features."""
    global selected_features
    selected_features = [] 