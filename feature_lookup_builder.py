import pandas as pd
from dash import html, dcc, Input, Output, State, no_update
import dash_bootstrap_components as dbc
from feature_lookup import FeatureLookup

# Global variables to store current selections and features
current_catalog = None
current_schema = None
current_table = None
current_eol_catalog = None
current_eol_schema = None
current_eol_table = None
selected_features = []

def is_timestamp_column(data_type):
    """Check if a column is a timestamp/date type."""
    if not data_type:
        return False
    
    data_type_lower = data_type.lower()
    timestamp_types = ['timestamp', 'date', 'datetime', 'time']
    return any(timestamp_type in data_type_lower for timestamp_type in timestamp_types)

def filter_columns_by_type(columns, exclude_timestamps=False, only_timestamps=False):
    """Filter columns based on whether they are timestamp columns."""
    if not columns:
        return []
    
    filtered_columns = []
    for col in columns:
        is_timestamp = is_timestamp_column(col.get('data_type', ''))
        if exclude_timestamps and not is_timestamp:
            filtered_columns.append(col)
        elif only_timestamps and is_timestamp:
            filtered_columns.append(col)
        elif not exclude_timestamps and not only_timestamps:
            filtered_columns.append(col)
    
    return filtered_columns

def get_catalogs(sqlQuery):
    """Get list of available catalogs."""
    try:
        query = "SHOW CATALOGS"
        catalogs_df = sqlQuery(query)
        if not catalogs_df.empty:
            return [row.iloc[0] for _, row in catalogs_df.iterrows()]
        return []
    except Exception as e:
        print(f"Error fetching catalogs: {str(e)}")
        return []

def get_schemas(catalog_name, sqlQuery):
    """Get list of schemas for a given catalog."""
    try:
        query = f"SHOW SCHEMAS IN {catalog_name}"
        schemas_df = sqlQuery(query)
        if not schemas_df.empty:
            return [row.iloc[0] for _, row in schemas_df.iterrows()]
        return []
    except Exception as e:
        print(f"Error fetching schemas: {str(e)}")
        return []

def get_tables(catalog_name, schema_name, sqlQuery):
    """Get list of tables for a given catalog and schema."""
    try:
        query = f"SHOW TABLES IN {catalog_name}.{schema_name}"
        tables_df = sqlQuery(query)
        if not tables_df.empty:
            # The table name is typically in the second column (tableName)
            # Let's check the structure and get the correct column
            if len(tables_df.columns) >= 2:
                return [row.iloc[1] for _, row in tables_df.iterrows()]  # Use second column for table name
            else:
                return [row.iloc[0] for _, row in tables_df.iterrows()]  # Fallback to first column
        return []
    except Exception as e:
        print(f"Error fetching tables: {str(e)}")
        return []

def get_table_columns(table_name, sqlQuery):
    """Get column information for a given table."""
    try:
        # Query to get column information
        query = f"DESCRIBE {table_name}"
        print(f"DEBUG: Executing query: {query}")
        columns_df = sqlQuery(query)
        print(f"DEBUG: Query result shape: {columns_df.shape}")
        print(f"DEBUG: Query result columns: {columns_df.columns.tolist()}")
        
        # Filter to only show column information (not partition info)
        if not columns_df.empty:
            # Look for the line that separates column info from partition info
            # Usually it's a line with just "col_name" or "data_type" or similar
            col_info = []
            for _, row in columns_df.iterrows():
                # Convert row to string and check if it looks like column info
                row_str = str(row.iloc[0]) if len(row) > 0 else ""
                if row_str and not row_str.startswith('#') and 'col_name' not in row_str.lower():
                    # This should be actual column data
                    if len(row) >= 2:
                        col_info.append({
                            'column_name': str(row.iloc[0]),
                            'data_type': str(row.iloc[1]) if len(row) > 1 else 'Unknown'
                        })
            
            print(f"DEBUG: Extracted {len(col_info)} columns")
            return col_info
        return []
    except Exception as e:
        print(f"Error fetching table columns: {str(e)}")
        return []

def create_feature_lookup_builder_layout():
    """Create the feature lookup builder layout."""
    return dbc.Row([
        dbc.Col([
            html.H4("Build Feature Lookup Configuration"),
            
            # EOL Table Section
            html.H5("EOL Table Configuration", className='mt-4'),
            dbc.Row([
                dbc.Col([
                    dbc.Label("EOL Catalog"),
                    dcc.Dropdown(
                        id='eol-catalog-dropdown',
                        options=[],
                        placeholder="Select EOL catalog...",
                        value=None
                    )
                ], width=4),
                dbc.Col([
                    dbc.Label("EOL Schema"),
                    dcc.Dropdown(
                        id='eol-schema-dropdown',
                        options=[],
                        placeholder="Select EOL schema...",
                        value=None,
                        disabled=True
                    )
                ], width=4),
                dbc.Col([
                    dbc.Label("EOL Table"),
                    dcc.Dropdown(
                        id='eol-table-dropdown',
                        options=[],
                        placeholder="Select EOL table...",
                        value=None,
                        disabled=True
                    )
                ], width=4)
            ], className='mt-3'),
            
            # Features Section
            html.H5("Feature Tables", className='mt-4'),
            dbc.Row([
                dbc.Col([
                    dbc.Label("Catalog"),
                    dcc.Dropdown(
                        id='catalog-dropdown',
                        options=[],
                        placeholder="Select catalog...",
                        value=None
                    )
                ], width=3),
                dbc.Col([
                    dbc.Label("Schema"),
                    dcc.Dropdown(
                        id='schema-dropdown',
                        options=[],
                        placeholder="Select schema...",
                        value=None,
                        disabled=True
                    )
                ], width=3),
                dbc.Col([
                    dbc.Label("Table"),
                    dcc.Dropdown(
                        id='table-dropdown',
                        options=[],
                        placeholder="Select table...",
                        value=None,
                        disabled=True
                    )
                ], width=3),
                dbc.Col([
                    dbc.Label("Columns"),
                    dcc.Dropdown(
                        id='columns-dropdown',
                        options=[],
                        placeholder="Select columns...",
                        value=[],
                        multi=True,
                        disabled=True
                    )
                ], width=3)
            ], className='mt-3'),
            dbc.Row([
                dbc.Col([
                    dbc.Label("Lookup Key"),
                    dcc.Dropdown(
                        id='lookup-key-dropdown',
                        options=[],
                        placeholder="Select lookup key...",
                        value=None,
                        disabled=True
                    )
                ], width=6),
                dbc.Col([
                    dbc.Label("Timestamp Key (Optional)"),
                    dcc.Dropdown(
                        id='timestamp-key-dropdown',
                        options=[],
                        placeholder="Select timestamp key...",
                        value=None,
                        disabled=True
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
        [Output('catalog-dropdown', 'options'),
         Output('catalog-dropdown', 'value'),
         Output('eol-catalog-dropdown', 'options'),
         Output('eol-catalog-dropdown', 'value')],
        Input('dummy-trigger', 'children')
    )
    def load_catalogs_initial(dummy):
        """Load catalogs on initial page load."""
        catalogs = get_catalogs(sqlQuery)
        catalog_options = [{'label': cat, 'value': cat} for cat in catalogs]
        first_catalog = catalogs[0] if catalogs else None
        print(f"DEBUG: load_catalogs_initial - catalogs={catalogs}, first_catalog={first_catalog}")
        return catalog_options, first_catalog, catalog_options, first_catalog

    @app.callback(
        [Output('schema-dropdown', 'options'),
         Output('schema-dropdown', 'value'),
         Output('schema-dropdown', 'disabled'),
         Output('table-dropdown', 'options'),
         Output('table-dropdown', 'value'),
         Output('table-dropdown', 'disabled'),
         Output('columns-dropdown', 'options'),
         Output('columns-dropdown', 'value'),
         Output('columns-dropdown', 'disabled')],
        Input('catalog-dropdown', 'value')
    )
    def update_schema_dropdown_and_reset_downstream(catalog_value):
        """Update the schema dropdown and reset all downstream dropdowns when catalog changes."""
        global current_catalog
        current_catalog = catalog_value
        print(f"DEBUG: update_schema_dropdown_and_reset_downstream - catalog_value={catalog_value}, current_catalog={current_catalog}")
        
        if catalog_value:
            schemas = get_schemas(catalog_value, sqlQuery)
            schema_options = [{'label': schema, 'value': schema} for schema in schemas]
            first_schema = schemas[0] if schemas else None
            
            return schema_options, first_schema, False, [], None, True, [], [], True
        return [], None, True, [], None, True, [], [], True

    @app.callback(
        [Output('eol-schema-dropdown', 'options'),
         Output('eol-schema-dropdown', 'value'),
         Output('eol-schema-dropdown', 'disabled'),
         Output('eol-table-dropdown', 'options'),
         Output('eol-table-dropdown', 'value'),
         Output('eol-table-dropdown', 'disabled')],
        Input('eol-catalog-dropdown', 'value')
    )
    def update_eol_schema_dropdown_and_reset_downstream(eol_catalog_value):
        """Update the EOL schema dropdown and reset EOL table when EOL catalog changes."""
        global current_eol_catalog
        current_eol_catalog = eol_catalog_value
        
        if eol_catalog_value:
            schemas = get_schemas(eol_catalog_value, sqlQuery)
            schema_options = [{'label': schema, 'value': schema} for schema in schemas]
            first_schema = schemas[0] if schemas else None
            
            # Get tables for the first schema to populate EOL table dropdown
            eol_table_options = []
            if first_schema:
                tables = get_tables(eol_catalog_value, first_schema, sqlQuery)
                eol_table_options = [{'label': table, 'value': table} for table in tables]
            
            return schema_options, first_schema, False, eol_table_options, None, False
        return [], None, True, [], None, True

    @app.callback(
        [Output('eol-table-dropdown', 'options', allow_duplicate=True),
         Output('eol-table-dropdown', 'value', allow_duplicate=True),
         Output('eol-table-dropdown', 'disabled', allow_duplicate=True)],
        Input('eol-schema-dropdown', 'value'),
        prevent_initial_call=True
    )
    def update_eol_table_dropdown(eol_schema_value):
        """Update the EOL table dropdown when EOL schema changes."""
        global current_eol_schema
        current_eol_schema = eol_schema_value
        
        if eol_schema_value and current_eol_catalog:
            tables = get_tables(current_eol_catalog, eol_schema_value, sqlQuery)
            return [{'label': table, 'value': table} for table in tables], tables[0] if tables else None, False
        return [], None, True

    @app.callback(
        [Output('table-dropdown', 'options', allow_duplicate=True),
         Output('table-dropdown', 'value', allow_duplicate=True),
         Output('table-dropdown', 'disabled', allow_duplicate=True),
         Output('columns-dropdown', 'options', allow_duplicate=True),
         Output('columns-dropdown', 'value', allow_duplicate=True),
         Output('columns-dropdown', 'disabled', allow_duplicate=True)],
        Input('schema-dropdown', 'value'),
        prevent_initial_call=True
    )
    def update_table_dropdown_and_reset_columns(schema_value):
        """Update the table dropdown and reset columns when schema changes."""
        global current_schema
        current_schema = schema_value
        print(f"DEBUG: update_table_dropdown_and_reset_columns - schema_value={schema_value}, current_schema={current_schema}, current_catalog={current_catalog}")
        
        if schema_value and current_catalog:
            tables = get_tables(current_catalog, schema_value, sqlQuery)
            return [{'label': table, 'value': table} for table in tables], tables[0] if tables else None, False, [], [], True
        return [], None, True, [], [], True

    @app.callback(
        [Output('columns-dropdown', 'options', allow_duplicate=True),
         Output('columns-dropdown', 'value', allow_duplicate=True),
         Output('columns-dropdown', 'disabled', allow_duplicate=True)],
        Input('table-dropdown', 'value'),
        prevent_initial_call=True
    )
    def update_columns_dropdown(table_value):
        """Update the columns dropdown when table changes."""
        global current_table
        current_table = table_value
        
        print(f"DEBUG: update_columns_dropdown called - table_value={table_value}, current_catalog={current_catalog}, current_schema={current_schema}")
        
        if table_value and current_catalog and current_schema:
            full_table_name = f"{current_catalog}.{current_schema}.{table_value}"
            print(f"DEBUG: Full table name: {full_table_name}")
            columns = get_table_columns(full_table_name, sqlQuery)
            print(f"DEBUG: Retrieved {len(columns)} columns")
            column_options = [{'label': col['column_name'], 'value': col['column_name']} for col in columns]
            print(f"DEBUG: Column options: {column_options}")
            return column_options, [], False
        return [], [], True

    @app.callback(
        [Output('lookup-key-dropdown', 'options'),
         Output('lookup-key-dropdown', 'value'),
         Output('lookup-key-dropdown', 'disabled'),
         Output('timestamp-key-dropdown', 'options'),
         Output('timestamp-key-dropdown', 'value'),
         Output('timestamp-key-dropdown', 'disabled')],
        Input('eol-table-dropdown', 'value')
    )
    def update_eol_table_dependent_dropdowns(eol_table_value):
        """Update lookup key and timestamp key dropdowns when EOL table changes."""
        global current_eol_table
        current_eol_table = eol_table_value
        
        if eol_table_value and current_eol_catalog and current_eol_schema:
            full_eol_table_name = f"{current_eol_catalog}.{current_eol_schema}.{eol_table_value}"
            columns = get_table_columns(full_eol_table_name, sqlQuery)
            
            # Filter columns for lookup key (exclude timestamps)
            lookup_key_columns = filter_columns_by_type(columns, exclude_timestamps=True)
            lookup_key_options = [{'label': col['column_name'], 'value': col['column_name']} for col in lookup_key_columns]
            
            # Filter columns for timestamp key (only timestamps)
            timestamp_columns = filter_columns_by_type(columns, only_timestamps=True)
            timestamp_options = [{'label': col['column_name'], 'value': col['column_name']} for col in timestamp_columns]
            
            return lookup_key_options, None, False, timestamp_options, None, False
        return [], None, True, [], None, True

    @app.callback(
        Output('add-features-button', 'disabled'),
        [Input('columns-dropdown', 'value'),
         Input('eol-table-dropdown', 'value'),
         Input('lookup-key-dropdown', 'value')]
    )
    def update_add_features_button(columns_value, eol_table_value, lookup_key_value):
        """Enable/disable the 'Add to Features' button based on selected columns, EOL table, and lookup key."""
        return not columns_value or len(columns_value) == 0 or not eol_table_value or not lookup_key_value

    @app.callback(
        [Output('columns-dropdown', 'value', allow_duplicate=True),
         Output('lookup-key-dropdown', 'value', allow_duplicate=True),
         Output('timestamp-key-dropdown', 'value', allow_duplicate=True)],
        Input('add-features-button', 'n_clicks'),
        State('columns-dropdown', 'value'),
        State('lookup-key-dropdown', 'value'),
        State('timestamp-key-dropdown', 'value'),
        prevent_initial_call=True
    )
    def clear_form_after_add(n_clicks, columns_value, lookup_key, timestamp_key):
        """Clear the form after adding features."""
        if n_clicks and columns_value:
            return [], None, None  # Clear columns, lookup key, and timestamp key
        return no_update, no_update, no_update

    @app.callback(
        Output('selected-features-list', 'children'),
        Input('add-features-button', 'n_clicks'),
        State('columns-dropdown', 'value'),
        State('lookup-key-dropdown', 'value'),
        State('timestamp-key-dropdown', 'value')
    )
    def add_features_to_list(n_clicks, columns_value, lookup_key, timestamp_key):
        """Add selected features to the features list."""
        global selected_features, current_catalog, current_schema, current_table, current_eol_catalog, current_eol_schema, current_eol_table
        
        print(f"DEBUG: add_features_to_list called - n_clicks={n_clicks}, columns_value={columns_value}, current_table={current_table}")
        print(f"DEBUG: selected_features before: {len(selected_features)}")
        
        if not n_clicks or not columns_value or not current_table or not current_eol_table:
            # Return existing features if no new ones to add
            if selected_features:
                # Create table header
                table_header = html.Thead(html.Tr([
                    html.Th("Table Name"),
                    html.Th("Features"),
                    html.Th("EOL Table"),
                    html.Th("Lookup Key"),
                    html.Th("Timestamp Key"),
                    html.Th("Actions")
                ]))
                
                # Create table body
                table_rows = []
                for i, feature in enumerate(selected_features):
                    table_rows.append(
                        html.Tr([
                            html.Td(feature['table_name']),
                            html.Td(', '.join(feature['features'])),
                            html.Td(feature['eol_table']),
                            html.Td(feature['lookup_key']),
                            html.Td(feature.get('timestamp_key', '')),
                            html.Td(dbc.Button("Remove", id={'type': 'remove-feature', 'index': i}, color='danger', size='sm'))
                        ])
                    )
                
                table_body = html.Tbody(table_rows)
                feature_table = dbc.Table([table_header, table_body], striped=True, bordered=True, hover=True)
                
                print(f"DEBUG: Returning table with {len(selected_features)} features")
                return [feature_table]
            else:
                return [html.P("No features added yet.")]
        
        # Add new features if button was clicked
        if n_clicks and columns_value and current_table and current_eol_table:
            full_table_name = f"{current_catalog}.{current_schema}.{current_table}"
            full_eol_table_name = f"{current_eol_catalog}.{current_eol_schema}.{current_eol_table}"
            new_feature = {
                'table_name': full_table_name,
                'features': columns_value,
                'eol_table': full_eol_table_name,
                'lookup_key': lookup_key or '',
                'timestamp_key': timestamp_key or None
            }
            selected_features.append(new_feature)
            print(f"DEBUG: Added feature: {new_feature}")
            print(f"DEBUG: selected_features after: {len(selected_features)}")
        
        # Update display
        if selected_features:
            # Create table header
            table_header = html.Thead(html.Tr([
                html.Th("Table Name"),
                html.Th("Features"),
                html.Th("EOL Table"),
                html.Th("Lookup Key"),
                html.Th("Timestamp Key"),
                html.Th("Actions")
            ]))
            
            # Create table body
            table_rows = []
            for i, feature in enumerate(selected_features):
                table_rows.append(
                    html.Tr([
                        html.Td(feature['table_name']),
                        html.Td(', '.join(feature['features'])),
                        html.Td(feature['eol_table']),
                        html.Td(feature['lookup_key']),
                        html.Td(feature.get('timestamp_key', '')),
                        html.Td(dbc.Button("Remove", id={'type': 'remove-feature', 'index': i}, color='danger', size='sm'))
                    ])
                )
            
            table_body = html.Tbody(table_rows)
            feature_table = dbc.Table([table_header, table_body], striped=True, bordered=True, hover=True)
            
            print(f"DEBUG: Returning table with {len(selected_features)} features")
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
            python_code += f"      table_name='{feature['table_name']}',\n"
            python_code += f"      feature_names={feature['features']},\n"
            python_code += f"      lookup_key='{feature['lookup_key']}',\n"
            python_code += f"      eol_table='{feature['eol_table']}'"
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