import dash
import json
from dash import Input, Output, State, callback_context, ALL, html
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from utils.db import (
    get_feature_lookups,
    get_feature_lookup_by_id,
    create_feature_lookup,
    update_feature_lookup,
    delete_feature_lookup,
    get_eol_definitions,
    get_eol_definition_by_id,
    get_eol_view_columns,
    get_eol_view_timestamp_columns,
    get_catalogs,
    get_schemas,
    get_tables,
    get_columns
)

def register_feature_lookup_callbacks(app):
    """Register callbacks for the Feature Lookups tab."""
    # Populate EOL definitions dropdown based on selected project
    @app.callback(
        Output('feature-lookup-eol-dropdown', 'options'),
        Input('list-store', 'data')
    )
    def update_eol_dropdown(store_data):
        project_id = None
        if isinstance(store_data, dict):
            project_id = store_data.get('active_project_id')
        # Fetch EOL definitions for project
        if not project_id:
            return []
        df = get_eol_definitions(project_id)
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
    # Populate catalog dropdown for table selection
    @app.callback(
        Output('feature-lookup-catalog-dropdown', 'options'),
        Input('list-store', 'data')
    )
    def update_catalogs_dropdown(store_data):
        # Fetch all catalogs from metastore
        catalogs = get_catalogs()
        return [{'label': c, 'value': c} for c in catalogs]

    # Populate schema dropdown based on selected catalog
    @app.callback(
        Output('feature-lookup-schema-dropdown', 'options'),
        Input('feature-lookup-catalog-dropdown', 'value')
    )
    def update_schemas_dropdown(catalog):
        if not catalog:
            return []
        schemas = get_schemas(catalog)
        return [{'label': s, 'value': s} for s in schemas]

    # Populate table dropdown based on selected catalog and schema
    @app.callback(
        Output('feature-lookup-table-dropdown', 'options'),
        Input('feature-lookup-catalog-dropdown', 'value'),
        Input('feature-lookup-schema-dropdown', 'value')
    )
    def update_tables_dropdown(catalog, schema):
        if not catalog or not schema:
            return []
        tables = get_tables(catalog, schema)
        return [{'label': t, 'value': t} for t in tables]

    # Populate column dropdown based on selected catalog, schema, and table
    @app.callback(
        Output('feature-lookup-column-dropdown', 'options'),
        Input('feature-lookup-catalog-dropdown', 'value'),
        Input('feature-lookup-schema-dropdown', 'value'),
        Input('feature-lookup-table-dropdown', 'value')
    )
    def update_columns_dropdown(catalog, schema, table):
        if not catalog or not schema or not table:
            return []
        cols = get_columns(catalog, schema, table)
        print(f"cols: {cols}")
        return [{'label': c, 'value': c} for c in cols]

    # Populate lookup key dropdown based on selected EOL definition
    @app.callback(
        Output('feature-lookup-lookup-key-dropdown', 'options'),
        Input('feature-lookup-eol-dropdown', 'value')
    )
    def update_lookup_key_dropdown(eol_id):
        if not eol_id:
            return []
        try:
            eol_id_int = int(eol_id)
            cols = get_eol_view_columns(eol_id_int)
            return [{'label': c, 'value': c} for c in cols]
        except Exception as e:
            print(f"Error populating lookup key dropdown: {e}")
            return []

    # Populate timestamp key dropdown based on selected EOL definition
    @app.callback(
        Output('feature-lookup-timestamp-key-dropdown', 'options'),
        Input('feature-lookup-eol-dropdown', 'value')
    )
    def update_timestamp_key_dropdown(eol_id):
        if not eol_id:
            return []
        try:
            eol_id_int = int(eol_id)
            cols = get_eol_view_timestamp_columns(eol_id_int)
            return [{'label': c, 'value': c} for c in cols]
        except Exception as e:
            print(f"Error populating timestamp key dropdown: {e}")
            return []

    # Store for pending table operations (add/delete requests)
    @app.callback(
        Output('feature-lookup-table-store', 'data'),
        Input('feature-lookup-add-table-button', 'n_clicks'),
        Input({'type': 'feature-lookup-delete-table-button', 'index': ALL}, 'n_clicks'),
        Input('feature-lookup-store', 'data'),  # Also listen to store changes to load initial tables
        State('feature-lookup-catalog-dropdown', 'value'),
        State('feature-lookup-schema-dropdown', 'value'),
        State('feature-lookup-table-dropdown', 'value'),
        State('feature-lookup-column-dropdown', 'value'),
        State('feature-lookup-lookup-key-dropdown', 'value'),
        State('feature-lookup-timestamp-key-dropdown', 'value'),
        State('feature-lookup-table-store', 'data'),
        prevent_initial_call=True
    )
    def manage_table_store(add_clicks, delete_clicks, feature_store_data, catalog, schema, selected_table, selected_columns, lookup_key, timestamp_key, current_items):
        """Central manager for all table store operations."""
        ctx = callback_context
        trig = ctx.triggered[0]['prop_id'] if ctx.triggered else ''
        
        print(f"manage_table_store - trigger: {trig}")
        print(f"manage_table_store - current items: {current_items}")
        
        # Handle feature lookup selection (load tables from DB)
        if trig == 'feature-lookup-store.data':
            active_id = feature_store_data.get('active_id') if isinstance(feature_store_data, dict) else None
            items = feature_store_data.get('items', []) if isinstance(feature_store_data, dict) else []
            
            # Track the previously loaded ID to avoid unnecessary reloads
            if not hasattr(manage_table_store, 'previous_active_id'):
                manage_table_store.previous_active_id = None
            
            if active_id == manage_table_store.previous_active_id:
                print(f"manage_table_store - Same active_id, keeping current tables")
                return current_items if current_items is not None else []
            
            manage_table_store.previous_active_id = active_id
            
            if active_id is None:
                print(f"manage_table_store - No active_id, returning empty")
                return []
            
            # Load tables from database
            tables = []
            for rec in items:
                if rec.get('id') == active_id:
                    raw_feats = rec.get('features') or []
                    print(f"manage_table_store - Loading features from DB: {raw_feats}")
                    
                    for feat in raw_feats:
                        if isinstance(feat, dict):
                            # Ensure backwards compatibility - add missing keys
                            table_entry = feat.copy()
                            if 'lookup_key' not in table_entry:
                                table_entry['lookup_key'] = None
                            if 'timestamp_key' not in table_entry:
                                table_entry['timestamp_key'] = None
                            tables.append(table_entry)
                        elif isinstance(feat, str):
                            # Parse string representations
                            try:
                                import json
                                parsed = json.loads(feat)
                                if isinstance(parsed, dict) and 'table' in parsed:
                                    # Ensure backwards compatibility
                                    if 'lookup_key' not in parsed:
                                        parsed['lookup_key'] = None
                                    if 'timestamp_key' not in parsed:
                                        parsed['timestamp_key'] = None
                                    tables.append(parsed)
                                    continue
                            except Exception:
                                pass
                            
                            # Try regex for non-standard format
                            import re
                            table_match = re.search(r'{table:\s*([^,}]+),\s*features:\s*\[([^\]]*)\]}', feat)
                            if table_match:
                                table_name = table_match.group(1).strip()
                                features_str = table_match.group(2).strip()
                                if features_str:
                                    features = [f.strip() for f in features_str.split(',')]
                                else:
                                    features = []
                                tables.append({
                                    'table': table_name, 
                                    'features': features,
                                    'lookup_key': None,
                                    'timestamp_key': None
                                })
                                continue
                            
                            # Fallback
                            tables.append({
                                'table': str(feat), 
                                'features': [],
                                'lookup_key': None,
                                'timestamp_key': None
                            })
                    break
            
            print(f"manage_table_store - Loaded {len(tables)} tables from DB")
            return tables
        
        # Handle delete table button
        if 'feature-lookup-delete-table-button' in trig:
            items = current_items if current_items is not None else []
            id_str = trig.split('.')[0]
            try:
                idx = json.loads(id_str).get('index')
            except Exception:
                return items
            if idx is None or idx >= len(items):
                return items
            result = [it for i, it in enumerate(items) if i != idx]
            print(f"manage_table_store - After delete: {result}")
            return result

        # Handle add table button
        if trig == 'feature-lookup-add-table-button.n_clicks':
            items = current_items if current_items is not None else []
            if not (catalog and schema and selected_table):
                print(f"manage_table_store - Missing dropdown values for add")
                return items
            
            if not lookup_key:
                print(f"manage_table_store - Missing required lookup key")
                return items
            
            fq = f"{catalog}.{schema}.{selected_table}"
            features = selected_columns or []
            new_item = {
                'table': fq, 
                'features': features,
                'lookup_key': lookup_key,
                'timestamp_key': timestamp_key  # Optional, can be None
            }
            
            # Check if table already exists
            exists = False
            for item in items:
                if isinstance(item, dict) and item.get('table') == fq:
                    exists = True
                    break
            
            if not exists:
                items.append(new_item)
            print(f"manage_table_store - After add: {items}")
            return items

        # Default: return current items
        return current_items if current_items is not None else []
    
    # Callback to render selected tables list
    @app.callback(
        Output('feature-lookup-table-list', 'children'),
        Input('feature-lookup-table-store', 'data')
    )
    def render_table_list(tables):
        print(f"render_table_list - Rendering {len(tables) if tables else 0} tables: {tables}")
        
        if not tables:
            return html.P("No tables selected.", className="text-muted")
        children = []
        for idx, entry in enumerate(tables):
            # entry may be a dict with table and features, or a raw string
            if isinstance(entry, dict):
                tbl = entry.get('table') or ''
                feats = entry.get('features') or []
                lookup_key = entry.get('lookup_key')
                timestamp_key = entry.get('timestamp_key')
                
                # Build display string with all information
                display_parts = [f"Table: {tbl}"]
                if feats:
                    display_parts.append(f"Features: {', '.join(feats)}")
                if lookup_key:
                    display_parts.append(f"Lookup Key: {lookup_key}")
                if timestamp_key:
                    display_parts.append(f"Timestamp Key: {timestamp_key}")
                
                display = " | ".join(display_parts)
            else:
                display = str(entry)
            children.append(
                dbc.Row([
                    dbc.Col(html.Span(display), width=10),
                    dbc.Col(
                        dbc.Button(
                            "Delete",
                            id={'type': 'feature-lookup-delete-table-button', 'index': idx},
                            color="danger",
                            size="sm"
                        ),
                        width=2
                    )
                ], className="mb-1")
            )
        return children

    @app.callback(
        Output('feature-lookup-list', 'children'),
        Input('feature-lookup-store', 'data'),
        prevent_initial_call=True
    )
    def refresh_feature_lookup_list(store_data):
        items = store_data.get('items', []) if isinstance(store_data, dict) else []
        active_id = store_data.get('active_id') if isinstance(store_data, dict) else None
        list_items = []
        for itm in items:
            list_items.append(
                dbc.ListGroupItem(
                    itm.get('name'),
                    id={'type': 'feature-lookup-item', 'index': itm.get('id')},
                    action=True,
                    active=(itm.get('id') == active_id)
                )
            )
        if not list_items:
            list_items = [
                dbc.ListGroupItem(
                    "No feature lookups found.",
                    id={'type': 'feature-lookup-item', 'index': -1},
                    disabled=True
                )
            ]
        return list_items

    @app.callback(
        Output('feature-lookup-store', 'data', allow_duplicate=True),
        Input({'type': 'feature-lookup-item', 'index': ALL}, 'n_clicks'),
        State('feature-lookup-store', 'data'),
        prevent_initial_call=True
    )
    def select_feature_lookup(n_clicks, store_data):
        ctx = callback_context
        if not ctx.triggered_id or not isinstance(ctx.triggered_id, dict) or ctx.triggered_id.get('type') != 'feature-lookup-item':
            raise PreventUpdate
        fl_id = ctx.triggered_id['index']
        if fl_id == -1:
            raise PreventUpdate
        items = store_data.get('items', []) if isinstance(store_data, dict) else []
        
        print(f"select_feature_lookup - Selected feature lookup id: {fl_id}")
        
        return {'items': items, 'active_id': fl_id}

    @app.callback(
        Output('feature-lookup-store', 'data', allow_duplicate=True),
        Output('feature-lookup-name', 'value', allow_duplicate=True),
        Output('feature-lookup-eol-dropdown', 'value', allow_duplicate=True),
        Output('feature-lookup-table-store', 'data', allow_duplicate=True),
        Output('feature-lookup-table-dropdown', 'value', allow_duplicate=True),
        Input('create-feature-lookup-button', 'n_clicks'),
        State('feature-lookup-name', 'value'),
        State('feature-lookup-eol-dropdown', 'value'),
        State('feature-lookup-table-store', 'data'),
        State('list-store', 'data'),
        prevent_initial_call=True
    )
    def create_fl_callback(n_clicks, name, eol_id, tables, project_store):
        # Determine current project
        project_id = project_store.get('active_project_id') if isinstance(project_store, dict) else None
        if project_id is None:
            # Nothing to do: no update to store or form
            return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
        # Set default name if none provided
        lookup_name = name if name else 'new feature lookup'
        
        # Prepare feature list from selected tables
        feats = []
        if tables:
            for tbl_entry in tables:
                if isinstance(tbl_entry, dict):
                    table_name = tbl_entry.get('table', '')
                    cols = tbl_entry.get('features', [])
                    lookup_key = tbl_entry.get('lookup_key')
                    timestamp_key = tbl_entry.get('timestamp_key')
                    # Store as properly formatted JSON string with all fields
                    import json
                    entry_dict = {
                        'table': table_name, 
                        'features': cols,
                        'lookup_key': lookup_key,
                        'timestamp_key': timestamp_key
                    }
                    feats.append(json.dumps(entry_dict))
                else:
                    # Store as simple string (backward compatibility)
                    feats.append(str(tbl_entry))
        
        # Create feature lookup in DB
        if not create_feature_lookup(project_id, eol_id, lookup_name, feats):
            return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
        # Refresh the list of feature lookups
        df = get_feature_lookups(project_id)
        records = df.to_dict('records') if not df.empty else []
        items = [
            {'id': int(rec['id']), 'name': rec.get('name'), 'eol_id': rec.get('eol_id'), 'features': rec.get('features')}
            for rec in records
        ]
        
        # After creation, clear form inputs (no selection)
        return {'items': items, 'active_id': None}, '', None, [], None

    @app.callback(
        Output('feature-lookup-store', 'data', allow_duplicate=True),
        Input('update-feature-lookup-button', 'n_clicks'),
        State('feature-lookup-store', 'data'),
        State('feature-lookup-name', 'value'),
        State('feature-lookup-eol-dropdown', 'value'),
        State('feature-lookup-table-store', 'data'),
        State('list-store', 'data'),
        prevent_initial_call=True
    )
    def update_fl_callback(n_clicks, store_data, name, eol_id, tables, project_store):
        fl_id = store_data.get('active_id') if isinstance(store_data, dict) else None
        if fl_id is None or not name:
            return dash.no_update
        
        # Prepare feature list from selected tables
        feats = []
        if tables:
            for tbl_entry in tables:
                if isinstance(tbl_entry, dict):
                    table_name = tbl_entry.get('table', '')
                    cols = tbl_entry.get('features', [])
                    lookup_key = tbl_entry.get('lookup_key')
                    timestamp_key = tbl_entry.get('timestamp_key')
                    # Store as properly formatted JSON string with all fields
                    import json
                    entry_dict = {
                        'table': table_name, 
                        'features': cols,
                        'lookup_key': lookup_key,
                        'timestamp_key': timestamp_key
                    }
                    feats.append(json.dumps(entry_dict))
                else:
                    # Store as simple string (backward compatibility)
                    feats.append(str(tbl_entry))
        
        if not update_feature_lookup(fl_id, name, eol_id, feats):
            return dash.no_update
        project_id = project_store.get('active_project_id') if isinstance(project_store, dict) else None
        df = get_feature_lookups(project_id)
        records = df.to_dict('records') if not df.empty else []
        items = [
            {'id': int(rec['id']), 'name': rec.get('name'), 'eol_id': rec.get('eol_id'), 'features': rec.get('features')}
            for rec in records
        ]
        return {'items': items, 'active_id': fl_id}

    @app.callback(
        Output('feature-lookup-store', 'data', allow_duplicate=True),
        Input('delete-feature-lookup-button', 'n_clicks'),
        State('feature-lookup-store', 'data'),
        State('list-store', 'data'),
        prevent_initial_call=True
    )
    def delete_fl_callback(n_clicks, store_data, project_store):
        fl_id = store_data.get('active_id') if isinstance(store_data, dict) else None
        if fl_id is None:
            return dash.no_update
        if not delete_feature_lookup(fl_id):
            return dash.no_update
        project_id = project_store.get('active_project_id') if isinstance(project_store, dict) else None
        df = get_feature_lookups(project_id)
        records = df.to_dict('records') if not df.empty else []
        items = [
            {'id': int(rec['id']), 'name': rec.get('name'), 'eol_id': rec.get('eol_id'), 'features': rec.get('features')}
            for rec in records
        ]
        active_id = items[0]['id'] if items else None
        return {'items': items, 'active_id': active_id}
    
    @app.callback(
        Output('feature-lookup-name', 'value', allow_duplicate=True),
        Output('feature-lookup-eol-dropdown', 'value', allow_duplicate=True),
        Input('feature-lookup-store', 'data'),
        prevent_initial_call=True
    )
    def populate_feature_lookup_form(store_data):
        """Populate form inputs when the feature lookup store updates."""
        active_id = store_data.get('active_id') if isinstance(store_data, dict) else None
        items = store_data.get('items', []) if isinstance(store_data, dict) else []
        
        if active_id is None:
            return dash.no_update, dash.no_update
            
        for rec in items:
            if rec.get('id') == active_id:
                name = rec.get('name') or ''
                eol_id = rec.get('eol_id') if rec.get('eol_id') is not None else ''
                return name, eol_id
        
        return dash.no_update, dash.no_update