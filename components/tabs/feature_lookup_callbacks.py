import dash
import json
from dash import Input, Output, State, callback_context, ALL, html
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from utils.db_universal import (
    get_feature_lookups,
    get_feature_lookup_by_id,
    create_feature_lookup,
    update_feature_lookup,
    delete_feature_lookup,
    get_eol_definitions,
    get_eol_definition_by_id,
    get_eol_view_columns,
    get_eol_view_timestamp_columns
)
from utils.db_metadata import (
    get_catalogs,
    get_schemas,
    get_tables,
    get_columns
)

def register_feature_lookup_callbacks(app):
    """Register callbacks for the Feature Lookups tab."""
    
    # Refresh feature lookup list when project changes
    @app.callback(
        Output('feature-lookup-store', 'data', allow_duplicate=True),
        Input('list-store', 'data'),
        State('feature-lookup-store', 'data'),
        prevent_initial_call=True
    )
    def refresh_feature_lookup_store_on_project_change(project_store, current_feature_lookup_store):
        """Refresh feature lookup store when project selection changes."""
        project_id = project_store.get('active_project_id') if isinstance(project_store, dict) else None
        
        # Fetch feature lookups for the current project
        df = get_feature_lookups(project_id)
        items = []
        if not df.empty:
            records = df.to_dict('records')
            items = [
                {'id': int(rec['id']), 'name': rec.get('name'), 'eol_id': rec.get('eol_id'), 'features': rec.get('features')}
                for rec in records
            ]
        
        # Preserve current selection if it exists and is still valid, otherwise auto-select first item
        # But don't auto-select if we're in "create mode"
        current_active_id = current_feature_lookup_store.get('active_id') if current_feature_lookup_store else None
        in_create_mode = current_feature_lookup_store.get('create_mode', False) if current_feature_lookup_store else False
        
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
            
        print(f"refresh_feature_lookup_store_on_project_change - project_id: {project_id}, found {len(items)} feature lookups, preserving active_id: {active_id}, create_mode: {create_mode}")
        
        return {'items': items, 'active_id': active_id, 'create_mode': create_mode}
    
    # Populate EOL definitions dropdown based on selected project
    @app.callback(
        Output('feature-lookup-eol-dropdown', 'options'),
        Input('list-store', 'data')
    )
    def update_eol_dropdown(store_data):
        print(f"DEBUG: feature_lookup update_eol_dropdown called with store_data: {store_data}")
        project_id = None
        if isinstance(store_data, dict):
            project_id = store_data.get('active_project_id')
        elif isinstance(store_data, list) and len(store_data) > 0:
            project_id = store_data[0].get('id') if store_data[0] else None
        
        print(f"DEBUG: feature_lookup update_eol_dropdown project_id: {project_id}")
        
        # Fetch EOL definitions for project
        if not project_id:
            print("DEBUG: feature_lookup - No project_id found, returning empty options")
            return []
        
        try:
            df = get_eol_definitions(project_id)
            print(f"DEBUG: feature_lookup - get_eol_definitions returned {len(df)} rows")
            
            if df.empty:
                print("DEBUG: feature_lookup - No EOL definitions found for project")
                return []
            
            print(f"DEBUG: feature_lookup - DataFrame columns: {df.columns.tolist()}")
            
            # Build dropdown options: label=name, value=id
            opts = []
            for idx, row in df.iterrows():
                try:
                    val = int(row['id'])
                    name = row.get('name')
                    print(f"DEBUG: feature_lookup - Adding EOL option: {name} (id={val})")
                    opts.append({'label': name, 'value': val})
                except Exception as e:
                    print(f"DEBUG: feature_lookup - Error processing row at index {idx}: {e}")
                    print(f"DEBUG: feature_lookup - Row data: {row.to_dict()}")
                    continue
            
            print(f"DEBUG: feature_lookup - Returning {len(opts)} EOL dropdown options: {opts}")
            return opts
        except Exception as e:
            print(f"ERROR: feature_lookup - Exception in update_eol_dropdown: {e}")
            import traceback
            traceback.print_exc()
            return []
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
        Output('feature-lookup-selected-table', 'data', allow_duplicate=True),
        Input('feature-lookup-add-table-button', 'n_clicks'),
        Input('feature-lookup-update-table-button', 'n_clicks'),
        Input({'type': 'feature-lookup-delete-table-button', 'index': ALL}, 'n_clicks'),
        Input('feature-lookup-store', 'data'),  # Also listen to store changes to load initial tables
        State('feature-lookup-catalog-dropdown', 'value'),
        State('feature-lookup-schema-dropdown', 'value'),
        State('feature-lookup-table-dropdown', 'value'),
        State('feature-lookup-column-dropdown', 'value'),
        State('feature-lookup-lookup-key-dropdown', 'value'),
        State('feature-lookup-timestamp-key-dropdown', 'value'),
        State('feature-lookup-table-store', 'data'),
        State('feature-lookup-selected-table', 'data'),
        prevent_initial_call=True
    )
    def manage_table_store(add_clicks, update_clicks, delete_clicks, feature_store_data, catalog, schema, selected_table, selected_columns, lookup_key, timestamp_key, current_items, selected_table_idx):
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
                return current_items if current_items is not None else [], selected_table_idx
            
            manage_table_store.previous_active_id = active_id
            
            if active_id is None:
                print(f"manage_table_store - No active_id, returning empty")
                return [], None
            
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
            return tables, None  # Clear selection when loading new data
        
        # Handle delete table button
        if 'feature-lookup-delete-table-button' in trig:
            items = current_items if current_items is not None else []
            id_str = trig.split('.')[0]
            try:
                idx = json.loads(id_str).get('index')
            except Exception:
                return items, selected_table_idx
            if idx is None or idx >= len(items):
                return items, selected_table_idx
            result = [it for i, it in enumerate(items) if i != idx]
            # Clear selection if we deleted the selected table
            new_selected = None if selected_table_idx == idx else (selected_table_idx - 1 if selected_table_idx is not None and selected_table_idx > idx else selected_table_idx)
            print(f"manage_table_store - After delete: {result}")
            return result, new_selected

        # Handle add table button
        if trig == 'feature-lookup-add-table-button.n_clicks':
            items = current_items if current_items is not None else []
            if not (catalog and schema and selected_table):
                print(f"manage_table_store - Missing dropdown values for add")
                return items, selected_table_idx
            
            if not lookup_key:
                print(f"manage_table_store - Missing required lookup key")
                return items, selected_table_idx
            
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
            return items, None  # Clear selection after adding

        # Handle update table button
        if trig == 'feature-lookup-update-table-button.n_clicks':
            items = current_items if current_items is not None else []
            if selected_table_idx is None or selected_table_idx >= len(items):
                print(f"manage_table_store - No table selected for update")
                return items, selected_table_idx
            
            if not (catalog and schema and selected_table):
                print(f"manage_table_store - Missing dropdown values for update")
                return items, selected_table_idx
            
            if not lookup_key:
                print(f"manage_table_store - Missing required lookup key for update")
                return items, selected_table_idx
            
            fq = f"{catalog}.{schema}.{selected_table}"
            features = selected_columns or []
            updated_item = {
                'table': fq, 
                'features': features,
                'lookup_key': lookup_key,
                'timestamp_key': timestamp_key  # Optional, can be None
            }
            
            # Update the selected table
            items[selected_table_idx] = updated_item
            print(f"manage_table_store - After update: {items}")
            return items, None  # Clear selection after updating

        # Default: return current items
        return current_items if current_items is not None else [], selected_table_idx
    
    # Callback to render selected tables list
    @app.callback(
        Output('feature-lookup-table-list', 'children'),
        Input('feature-lookup-table-store', 'data'),
        Input('feature-lookup-selected-table', 'data')
    )
    def render_table_list(tables, selected_table_index):
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
            
            # Determine if this table is selected
            is_selected = (selected_table_index == idx)
            
            children.append(
                dbc.Row([
                    dbc.Col(
                        dbc.Button(
                            display,
                            id={'type': 'feature-lookup-table-item', 'index': idx},
                            color="primary" if is_selected else "secondary",
                            outline=not is_selected,
                            className="text-start w-100 text-dark" if not is_selected else "text-start w-100",
                            size="sm"
                        ), 
                        width=10
                    ),
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

    # Handle table selection
    @app.callback(
        Output('feature-lookup-selected-table', 'data'),
        Output('feature-lookup-catalog-dropdown', 'value', allow_duplicate=True),
        Output('feature-lookup-schema-dropdown', 'value', allow_duplicate=True),
        Output('feature-lookup-table-dropdown', 'value', allow_duplicate=True),
        Output('feature-lookup-column-dropdown', 'value', allow_duplicate=True),
        Output('feature-lookup-lookup-key-dropdown', 'value', allow_duplicate=True),
        Output('feature-lookup-timestamp-key-dropdown', 'value', allow_duplicate=True),
        Input({'type': 'feature-lookup-table-item', 'index': ALL}, 'n_clicks'),
        State('feature-lookup-table-store', 'data'),
        State('feature-lookup-selected-table', 'data'),
        prevent_initial_call=True
    )
    def select_table_for_editing(n_clicks, tables, current_selected):
        ctx = callback_context
        if not ctx.triggered_id or not isinstance(ctx.triggered_id, dict) or ctx.triggered_id.get('type') != 'feature-lookup-table-item':
            raise PreventUpdate
        
        table_idx = ctx.triggered_id['index']
        if not tables or table_idx >= len(tables):
            raise PreventUpdate
        
        # If clicking the same table, deselect it
        if current_selected == table_idx:
            return None, None, None, None, None, None, None
        
        # Get the selected table data
        selected_table = tables[table_idx]
        if not isinstance(selected_table, dict):
            return table_idx, None, None, None, None, None, None
        
        # Parse the table name to get catalog, schema, table
        table_name = selected_table.get('table', '')
        features = selected_table.get('features', [])
        lookup_key = selected_table.get('lookup_key')
        timestamp_key = selected_table.get('timestamp_key')
        
        # Parse catalog.schema.table format
        catalog, schema, table = None, None, None
        if '.' in table_name:
            parts = table_name.split('.')
            if len(parts) == 3:
                catalog, schema, table = parts
            elif len(parts) == 2:
                schema, table = parts
        
        print(f"Selected table for editing: {table_name} -> catalog={catalog}, schema={schema}, table={table}")
        print(f"Features: {features}, Lookup Key: {lookup_key}, Timestamp Key: {timestamp_key}")
        
        return table_idx, catalog, schema, table, features, lookup_key, timestamp_key

    # Secondary callback to populate lookup/timestamp key dropdowns when table is selected
    # This runs after the EOL dropdown options are populated
    @app.callback(
        Output('feature-lookup-lookup-key-dropdown', 'value', allow_duplicate=True),
        Output('feature-lookup-timestamp-key-dropdown', 'value', allow_duplicate=True),
        Input('feature-lookup-selected-table', 'data'),
        Input('feature-lookup-lookup-key-dropdown', 'options'),  # Wait for options to be available
        Input('feature-lookup-timestamp-key-dropdown', 'options'),  # Wait for options to be available
        State('feature-lookup-table-store', 'data'),
        prevent_initial_call=True
    )
    def populate_keys_for_selected_table(selected_table_idx, lookup_options, timestamp_options, tables):
        ctx = callback_context
        if not ctx.triggered:
            return dash.no_update, dash.no_update
        
        # Only react to table selection changes, not option changes
        if ctx.triggered[0]['prop_id'] != 'feature-lookup-selected-table.data':
            return dash.no_update, dash.no_update
        
        if selected_table_idx is None or not tables or selected_table_idx >= len(tables):
            return dash.no_update, dash.no_update
        
        # Need options to be available before setting values
        if not lookup_options and not timestamp_options:
            return dash.no_update, dash.no_update
        
        selected_table = tables[selected_table_idx]
        if not isinstance(selected_table, dict):
            return dash.no_update, dash.no_update
        
        lookup_key = selected_table.get('lookup_key')
        timestamp_key = selected_table.get('timestamp_key')
        
        print(f"populate_keys_for_selected_table - EOL options available, setting lookup_key: {lookup_key}, timestamp_key: {timestamp_key}")
        print(f"Available lookup options: {[opt['value'] for opt in lookup_options] if lookup_options else []}")
        print(f"Available timestamp options: {[opt['value'] for opt in timestamp_options] if timestamp_options else []}")
        
        return lookup_key, timestamp_key

    # Enable/disable Update Table button based on selection
    @app.callback(
        Output('feature-lookup-update-table-button', 'disabled'),
        Input('feature-lookup-selected-table', 'data')
    )
    def toggle_update_button(selected_table):
        return selected_table is None

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
        
        # Just update the store - form population will be handled by populate_feature_lookup_form
        # Clear create_mode when selecting an item
        return {'items': items, 'active_id': fl_id, 'create_mode': False}

    @app.callback(
        [Output('feature-lookup-name', 'value', allow_duplicate=True),
         Output('feature-lookup-eol-dropdown', 'value', allow_duplicate=True),
         Output('feature-lookup-store', 'data', allow_duplicate=True)],
        Input('create-feature-lookup-button', 'n_clicks'),
        State('feature-lookup-store', 'data'),
        State('list-store', 'data'),
        prevent_initial_call=True
    )
    def create_feature_lookup_callback(n_clicks, feature_lookup_store, list_store):
        """Create a new feature lookup with default values when Create button is clicked."""
        if not n_clicks:
            raise PreventUpdate
            
        # Get current project ID
        project_id = list_store.get('active_project_id') if isinstance(list_store, dict) else None
        if not project_id:
            print("No active project for new feature lookup")
            raise PreventUpdate
            
        # Create a new feature lookup with default values
        new_fl_id = create_feature_lookup(
            name="New Feature Lookup",
            eol_id=None,
            project_id=project_id,
            features=[]
        )
        
        if new_fl_id is None:
            print("Failed to create new feature lookup")
            raise PreventUpdate
        
        # Refresh the feature lookup list
        df = get_feature_lookups(project_id)
        items = []
        if not df.empty:
            records = df.to_dict('records')
            items = [
                {'id': int(rec['id']), 'name': rec.get('name'), 'eol_id': rec.get('eol_id'), 'features': rec.get('features')}
                for rec in records
            ]
        
        # Select the newly created feature lookup and return form values
        store_data = {'items': items, 'active_id': new_fl_id, 'create_mode': False}
        return "New Feature Lookup", None, store_data

    @app.callback(
        Output('feature-lookup-name', 'value', allow_duplicate=True),
        Output('feature-lookup-eol-dropdown', 'value', allow_duplicate=True),
        Input('feature-lookup-store', 'data'),
        prevent_initial_call=True
    )
    def populate_feature_lookup_form(store_data):
        """Populate form inputs when the feature lookup store updates (like projects tab)."""
        if not isinstance(store_data, dict):
            return '', None
            
        active_id = store_data.get('active_id')
        items = store_data.get('items', [])
        
        # If no active selection, clear the form
        if active_id is None or not items:
            return '', None
            
        # Find the selected feature lookup
        for rec in items:
            if rec.get('id') == active_id:
                name = rec.get('name') or ''
                eol_id = rec.get('eol_id')
                
                # Convert eol_id to proper format
                eol_id_value = None
                if eol_id is not None and eol_id != '' and str(eol_id).lower() != 'none':
                    try:
                        eol_id_value = int(eol_id)
                    except (ValueError, TypeError):
                        eol_id_value = eol_id
                
                return name, eol_id_value
        
        # If no matching record found, clear the form
        return '', None

    @app.callback(
        Output('feature-lookup-store', 'data', allow_duplicate=True),
        Output('feature-lookup-form-alert', 'children', allow_duplicate=True),
        Input('update-feature-lookup-button', 'n_clicks'),
        State('feature-lookup-store', 'data'),
        State('feature-lookup-name', 'value'),
        State('feature-lookup-eol-dropdown', 'value'),
        State('feature-lookup-table-store', 'data'),
        State('list-store', 'data'),
        prevent_initial_call=True
    )
    def update_fl_callback(n_clicks, store_data, name, eol_id, tables, project_store):
        print(f"DEBUG: update_fl_callback called with n_clicks={n_clicks}, name='{name}', eol_id={eol_id}")
        print(f"DEBUG: store_data={store_data}")
        print(f"DEBUG: tables={tables}")
        
        fl_id = store_data.get('active_id') if isinstance(store_data, dict) else None
        print(f"DEBUG: extracted fl_id={fl_id}")
        
        if fl_id is None:
            alert = dbc.Alert("No feature lookup selected", color="warning", dismissable=True)
            return dash.no_update, alert
        
        if not name:
            alert = dbc.Alert("Feature lookup name is required", color="warning", dismissable=True)
            return dash.no_update, alert
        
        # Prepare feature list from selected tables
        feats = []
        if tables:
            for tbl_entry in tables:
                if isinstance(tbl_entry, dict):
                    table_name = tbl_entry.get('table', '')
                    cols = tbl_entry.get('features', [])
                    lookup_key = tbl_entry.get('lookup_key')
                    timestamp_key = tbl_entry.get('timestamp_key')
                    # Store as dictionary - let the DB function handle JSON encoding
                    entry_dict = {
                        'table': table_name, 
                        'features': cols,
                        'lookup_key': lookup_key,
                        'timestamp_key': timestamp_key
                    }
                    feats.append(entry_dict)
                else:
                    # Store as simple string (backward compatibility)
                    feats.append(str(tbl_entry))
        
        print(f"DEBUG: Calling update_feature_lookup with fl_id={fl_id}, name='{name}', eol_id={eol_id}, feats={feats}")
        
        if not update_feature_lookup(fl_id, name, eol_id, feats):
            print("DEBUG: update_feature_lookup failed")
            alert = dbc.Alert("Failed to update feature lookup", color="danger", dismissable=True)
            return dash.no_update, alert
            
        print("DEBUG: update_feature_lookup succeeded, refreshing data")
        
        project_id = project_store.get('active_project_id') if isinstance(project_store, dict) else None
        df = get_feature_lookups(project_id)
        records = df.to_dict('records') if not df.empty else []
        items = [
            {'id': int(rec['id']), 'name': rec.get('name'), 'eol_id': rec.get('eol_id'), 'features': rec.get('features')}
            for rec in records
        ]
        
        alert = dbc.Alert("Feature lookup updated successfully!", color="success", dismissable=True)
        return {'items': items, 'active_id': fl_id, 'create_mode': False}, alert

    # Clear alert when feature lookup selection changes
    @app.callback(
        Output('feature-lookup-form-alert', 'children', allow_duplicate=True),
        Input('feature-lookup-store', 'data'),
        prevent_initial_call=True
    )
    def clear_feature_lookup_alert(store_data):
        return None

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
        return {'items': items, 'active_id': active_id, 'create_mode': False}
