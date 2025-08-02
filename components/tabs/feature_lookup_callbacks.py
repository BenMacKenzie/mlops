import dash
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
    get_catalogs,
    get_schemas,
    get_tables
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

    # Combined callback to add or delete tables in the table store
    @app.callback(
        Output('feature-lookup-table-store', 'data', allow_duplicate=True),
        Input('feature-lookup-add-table-button', 'n_clicks'),
        Input({'type': 'feature-lookup-delete-table-button', 'index': ALL}, 'n_clicks'),
        State('feature-lookup-catalog-dropdown', 'value'),
        State('feature-lookup-schema-dropdown', 'value'),
        State('feature-lookup-table-dropdown', 'value'),
        State('feature-lookup-table-store', 'data'),
        prevent_initial_call=True
    )
    def modify_table_list(add_clicks, delete_clicks, catalog, schema, selected_table, current_tables):
        """Handle adding a new table or deleting an existing one based on which button was clicked."""

        print(f"***** curent_tables: {current_tables}")
        print(f"***** selected_table: {selected_table}")
        ctx = callback_context
        triggered = ctx.triggered_id
        if not triggered:
            raise PreventUpdate
        # Ensure tables is a list
        # Normalize current_tables to a mutable Python list
        raw = current_tables
        if raw is None:
            tables = []
        elif isinstance(raw, list):
            tables = raw.copy()
        elif isinstance(raw, str):
            tables = [raw]
        else:
            try:
                tables = list(raw)
            except Exception:
                tables = []
        # Delete case: a delete button was clicked
        if isinstance(triggered, dict) and triggered.get('type') == 'feature-lookup-delete-table-button':
            idx = triggered.get('index')
            if idx is None or idx >= len(tables):
                raise PreventUpdate
            tables = [t for i, t in enumerate(tables) if i != idx]
            return tables
        # Add case: add-table button clicked
        if not selected_table or not catalog or not schema:
            raise PreventUpdate
        fq = f"{catalog}.{schema}.{selected_table}"
        print(f"size of list before: {len(tables)}")
        if fq not in tables:
            tables.append(fq)
        print(f"size of list after: {len(tables)}")
        return tables
    
    # Callback to render selected tables list
    @app.callback(
        Output('feature-lookup-table-list', 'children'),
        Input('feature-lookup-table-store', 'data')
    )
    def render_table_list(tables):
        
        if not tables:
            return html.P("No tables selected.", className="text-muted")
        children = []
        for idx, fq in enumerate(tables):
            children.append(
                dbc.Row([
                    dbc.Col(html.Span(fq), width=10),
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
        return {'items': items, 'active_id': fl_id}

    @app.callback(
        Output('feature-lookup-store', 'data', allow_duplicate=True),
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
            return dash.no_update
        # Set default name if none provided
        lookup_name = name if name else 'new feature lookup'
        # Prepare feature list from selected tables
        feats = [str(t).strip() for t in (tables or []) if t and str(t).strip()]
        # Create feature lookup in DB
        if not create_feature_lookup(project_id, eol_id, lookup_name, feats):
            return dash.no_update
        # Refresh the list of feature lookups
        df = get_feature_lookups(project_id)
        records = df.to_dict('records') if not df.empty else []
        items = [
            {'id': int(rec['id']), 'name': rec.get('name'), 'eol_id': rec.get('eol_id'), 'features': rec.get('features')}
            for rec in records
        ]
        active_id = items[-1]['id'] if items else None
        return {'items': items, 'active_id': active_id}

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
        if fl_id is None or not name or eol_id is None:
            return dash.no_update
        # Prepare feature list from selected tables
        feats = [str(t).strip() for t in (tables or []) if t and str(t).strip()]
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
        [
            Output('feature-lookup-name', 'value'),
            Output('feature-lookup-eol-dropdown', 'value'),
            Output('feature-lookup-table-store', 'data', allow_duplicate=True),
            Output('feature-lookup-table-dropdown', 'value')
        ],
        Input('feature-lookup-store', 'data'),
        prevent_initial_call='initial_duplicate'
    )
    def populate_feature_lookup_form(store_data):
        """Populate form inputs when the feature lookup store updates."""
        active_id = store_data.get('active_id') if isinstance(store_data, dict) else None
        items = store_data.get('items', []) if isinstance(store_data, dict) else []
        if active_id is None:
            return dash.no_update, dash.no_update, dash.no_update, dash.no_update
        for rec in items:
            if rec.get('id') == active_id:
                name = rec.get('name') or ''
                eol_id = rec.get('eol_id') if rec.get('eol_id') is not None else ''
                # Initialize table store with existing features, normalize to Python list
                raw_feats = rec.get('features')
                # Determine table list
                if raw_feats is None:
                    tables = []
                elif isinstance(raw_feats, list):
                    tables = raw_feats.copy()
                else:
                    # Try common array -> list conversion
                    try:
                        # numpy, pandas, or pyarrow objects
                        tables = raw_feats.tolist()
                    except Exception:
                        try:
                            tables = list(raw_feats)
                        except Exception:
                            tables = [raw_feats]
                return name, eol_id, tables, None
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update