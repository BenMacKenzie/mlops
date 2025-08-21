import dash_bootstrap_components as dbc
from dash import html, dcc
from utils.db_universal import get_feature_lookups

def create_feature_lookup_tab():
    """Create the Feature Lookups tab layout and initial store."""
    # Initialize with empty store - will be populated by callback when project is selected
    # This matches the pattern used in projects tab where data is loaded by callbacks
    items = []
    active_id = None
    
    # Initialize with empty tables - will be populated by callback when feature lookup is selected
    initial_tables = []
    
    # Store to maintain list of feature lookups and active selection
    store = dcc.Store(id='feature-lookup-store', data={'items': items, 'active_id': active_id})
    # Store to maintain list of selected tables for current feature lookup form
    table_store = dcc.Store(id='feature-lookup-table-store', data=initial_tables)
    # Store to track if tables have been modified (to prevent reload)
    table_modified_store = dcc.Store(id='feature-lookup-table-modified', data=False)
    # Store to track which table is currently selected for editing
    selected_table_store = dcc.Store(id='feature-lookup-selected-table', data=None)

    # List group of feature lookups
    list_items = []
    for itm in items:
        list_items.append(
            dbc.ListGroupItem(
                itm['name'],
                id={'type': 'feature-lookup-item', 'index': itm['id']},
                action=True,
                active=(itm['id'] == active_id)
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
    list_group = dbc.ListGroup(list_items, id='feature-lookup-list')

    # Form for create/update/delete
    form = dbc.Form([
        dbc.Row([
            dbc.Col([
                dbc.Label("Name", html_for="feature-lookup-name"),
                dbc.Input(type="text", id="feature-lookup-name", placeholder="Enter lookup name")
            ], width=12)
        ], className="mb-3"),
        # EOL Definition dropdown
        dbc.Row([
            dbc.Col([
                dbc.Label("EOL Definition", html_for="feature-lookup-eol-dropdown"),
                dcc.Dropdown(
                    id="feature-lookup-eol-dropdown",
                    options=[],
                    placeholder="Select EOL definition",
                    clearable=True
                )
            ], width=12)
        ], className="mb-3"),
        # Catalog dropdown for table selection
        dbc.Row([
            dbc.Col([
                dbc.Label("Catalog", html_for="feature-lookup-catalog-dropdown"),
                dcc.Dropdown(
                    id="feature-lookup-catalog-dropdown",
                    options=[],  # Populated dynamically
                    placeholder="Select catalog",
                    clearable=True
                )
            ], width=12)
        ], className="mb-3"),
        # Schema dropdown based on selected catalog
        dbc.Row([
            dbc.Col([
                dbc.Label("Schema", html_for="feature-lookup-schema-dropdown"),
                dcc.Dropdown(
                    id="feature-lookup-schema-dropdown",
                    options=[],  # Populated dynamically
                    placeholder="Select schema",
                    clearable=True
                )
            ], width=12)
        ], className="mb-3"),
        # Table selection and add button
        dbc.Row([
            dbc.Col([
                dbc.Label("Table", html_for="feature-lookup-table-dropdown"),
                dcc.Dropdown(
                    id="feature-lookup-table-dropdown",
                    options=[],  # Populated dynamically from database
                    placeholder="Select table",
                    clearable=True
                )
            ], width=9),
            
        ], className="mb-3"),
        # Column selection dropdown for chosen table
        dbc.Row([
            dbc.Col([
                dbc.Label("Columns", html_for="feature-lookup-column-dropdown"),
                dcc.Dropdown(
                    id="feature-lookup-column-dropdown",
                    options=[],  # Populated dynamically based on selected table
                    multi=True,
                    placeholder="Select columns",
                    clearable=True
                )
            ], width=12)
        ], className="mb-3"),
        # Lookup key and timestamp key dropdowns
        dbc.Row([
            dbc.Col([
                dbc.Label("Lookup Key", html_for="feature-lookup-lookup-key-dropdown"),
                dcc.Dropdown(
                    id="feature-lookup-lookup-key-dropdown",
                    options=[],  # Populated from EOL view columns
                    placeholder="Select lookup key",
                    clearable=True
                )
            ], width=6),
            dbc.Col([
                dbc.Label("Timestamp Key (Optional)", html_for="feature-lookup-timestamp-key-dropdown"),
                dcc.Dropdown(
                    id="feature-lookup-timestamp-key-dropdown",
                    options=[],  # Populated from EOL view date/time columns
                    placeholder="Select timestamp key",
                    clearable=True
                )
            ], width=6)
        ], className="mb-3"),
        dbc.Row([
            dbc.Col([
                dbc.Button("Add Table", id="feature-lookup-add-table-button", color="success", className="mt-4 me-2")
            ], width=3),
            dbc.Col([
                dbc.Button("Update Table", id="feature-lookup-update-table-button", color="primary", className="mt-4", disabled=True)
            ], width=3)
        ], className="mb-3"),    # List o
            # f selected tables with delete buttons
        dbc.Row([
            dbc.Col([
                dbc.Label("Selected Tables"),
                html.Div(id="feature-lookup-table-list")
            ], width=12)
        ], className="mb-3"),
        dbc.Row([
            dbc.Col([
                dbc.Button("Create Feature Lookup", id="create-feature-lookup-button", color="success", className="me-2"),
                dbc.Button("Update Feature Lookup", id="update-feature-lookup-button", color="primary", className="me-2"),
                dbc.Button("Delete Feature Lookup", id="delete-feature-lookup-button", color="danger")
            ], width=12)
        ], className="mt-3 mb-3"),
        html.Div(id="feature-lookup-form-alert")
    ])

    # Combine list, form, and table store side by side
    layout = dbc.Container([
        table_store,
        table_modified_store,
        selected_table_store,
        dbc.Row([
            dbc.Col(list_group, width=6),
            dbc.Col(form, width=6)
        ])
    ], fluid=True)

    tab = dbc.Tab(layout, label="Feature Lookups", tab_id="tab-feature-lookups")
    return tab, store