import dash_bootstrap_components as dbc
from dash import html, dcc

def create_dataset_tab():
    """Create the Datasets tab layout and initial store."""
    # Initialize with empty store - will be populated by callback when project is selected
    # This matches the pattern used in projects and feature lookups tabs
    items = []
    active_id = None

    # Store to maintain list of datasets and active selection
    store = dcc.Store(id='dataset-store', data={'items': items, 'active_id': active_id})

    # List group of datasets
    list_items = [
        dbc.ListGroupItem(
            "No datasets found.",
            id={'type': 'dataset-item', 'index': -1},
            disabled=True
        )
    ]
    list_group = dbc.ListGroup(list_items, id='dataset-list')

    # Form for create/update/delete
    form = dbc.Form([
        dbc.Row([
            dbc.Col([
                dbc.Label("Name", html_for="dataset-name"),
                dbc.Input(type="text", id="dataset-name", placeholder="Enter dataset name")
            ], width=12)
        ], className="mb-3"),
        
        # Feature Lookup dropdown
        dbc.Row([
            dbc.Col([
                dbc.Label("Feature Lookup", html_for="dataset-feature-lookup-dropdown"),
                dcc.Dropdown(
                    id="dataset-feature-lookup-dropdown",
                    options=[],
                    placeholder="Select feature lookup",
                    clearable=True
                )
            ], width=12)
        ], className="mb-3"),
        
        # Evaluation Type dropdown
        dbc.Row([
            dbc.Col([
                dbc.Label("Evaluation Type", html_for="dataset-evaluation-type-dropdown"),
                dcc.Dropdown(
                    id="dataset-evaluation-type-dropdown",
                    options=[
                        {'label': 'Random', 'value': 'random'},
                        {'label': 'Timestamp', 'value': 'timestamp'}
                    ],
                    placeholder="Select evaluation type",
                    clearable=True
                )
            ], width=12)
        ], className="mb-3"),
        
        # Percentage (for split)
        dbc.Row([
            dbc.Col([
                dbc.Label("Percentage", html_for="dataset-percentage"),
                dbc.Input(
                    type="number", 
                    id="dataset-percentage", 
                    placeholder="Enter percentage (e.g., 80.0)",
                    min=0,
                    max=100,
                    step=0.1
                )
            ], width=12)
        ], className="mb-3"),
        
        # Status display (shows materialization job status)
        dbc.Row([
            dbc.Col([
                dbc.Label("Status"),
                html.Div([
                    dbc.Badge(
                        "NOT_STARTED", 
                        id="dataset-status-display",
                        color="secondary",
                        className="ms-2"
                    )
                ])
            ], width=12)
        ], className="mb-3"),
        
        # Training and Eval table names (read-only for now)
        dbc.Row([
            dbc.Col([
                dbc.Label("Training Table Name", html_for="dataset-training-table"),
                dbc.Input(
                    type="text", 
                    id="dataset-training-table", 
                    placeholder="Not materialized",
                    disabled=True
                )
            ], width=6),
            dbc.Col([
                dbc.Label("Eval Table Name", html_for="dataset-eval-table"),
                dbc.Input(
                    type="text", 
                    id="dataset-eval-table", 
                    placeholder="Not materialized",
                    disabled=True
                )
            ], width=6)
        ], className="mb-3"),
        
        # Run ID and Run URL (read-only)
        dbc.Row([
            dbc.Col([
                dbc.Label("Run ID", html_for="dataset-run-id"),
                dbc.Input(
                    type="text", 
                    id="dataset-run-id", 
                    placeholder="Not materialized",
                    disabled=True
                )
            ], width=6),
            dbc.Col([
                dbc.Label("Run URL", html_for="dataset-run-url"),
                html.Div([
                    dbc.Input(
                        type="text", 
                        id="dataset-run-url", 
                        placeholder="Not materialized",
                        disabled=True,
                        style={"display": "none"}
                    ),
                    html.A(
                        "View Run",
                        id="dataset-run-url-link",
                        href="#",
                        target="_blank",
                        style={"display": "none"}
                    ),
                    html.Span(
                        "Not materialized",
                        id="dataset-run-url-placeholder"
                    )
                ])
            ], width=6)
        ], className="mb-3"),
        
        # Action buttons
        dbc.Row([
            dbc.Col([
                dbc.Button("Create Dataset", id="create-dataset-button", color="success", className="me-2"),
                dbc.Button("Update Dataset", id="update-dataset-button", color="primary", className="me-2"),
                dbc.Button("Materialize", id="materialize-dataset-button", color="info", className="me-2"),
                dbc.Button("Delete Dataset", id="delete-dataset-button", color="danger")
            ], width=12)
        ], className="mt-3 mb-3"),
        html.Div(id="dataset-form-alert")
    ])

    # Combine list, form, and store side by side
    layout = dbc.Container([
        store,
        dbc.Row([
            dbc.Col(list_group, width=6),
            dbc.Col(form, width=6)
        ])
    ], fluid=True)

    tab = dbc.Tab(layout, label="Datasets", tab_id="tab-datasets")
    return tab