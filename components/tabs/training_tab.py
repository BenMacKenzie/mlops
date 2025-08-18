import dash_bootstrap_components as dbc
from dash import dcc, html

def create_training_tab():
    return dbc.Tab(
        label="Train Model",
        id="tab-train",
        tab_id="tab-train",
        children=[
            dbc.Row([
                dbc.Col([
                    html.Hr(),
                    html.H5("Select Dataset for Training:"),
                    dcc.Dropdown(
                        id='train-dataset-dropdown',
                        placeholder="Select a dataset...",
                        style={'margin-bottom': '1rem'}
                    ),
                    html.H5("Training Parameters (JSON):"),
                    dbc.Textarea(
                        id="train-parameters-input",
                        placeholder='{\n  "learning_rate": 0.01,\n  "epochs": 10,\n  "batch_size": 32\n}',
                        style={'height': '150px'},
                        value='{\n  "learning_rate": 0.01,\n  "epochs": 10,\n  "batch_size": 32\n}'
                    ),
                    html.Br(),
                    dbc.Button("Train Model", id="train-run-button", color="primary", n_clicks=0),
                    html.Br(),
                    html.Br(),
                    dcc.Loading(
                        id="loading-train-output",
                        type="default",
                        children=html.Div(id="train-status-output")
                    ),
                    html.Hr(),
                    html.H5("Training Runs"),
                    dcc.Loading(
                        id="loading-training-runs",
                        type="default",
                        children=dbc.Table([
                            html.Thead(html.Tr([
                                html.Th("Dataset"),
                                html.Th("Job ID"),
                                html.Th("Run ID"),
                                html.Th("Status"),
                                html.Th("Started"),
                                html.Th("Actions")
                            ])),
                            html.Tbody(id="train-runs-list", children=[
                                html.Tr(html.Td(children=["Select a project to see training runs."], colSpan=6))
                            ])
                        ], bordered=True, hover=True, responsive=True, striped=True)
                    )
                ], width=12)
            ], className="p-3")
        ]
    )