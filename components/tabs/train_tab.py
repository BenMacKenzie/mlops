import dash_bootstrap_components as dbc
from dash import dcc, html

def create_train_tab():
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
                        placeholder='{\n  "learning_rate": 0.01,\n  "epochs": 10\n}',
                        style={'height': '150px'}
                    ),
                    html.Br(),
                    dbc.Button("Train / Run Job", id="train-run-button", color="primary", n_clicks=0),
                    html.Br(),
                    html.Br(),
                    dcc.Loading(
                        id="loading-train-output",
                        type="default",
                        children=html.Div(id="train-status-output")
                    ),
                    html.Hr(),
                    html.H5("MLflow Runs"),
                    dcc.Loading(
                        id="loading-mlflow-runs",
                        type="default",
                        children=dbc.Table([
                            html.Thead(html.Tr([
                                html.Th("Dataset"),
                                html.Th("Metrics"),
                                html.Th("Run ID"),
                                html.Th("Source"),
                                html.Th("Reg. Model"),
                                html.Th("Version"),
                                html.Th("Actions")
                            ])),
                            html.Tbody(id="train-mlflow-runs-list", children=[
                                html.Tr(html.Td(children=["Select a project to see runs."], colSpan=7))
                            ])
                        ], bordered=True, hover=True, responsive=True, striped=True)
                    ),
                    html.Hr(),
                    html.H5("Logged Models"),
                    dcc.Loading(
                        id="loading-logged-models",
                        type="default",
                        children=dbc.Table([
                            html.Thead(html.Tr([
                                html.Th("Model Name"),
                                html.Th("Model ID"),
                                html.Th("Created"),
                                html.Th("User"),
                                html.Th("Metrics"),
                                html.Th("Parameters")
                            ])),
                            html.Tbody(id="train-logged-models-list", children=[
                                html.Tr(html.Td(children=["Select a project to see logged models."], colSpan=6))
                            ])
                        ], bordered=True, hover=True, responsive=True, striped=True)
                    )
                ], width=12)
            ], className="p-3")
        ]
    )