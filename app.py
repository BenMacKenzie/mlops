import os
from databricks import sql
import pandas as pd
import dash
from dash import dcc, html, Input, Output, State, callback_context, ALL, MATCH, no_update
import yaml

import dash_bootstrap_components as dbc
import dash_ag_grid as dag
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
from mlflow_service import mlflow_workspace_service as mlflow_service

from components.tabs.eol_table_tab import create_eol_tab

from components.tabs.mlops_tab import create_mlops_tab, register_mlops_callbacks
from components.tabs.project_tab import create_project_tab
from components.tabs.project_callbacks import register_new_project_callbacks
from components.tabs.feature_lookup_tab import create_feature_lookup_tab
from components.tabs.feature_lookup_callbacks import register_feature_lookup_callbacks
from components.tabs.eol_table_callbacks import register_eol_callbacks

# Check environment variable but don't fail if not set
warehouse_id = os.getenv('DATABRICKS_WAREHOUSE_ID')
if not warehouse_id:
    print("Warning: DATABRICKS_WAREHOUSE_ID not set. Some features may not work.")

def sqlQuery(query: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    cfg = Config()  # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

# Initialize the Dash app with Bootstrap styling
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

# Create tabs
project_tab, project_store = create_project_tab()
eol_tab = create_eol_tab()
# Feature lookups tab
feature_lookup_tab, feature_lookup_store = create_feature_lookup_tab()
mlops_tab = create_mlops_tab()

# Define the app layout
app.layout = dbc.Container([
    html.Div(id='dummy-trigger', style={'display': 'none'}),
    dbc.Row([dbc.Col(html.H1("MLops Dashboard"), width=12)]),
    
    # Store components
    project_store,
    feature_lookup_store,
    
    # Tabs
    dbc.Tabs([
        project_tab,
        eol_tab,
        feature_lookup_tab,
        mlops_tab
    ],
    id="tabs",
    active_tab="tab-project"
    )
], fluid=True)


register_new_project_callbacks(app)
register_feature_lookup_callbacks(app)
register_mlops_callbacks(app)
register_eol_callbacks(app)


if __name__ == "__main__":
    app.run(debug=True, port=8052)