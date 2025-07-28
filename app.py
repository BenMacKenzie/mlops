import os
from databricks import sql
import pandas as pd
import dash
from dash import dcc, html, Input, Output, State, callback_context, ALL, MATCH, no_update

import dash_bootstrap_components as dbc
import dash_ag_grid as dag
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
from mlflow_service import mlflow_workspace_service as mlflow_service
from feature_lookup import FeatureLookup, feature_lookups
from feature_lookup_builder import create_feature_lookup_builder_layout, register_feature_lookup_callbacks


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



def create_logged_models_column_defs(columns):
    """Create column definitions for logged models with special handling for metrics and parameters."""
    column_defs = []
    
    # Sort columns to maintain dataset grouping
    # Standard columns first, then dataset-specific metrics grouped by dataset, then general metrics, then parameters
    standard_columns = ['model_id', 'model_name', 'creation_timestamp', 'last_updated_timestamp', 'user_id', 'description']
    param_columns = [col for col in columns if col.startswith('param_')]
    metric_columns = [col for col in columns if col not in standard_columns and not col.startswith('param_')]
    
    # Group metric columns by dataset
    dataset_metrics = {}
    general_metrics = []
    
    for col in metric_columns:
        parts = col.split('_', 1)
        if len(parts) > 1:
            dataset_name = parts[0]
            if dataset_name.lower() in ['train', 'validation', 'val', 'test', 'eval', 'evaluation']:
                if dataset_name not in dataset_metrics:
                    dataset_metrics[dataset_name] = []
                dataset_metrics[dataset_name].append(col)
            else:
                general_metrics.append(col)
        else:
            general_metrics.append(col)
    
    # Add standard columns first
    for col in standard_columns:
        if col in columns:
            column_defs.append({
                "headerName": col.replace('_', ' ').title(),
                "field": col,
                "sortable": True,
                "filter": True,
                "resizable": True
            })
    
    # Add dataset-specific metrics grouped by dataset
    for dataset_name in sorted(dataset_metrics.keys()):
        for col in sorted(dataset_metrics[dataset_name]):
            parts = col.split('_', 1)
            metric_name = parts[1].replace('_', ' ').title()
            header_name = f"{dataset_name.upper()}: {metric_name}"
            
            column_defs.append({
                "headerName": header_name,
                "field": col,
                "sortable": True,
                "filter": True,
                "resizable": True,
                "width": 120,
                "cellStyle": {"backgroundColor": "#f0f8ff"}  # Light blue background for metrics
            })
    
    # Add general metrics
    for col in sorted(general_metrics):
        header_name = col.replace('_', ' ').title()
        column_defs.append({
            "headerName": header_name,
            "field": col,
            "sortable": True,
            "filter": True,
            "resizable": True,
            "width": 120,
            "cellStyle": {"backgroundColor": "#f0f8ff"}  # Light blue background for metrics
        })
    
    # Add parameter columns last
    for col in sorted(param_columns):
        param_name = col.replace('param_', '')
        column_defs.append({
            "headerName": f"Param: {param_name.replace('_', ' ').title()}",
            "field": col,
            "sortable": True,
            "filter": True,
            "resizable": True,
            "width": 120,
            "cellStyle": {"backgroundColor": "#fff8f0"}  # Light orange background for parameters
        })
    
    return column_defs

# Fetch MLflow runs
mlflow_runs = mlflow_service.get_runs()

# Fetch logged models
logged_models = mlflow_service.get_logged_models()

# Jobs service functions
def get_jobs_data():
    """Fetch all jobs from Databricks workspace."""
    try:
        wc = WorkspaceClient()
        jobs = list(wc.jobs.list())
        
        jobs_data = []
        for job in jobs:
            job_info = {
                'job_id': job.job_id,
                'job_name': job.settings.name if job.settings and job.settings.name else 'Unnamed Job',
                'created_time': pd.to_datetime(job.created_time / 1000, unit='s') if job.created_time else None,
                'creator_user_name': job.creator_user_name,
                'run_as_user_name': job.settings.run_as.user_name if job.settings and job.settings.run_as else None,
                'max_concurrent_runs': job.settings.max_concurrent_runs if job.settings else None,
                'timeout_seconds': job.settings.timeout_seconds if job.settings else None,
                'schedule': job.settings.schedule.quartz_cron_expression if job.settings and job.settings.schedule else None,
                'email_notifications': bool(job.settings.email_notifications) if job.settings else False,
                'webhook_notifications': bool(job.settings.webhook_notifications) if job.settings else False,
                'continuous': job.settings.continuous.enabled if job.settings and job.settings.continuous else False,
                'tags': str(job.settings.tags) if job.settings and job.settings.tags else None
            }
            jobs_data.append(job_info)
        
        return pd.DataFrame(jobs_data)
    except Exception as e:
        print(f"Error fetching jobs: {str(e)}")
        return pd.DataFrame()




# Fetch jobs data
jobs_data = get_jobs_data()

# Initialize the Dash app with Bootstrap styling
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

# Define the app layout
app.layout = dbc.Container([
    html.Div(id='dummy-trigger', style={'display': 'none'}),
    dbc.Row([dbc.Col(html.H1("MLops Dashboard"), width=12)]),
    
    # MLflow Runs Section
    dbc.Row([dbc.Col(html.H2("MLflow Experiment Runs"), width=12, className='mt-4')]),
    dbc.Row([
        dbc.Col([
            html.H4(f"Experiment: {mlflow_service.get_experiment_summary()['experiment_name']}"),
            html.P(f"Total Runs: {mlflow_service.get_experiment_summary()['total_runs']}"),
            dbc.Button("Refresh Runs", id='refresh-runs-button', color='secondary', className='mt-2')
        ], width=12)
    ]),
    dbc.Row([
        dbc.Col([
            dag.AgGrid(
                id='mlflow-runs-grid',
                columnDefs=[{"headerName": col.replace('_', ' ').title(), "field": col} for col in mlflow_runs.columns] if not mlflow_runs.empty else [],
                rowData=mlflow_runs.to_dict('records') if not mlflow_runs.empty else [],
                defaultColDef={"sortable": True, "filter": True, "resizable": True},
                style={'height': '400px', 'width': '100%'}
            )
        ], width=12)
    ]),
    
    # Logged Models Section
    dbc.Row([dbc.Col(html.H2("Logged Models"), width=12, className='mt-4')]),
    dbc.Row([
        dbc.Col([
            html.H4(f"Total Logged Models: {len(logged_models)}"),
            dbc.Button("Refresh Models", id='refresh-models-button', color='secondary', className='mt-2')
        ], width=12)
    ]),
    
    dbc.Row([
        dbc.Col([
            dag.AgGrid(
                id='logged-models-grid',
                columnDefs=create_logged_models_column_defs(logged_models.columns),
                rowData=logged_models.to_dict('records') if not logged_models.empty else [],
                defaultColDef={"sortable": True, "filter": True, "resizable": True},
                style={'height': '400px', 'width': '100%'}
            )
        ], width=12)
    ]),
    
    # Jobs Section
    dbc.Row([dbc.Col(html.H2("Databricks Jobs"), width=12, className='mt-4')]),
    dbc.Row([
        dbc.Col([
            html.H4(f"Total Jobs: {len(jobs_data)}"),
            dbc.Button("Refresh Jobs", id='refresh-jobs-button', color='secondary', className='mt-2')
        ], width=12)
    ]),
    
    dbc.Row([
        dbc.Col([
            dag.AgGrid(
                id='jobs-grid',
                columnDefs=[{"headerName": col.replace('_', ' ').title(), "field": col} for col in jobs_data.columns] if not jobs_data.empty else [],
                rowData=jobs_data.to_dict('records') if not jobs_data.empty else [],
                defaultColDef={"sortable": True, "filter": True, "resizable": True},
                style={'height': '400px', 'width': '100%'}
            )
        ], width=12)
    ]),
    
    # Feature Lookup Builder Section
    dbc.Row([dbc.Col(html.H2("Feature Lookup Builder"), width=12, className='mt-4')]),
    create_feature_lookup_builder_layout(),
    
], fluid=True)



@app.callback(
    [Output('mlflow-runs-grid', 'columnDefs'),
     Output('mlflow-runs-grid', 'rowData')],
    Input('refresh-runs-button', 'n_clicks')
)
def refresh_mlflow_runs(n_clicks):
    """Refresh MLflow runs data."""
    global mlflow_runs
    if n_clicks:
        mlflow_runs = mlflow_service.get_runs()
    
    if not mlflow_runs.empty:
        column_defs = [{"headerName": col.replace('_', ' ').title(), "field": col} for col in mlflow_runs.columns]
        row_data = mlflow_runs.to_dict('records')
    else:
        column_defs = []
        row_data = []
    
    return column_defs, row_data

@app.callback(
    [Output('logged-models-grid', 'columnDefs'),
     Output('logged-models-grid', 'rowData')],
    Input('refresh-models-button', 'n_clicks')
)
def refresh_logged_models(n_clicks):
    """Refresh logged models data."""
    global logged_models
    if n_clicks:
        logged_models = mlflow_service.get_logged_models()
    
    if not logged_models.empty:
        column_defs = create_logged_models_column_defs(logged_models.columns)
        row_data = logged_models.to_dict('records')
    else:
        column_defs = []
        row_data = []
    
    return column_defs, row_data

@app.callback(
    [Output('jobs-grid', 'columnDefs'),
     Output('jobs-grid', 'rowData')],
    Input('refresh-jobs-button', 'n_clicks')
)
def refresh_jobs(n_clicks):
    """Refresh jobs data."""
    global jobs_data
    if n_clicks:
        jobs_data = get_jobs_data()
    
    if not jobs_data.empty:
        column_defs = [{"headerName": col.replace('_', ' ').title(), "field": col} for col in jobs_data.columns]
        row_data = jobs_data.to_dict('records')
    else:
        column_defs = []
        row_data = []
    
    return column_defs, row_data



# Register feature lookup builder callbacks
register_feature_lookup_callbacks(app, sqlQuery)



if __name__ == "__main__":
    app.run(debug=True)