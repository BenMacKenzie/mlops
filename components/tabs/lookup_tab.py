import dash_bootstrap_components as dbc
from dash import html
from feature_lookup_builder import create_feature_lookup_builder_layout

def create_lookup_tab():
    """Create the lookup tab layout."""
    tab = dbc.Tab(
        dbc.Container([
            dbc.Row([dbc.Col(html.H2("Feature Lookup Builder"), width=12, className='mt-4')]),
            create_feature_lookup_builder_layout()
        ], fluid=True),
        label="Feature Lookup",
        tab_id="tab-lookup"
    )
    
    return tab 