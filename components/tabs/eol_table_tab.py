import dash_bootstrap_components as dbc
from dash import html
import pandas as pd
from dash import html, dcc, Input, Output, State, no_update, ALL, callback_context
import dash_bootstrap_components as dbc
from utils.db import get_eol_definitions, create_eol_definition, delete_eol_definition, get_eol_definition_by_name, update_eol_definition, get_project_by_id
import yaml, json
import yaml



def create_eol_definition_layout():
    """Layout for EOL definition creation and listing."""
    return [
        html.H5("EOL Definitions", className='mt-4'),
        dbc.Row([
            dbc.Col([html.Div(id='eol-definitions-list', className='mt-2')], width=8),
            dbc.Col([
                dcc.Store(id='eol-form-store', data={'old_name': None}),
                html.H5("Create EOL Definition", className='mt-4'),
                dbc.Form([
                    html.Div([
                        dbc.Label("Name", html_for="eol-name-input"),
                        dbc.Input(
                            id='eol-name-input',
                            type="text",
                            placeholder="Enter EOL definition name...",
                            value='new'
                        ),
                    ], className="mb-3"),
                    html.Div([
                        dbc.Label("SQL Definition", html_for="eol-sql-definition-input"),
                        dbc.Textarea(
                            id='eol-sql-definition-input',
                            placeholder="Enter SQL definition for the EOL table...",
                            rows=8
                        ),
                    ], className="mb-3"),
                    html.Div([
                        dbc.Button("New EOL Definition", id='new-eol-button', color='secondary', className='me-2'),
                        dbc.Button("Save EOL Definition", id='save-eol-button', color='success', className='me-2'),
                        dbc.Button("Delete EOL Definition", id='delete-eol-button', color='danger'),
                    ], className="mt-3"),
                    html.Div(id='eol-form-alert', className='mt-3')
                ])
            ], width=4)
        ], className='mt-2')
    ]


def create_eol_tab():
    """Create the EOL definitions tab layout."""
    tab = dbc.Tab(
        dbc.Container([
            dbc.Row([
                dbc.Col(html.H2("EOL Definitions"), width=12, className='mt-4')
            ]),
            *create_eol_definition_layout()
        ], fluid=True),
        label="EOL Definitions",
        tab_id="tab-eol"
    )
    return tab

