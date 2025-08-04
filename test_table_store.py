"""
Debug script to test the table store issue
"""
import dash
from dash import html, dcc, Input, Output, State, callback_context
import dash_bootstrap_components as dbc

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    dcc.Store(id='table-store', data=[{'table': 'initial.table', 'features': ['col1']}]),
    html.Div(id='display'),
    dbc.Button('Add Item', id='add-btn', color='primary'),
    html.Hr(),
    html.Div(id='log')
])

@app.callback(
    Output('table-store', 'data'),
    Input('add-btn', 'n_clicks'),
    State('table-store', 'data'),
    prevent_initial_call=True
)
def add_item(n, current):
    print(f"Add callback - current data: {current}")
    if current is None:
        current = []
    new_item = {'table': f'table_{n}', 'features': []}
    current.append(new_item)
    return current

@app.callback(
    Output('display', 'children'),
    Input('table-store', 'data')
)
def display(data):
    print(f"Display callback - data: {data}")
    if not data:
        return "No data"
    return html.Ul([html.Li(str(item)) for item in data])

@app.callback(
    Output('log', 'children'),
    Input('table-store', 'data'),
    State('table-store', 'data')
)
def log_store(data_input, data_state):
    ctx = callback_context
    print(f"Log callback - trigger: {ctx.triggered}")
    print(f"Log callback - input data: {data_input}")
    print(f"Log callback - state data: {data_state}")
    return f"Store contains {len(data_input) if data_input else 0} items"

if __name__ == '__main__':
    app.run_server(debug=True, port=8051)