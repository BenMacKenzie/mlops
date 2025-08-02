import dash
from dash import Input, Output, State, callback_context, no_update, ALL
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from utils.db import get_projects, create_project, update_project, delete_project, get_project_by_id
import requests
import json

# --- Import for fetching notebook files --- #
from components.tabs.project_tab import fetch_notebook_files_from_github
# --- End Import --- #

# --- Helper function for datetime formatting (optional) --- #
from datetime import datetime
def format_timestamp(ts):
    if ts is None:
        return "N/A"
    # Convert milliseconds timestamp (common in MLflow) to datetime
    try:
        dt_object = datetime.fromtimestamp(ts / 1000)
        return dt_object.strftime("%Y-%m-%d %H:%M:%S")
    except TypeError:
        return str(ts) # Fallback if it's not a standard timestamp
# --- End Helper --- #

def register_new_project_callbacks(app):

    @app.callback(
        Output('list-store', 'data', allow_duplicate=True),
        Input('url', 'pathname'),  # Trigger on page load
        prevent_initial_call=True
    )
    def update_store_on_refresh(_):
        print("update_store_on_refresh")
        df = get_projects()
        items = []
        if not df.empty:
            try:
                records = df.to_dict(orient='records')
                # Validate and transform records
                items = []
                for rec in records:
                    if isinstance(rec, dict) and 'id' in rec:
                        items.append({
                            'id': int(rec['id']),
                            'text': str(rec.get('name', '')),
                            'description': str(rec.get('description', '')),
                            'catalog': str(rec.get('catalog', '')),
                            'schema': str(rec.get('schema', '')),
                            'git_url': str(rec.get('git_url', '')),
                            'training_notebook': str(rec.get('training_notebook', ''))
                        })
            except Exception as e:
                print(f"Error processing project records: {e}")
                items = []
                
        # Set active project to first item if available, otherwise None
        active_project_id = items[0]['id'] if items else None
        print("Processed items:", items)
        print("Active project ID:", active_project_id)
        
        # Always return a dictionary with both items and active_project_id
        return {'items': items, "active_project_id": active_project_id}

    @app.callback(
        Output("list-group", "children", allow_duplicate=True),
        Input("list-store", "data"),
        prevent_initial_call=True
    )
    def refresh_project_list(store_data):
        print("refresh_project_list")
        print("store_data type:", type(store_data))
        print("store_data:", store_data)
        
        # Handle both list and dictionary data structures
        if isinstance(store_data, dict):
            new_items = store_data.get('items', []) or []
            active_project_id = store_data.get('active_project_id', None)
        elif isinstance(store_data, list):
            new_items = store_data
            # More robust handling of active_project_id for list case
            try:
                active_project_id = new_items[0].get('id') if new_items and isinstance(new_items[0], dict) else None
            except (IndexError, AttributeError, KeyError) as e:
                print(f"Error getting active_project_id from list: {e}")
                active_project_id = None
        else:
            print(f"Unexpected store_data type: {type(store_data)}")
            new_items = []
            active_project_id = None
            
        # Validate items structure
        valid_items = []
        for item in new_items:
            if isinstance(item, dict) and 'id' in item and 'text' in item:
                valid_items.append(item)
            else:
                print(f"Skipping invalid item: {item}")
        
        # Create list items only from valid items
        list_items = [
            dbc.ListGroupItem(
                itm['text'],
                id={"type": "list-group-item", "index": itm['id']},
                action=True,
                active=(itm['id'] == active_project_id)
            ) for itm in valid_items
        ]
        
        # If no valid items, show a message
        if not list_items:
            list_items = [
                dbc.ListGroupItem(
                    "No projects found.",
                    id={"type": "list-group-item", "index": -1},
                    disabled=True
                )
            ]
       
        return list_items

    @app.callback(
        Output("list-store", "data"),
        Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks'),
        State("list-store", "data"),
        prevent_initial_call=True
    )
    def select_project_callback(clicks, store_data):
        ctx = callback_context
        if not ctx.triggered_id or not isinstance(ctx.triggered_id, dict) or ctx.triggered_id.get('type') != 'list-group-item':
            raise PreventUpdate
        project_id = ctx.triggered_id["index"]
        
        # Handle both list and dictionary cases for store_data
        if isinstance(store_data, dict):
            items = store_data.get('items', []) or []
        elif isinstance(store_data, list):
            items = store_data
        else:
            items = []
            
        return {'items': items, "active_project_id": project_id}

    @app.callback(
        Output("list-store", "data", allow_duplicate=True),
        Input("create-project-button", "n_clicks"),
        State("project-notebook-dropdown", "value"),
        State("list-store", "data"),
        prevent_initial_call=True
    )
    def create_project_callback(create_clicks, training_notebook_file, store_data):
        print("create_project_callback")
        print("store_data type:", type(store_data))
        print("store_data:", store_data)
        
        # Ensure store_data is a dictionary
        if isinstance(store_data, list):
            store_data = {'items': store_data, 'active_project_id': store_data[0]['id'] if store_data else None}
        
        # Create project with default values for required fields
        project_id = create_project(
            name="New Project",  # Default name
            description="No description",  # Default description
            catalog="default_catalog",  # Default catalog
            schema="default_schema",  # Default schema
            git_url="https://github.com/example/repo",  # Default git URL
            training_notebook=training_notebook_file or "default_notebook.py"  # Use provided notebook or default
        )
        
        if project_id is None:
            print("Failed to create project")
            return no_update
            
        # Refresh the entire list from the database instead of manually adding
        df = get_projects()
        items = []
        if not df.empty:
            records = df.to_dict(orient='records')
            items = [
                {
                    'id': int(rec['id']),
                    'text': rec.get('name'),
                    'description': rec.get('description'),
                    'catalog': rec.get('catalog'),
                    'schema': rec.get('schema'),
                    'git_url': rec.get('git_url'),
                    'training_notebook': rec.get('training_notebook')
                }
                for rec in records
            ]
        
        print("Refreshed items after project creation:", items)  # Debug print
     
        return {'items': items, "active_project_id": project_id}
    
    @app.callback(
        Output("list-store", "data", allow_duplicate=True),
        Input("update-project-button", "n_clicks"),
        State("project-name", "value"),
        State("project-description", "value"),
        State("project-catalog", "value"),
        State("project-schema", "value"),
        State("project-git-url", "value"),
        State("project-notebook-dropdown", "value"),
        State("list-store", "data"),
        State({"type": "list-group-item", "index": ALL}, "active"),
        prevent_initial_call=True
    )
    def update_project_callback(update_clicks, name, description, catalog, schema, git_url,
                              training_notebook_file,
                              store_data, active_states):
        # choose index
        try:
            idx = active_states.index(True)
        except (ValueError, TypeError):
            idx = len(store_data.get('items', [])) - 1
        if idx < 0:
            return no_update
        project_item = store_data.get('items', [])[idx]
        project_id = project_item.get('id')
        updated = update_project(project_id, name, description, catalog, schema, git_url, training_notebook_file)
        if updated is None:
            return no_update
        # Refresh list
        df = get_projects()
        items = []
        if not df.empty:
            for rec in df.to_dict(orient='records'):
                items.append({
                    'id': int(rec.get('id')),
                    'text': rec.get('name'),
                    'description': rec.get('description'),
                    'catalog': rec.get('catalog'),
                    'schema': rec.get('schema'),
                    'git_url': rec.get('git_url'),
                    'training_notebook': rec.get('training_notebook')
                })
       
        return {'items': items, "active_project_id": project_id}
    
    def get_project_from_store(store_data, project_id):
        """
        Get a project record from the store data by its ID.
        Returns the project dictionary if found, None otherwise.
        """
        # Handle both list and dictionary cases for store_data
        if isinstance(store_data, dict):
            items = store_data.get('items', [])
        elif isinstance(store_data, list):
            items = store_data
        else:
            return None

        # Debug print to see what we're working with
        print("Store data items:", items)
        
        for item in items:
            # Handle both dictionary and object cases, and check for both 'id' and 'index' keys
            item_id = None
            if isinstance(item, dict):
                item_id = item.get('id') or item.get('index')
            elif hasattr(item, 'id'):
                item_id = item.id
            elif hasattr(item, 'index'):
                item_id = item.index
                
            if item_id is not None and item_id == project_id:
                return item
        return None

    @app.callback(
        Output("project-name", "value"),
        Output("project-description", "value"),
        Output("project-catalog", "value"),
        Output("project-schema", "value"),
        Output("project-git-url", "value"),
        Output("project-notebook-dropdown", "value"),
        Input("list-store", "data"),
        prevent_initial_call=True
    )
    def populate_form(store_data):
        # Populate the form inputs based on the selected project
        print("populate_form - store_data:", store_data)
        
        # Handle empty or invalid store_data
        if not store_data:
            # Return empty values for all form fields
            return '', '', '', '', '', None
            
        # Get active project ID, handling both dict and list cases
        if isinstance(store_data, dict):
            active_project_id = store_data.get("active_project_id")
            items = store_data.get('items', [])
        else:
            # If it's a list, try to get the first item's ID
            items = store_data if isinstance(store_data, list) else []
            active_project_id = items[0].get('id') if items and isinstance(items[0], dict) else None
            
        # If no active project or no items, return empty values
        if not active_project_id or not items:
            print("No active project ID or items found")
            return '', '', '', '', '', None
            
        # Use the helper function to get the project based on active_project_id
        project = get_project_from_store(store_data, active_project_id)
        if not project:
            print(f"No project found for ID {active_project_id}")
            return '', '', '', '', '', None
            
        # Handle both dictionary and object cases for project
        if isinstance(project, dict):
            return (
                project.get('text', ''),
                project.get('description', ''),
                project.get('catalog', ''),
                project.get('schema', ''),
                project.get('git_url', ''),
                project.get('training_notebook', None)
            )
        else:
            # Handle object case
            return (
                getattr(project, 'text', ''),
                getattr(project, 'description', ''),
                getattr(project, 'catalog', ''),
                getattr(project, 'schema', ''),
                getattr(project, 'git_url', ''),
                getattr(project, 'training_notebook', None)
            )



    @app.callback(
        Output("project-notebook-dropdown", "options"),
        [Input("project-git-url", "value"),
         Input({"type": "list-group-item", "index": ALL}, "n_clicks"),
         Input("list-store", "data")],
        prevent_initial_call=True
    )
    def update_notebook_options(git_url, project_clicks, store_data):
        """Update notebook dropdown options based on GitHub URL or project selection."""
        ctx = callback_context
        if not ctx.triggered:
            return []
        
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        # If triggered by store_data change (project selection), we need to fetch options for the selected project's git_url
        if trigger_id == "list-store":
            # Get active project ID
            if isinstance(store_data, dict):
                active_project_id = store_data.get("active_project_id")
                items = store_data.get('items', [])
            else:
                items = store_data if isinstance(store_data, list) else []
                active_project_id = items[0].get('id') if items and isinstance(items[0], dict) else None
            
            if active_project_id:
                # Get project details to get the git_url
                project = get_project_by_id(active_project_id)
                if project is not None and project.get('git_url'):
                    git_url = project['git_url']
                else:
                    return []
            else:
                return []
        
        # If triggered by project selection, we need to fetch options for the selected project's git_url
        elif trigger_id.startswith('{"type":"list-group-item","index":'):
            try:
                import json
                item_data = json.loads(trigger_id)
                project_id = item_data['index']
                
                if project_id == -1:  # No projects found
                    return []
                
                # Get project details to get the git_url
                project = get_project_by_id(project_id)
                if project is not None and project.get('git_url'):
                    git_url = project['git_url']
                else:
                    return []
            except Exception as e:
                print(f"Error getting project git_url: {str(e)}")
                return []
        
        # If no git_url or not a GitHub URL, return empty
        if not git_url or not git_url.startswith("https://github.com/"):
            return []
        
        try:
            # Import the function from project_tab
            from components.tabs.project_tab import fetch_notebook_files_from_github
            options = fetch_notebook_files_from_github(git_url)
            print(f"[DEBUG] Fetched {len(options)} notebook options for git_url: {git_url}")
            return options
        except Exception as e:
            print(f"Error fetching notebook files: {str(e)}")
            return []

    @app.callback(
        Output("list-store", "data", allow_duplicate=True),
        Input("delete-project-button", "n_clicks"),
        State("list-store", "data"),
        prevent_initial_call=True
    )
    def delete_project_callback(n_clicks, store_data):
        if not n_clicks:
            raise PreventUpdate
        if isinstance(store_data, dict):
            active_project_id = store_data.get("active_project_id")
            items = store_data.get("items", []) or []
        elif isinstance(store_data, list):
            items = store_data
            active_project_id = items[0].get("id") if items else None
        else:
            raise PreventUpdate
        if not active_project_id:
            raise PreventUpdate
        success = delete_project(active_project_id)
        if not success:
            return no_update
        df = get_projects()
        items = []
        if not df.empty:
            records = df.to_dict(orient="records")
            items = [
                {
                    "id": int(rec["id"]),
                    "text": rec.get("name"),
                    "description": rec.get("description"),
                    "catalog": rec.get("catalog"),
                    "schema": rec.get("schema"),
                    "git_url": rec.get("git_url"),
                    "training_notebook": rec.get("training_notebook"),
                }
                for rec in records
            ]
        new_active_id = items[0]["id"] if items else None
        return {"items": items, "active_project_id": new_active_id}

 