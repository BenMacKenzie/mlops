import dash
from dash import Input, Output, State, callback_context, no_update, ALL
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from utils.db_universal import get_projects, create_project, update_project, delete_project, get_project_by_id
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
                
        # Don't auto-select any project - let user or other callbacks handle selection
        active_project_id = None
        print("Processed items:", items)
        print("Active project ID: None (no auto-selection)")
        
        # Always return a dictionary with both items and active_project_id
        return {'items': items, "active_project_id": active_project_id, 'create_mode': False}

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
            
        # Clear create_mode when selecting a project
        return {'items': items, "active_project_id": project_id, 'create_mode': False}

    @app.callback(
        [Output("project-name", "value", allow_duplicate=True),
         Output("project-description", "value", allow_duplicate=True),
         Output("project-catalog", "value", allow_duplicate=True),
         Output("project-schema", "value", allow_duplicate=True),
         Output("project-git-url", "value", allow_duplicate=True),
         Output("project-notebook-dropdown", "value", allow_duplicate=True),
         Output("project-notebook-dropdown", "options", allow_duplicate=True),
         Output("list-store", "data", allow_duplicate=True)],
        Input("create-project-button", "n_clicks"),
        State("list-store", "data"),
        prevent_initial_call=True
    )
    def create_project_callback(create_clicks, store_data):
        """Create a new project with default values when create button is clicked."""
        print("=== CREATE PROJECT CALLBACK (Create New Project) ===")
        
        # Create a new project with default values
        new_project_id = create_project(
            name="New Project",
            description="",
            catalog="",
            schema="",
            git_url="",
            training_notebook=""
        )
        
        if new_project_id is None:
            print("Failed to create new project")
            raise PreventUpdate
        
        # Refresh the project list
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
        
        # Select the newly created project and directly set form values
        store_data = {'items': items, "active_project_id": new_project_id, 'create_mode': False}
        
        # Return the form values for the new project directly
        return "New Project", "", "", "", "", None, [], store_data
    
    @app.callback(
        [Output("project-name", "value", allow_duplicate=True),
         Output("project-description", "value", allow_duplicate=True),
         Output("project-catalog", "value", allow_duplicate=True),
         Output("project-schema", "value", allow_duplicate=True),
         Output("project-git-url", "value", allow_duplicate=True),
         Output("project-notebook-dropdown", "value", allow_duplicate=True),
         Output("project-notebook-dropdown", "options", allow_duplicate=True),
         Output("list-store", "data", allow_duplicate=True)],
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
        # Get the active project to update
        active_project_id = store_data.get('active_project_id') if isinstance(store_data, dict) else None
        
        if not active_project_id:
            print("No active project to update")
            raise PreventUpdate
            
        # Update the project
        updated = update_project(active_project_id, name, description, catalog, schema, git_url, training_notebook_file)
        if updated is None:
            raise PreventUpdate
        
        project_id = active_project_id
        
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
       
        store_data = {'items': items, "active_project_id": project_id, 'create_mode': False}
        
        # Find the updated project and return its current form values
        updated_project = None
        for item in items:
            if item['id'] == project_id:
                updated_project = item
                break
                
        if updated_project:
            # Return the updated project's values directly
            git_url = updated_project.get('git_url', '')
            notebook_options = []
            if git_url and git_url.startswith("https://github.com/"):
                try:
                    from components.tabs.project_tab import fetch_notebook_files_from_github
                    notebook_options = fetch_notebook_files_from_github(git_url)
                except Exception as e:
                    print(f"Error fetching notebook files: {str(e)}")
                    notebook_options = []
            
            return (updated_project.get('text', ''), 
                   updated_project.get('description', ''), 
                   updated_project.get('catalog', ''), 
                   updated_project.get('schema', ''), 
                   updated_project.get('git_url', ''), 
                   updated_project.get('training_notebook', None), 
                   notebook_options,
                   store_data)
        else:
            # Fallback - return current form values
            return name, description, catalog, schema, git_url, training_notebook_file, [], store_data
    
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
        Output("project-notebook-dropdown", "options"),
        Input("list-store", "data"),
        prevent_initial_call=True
    )
    def populate_form(store_data):
        # Populate the form inputs based on the selected project
        print(f"=== POPULATE_FORM ===")
        print(f"store_data type: {type(store_data)}")
        print(f"store_data: {store_data}")
        
        # Handle empty or invalid store_data
        if not store_data:
            print("No store_data - returning empty form")
            return '', '', '', '', '', None, []
            
        # Get active project ID, handling both dict and list cases
        if isinstance(store_data, dict):
            active_project_id = store_data.get("active_project_id")
            items = store_data.get('items', [])
            print(f"Dict format - active_project_id: {active_project_id}, items count: {len(items)}")
        else:
            # Legacy list format - this shouldn't happen with our new create callbacks
            items = store_data if isinstance(store_data, list) else []
            active_project_id = None  # Don't auto-select first item in legacy mode
            print(f"WARNING: List format detected (should not happen) - items count: {len(items)}")
            print(f"Legacy store_data: {store_data}")
            
        # If no active project, return empty form
        if active_project_id is None:
            print("No active project ID - returning empty form")
            return '', '', '', '', '', None, []
        
        # If no items, return empty values    
        if not items:
            print("No items found")
            return '', '', '', '', '', None, []
            
        # Use the helper function to get the project based on active_project_id
        print(f"Looking for project with ID: {active_project_id}")
        project = get_project_from_store(store_data, active_project_id)
        if not project:
            print(f"ERROR: No project found for ID {active_project_id}")
            print(f"Available items: {[item.get('id') for item in items if isinstance(item, dict)]}")
            return '', '', '', '', '', None, []
        
        print(f"Found project: {project.get('text', 'Unknown')} (ID: {project.get('id')})")
            
        # Handle both dictionary and object cases for project
        if isinstance(project, dict):
            git_url = project.get('git_url', '')
            training_notebook = project.get('training_notebook', None)
            
            # Fetch notebook options if git_url is available
            notebook_options = []
            if git_url and git_url.startswith("https://github.com/"):
                try:
                    from components.tabs.project_tab import fetch_notebook_files_from_github
                    notebook_options = fetch_notebook_files_from_github(git_url)
                    print(f"[DEBUG] Fetched {len(notebook_options)} notebook options in populate_form")
                except Exception as e:
                    print(f"Error fetching notebook files in populate_form: {str(e)}")
                    notebook_options = []
            
            return (
                project.get('text', ''),
                project.get('description', ''),
                project.get('catalog', ''),
                project.get('schema', ''),
                git_url,
                training_notebook,
                notebook_options
            )
        else:
            # Handle object case
            git_url = getattr(project, 'git_url', '')
            training_notebook = getattr(project, 'training_notebook', None)
            
            # Fetch notebook options if git_url is available
            notebook_options = []
            if git_url and git_url.startswith("https://github.com/"):
                try:
                    from components.tabs.project_tab import fetch_notebook_files_from_github
                    notebook_options = fetch_notebook_files_from_github(git_url)
                    print(f"[DEBUG] Fetched {len(notebook_options)} notebook options in populate_form (object case)")
                except Exception as e:
                    print(f"Error fetching notebook files in populate_form (object case): {str(e)}")
                    notebook_options = []
            
            return (
                getattr(project, 'text', ''),
                getattr(project, 'description', ''),
                getattr(project, 'catalog', ''),
                getattr(project, 'schema', ''),
                git_url,
                training_notebook,
                notebook_options
            )



    # NOTE: Removed separate update_notebook_options callback to avoid conflicts
    # The notebook options are now handled in the populate_form callback above
    
    @app.callback(
        Output("project-notebook-dropdown", "options", allow_duplicate=True),
        Input("project-git-url", "value"),
        prevent_initial_call=True
    )
    def update_notebook_options_on_git_url_change(git_url):
        """Update notebook dropdown options when git URL is manually changed."""
        if not git_url or not git_url.startswith("https://github.com/"):
            return []
        
        try:
            from components.tabs.project_tab import fetch_notebook_files_from_github
            options = fetch_notebook_files_from_github(git_url)
            print(f"[DEBUG] Fetched {len(options)} notebook options for manually changed git_url: {git_url}")
            return options
        except Exception as e:
            print(f"Error fetching notebook files on git URL change: {str(e)}")
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
        print(f"About to delete project with ID: {active_project_id}")
        success = delete_project(active_project_id)
        print(f"Delete project result: {success}")
        if not success:
            return no_update
        
        print("Fetching projects after deletion...")
        df = get_projects()
        print(f"get_projects() returned: {type(df)}, empty: {df.empty if df is not None else 'df is None'}")
        
        items = []
        if df is not None and not df.empty:
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
        return {"items": items, "active_project_id": new_active_id, 'create_mode': False}

 