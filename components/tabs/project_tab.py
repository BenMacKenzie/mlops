import dash_bootstrap_components as dbc
from dash import html, dcc
from dash.dependencies import Input, Output, State
import requests
import json
from utils.db import get_projects

# Helper function to fetch notebook files from GitHub
def fetch_notebook_files_from_github(github_repo_url: str, folder_path: str = "notebooks") -> list[dict]:
    """
    Retrieves all file names from the specified folder (defaulting to "notebooks") 
    in a public GitHub repository.

    Args:
        github_repo_url (str): The full URL of the public GitHub repository.
        folder_path (str): The specific folder path to look for files in. 
                           Defaults to "notebooks".

    Returns:
        list[dict]: A list of dictionaries suitable for dcc.Dropdown options 
                    (e.g., [{'label': 'file.txt', 'value': 'file.txt'}]),
                    or an empty list if no files are found or an error occurs.
    """
    print(f"\n[DEBUG] fetch_notebook_files_from_github called with URL: '{github_repo_url}', Folder: '{folder_path}'")

    if not github_repo_url or not github_repo_url.startswith("https://github.com/"):
        print(f"[DEBUG] Invalid or empty GitHub URL: '{github_repo_url}'")
        return []

    try:
        parts = github_repo_url.strip("/").split("/")
        if len(parts) < 5:
            print(f"[DEBUG] URL format incorrect for parsing owner/repo: {parts}")
            raise ValueError("URL format is incorrect for parsing owner/repo.")
        owner = parts[3]
        repo_name = parts[4]
        print(f"[DEBUG] Parsed Owner: '{owner}', Repo: '{repo_name}'")
    except (IndexError, ValueError) as e:
        print(f"[DEBUG] Error parsing owner/repo from URL '{github_repo_url}': {e}")
        return []

    headers = {"Accept": "application/vnd.github.v3+json"}
    
    files_options = []

    target_folder = folder_path.strip('/') if folder_path else "notebooks" 
    if not target_folder:
        target_folder = "notebooks"
        print(f"[DEBUG] Target folder was empty, defaulted to: '{target_folder}'")

    api_url = f"https://api.github.com/repos/{owner}/{repo_name}/contents/{target_folder}"
    print(f"[DEBUG] Fetching from API URL: {api_url}")
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        print(f"[DEBUG] API Response Status Code: {response.status_code}")
        response.raise_for_status()
        contents = response.json()
        print(f"[DEBUG] Raw API Response Contents (first 500 chars):\n{json.dumps(contents, indent=2)[:500]}...")

        if isinstance(contents, list):
            print(f"[DEBUG] API returned a list of {len(contents)} items.")
            for item in contents:
                item_name = item.get('name', '')
                item_type = item.get('type', '')
                print(f"[DEBUG] Processing item: Name='{item_name}', Type='{item_type}'")
                if item_type == 'file':
                    print(f"[DEBUG] Found file: '{item_name}'")
                    files_options.append({'label': item_name, 'value': item_name})
                elif item_type == 'dir':
                    print(f"[DEBUG] Found directory (skipped): '{item_name}'")
        else:
            print(f"[DEBUG] API did not return a list. Response type: {type(contents)}")
            if isinstance(contents, dict) and 'message' in contents:
                print(f"[DEBUG] API Error Message: {contents['message']}")
        
    except requests.exceptions.HTTPError as http_err:
        print(f"[DEBUG] HTTP error for {api_url}: {http_err}")
        if hasattr(http_err, 'response') and http_err.response is not None:
            print(f"[DEBUG] HTTP error response content: {http_err.response.text}")
        pass 
    except requests.exceptions.RequestException as req_err:
        print(f"[DEBUG] Request error for {api_url}: {req_err}")
        pass 
    except Exception as e:
        print(f"[DEBUG] Unexpected error for {api_url}: {e}")
        import traceback
        print(traceback.format_exc()) # Print full traceback for unexpected errors
        pass 
            
    print(f"[DEBUG] Final files_options: {files_options}")
    return files_options


def create_project_tab():
    # Retrieve projects from the database


   
    df = get_projects()
    # Convert to items for storage and display
    items = []
    if not df.empty:
        records = df.to_dict(orient='records')
        # Build list of projects with full details
        items = [
            {
                'id': int(rec['id']),
                'text': rec.get('name'),
                'description': rec.get('description'),
                'catalog': rec.get('catalog'),
                'schema': rec.get('schema'),
                'git_url': rec.get('git_url'),
            }
            for rec in records
        ]

    active_project_id = items[0]['id'] if items else None# Store component to maintain the list of projects
    store = dcc.Store(id='list-store', data={'items': items, "active_project_id": active_project_id})


    # Create list group items
    list_items = []
    for idx, item in enumerate(items):
        list_items.append(
            dbc.ListGroupItem(
                item['text'],
                id={"type": "list-group-item", "index": item['id']},
                action=True,
                active=(idx == 0)
            )
        )
    # Show message if no projects found
    if not list_items:
        list_items = [
            dbc.ListGroupItem(
                "No projects found.",
                id={"type": "list-group-item", "index": -1},
                disabled=True
            )
        ]

    listgroup = dbc.ListGroup(list_items, id="list-group")

    # Form to create a new project
    create_form = dbc.Form([
        html.Div([
            dbc.Label("Name", html_for="project-name"),
            dbc.Input(type="text", id="project-name", placeholder="Enter project name"),
        ], className="mb-3"),
        html.Div([
            dbc.Label("Description", html_for="project-description"),
            dbc.Input(type="text", id="project-description", placeholder="Enter description"),
        ], className="mb-3"),
        html.Div([
            dbc.Label("Catalog", html_for="project-catalog"),
            dbc.Input(type="text", id="project-catalog", placeholder="Enter catalog"),
        ], className="mb-3"),
        html.Div([
            dbc.Label("Schema", html_for="project-schema"),
            dbc.Input(type="text", id="project-schema", placeholder="Enter schema"),
        ], className="mb-3"),
        html.Div([
            dbc.Label("Git URL", html_for="project-git-url"),
            dbc.Input(type="text", id="project-git-url", placeholder="Enter Git URL", debounce=True),
        ], className="mb-3"),
        html.Div([
            dbc.Label("Training Notebook", html_for="project-notebook-dropdown"),
            dcc.Dropdown(
                id="project-notebook-dropdown",
                placeholder="Enter Git URL above to see files",
                options=[], # Initially empty, will be populated by callback
                # value=None # Optionally set a default value or manage through callback
            ),
        ], className="mb-3"),
        html.Div([
            dbc.Button("Create Project", id="create-project-button", color="success", className="me-2"),
            dbc.Button("Update Project", id="update-project-button", color="primary", className="me-2"),
            dbc.Button("Delete Project", id="delete-project-button", color="danger"),
        ], className="mt-3"),
        html.Div(id="project-form-alert", className="mt-3")
    ])
    # Layout: project list and creation form side by side
    return dbc.Tab([
        dbc.Row([
            dbc.Col(listgroup, width=8),
            dbc.Col(create_form, width=4)
        ])
    ], label="Projects", tab_id="tab-project"), store