import yaml
import pandas as pd
from databricks import sql
from databricks.sdk.core import Config
import os

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

def get_projects():
    """Fetch all projects from the database."""
    try:
        # Get database config
        with open('db_config.yaml', 'r') as file:
            db_config = yaml.safe_load(file)
        
        catalog_name = db_config['database']['catalog']
        schema_name = db_config['database']['schema']
        
        query = f"SELECT * FROM {catalog_name}.{schema_name}.project ORDER BY name"
        return sqlQuery(query)
    except Exception as e:
        print(f"Error fetching projects: {str(e)}")
        return pd.DataFrame()

def create_project(name: str, description: str, catalog: str, schema: str, git_url: str, training_notebook: str):
    """Create a new project in the database."""
    try:
        # Get database config
        with open('db_config.yaml', 'r') as file:
            db_config = yaml.safe_load(file)
        
        catalog_name = db_config['database']['catalog']
        schema_name = db_config['database']['schema']
        
        # Insert the new project and get the ID
        query = f"""
        INSERT INTO {catalog_name}.{schema_name}.project (name, description, catalog, schema, git_url, training_notebook)
        VALUES ('{name}', '{description}', '{catalog}', '{schema}', '{git_url}', '{training_notebook}')
        """
        
        sqlQuery(query)
        
        # Get the ID of the newly created project
        get_id_query = f"""
        SELECT id FROM {catalog_name}.{schema_name}.project 
        WHERE name = '{name}' AND description = '{description}' AND catalog = '{catalog}' 
        AND schema = '{schema}' AND git_url = '{git_url}' AND training_notebook = '{training_notebook}'
        ORDER BY id DESC LIMIT 1
        """
        
        result = sqlQuery(get_id_query)
        if not result.empty:
            return int(result.iloc[0]['id'])
        else:
            print("Error: Could not retrieve the ID of the newly created project")
            return None
    except Exception as e:
        print(f"Error creating project: {str(e)}")
        return None

def update_project(project_id: int, name: str, description: str, catalog: str, schema: str, git_url: str, training_notebook: str):
    """Update an existing project in the database."""
    try:
        # Get database config
        with open('db_config.yaml', 'r') as file:
            db_config = yaml.safe_load(file)
        
        catalog_name = db_config['database']['catalog']
        schema_name = db_config['database']['schema']
        
        # Update the project
        query = f"""
        UPDATE {catalog_name}.{schema_name}.project 
        SET name = '{name}', description = '{description}', catalog = '{catalog}', 
            schema = '{schema}', git_url = '{git_url}', training_notebook = '{training_notebook}'
        WHERE id = {project_id}
        """
        
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error updating project: {str(e)}")
        return False

def delete_project(project_id: int):
    """Delete a project from the database."""
    try:
        # Get database config
        with open('db_config.yaml', 'r') as file:
            db_config = yaml.safe_load(file)
        
        catalog_name = db_config['database']['catalog']
        schema_name = db_config['database']['schema']
        
        # Delete the project
        query = f"DELETE FROM {catalog_name}.{schema_name}.project WHERE id = {project_id}"
        
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error deleting project: {str(e)}")
        return False

def get_project_by_id(project_id: int):
    """Get a specific project by ID."""
    try:
        # Get database config
        with open('db_config.yaml', 'r') as file:
            db_config = yaml.safe_load(file)
        
        catalog_name = db_config['database']['catalog']
        schema_name = db_config['database']['schema']
        
        query = f"SELECT * FROM {catalog_name}.{schema_name}.project WHERE id = {project_id}"
        result = sqlQuery(query)
        
        if not result.empty:
            return result.iloc[0]
        return None
    except Exception as e:
        print(f"Error fetching project: {str(e)}")
        return None

def get_eol_definitions(project_id: int = None):
    """Fetch EOL definitions, optionally filtered by project_id."""
    try:
        # Get database config
        with open('db_config.yaml', 'r') as file:
            db_config = yaml.safe_load(file)
        
        catalog_name = db_config['database']['catalog']
        schema_name = db_config['database']['schema']
        
        if project_id is not None:
            query = f"SELECT * FROM {catalog_name}.{schema_name}.eol_definition WHERE project_id = {project_id} ORDER BY name"
        else:
            query = f"SELECT * FROM {catalog_name}.{schema_name}.eol_definition ORDER BY name"
        
        return sqlQuery(query)
    except Exception as e:
        print(f"Error fetching EOL definitions: {str(e)}")
        return pd.DataFrame()

def create_eol_definition(name: str, sql_definition: str, project_id: int):
    """Create a new EOL definition in the database."""
    try:
        # Get database config
        with open('db_config.yaml', 'r') as file:
            db_config = yaml.safe_load(file)
        
        catalog_name = db_config['database']['catalog']
        schema_name = db_config['database']['schema']
        
        # Escape single quotes in strings
        name_escaped = name.replace("'", "''") if name else ""
        sql_def_escaped = sql_definition.replace("'", "''") if sql_definition else ""
        
        # Insert the new EOL definition
        query = f"""
        INSERT INTO {catalog_name}.{schema_name}.eol_definition (name, sql_definition, project_id)
        VALUES ('{name_escaped}', '{sql_def_escaped}', {project_id})
        """
        
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error creating EOL definition: {str(e)}")
        return False

def update_eol_definition(old_name: str, name: str, sql_definition: str, project_id: int):
    """Update an existing EOL definition in the database."""
    try:
        # Get database config
        with open('db_config.yaml', 'r') as file:
            db_config = yaml.safe_load(file)
        
        catalog_name = db_config['database']['catalog']
        schema_name = db_config['database']['schema']
        
        # Escape single quotes in strings
        old_name_escaped = old_name.replace("'", "''") if old_name else ""
        name_escaped = name.replace("'", "''") if name else ""
        sql_def_escaped = sql_definition.replace("'", "''") if sql_definition else ""
        
        # Update the EOL definition
        query = f"""
        UPDATE {catalog_name}.{schema_name}.eol_definition 
        SET name = '{name_escaped}', sql_definition = '{sql_def_escaped}'
        WHERE name = '{old_name_escaped}' AND project_id = {project_id}
        """
        
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error updating EOL definition: {str(e)}")
        return False

def delete_eol_definition(name: str, project_id: int):
    """Delete an EOL definition from the database."""
    try:
        # Get database config
        with open('db_config.yaml', 'r') as file:
            db_config = yaml.safe_load(file)
        
        catalog_name = db_config['database']['catalog']
        schema_name = db_config['database']['schema']
        
        # Escape single quotes in name
        name_escaped = name.replace("'", "''") if name else ""
        
        # Delete the EOL definition
        query = f"DELETE FROM {catalog_name}.{schema_name}.eol_definition WHERE name = '{name_escaped}' AND project_id = {project_id}"
        
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error deleting EOL definition: {str(e)}")
        return False

def get_eol_definition_by_name(name: str, project_id: int):
    """Get a specific EOL definition by name."""
    try:
        # Get database config
        with open('db_config.yaml', 'r') as file:
            db_config = yaml.safe_load(file)
        
        catalog_name = db_config['database']['catalog']
        schema_name = db_config['database']['schema']
        
        # Escape single quotes in name
        name_escaped = name.replace("'", "''") if name else ""
        
        query = f"SELECT * FROM {catalog_name}.{schema_name}.eol_definition WHERE name = '{name_escaped}' AND project_id = {project_id}"
        result = sqlQuery(query)
        
        if not result.empty:
            return result.iloc[0]
        return None
    except Exception as e:
        print(f"Error fetching EOL definition: {str(e)}")
        return None 