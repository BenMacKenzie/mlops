import yaml
import pandas as pd
from databricks import sql
from databricks.sdk.core import Config
import os
# Load DB config once
try:
    with open('db_config.yaml', 'r') as _f:
        _db_conf = yaml.safe_load(_f)
    CATALOG_NAME = _db_conf['database']['catalog']
    SCHEMA_NAME = _db_conf['database']['schema']
    print(f"Loaded DB config: catalog={CATALOG_NAME}, schema={SCHEMA_NAME}")
except Exception as e:
    print(f"Error loading DB config: {e}")
    CATALOG_NAME = None
    SCHEMA_NAME = None

def sqlQuery(query: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    print(f"sqlQuery executing: {query}")
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
    print(f"get_projects called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}")
    try:
        query = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.project ORDER BY name"
        return sqlQuery(query)
    except Exception as e:
        print(f"Error fetching projects: {e}")
        return pd.DataFrame()

def create_project(name: str, description: str, catalog: str, schema: str, git_url: str, training_notebook: str):
    """Create a new project in the database."""
    print(f"create_project called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}")
    try:
        # Insert the new project and get the ID
        query = f"""
        INSERT INTO {CATALOG_NAME}.{SCHEMA_NAME}.project (name, description, catalog, schema, git_url, training_notebook)
        VALUES ('{name}', '{description}', '{catalog}', '{schema}', '{git_url}', '{training_notebook}')
        """
        sqlQuery(query)
        # Get the ID of the newly created project
        get_id_query = f"""
        SELECT id FROM {CATALOG_NAME}.{SCHEMA_NAME}.project
        WHERE name = '{name}' AND description = '{description}' AND catalog = '{catalog}'
          AND schema = '{schema}' AND git_url = '{git_url}' AND training_notebook = '{training_notebook}'
        ORDER BY id DESC
        LIMIT 1
        """
        result = sqlQuery(get_id_query)
        if not result.empty:
            return int(result.iloc[0]['id'])
        print("Error: Could not retrieve the ID of the newly created project")
        return None
    except Exception as e:
        print(f"Error creating project: {e}")
        return None

def update_project(project_id: int, name: str, description: str, catalog: str, schema: str, git_url: str, training_notebook: str):
    """Update an existing project in the database."""
    print(f"update_project called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, project_id={project_id}")
    try:
        # Update the project
        query = f"""
        UPDATE {CATALOG_NAME}.{SCHEMA_NAME}.project
        SET name = '{name}', description = '{description}', catalog = '{catalog}',
            schema = '{schema}', git_url = '{git_url}', training_notebook = '{training_notebook}'
        WHERE id = {project_id}
        """
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error updating project: {e}")
    return False
  

def delete_project(project_id: int):
    """Delete a project from the database."""
    print(f"delete_project called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, project_id={project_id}")
    try:
        # Delete the project
        query = f"DELETE FROM {CATALOG_NAME}.{SCHEMA_NAME}.project WHERE id = {project_id}"
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error deleting project: {e}")
        return False

def get_project_by_id(project_id: int):
    """Get a specific project by ID."""
    print(f"get_project_by_id called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, project_id={project_id}")
    try:
        query = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.project WHERE id = {project_id}"
        result = sqlQuery(query)
        if not result.empty:
            return result.iloc[0]
        return None
    except Exception as e:
        print(f"Error fetching project: {e}")
        return None

def get_eol_definitions(project_id: int = None):
    """Fetch EOL definitions, optionally filtered by project_id."""
    print(f"get_eol_definitions called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, project_id={project_id}")
    try:
        if project_id is not None:
            query = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.eol_definition WHERE project_id = {project_id} ORDER BY name"
        else:
            query = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.eol_definition ORDER BY name"
        return sqlQuery(query)
    except Exception as e:
        print(f"Error fetching EOL definitions: {e}")
        return pd.DataFrame()

def create_eol_definition(name: str, sql_definition: str, project_id: int):
    """Create a new EOL definition in the database."""
    print(f"create_eol_definition called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, project_id={project_id}")
    try:
        # Escape single quotes in strings
        name_escaped = name.replace("'", "''") if name else ""
        sql_def_escaped = sql_definition.replace("'", "''") if sql_definition else ""
        query = f"""
        INSERT INTO {CATALOG_NAME}.{SCHEMA_NAME}.eol_definition (name, sql_definition, project_id)
        VALUES ('{name_escaped}', '{sql_def_escaped}', {project_id})
        """
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error creating EOL definition: {e}")
        return False

def update_eol_definition(old_name: str, name: str, sql_definition: str, project_id: int):
    """Update an existing EOL definition in the database."""
    print(f"update_eol_definition called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, project_id={project_id}")
    try:
        # Escape single quotes in strings
        old_name_escaped = old_name.replace("'", "''") if old_name else ""
        name_escaped = name.replace("'", "''") if name else ""
        sql_def_escaped = sql_definition.replace("'", "''") if sql_definition else ""
        query = f"""
        UPDATE {CATALOG_NAME}.{SCHEMA_NAME}.eol_definition
        SET name = '{name_escaped}', sql_definition = '{sql_def_escaped}'
        WHERE name = '{old_name_escaped}' AND project_id = {project_id}
        """
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error updating EOL definition: {e}")
        return False

def delete_eol_definition(name: str, project_id: int):
    """Delete an EOL definition from the database."""
    print(f"delete_eol_definition called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, project_id={project_id}")
    try:
        # Escape single quotes in name
        name_escaped = name.replace("'", "''") if name else ""
        query = f"DELETE FROM {CATALOG_NAME}.{SCHEMA_NAME}.eol_definition WHERE name = '{name_escaped}' AND project_id = {project_id}"
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error deleting EOL definition: {e}")
        return False

def get_eol_definition_by_name(name: str, project_id: int):
    """Get a specific EOL definition by name."""
    print(f"get_eol_definition_by_name called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, project_id={project_id}")
    try:
        # Escape single quotes in name
        name_escaped = name.replace("'", "''") if name else ""
        query = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.eol_definition WHERE name = '{name_escaped}' AND project_id = {project_id}"
        result = sqlQuery(query)
        if not result.empty:
            return result.iloc[0]
        return None
    except Exception as e:
        print(f"Error fetching EOL definition: {e}")
        return None
## Feature Lookup CRUD operations
def get_feature_lookups(project_id: int = None) -> pd.DataFrame:
    """Fetch feature lookups, optionally filtered by project_id."""
    print(f"get_feature_lookups called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, project_id={project_id}")
    try:
        if project_id is not None:
            query = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.feature_lookups WHERE project_id = {project_id} ORDER BY name"
        else:
            query = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.feature_lookups ORDER BY name"
        return sqlQuery(query)
    except Exception as e:
        print(f"Error fetching feature lookups: {e}")
        return pd.DataFrame()

def create_feature_lookup(project_id: int, eol_id: int, name: str, features: list) -> bool:
    """Create a new feature lookup in the database."""
    print(f"create_feature_lookup called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, project_id={project_id}")
    try:
        name_escaped = name.replace("'", "''") if name else ''
        feats = [str(f).strip() for f in features if f]
        feats_sql = ', '.join(f"'{f.replace("'", "''")}'" for f in feats)
        try:
            eol_int = int(eol_id)
            eol_sql = str(eol_int)
        except (TypeError, ValueError):
            eol_sql = 'NULL'
        query = (
            f"INSERT INTO {CATALOG_NAME}.{SCHEMA_NAME}.feature_lookups "
            f"(project_id, eol_id, name, features) VALUES "
            f"({project_id}, {eol_sql}, '{name_escaped}', array({feats_sql}))"
        )
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error creating feature lookup: {e}")
        return False

def get_feature_lookup_by_id(feature_lookup_id: int):
    """Get a specific feature lookup by ID."""
    print(f"get_feature_lookup_by_id called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, id={feature_lookup_id}")
    try:
        query = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.feature_lookups WHERE id = {feature_lookup_id}"
        result = sqlQuery(query)
        if not result.empty:
            return result.iloc[0]
        return None
    except Exception as e:
        print(f"Error fetching feature lookup: {e}")
        return None

def update_feature_lookup(feature_lookup_id: int, name: str, eol_id: int, features: list) -> bool:
    """Update an existing feature lookup in the database."""
    print(f"update_feature_lookup called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, id={feature_lookup_id}")
    try:
        name_escaped = name.replace("'", "''") if name else ''
        feats = [str(f).strip() for f in features if f]
        feats_sql = ', '.join(f"'{f.replace("'", "''")}'" for f in feats)
        try:
            eol_int = int(eol_id)
            eol_sql = str(eol_int)
        except (TypeError, ValueError):
            eol_sql = 'NULL'
        query = (
            f"UPDATE {CATALOG_NAME}.{SCHEMA_NAME}.feature_lookups SET "
            f"name = '{name_escaped}', eol_id = {eol_sql}, features = array({feats_sql}) "
            f"WHERE id = {feature_lookup_id}"
        )
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error updating feature lookup: {e}")
        return False

def delete_feature_lookup(feature_lookup_id: int) -> bool:
    """Delete a feature lookup from the database."""
    print(f"delete_feature_lookup called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, id={feature_lookup_id}")
    try:
        query = f"DELETE FROM {CATALOG_NAME}.{SCHEMA_NAME}.feature_lookups WHERE id = {feature_lookup_id}"
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error deleting feature lookup: {e}")
        return False
  
# -----------------------------------------------------------------------------
# Fetch all table names from the configured catalog and schema
##
# -----------------------------------------------------------------------------
# Fetch all catalogs and schemas for dynamic table selection
def get_catalogs() -> list:
    """Fetch all catalogs available in the metastore."""
    print(f"get_catalogs called")
    try:
        df = sqlQuery("SHOW CATALOGS")
        if df.empty:
            return []
        # Pick first column containing catalog names
        cols = [c for c in df.columns if 'catalog' in c.lower() or 'name' in c.lower()]
        col = cols[0] if cols else df.columns[0]
        return [str(v) for v in df[col].tolist()]
    except Exception as e:
        print(f"Error fetching catalogs: {e}")
        return []

def get_schemas(catalog: str) -> list:
    """Fetch all schemas within the specified catalog."""
    print(f"get_schemas called with catalog={catalog}")
    try:
        query = f"SHOW SCHEMAS IN {catalog}"
        df = sqlQuery(query)
        if df.empty:
            return []
        # Pick first column containing schema names
        cols = [c for c in df.columns if 'schema' in c.lower() or 'name' in c.lower()]
        col = cols[0] if cols else df.columns[0]
        return [str(v) for v in df[col].tolist()]
    except Exception as e:
        print(f"Error fetching schemas: {e}")
        return []

##
# -----------------------------------------------------------------------------
def get_tables(catalog: str = None, schema: str = None) -> list:
    """Fetch all table names from the specified or configured catalog and schema."""
    cat = catalog or CATALOG_NAME
    sch = schema or SCHEMA_NAME
    print(f"get_tables called with catalog={cat}, schema={sch}")
    try:
        query = f"SHOW TABLES IN {cat}.{sch}"
        df = sqlQuery(query)
        if df.empty:
            return []
        # Determine the column containing table names
        cols = [c for c in df.columns if 'name' in c.lower()]
        col = cols[0] if cols else df.columns[0]
        return [str(t) for t in df[col].tolist()]
    except Exception as e:
        print(f"Error fetching tables: {e}")
        return []