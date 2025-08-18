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

def create_eol_definition(name: str, sql_definition: str, project_id: int, label: str = None):
    """Create a new EOL definition in the database."""
    print(f"create_eol_definition called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, project_id={project_id}")
    try:
        # Escape single quotes in strings
        name_escaped = name.replace("'", "''") if name else ""
        sql_def_escaped = sql_definition.replace("'", "''") if sql_definition else ""
        label_escaped = label.replace("'", "''") if label else ""
        query = f"""
        INSERT INTO {CATALOG_NAME}.{SCHEMA_NAME}.eol_definition (name, sql_definition, project_id, label)
        VALUES ('{name_escaped}', '{sql_def_escaped}', {project_id}, '{label_escaped}')
        """
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error creating EOL definition: {e}")
        return False

def update_eol_definition(old_name: str, name: str, sql_definition: str, project_id: int, label: str = None):
    """Update an existing EOL definition in the database."""
    print(f"update_eol_definition called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, project_id={project_id}")
    try:
        # Escape single quotes in strings
        old_name_escaped = old_name.replace("'", "''") if old_name else ""
        name_escaped = name.replace("'", "''") if name else ""
        sql_def_escaped = sql_definition.replace("'", "''") if sql_definition else ""
        label_escaped = label.replace("'", "''") if label else ""
        query = f"""
        UPDATE {CATALOG_NAME}.{SCHEMA_NAME}.eol_definition
        SET name = '{name_escaped}', sql_definition = '{sql_def_escaped}', label = '{label_escaped}'
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
        # Simplified query - name should be unique within project, so no need for both filters
        query = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.eol_definition WHERE name = '{name_escaped}' AND project_id = {project_id}"
        result = sqlQuery(query)
        if not result.empty:
            return result.iloc[0]
        return None
    except Exception as e:
        print(f"Error fetching EOL definition: {e}")
        return None

def get_eol_definition_by_id(eol_id: int):
    """Get a specific EOL definition by ID."""
    print(f"get_eol_definition_by_id called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, eol_id={eol_id}")
    try:
        query = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.eol_definition WHERE id = {eol_id}"
        result = sqlQuery(query)
        if not result.empty:
            return result.iloc[0]
        return None
    except Exception as e:
        print(f"Error fetching EOL definition: {e}")
        return None

def check_eol_view_exists(eol_id: int) -> bool:
    """Check if the EOL view exists for the given EOL definition."""
    print(f"check_eol_view_exists called with eol_id={eol_id}")
    try:
        # Get the EOL definition
        eol_def = get_eol_definition_by_id(eol_id)
        if eol_def is None:
            print(f"No EOL definition found for eol_id={eol_id}")
            return False
            
        # Get the project details to construct view name
        project_id = eol_def.get('project_id')
        project = get_project_by_id(project_id)
        if project is None:
            print(f"No project found for project_id={project_id}")
            return False
            
        catalog = project.get('catalog')
        schema = project.get('schema')
        view_name = eol_def.get('name')
        
        # Check if view exists
        check_query = f"SHOW TABLES IN {catalog}.{schema} LIKE '{view_name}'"
        result = sqlQuery(check_query)
        exists = not result.empty
        print(f"View {catalog}.{schema}.{view_name} exists: {exists}")
        return exists
        
    except Exception as e:
        print(f"Error checking if EOL view exists: {e}")
        return False

def get_eol_view_columns(eol_id: int) -> list:
    """Get columns from the EOL definition view."""
    print(f"get_eol_view_columns called with eol_id={eol_id}")
    try:
        # Get the EOL definition
        eol_def = get_eol_definition_by_id(eol_id)
        if eol_def is None:
            print(f"No EOL definition found for eol_id={eol_id}")
            return []
        
        sql_definition = eol_def.get('sql_definition')
        if not sql_definition:
            print(f"No SQL definition found for eol_id={eol_id}")
            return []
        
        print(f"EOL SQL definition: {sql_definition}")
        
        # Try different approaches to get column information
        try:
            # First, try DESCRIBE with subquery
            describe_query = f"DESCRIBE ({sql_definition})"
            print(f"Trying DESCRIBE query: {describe_query}")
            result = sqlQuery(describe_query)
        except Exception as e1:
            print(f"DESCRIBE subquery failed: {e1}")
            try:
                # Try creating a temporary view and describing it
                temp_view_name = f"temp_eol_view_{eol_id}"
                create_view_query = f"CREATE OR REPLACE TEMPORARY VIEW {temp_view_name} AS {sql_definition}"
                print(f"Creating temp view: {create_view_query}")
                sqlQuery(create_view_query)
                
                describe_query = f"DESCRIBE {temp_view_name}"
                print(f"Describing temp view: {describe_query}")
                result = sqlQuery(describe_query)
                
                # Clean up the temporary view
                try:
                    sqlQuery(f"DROP VIEW {temp_view_name}")
                except:
                    pass
            except Exception as e2:
                print(f"Temporary view approach failed: {e2}")
                try:
                    # Last resort: try LIMIT 0 to get schema
                    schema_query = f"SELECT * FROM ({sql_definition}) LIMIT 0"
                    print(f"Trying schema query: {schema_query}")
                    result = sqlQuery(schema_query)
                    # Convert result columns to a DataFrame that looks like DESCRIBE output
                    if not result.empty or result.columns.tolist():
                        column_names = [str(col) for col in result.columns.tolist()]
                        print(f"Found columns from schema query: {column_names}")
                        return column_names
                    return []
                except Exception as e3:
                    print(f"Schema query approach failed: {e3}")
                    return []
        
        if result.empty:
            return []
        
        # Extract column names from the describe result
        column_names = []
        for _, row in result.iterrows():
            col_name = row.get('col_name') or row.get('column_name') or row.get('name')
            if col_name:
                column_names.append(str(col_name))
        
        print(f"Found EOL view columns: {column_names}")
        return column_names
        
    except Exception as e:
        print(f"Error getting EOL view columns: {e}")
        return []

def get_eol_view_timestamp_columns(eol_id: int) -> list:
    """Get timestamp/date columns from the EOL definition view."""
    print(f"get_eol_view_timestamp_columns called with eol_id={eol_id}")
    try:
        # Get the EOL definition
        eol_def = get_eol_definition_by_id(eol_id)
        if eol_def is None:
            print(f"No EOL definition found for eol_id={eol_id}")
            return []
        
        sql_definition = eol_def.get('sql_definition')
        if not sql_definition:
            print(f"No SQL definition found for eol_id={eol_id}")
            return []
        
        # Try different approaches to get column information with data types
        try:
            # First, try DESCRIBE with subquery
            describe_query = f"DESCRIBE ({sql_definition})"
            print(f"Trying DESCRIBE query for timestamps: {describe_query}")
            result = sqlQuery(describe_query)
        except Exception as e1:
            print(f"DESCRIBE subquery failed for timestamps: {e1}")
            try:
                # Try creating a temporary view and describing it
                temp_view_name = f"temp_eol_view_{eol_id}_ts"
                create_view_query = f"CREATE OR REPLACE TEMPORARY VIEW {temp_view_name} AS {sql_definition}"
                print(f"Creating temp view for timestamps: {create_view_query}")
                sqlQuery(create_view_query)
                
                describe_query = f"DESCRIBE {temp_view_name}"
                print(f"Describing temp view for timestamps: {describe_query}")
                result = sqlQuery(describe_query)
                
                # Clean up the temporary view
                try:
                    sqlQuery(f"DROP VIEW {temp_view_name}")
                except:
                    pass
            except Exception as e2:
                print(f"Temporary view approach failed for timestamps: {e2}")
                # For timestamps, we need data types, so if we can't get DESCRIBE to work,
                # we'll return an empty list rather than trying LIMIT 0 (which doesn't give types)
                return []
        
        if result.empty:
            return []
        
        # Filter for timestamp/date columns
        timestamp_columns = []
        for _, row in result.iterrows():
            col_name = row.get('col_name') or row.get('column_name') or row.get('name')
            data_type = row.get('data_type') or row.get('type') or ''
            
            if col_name and data_type:
                data_type_lower = str(data_type).lower()
                # Check if the data type indicates a timestamp or date column
                if any(ts_type in data_type_lower for ts_type in ['timestamp', 'date', 'datetime', 'time']):
                    timestamp_columns.append(str(col_name))
        
        print(f"Found EOL view timestamp columns: {timestamp_columns}")
        return timestamp_columns
        
    except Exception as e:
        print(f"Error getting EOL view timestamp columns: {e}")
        return []
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
        feats_sql = ', '.join(f"'{f.replace(chr(39), chr(39)*2)}'" for f in feats)
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
        feats_sql = ', '.join(f"'{f.replace(chr(39), chr(39)*2)}'" for f in feats)
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

## Dataset CRUD operations
def get_datasets(project_id: int = None) -> pd.DataFrame:
    """Fetch datasets, optionally filtered by project_id."""
    print(f"get_datasets called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, project_id={project_id}")
    try:
        if project_id is not None:
            query = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.datasets WHERE project_id = {project_id} ORDER BY name"
        else:
            query = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.datasets ORDER BY name"
        return sqlQuery(query)
    except Exception as e:
        print(f"Error fetching datasets: {e}")
        return pd.DataFrame()

def create_dataset(project_id: int, feature_lookup_id: int, name: str, evaluation_type: str, percentage: float, materialized: bool) -> bool:
    """Create a new dataset in the database."""
    print(f"create_dataset called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, project_id={project_id}")
    try:
        name_escaped = name.replace("'", "''") if name else ''
        evaluation_type_escaped = evaluation_type.replace("'", "''") if evaluation_type else ''
        
        # Don't set table names until materialization - they include timestamps
        query = f"""
        INSERT INTO {CATALOG_NAME}.{SCHEMA_NAME}.datasets 
        (project_id, feature_lookup_id, name, evaluation_type, percentage, materialized, training_table_name, eval_table_name)
        VALUES ({project_id}, {feature_lookup_id}, '{name_escaped}', '{evaluation_type_escaped}', {percentage}, {materialized}, NULL, NULL)
        """
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error creating dataset: {e}")
        return False

def get_dataset_by_id(dataset_id: int):
    """Get a specific dataset by ID."""
    print(f"get_dataset_by_id called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, id={dataset_id}")
    try:
        query = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.datasets WHERE id = {dataset_id}"
        result = sqlQuery(query)
        if not result.empty:
            return result.iloc[0]
        return None
    except Exception as e:
        print(f"Error fetching dataset: {e}")
        return None

def update_dataset(dataset_id: int, name: str, feature_lookup_id: int, evaluation_type: str, percentage: float, materialized: bool) -> bool:
    """Update an existing dataset in the database."""
    print(f"update_dataset called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, id={dataset_id}")
    try:
        name_escaped = name.replace("'", "''") if name else ''
        evaluation_type_escaped = evaluation_type.replace("'", "''") if evaluation_type else ''
        
        # Don't update table names - they're set during materialization
        query = f"""
        UPDATE {CATALOG_NAME}.{SCHEMA_NAME}.datasets SET 
        name = '{name_escaped}', 
        feature_lookup_id = {feature_lookup_id}, 
        evaluation_type = '{evaluation_type_escaped}', 
        percentage = {percentage}, 
        materialized = {materialized}
        WHERE id = {dataset_id}
        """
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error updating dataset: {e}")
        return False

def delete_dataset(dataset_id: int) -> bool:
    """Delete a dataset from the database."""
    print(f"delete_dataset called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, id={dataset_id}")
    try:
        query = f"DELETE FROM {CATALOG_NAME}.{SCHEMA_NAME}.datasets WHERE id = {dataset_id}"
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error deleting dataset: {e}")
        return False

def update_dataset_run_info(dataset_id: int, run_id: str, run_url: str, training_table_name: str = None, eval_table_name: str = None) -> bool:
    """Update dataset with run information after materialization."""
    print(f"update_dataset_run_info called with catalog={CATALOG_NAME}, schema={SCHEMA_NAME}, id={dataset_id}, run_id={run_id}")
    try:
        run_id_escaped = run_id.replace("'", "''") if run_id else ''
        run_url_escaped = run_url.replace("'", "''") if run_url else ''
        
        # Build update query
        update_parts = [
            f"run_id = '{run_id_escaped}'",
            f"run_url = '{run_url_escaped}'",
            f"materialized = true"
        ]
        
        if training_table_name:
            training_table_escaped = training_table_name.replace("'", "''")
            update_parts.append(f"training_table_name = '{training_table_escaped}'")
        
        if eval_table_name:
            eval_table_escaped = eval_table_name.replace("'", "''")
            update_parts.append(f"eval_table_name = '{eval_table_escaped}'")
        
        query = f"""
        UPDATE {CATALOG_NAME}.{SCHEMA_NAME}.datasets SET 
        {', '.join(update_parts)}
        WHERE id = {dataset_id}
        """
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error updating dataset run info: {e}")
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
  
def get_columns(catalog: str = None, schema: str = None, table: str = None) -> list:
    """Fetch all column names from the specified catalog.schema.table."""
    cat = catalog or CATALOG_NAME
    sch = schema or SCHEMA_NAME
    if not table or not cat or not sch:
        return []
    try:
        # Describe table to get column metadata
        query = f"DESCRIBE TABLE {cat}.{sch}.{table}"
        df = sqlQuery(query)
        if df.empty:
            return []
        # Determine column containing column names
        cols = [c for c in df.columns if 'col' in c.lower()]
        col = cols[0] if cols else df.columns[0]
        return [str(v) for v in df[col].tolist()]
    except Exception as e:
        print(f"Error fetching columns for {cat}.{sch}.{table}: {e}")
        return []

# Training Runs Management Functions

def create_training_run(project_id: int, dataset_id: int, job_name: str, parameters: str = None, created_by: str = None):
    """Create a new training run record."""
    print(f"create_training_run called for project_id={project_id}, dataset_id={dataset_id}")
    try:
        params_sql = f"'{parameters}'" if parameters else "NULL"
        created_by_sql = f"'{created_by}'" if created_by else "NULL"
        
        query = f"""
        INSERT INTO {CATALOG_NAME}.{SCHEMA_NAME}.training_runs 
        (project_id, dataset_id, job_name, parameters, created_by, status)
        VALUES ({project_id}, {dataset_id}, '{job_name}', {params_sql}, {created_by_sql}, 'PENDING')
        """
        sqlQuery(query)
        
        # Get the last inserted ID
        id_query = f"""
        SELECT MAX(id) as id FROM {CATALOG_NAME}.{SCHEMA_NAME}.training_runs 
        WHERE project_id = {project_id} AND dataset_id = {dataset_id}
        """
        result = sqlQuery(id_query)
        if not result.empty:
            return int(result.iloc[0]['id'])
        return None
    except Exception as e:
        print(f"Error creating training run: {e}")
        return None

def update_training_run(run_id: int, **kwargs):
    """Update a training run record with new information."""
    print(f"update_training_run called for run_id={run_id}")
    try:
        # Build SET clause from kwargs
        set_clauses = []
        for key, value in kwargs.items():
            if value is None:
                set_clauses.append(f"{key} = NULL")
            elif isinstance(value, str):
                # Escape single quotes
                value_escaped = value.replace("'", "''")
                set_clauses.append(f"{key} = '{value_escaped}'")
            elif isinstance(value, (int, float)):
                set_clauses.append(f"{key} = {value}")
            else:
                # Convert to string for complex types
                value_str = str(value).replace("'", "''")
                set_clauses.append(f"{key} = '{value_str}'")
        
        if not set_clauses:
            return False
            
        set_clauses.append("updated_at = CURRENT_TIMESTAMP")
        set_clause = ", ".join(set_clauses)
        
        query = f"""
        UPDATE {CATALOG_NAME}.{SCHEMA_NAME}.training_runs
        SET {set_clause}
        WHERE id = {run_id}
        """
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error updating training run {run_id}: {e}")
        return False

def get_training_runs(project_id: int = None, dataset_id: int = None, limit: int = 100) -> pd.DataFrame:
    """Get training runs, optionally filtered by project or dataset."""
    print(f"get_training_runs called with project_id={project_id}, dataset_id={dataset_id}")
    try:
        where_clauses = []
        if project_id:
            where_clauses.append(f"project_id = {project_id}")
        if dataset_id:
            where_clauses.append(f"dataset_id = {dataset_id}")
        
        where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        query = f"""
        SELECT tr.*, p.name as project_name, d.name as dataset_name
        FROM {CATALOG_NAME}.{SCHEMA_NAME}.training_runs tr
        LEFT JOIN {CATALOG_NAME}.{SCHEMA_NAME}.project p ON tr.project_id = p.id
        LEFT JOIN {CATALOG_NAME}.{SCHEMA_NAME}.datasets d ON tr.dataset_id = d.id
        {where_clause}
        ORDER BY tr.created_at DESC
        LIMIT {limit}
        """
        return sqlQuery(query)
    except Exception as e:
        print(f"Error fetching training runs: {e}")
        return pd.DataFrame()

def get_training_run_by_job_id(job_id: int) -> pd.Series:
    """Get a training run by its Databricks job ID."""
    print(f"get_training_run_by_job_id called for job_id={job_id}")
    try:
        query = f"""
        SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.training_runs
        WHERE job_id = {job_id}
        ORDER BY created_at DESC
        LIMIT 1
        """
        result = sqlQuery(query)
        if not result.empty:
            return result.iloc[0]
        return None
    except Exception as e:
        print(f"Error fetching training run by job_id {job_id}: {e}")
        return None