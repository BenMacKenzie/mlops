"""
Universal database module that works with both Databricks and PostgreSQL.
Database type is configured in db_config.yaml.
"""

import yaml
import pandas as pd
import os
import psycopg2
import pandas as pd

# Load DB config once
try:
    with open('db_config.yaml', 'r') as _f:
        _db_conf = yaml.safe_load(_f)
    
    DB_TYPE = _db_conf['database'].get('type', 'databricks').lower()
    
    if DB_TYPE == 'databricks':
        CATALOG_NAME = _db_conf['database']['catalog']
        SCHEMA_NAME = _db_conf['database']['schema']
        print(f"Loaded Databricks config: catalog={CATALOG_NAME}, schema={SCHEMA_NAME}")
    elif DB_TYPE == 'postgres':
        CATALOG_NAME = None  # PostgreSQL doesn't have catalogs
        SCHEMA_NAME = _db_conf['database']['postgres']['schema']
        print(f"Loaded PostgreSQL config: schema={SCHEMA_NAME}")
    else:
        raise ValueError(f"Unsupported database type: {DB_TYPE}")
        
except Exception as e:
    print(f"Error loading DB config: {e}")
    DB_TYPE = None
    CATALOG_NAME = None
    SCHEMA_NAME = None

def get_table_name(table):
    """Get the fully qualified table name based on database type."""
    if DB_TYPE == 'databricks':
        return f"{CATALOG_NAME}.{SCHEMA_NAME}.{table}"
    elif DB_TYPE == 'postgres':
        # Use fully qualified name for postgres
        return f"{SCHEMA_NAME}.{table}"
    else:
        return table

def execute_insert_returning(query: str) -> pd.DataFrame:
    """Execute an INSERT query with RETURNING clause and return the result."""
    print(f"execute_insert_returning executing: {query}")
    
    try:
        if DB_TYPE == 'postgres':
            import psycopg2
            # Get connection details from environment variables
            host = os.getenv('DB_HOST', 'localhost')
            port = os.getenv('DB_PORT', '5432')
            user = os.getenv('DB_USER', 'postgres')
            password = os.getenv('DB_PASSWORD', '')
            database = os.getenv('DB_NAME', 'mlops')
            
            # Create connection string
            conn_str = f"host={host} port={port} user={user} password={password} dbname={database}"
            
            # Use explicit connection to ensure commit
            conn = psycopg2.connect(conn_str)
            try:
                cursor = conn.cursor()
                cursor.execute(f"SET search_path TO {SCHEMA_NAME}")
                cursor.execute(query)
                
                # Fetch the returned data
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                
                conn.commit()
                print(f"INSERT with RETURNING executed successfully, {len(data)} rows returned")
                cursor.close()
                
                # Convert to DataFrame
                return pd.DataFrame(data, columns=columns)
            finally:
                conn.close()
        else:
            # For non-PostgreSQL, fall back to regular sqlQuery
            return sqlQuery(query)
    except Exception as e:
        print(f"Error executing INSERT with RETURNING '{query}': {e}")
        return pd.DataFrame()

def execute_non_query(query: str) -> bool:
    """Execute a non-SELECT query (INSERT, UPDATE, DELETE) and commit the transaction."""
    print(f"execute_non_query executing: {query}")
    
    try:
        if DB_TYPE == 'postgres':
            import psycopg2
            # Get connection details from environment variables
            host = os.getenv('DB_HOST', 'localhost')
            port = os.getenv('DB_PORT', '5432')
            user = os.getenv('DB_USER', 'postgres')
            password = os.getenv('DB_PASSWORD', '')
            database = os.getenv('DB_NAME', 'mlops')
            
            # Create connection string
            conn_str = f"host={host} port={port} user={user} password={password} dbname={database}"
            
            # Use explicit connection to ensure commit
            conn = psycopg2.connect(conn_str)
            try:
                cursor = conn.cursor()
                cursor.execute(f"SET search_path TO {SCHEMA_NAME}")
                cursor.execute(query)
                affected_rows = cursor.rowcount
                conn.commit()
                print(f"Query executed successfully, {affected_rows} rows affected")
                cursor.close()
                return True
            finally:
                conn.close()
        elif DB_TYPE == 'databricks':
            # For Databricks, use sqlQuery (it auto-commits)
            sqlQuery(query)
            return True
        else:
            raise ValueError(f"Unsupported database type: {DB_TYPE}")
    except Exception as e:
        print(f"Error executing non-query '{query}': {e}")
        return False

def sqlQuery(query: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    print(f"sqlQuery executing: {query}")
    
    try:
        if DB_TYPE == 'databricks':
            from databricks import sql
            from databricks.sdk.core import Config
            
            cfg = Config()  # Pull environment variables for auth
            with sql.connect(
                server_hostname=cfg.host,
                http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
                credentials_provider=lambda: cfg.authenticate
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    return cursor.fetchall_arrow().to_pandas()
                    
        elif DB_TYPE == 'postgres':
            # Get connection details from environment variables
            host = os.getenv('DB_HOST', 'localhost')
            port = os.getenv('DB_PORT', '5432')
            user = os.getenv('DB_USER', 'postgres')
            password = os.getenv('DB_PASSWORD', '')
            database = os.getenv('DB_NAME', 'mlops')
            
            # Create connection string
            conn_str = f"host={host} port={port} user={user} password={password} dbname={database}"
            
            # Execute query and return DataFrame
            with psycopg2.connect(conn_str) as conn:
                # Check if this is a SELECT query or a modification query
                query_upper = query.strip().upper()
                if query_upper.startswith(('SELECT', 'WITH', 'SHOW', 'DESCRIBE')):
                    # Read query - set search_path and execute in same cursor
                    with conn.cursor() as cursor:
                        cursor.execute(f"SET search_path TO {SCHEMA_NAME}")
                        print(f"DEBUG: Set search_path to {SCHEMA_NAME}")
                        cursor.execute(query)
                        rows = cursor.fetchall()
                        if rows:
                            # Get column names
                            col_names = [desc[0] for desc in cursor.description]
                            # Create DataFrame
                            return pd.DataFrame(rows, columns=col_names)
                        else:
                            return pd.DataFrame()
                else:
                    # Write query (INSERT, UPDATE, DELETE, CREATE, etc.) - set search_path and execute
                    with conn.cursor() as cursor:
                        cursor.execute(f"SET search_path TO {SCHEMA_NAME}")
                        cursor.execute(query)
                        conn.commit()
                    return pd.DataFrame()  # Return empty DataFrame for non-SELECT queries
        else:
            raise ValueError(f"Unsupported database type: {DB_TYPE}")
    except Exception as e:
        print(f"Error executing query '{query}': {e}")
        return pd.DataFrame()

def get_projects():
    """Fetch all projects from the database."""
    print(f"get_projects called with db_type={DB_TYPE}")
    try:
        table_name = get_table_name('project')
        query = f"SELECT * FROM {table_name} ORDER BY name"
        return sqlQuery(query)
    except Exception as e:
        print(f"Error fetching projects: {e}")
        return pd.DataFrame()

def create_project(name: str, description: str, catalog: str, schema: str, git_url: str, training_notebook: str):
    """Create a new project in the database."""
    print(f"create_project called with db_type={DB_TYPE}")
    try:
        table_name = get_table_name('project')
        
        if DB_TYPE == 'postgres':
            # Use PostgreSQL INSERT with RETURNING
            query = f"""
            INSERT INTO {table_name} (name, description, catalog, schema, git_url, training_notebook)
            VALUES ('{name}', '{description}', '{catalog}', '{schema}', '{git_url}', '{training_notebook}')
            RETURNING id
            """
            result = execute_insert_returning(query)
        elif DB_TYPE == 'databricks':
            # Use Databricks INSERT syntax  
            query = f"""
            INSERT INTO {table_name} (name, description, catalog, schema, git_url, training_notebook)
            VALUES ('{name}', '{description}', '{catalog}', '{schema}', '{git_url}', '{training_notebook}')
            """
            success = execute_non_query(query)
            if not success:
                return None
            # Get the last inserted ID
            get_id_query = f"""
            SELECT id FROM {table_name}
            WHERE name = '{name}' AND description = '{description}' AND catalog = '{catalog}'
              AND schema = '{schema}' AND git_url = '{git_url}' AND training_notebook = '{training_notebook}'
            ORDER BY id DESC
            LIMIT 1
            """
            result = sqlQuery(get_id_query)
        
        if DB_TYPE == 'postgres' and not result.empty:
            return int(result.iloc[0]['id'])
        elif DB_TYPE == 'databricks':
            # Get the ID of the newly created project for Databricks
            get_id_query = f"""
            SELECT id FROM {table_name}
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
    print(f"update_project called with db_type={DB_TYPE}, project_id={project_id}")
    try:
        table_name = get_table_name('project')
        query = f"""
        UPDATE {table_name}
        SET name = '{name}', description = '{description}', catalog = '{catalog}',
            schema = '{schema}', git_url = '{git_url}', training_notebook = '{training_notebook}'
        WHERE id = {project_id}
        """
        return execute_non_query(query)
    except Exception as e:
        print(f"Error updating project: {e}")
        return False

def delete_project(project_id: int):
    """Delete a project from the database."""
    print(f"delete_project called with db_type={DB_TYPE}, project_id={project_id}")
    try:
        table_name = get_table_name('project')
        query = f"DELETE FROM {table_name} WHERE id = {project_id}"
        return execute_non_query(query)
    except Exception as e:
        print(f"Error deleting project: {e}")
        return False

def get_project_by_id(project_id: int):
    """Get a specific project by ID."""
    print(f"get_project_by_id called with db_type={DB_TYPE}, project_id={project_id}")
    try:
        table_name = get_table_name('project')
        query = f"SELECT * FROM {table_name} WHERE id = {project_id}"
        result = sqlQuery(query)
        if not result.empty:
            return result.iloc[0]
        return None
    except Exception as e:
        print(f"Error fetching project: {e}")
        return None

def get_eol_definitions(project_id: int = None):
    """Fetch EOL definitions, optionally filtered by project_id."""
    print(f"get_eol_definitions called with db_type={DB_TYPE}, project_id={project_id}")
    try:
        table_name = get_table_name('eol_definition')
        print(f"DEBUG: get_eol_definitions table_name: {table_name}")
        if project_id is not None:
            query = f"SELECT * FROM {table_name} WHERE project_id = {project_id} ORDER BY name"
        else:
            query = f"SELECT * FROM {table_name} ORDER BY name"
        print(f"DEBUG: get_eol_definitions query: {query}")
        result = sqlQuery(query)
        print(f"DEBUG: get_eol_definitions result: {len(result)} rows")
        if not result.empty:
            print(f"DEBUG: get_eol_definitions columns: {result.columns.tolist()}")
            print(f"DEBUG: get_eol_definitions first row: {result.iloc[0].to_dict()}")
        return result
    except Exception as e:
        print(f"Error fetching EOL definitions: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def create_eol_definition(name: str, sql_definition: str, project_id: int, label: str = None):
    """Create a new EOL definition in the database."""
    print(f"create_eol_definition called with db_type={DB_TYPE}, project_id={project_id}")
    try:
        table_name = get_table_name('eol_definition')
        # Escape single quotes in strings
        name_escaped = name.replace("'", "''") if name else ""
        sql_def_escaped = sql_definition.replace("'", "''") if sql_definition else ""
        label_escaped = label.replace("'", "''") if label else ""
        
        if DB_TYPE == 'postgres':
            # Use proper NULL handling for PostgreSQL
            label_value = f"'{label_escaped}'" if label else "NULL"
        else:
            # Databricks version
            label_value = f"'{label_escaped}'"
        
        query = f"""
        INSERT INTO {table_name} (name, sql_definition, project_id, label)
        VALUES ('{name_escaped}', '{sql_def_escaped}', {project_id}, {label_value})
        """
        return execute_non_query(query)
    except Exception as e:
        print(f"Error creating EOL definition: {e}")
        return False

def update_eol_definition(old_name: str, name: str, sql_definition: str, project_id: int, label: str = None):
    """Update an existing EOL definition in the database."""
    print(f"update_eol_definition called with db_type={DB_TYPE}, project_id={project_id}")
    try:
        table_name = get_table_name('eol_definition')
        # Escape single quotes in strings
        old_name_escaped = old_name.replace("'", "''") if old_name else ""
        name_escaped = name.replace("'", "''") if name else ""
        sql_def_escaped = sql_definition.replace("'", "''") if sql_definition else ""
        label_escaped = label.replace("'", "''") if label else ""
        
        if DB_TYPE == 'postgres':
            label_value = f"'{label_escaped}'" if label else "NULL"
        else:
            label_value = f"'{label_escaped}'"
        
        query = f"""
        UPDATE {table_name}
        SET name = '{name_escaped}', sql_definition = '{sql_def_escaped}', label = {label_value}
        WHERE name = '{old_name_escaped}' AND project_id = {project_id}
        """
        return execute_non_query(query)
    except Exception as e:
        print(f"Error updating EOL definition: {e}")
        return False

def delete_eol_definition(name: str, project_id: int):
    """Delete an EOL definition from the database."""
    print(f"delete_eol_definition called with db_type={DB_TYPE}, project_id={project_id}")
    try:
        table_name = get_table_name('eol_definition')
        # Escape single quotes in name
        name_escaped = name.replace("'", "''") if name else ""
        query = f"DELETE FROM {table_name} WHERE name = '{name_escaped}' AND project_id = {project_id}"
        return execute_non_query(query)
    except Exception as e:
        print(f"Error deleting EOL definition: {e}")
        return False

def get_eol_definition_by_name(name: str, project_id: int):
    """Get a specific EOL definition by name."""
    print(f"get_eol_definition_by_name called with db_type={DB_TYPE}, project_id={project_id}")
    try:
        table_name = get_table_name('eol_definition')
        # Escape single quotes in name
        name_escaped = name.replace("'", "''") if name else ""
        query = f"SELECT * FROM {table_name} WHERE name = '{name_escaped}' AND project_id = {project_id}"
        result = sqlQuery(query)
        if not result.empty:
            return result.iloc[0]
        return None
    except Exception as e:
        print(f"Error fetching EOL definition: {e}")
        return None

def get_eol_definition_by_id(eol_id: int):
    """Get a specific EOL definition by ID."""
    print(f"get_eol_definition_by_id called with db_type={DB_TYPE}, eol_id={eol_id}")
    try:
        table_name = get_table_name('eol_definition')
        query = f"SELECT * FROM {table_name} WHERE id = {eol_id}"
        result = sqlQuery(query)
        if not result.empty:
            return result.iloc[0]
        return None
    except Exception as e:
        print(f"Error fetching EOL definition: {e}")
        return None

def get_eol_view_columns(eol_id: int) -> list:
    """Get columns from the EOL definition view."""
    print(f"get_eol_view_columns called with db_type={DB_TYPE}, eol_id={eol_id}")
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
        
        # EOL views are always created in Databricks (ML data source), so always use Databricks to get columns
        try:
            # Import Databricks-specific sqlQuery for querying EOL view columns
            from utils.db_metadata import sqlQuery as databricks_sqlQuery
            
            # Use DESCRIBE on the SQL definition to get columns
            describe_query = f"DESCRIBE ({sql_definition})"
            print(f"Trying describe query against Databricks: {describe_query}")
            result = databricks_sqlQuery(describe_query)
            
            # Databricks result processing
            if not result.empty:
                column_names = []
                for _, row in result.iterrows():
                    col_name = row.get('col_name') or row.get('column_name') or row.get('name')
                    if col_name:
                        column_names.append(str(col_name))
                print(f"Found EOL view columns: {column_names}")
                return column_names
                    
        except Exception as e1:
            print(f"Error getting EOL view columns: {e1}")
            return []
        
        return []
        
    except Exception as e:
        print(f"Error getting EOL view columns: {e}")
        return []

def get_eol_view_timestamp_columns(eol_id: int) -> list:
    """Get timestamp/date columns from the EOL definition view."""
    print(f"get_eol_view_timestamp_columns called with db_type={DB_TYPE}, eol_id={eol_id}")
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
        
        # EOL views are always created in Databricks (ML data source), so always use Databricks to get columns
        try:
            # Import Databricks-specific sqlQuery for querying EOL view columns
            from utils.db_metadata import sqlQuery as databricks_sqlQuery
            
            # Databricks approach - get column info with data types
            describe_query = f"DESCRIBE ({sql_definition})"
            result = databricks_sqlQuery(describe_query)
            
            timestamp_columns = []
            for _, row in result.iterrows():
                col_name = row.get('col_name') or row.get('column_name') or row.get('name')
                data_type = row.get('data_type') or row.get('type') or ''
                
                if col_name and data_type:
                    data_type_lower = str(data_type).lower()
                    if any(ts_type in data_type_lower for ts_type in ['timestamp', 'date', 'datetime', 'time']):
                        timestamp_columns.append(str(col_name))
            
            print(f"Found EOL view timestamp columns: {timestamp_columns}")
            return timestamp_columns
            
        except Exception as e2:
            print(f"Error getting timestamp columns: {e2}")
            return []
        
    except Exception as e:
        print(f"Error getting EOL view timestamp columns: {e}")
        return []

# Feature Lookup CRUD operations
def get_feature_lookups(project_id: int = None) -> pd.DataFrame:
    """Fetch feature lookups, optionally filtered by project_id."""
    print(f"get_feature_lookups called with db_type={DB_TYPE}, project_id={project_id}")
    try:
        table_name = get_table_name('feature_lookups')
        if project_id is not None:
            query = f"SELECT * FROM {table_name} WHERE project_id = {project_id} ORDER BY name"
        else:
            query = f"SELECT * FROM {table_name} ORDER BY name"
        return sqlQuery(query)
    except Exception as e:
        print(f"Error fetching feature lookups: {e}")
        return pd.DataFrame()

def create_feature_lookup(name: str, eol_id: int, project_id: int, features: list) -> bool:
    """Create a new feature lookup in the database."""
    print(f"create_feature_lookup called with db_type={DB_TYPE}, project_id={project_id}")
    try:
        import json
        table_name = get_table_name('feature_lookups')
        name_escaped = name.replace("'", "''") if name else ''
        
        # Convert features to JSON strings if they are dictionaries
        feats = []
        for f in features:
            if f:  # Skip empty/None features
                if isinstance(f, dict):
                    feats.append(json.dumps(f))
                else:
                    feats.append(str(f).strip())
        
        try:
            eol_int = int(eol_id)
            eol_sql = str(eol_int)
        except (TypeError, ValueError):
            eol_sql = 'NULL'
        
        if DB_TYPE == 'databricks':
            # Databricks array syntax - handle empty array
            if feats:
                feats_sql = ', '.join(f"'{f.replace(chr(39), chr(39)*2)}'" for f in feats)
                features_value = f"array({feats_sql})"
            else:
                features_value = "array()"  # Empty array syntax for Databricks
            query = (
                f"INSERT INTO {table_name} "
                f"(project_id, eol_id, name, features) VALUES "
                f"({project_id}, {eol_sql}, '{name_escaped}', {features_value})"
            )
        elif DB_TYPE == 'postgres':
            # PostgreSQL array syntax - handle empty array
            if feats:
                escaped_feats = []
                for f in feats:
                    # Escape double quotes and backslashes for PostgreSQL array elements
                    escaped = f.replace('\\', '\\\\').replace('"', '\\"')
                    escaped_feats.append(f'"{escaped}"')
                feats_sql = '{' + ','.join(escaped_feats) + '}'
            else:
                feats_sql = '{}'  # Empty array syntax for PostgreSQL
            query = (
                f"INSERT INTO {table_name} "
                f"(project_id, eol_id, name, features) VALUES "
                f"({project_id}, {eol_sql}, '{name_escaped}', '{feats_sql}')"
            )
        
        print(f"DEBUG: Executing query: {query}")
        sqlQuery(query)
        print(f"DEBUG: Query executed successfully")
        return True
    except Exception as e:
        print(f"Error creating feature lookup: {e}")
        print(f"Failed query was: {query if 'query' in locals() else 'Query not constructed'}")
        return False

def get_feature_lookup_by_id(feature_lookup_id: int):
    """Get a specific feature lookup by ID."""
    print(f"get_feature_lookup_by_id called with db_type={DB_TYPE}, id={feature_lookup_id}")
    try:
        table_name = get_table_name('feature_lookups')
        query = f"SELECT * FROM {table_name} WHERE id = {feature_lookup_id}"
        result = sqlQuery(query)
        if not result.empty:
            return result.iloc[0]
        return None
    except Exception as e:
        print(f"Error fetching feature lookup: {e}")
        return None

def update_feature_lookup(feature_lookup_id: int, name: str, eol_id: int, features: list) -> bool:
    """Update an existing feature lookup in the database."""
    print(f"update_feature_lookup called with db_type={DB_TYPE}, id={feature_lookup_id}")
    try:
        import json
        table_name = get_table_name('feature_lookups')
        name_escaped = name.replace("'", "''") if name else ''
        
        # Convert features to JSON strings if they are dictionaries
        feats = []
        for f in features:
            if f:  # Skip empty/None features
                if isinstance(f, dict):
                    feats.append(json.dumps(f))
                else:
                    feats.append(str(f).strip())
        
        try:
            eol_int = int(eol_id)
            eol_sql = str(eol_int)
        except (TypeError, ValueError):
            eol_sql = 'NULL'
        
        if DB_TYPE == 'databricks':
            # Databricks array syntax
            feats_sql = ', '.join(f"'{f.replace(chr(39), chr(39)*2)}'" for f in feats)
            query = (
                f"UPDATE {table_name} SET "
                f"name = '{name_escaped}', eol_id = {eol_sql}, features = array({feats_sql}) "
                f"WHERE id = {feature_lookup_id}"
            )
        elif DB_TYPE == 'postgres':
            # PostgreSQL array syntax - escape quotes properly for JSON strings
            escaped_feats = []
            for f in feats:
                # Escape double quotes and backslashes for PostgreSQL array elements
                escaped = f.replace('\\', '\\\\').replace('"', '\\"')
                escaped_feats.append(f'"{escaped}"')
            feats_sql = '{' + ','.join(escaped_feats) + '}'
            query = (
                f"UPDATE {table_name} SET "
                f"name = '{name_escaped}', eol_id = {eol_sql}, features = '{feats_sql}' "
                f"WHERE id = {feature_lookup_id}"
            )
        
        print(f"DEBUG: Executing update query: {query}")
        sqlQuery(query)
        print(f"DEBUG: Update query executed successfully")
        return True
    except Exception as e:
        print(f"Error updating feature lookup: {e}")
        print(f"Failed query was: {query if 'query' in locals() else 'Query not constructed'}")
        return False

def delete_feature_lookup(feature_lookup_id: int) -> bool:
    """Delete a feature lookup from the database."""
    print(f"delete_feature_lookup called with db_type={DB_TYPE}, id={feature_lookup_id}")
    try:
        table_name = get_table_name('feature_lookups')
        query = f"DELETE FROM {table_name} WHERE id = {feature_lookup_id}"
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error deleting feature lookup: {e}")
        return False

# Dataset CRUD operations
def get_datasets(project_id: int = None) -> pd.DataFrame:
    """Fetch datasets, optionally filtered by project_id. Also checks and updates status for active runs."""
    print(f"get_datasets called with db_type={DB_TYPE}, project_id={project_id}")
    try:
        table_name = get_table_name('datasets')
        if project_id is not None:
            query = f"SELECT * FROM {table_name} WHERE project_id = {project_id} ORDER BY name"
        else:
            query = f"SELECT * FROM {table_name} ORDER BY name"
        
        df = sqlQuery(query)
        
        # Check and update status for any datasets with PENDING or RUNNING status
        if not df.empty:
            print(f"DEBUG: Checking {len(df)} datasets for status updates")
            for idx, row in df.iterrows():
                current_status = row.get('status', 'NOT_STARTED')
                run_id = row.get('run_id')
                dataset_id = row.get('id')
                
                print(f"DEBUG: Dataset {dataset_id}: current_status={current_status}, run_id={run_id}")
                
                # Check if we have a run_id and status needs updating
                # Also check NOT_STARTED with run_id (means we missed the initial status update)
                if run_id and current_status in ['PENDING', 'RUNNING', 'NOT_STARTED']:
                    print(f"Checking status for dataset {row['id']} with run_id {run_id}")
                    new_status = check_databricks_run_status(run_id)
                    
                    if new_status and new_status != current_status:
                        print(f"Updating dataset {row['id']} status from {current_status} to {new_status}")
                        
                        # Update the database
                        update_query = f"""
                        UPDATE {table_name} 
                        SET status = '{new_status}'
                        WHERE id = {row['id']}
                        """
                        
                        # If status is SUCCESS, also set materialized = TRUE
                        if new_status == 'SUCCESS':
                            if DB_TYPE == 'postgres':
                                materialized_val = 'TRUE'
                            else:
                                materialized_val = 'true'
                            update_query = f"""
                            UPDATE {table_name} 
                            SET status = '{new_status}', materialized = {materialized_val}
                            WHERE id = {row['id']}
                            """
                        
                        sqlQuery(update_query)
                        
                        # Update the dataframe
                        df.at[idx, 'status'] = new_status
                        if new_status == 'SUCCESS':
                            df.at[idx, 'materialized'] = True
        
        return df
    except Exception as e:
        print(f"Error fetching datasets: {e}")
        return pd.DataFrame()

def create_dataset(project_id: int, feature_lookup_id: int, name: str, evaluation_type: str, percentage: float, materialized: bool) -> bool:
    """Create a new dataset in the database."""
    print(f"create_dataset called with db_type={DB_TYPE}, project_id={project_id}")
    try:
        table_name = get_table_name('datasets')
        name_escaped = name.replace("'", "''") if name else ''
        evaluation_type_escaped = evaluation_type.replace("'", "''") if evaluation_type else ''
        
        # Handle NULL feature_lookup_id
        try:
            fl_int = int(feature_lookup_id)
            fl_sql = str(fl_int)
        except (TypeError, ValueError):
            fl_sql = 'NULL'
        
        # Convert boolean to database-specific format
        if DB_TYPE == 'postgres':
            materialized_val = 'TRUE' if materialized else 'FALSE'
        else:
            materialized_val = str(materialized).lower()
        
        query = f"""
        INSERT INTO {table_name} 
        (project_id, feature_lookup_id, name, evaluation_type, percentage, materialized, training_table_name, eval_table_name)
        VALUES ({project_id}, {fl_sql}, '{name_escaped}', '{evaluation_type_escaped}', {percentage}, {materialized_val}, NULL, NULL)
        """
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error creating dataset: {e}")
        return False

def get_dataset_by_id(dataset_id: int):
    """Get a specific dataset by ID."""
    print(f"get_dataset_by_id called with db_type={DB_TYPE}, id={dataset_id}")
    try:
        table_name = get_table_name('datasets')
        query = f"SELECT * FROM {table_name} WHERE id = {dataset_id}"
        result = sqlQuery(query)
        if not result.empty:
            return result.iloc[0]
        return None
    except Exception as e:
        print(f"Error fetching dataset: {e}")
        return None

def update_dataset(dataset_id: int, name: str, feature_lookup_id: int, evaluation_type: str, percentage: float) -> bool:
    """Update user-editable fields of an existing dataset in the database."""
    print(f"update_dataset called with db_type={DB_TYPE}, id={dataset_id}")
    try:
        table_name = get_table_name('datasets')
        name_escaped = name.replace("'", "''") if name else ''
        evaluation_type_escaped = evaluation_type.replace("'", "''") if evaluation_type else ''
        
        query = f"""
        UPDATE {table_name} SET 
        name = '{name_escaped}', 
        feature_lookup_id = {feature_lookup_id}, 
        evaluation_type = '{evaluation_type_escaped}', 
        percentage = {percentage}
        WHERE id = {dataset_id}
        """
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error updating dataset: {e}")
        return False

def update_dataset_materialized_status(dataset_id: int, materialized: bool) -> bool:
    """Update only the materialized status of a dataset - used internally by materialize job."""
    print(f"update_dataset_materialized_status called with db_type={DB_TYPE}, id={dataset_id}, materialized={materialized}")
    try:
        table_name = get_table_name('datasets')
        
        # Convert boolean to database-specific format
        if DB_TYPE == 'postgres':
            materialized_val = 'TRUE' if materialized else 'FALSE'
        else:
            materialized_val = str(materialized).lower()
        
        query = f"""
        UPDATE {table_name} SET 
        materialized = {materialized_val}
        WHERE id = {dataset_id}
        """
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error updating dataset materialized status: {e}")
        return False

def delete_dataset(dataset_id: int) -> bool:
    """Delete a dataset from the database."""
    print(f"delete_dataset called with db_type={DB_TYPE}, id={dataset_id}")
    try:
        table_name = get_table_name('datasets')
        query = f"DELETE FROM {table_name} WHERE id = {dataset_id}"
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error deleting dataset: {e}")
        return False

def check_databricks_run_status(run_id: str):
    """Check the status of a Databricks run."""
    print(f"DEBUG: Checking Databricks status for run_id: {run_id}")
    try:
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient()
        
        # Get run details
        print(f"DEBUG: Getting run details for run_id: {run_id}")
        run = client.jobs.get_run(run_id=int(run_id))
        print(f"DEBUG: Run details retrieved: {run}")
        
        # Map Databricks run states to our status values
        state_mapping = {
            'PENDING': 'PENDING',
            'RUNNING': 'RUNNING',
            'TERMINATING': 'RUNNING',
            'TERMINATED': 'TERMINATED',
            'SKIPPED': 'SKIPPED',
            'SUCCESS': 'SUCCESS',
            'FAILED': 'FAILED',
            'TIMEOUT': 'FAILED',
            'CANCELED': 'TERMINATED',
            'INTERNAL_ERROR': 'FAILED'
        }
        
        # Get the state from the run - check tasks for more granular status
        if hasattr(run, 'tasks') and run.tasks:
            # Look at the first task state (assuming single-task jobs)
            task = run.tasks[0]
            if hasattr(task, 'state') and hasattr(task.state, 'life_cycle_state'):
                db_state = task.state.life_cycle_state
                print(f"DEBUG: Databricks task life_cycle_state: {db_state}")
                
                # Convert enum to string for mapping
                db_state_str = str(db_state).split('.')[-1] if hasattr(db_state, 'name') else str(db_state)
                print(f"DEBUG: Converted task state string: {db_state_str}")
                
                status = state_mapping.get(db_state_str, 'PENDING')
                
                # Check for result state for any terminated task
                if hasattr(task.state, 'result_state') and task.state.result_state:
                    result_state = task.state.result_state
                    result_state_str = str(result_state).split('.')[-1] if hasattr(result_state, 'name') else str(result_state)
                    print(f"DEBUG: Databricks task result_state: {result_state_str}")
                    
                    if result_state_str == 'SUCCESS':
                        status = 'SUCCESS'
                    elif result_state_str == 'FAILED':
                        status = 'FAILED'
                    elif result_state_str == 'CANCELED':
                        status = 'TERMINATED'
                
                print(f"DEBUG: Final mapped status: {status} (Databricks task state: {db_state_str})")
                return status
        
        # Fallback to overall job state if no tasks
        if hasattr(run, 'state') and hasattr(run.state, 'life_cycle_state'):
            db_state = run.state.life_cycle_state
            print(f"DEBUG: Databricks overall life_cycle_state: {db_state}")
            
            # Convert enum to string for mapping
            db_state_str = str(db_state).split('.')[-1] if hasattr(db_state, 'name') else str(db_state)
            print(f"DEBUG: Converted overall state string: {db_state_str}")
            
            status = state_mapping.get(db_state_str, 'PENDING')
            
            # Check for result state for any terminated job (TERMINATED or INTERNAL_ERROR)
            if hasattr(run.state, 'result_state') and run.state.result_state:
                result_state = run.state.result_state
                result_state_str = str(result_state).split('.')[-1] if hasattr(result_state, 'name') else str(result_state)
                print(f"DEBUG: Databricks overall result_state: {result_state_str}")
                
                if result_state_str == 'SUCCESS':
                    status = 'SUCCESS'
                elif result_state_str == 'FAILED':
                    status = 'FAILED'
                elif result_state_str == 'CANCELED':
                    status = 'TERMINATED'
            
            print(f"DEBUG: Final mapped status: {status} (Databricks overall state: {db_state_str})")
            return status
        else:
            print(f"DEBUG: Could not determine status for run {run_id} - no state attribute")
            print(f"DEBUG: Run object attributes: {dir(run) if run else 'None'}")
            return None
            
    except Exception as e:
        print(f"Error checking Databricks run status: {e}")
        return None

def update_dataset_run_info(dataset_id: int, run_id: str, run_url: str, status: str = 'PENDING', training_table_name: str = None, eval_table_name: str = None) -> bool:
    """Update dataset with run information and status."""
    print(f"update_dataset_run_info called with db_type={DB_TYPE}, id={dataset_id}, run_id={run_id}, status={status}")
    try:
        table_name = get_table_name('datasets')
        run_id_escaped = run_id.replace("'", "''") if run_id else ''
        run_url_escaped = run_url.replace("'", "''") if run_url else ''
        status_escaped = status.replace("'", "''") if status else 'PENDING'
        
        # Only set materialized=TRUE when status is SUCCESS
        if DB_TYPE == 'postgres':
            materialized_val = 'TRUE' if status == 'SUCCESS' else 'FALSE'
        else:
            materialized_val = 'true' if status == 'SUCCESS' else 'false'
        
        # Build update query
        update_parts = [
            f"run_id = '{run_id_escaped}'",
            f"run_url = '{run_url_escaped}'",
            f"status = '{status_escaped}'",
            f"materialized = {materialized_val}"
        ]
        
        if training_table_name:
            training_table_escaped = training_table_name.replace("'", "''")
            update_parts.append(f"training_table_name = '{training_table_escaped}'")
        
        if eval_table_name:
            eval_table_escaped = eval_table_name.replace("'", "''")
            update_parts.append(f"eval_table_name = '{eval_table_escaped}'")
        
        query = f"""
        UPDATE {table_name} SET 
        {', '.join(update_parts)}
        WHERE id = {dataset_id}
        """
        sqlQuery(query)
        return True
    except Exception as e:
        print(f"Error updating dataset run info: {e}")
        return False

# Training Runs Management Functions
def create_training_run(project_id: int, dataset_id: int, job_name: str, parameters: str = None, created_by: str = None):
    """Create a new training run record."""
    print(f"create_training_run called for project_id={project_id}, dataset_id={dataset_id}")
    try:
        table_name = get_table_name('training_runs')
        params_sql = f"'{parameters}'" if parameters else "NULL"
        created_by_sql = f"'{created_by}'" if created_by else "NULL"
        
        if DB_TYPE == 'postgres':
            # Use RETURNING for PostgreSQL
            query = f"""
            INSERT INTO {table_name} 
            (project_id, dataset_id, job_name, parameters, created_by, status)
            VALUES ({project_id}, {dataset_id}, '{job_name}', {params_sql}, {created_by_sql}, 'PENDING')
            RETURNING id
            """
            result = sqlQuery(query)
            if not result.empty:
                return int(result.iloc[0]['id'])
        else:
            # Databricks version
            query = f"""
            INSERT INTO {table_name} 
            (project_id, dataset_id, job_name, parameters, created_by, status)
            VALUES ({project_id}, {dataset_id}, '{job_name}', {params_sql}, {created_by_sql}, 'PENDING')
            """
            sqlQuery(query)
            
            # Get the last inserted ID
            id_query = f"""
            SELECT MAX(id) as id FROM {table_name} 
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
        table_name = get_table_name('training_runs')
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
        
        if DB_TYPE == 'postgres':
            set_clauses.append("updated_at = CURRENT_TIMESTAMP")
        else:
            set_clauses.append("updated_at = CURRENT_TIMESTAMP")
            
        set_clause = ", ".join(set_clauses)
        
        query = f"""
        UPDATE {table_name}
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
        training_runs_table = get_table_name('training_runs')
        project_table = get_table_name('project')
        datasets_table = get_table_name('datasets')
        
        where_clauses = []
        if project_id:
            where_clauses.append(f"tr.project_id = {project_id}")
        if dataset_id:
            where_clauses.append(f"tr.dataset_id = {dataset_id}")
        
        where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        query = f"""
        SELECT tr.*, p.name as project_name, d.name as dataset_name
        FROM {training_runs_table} tr
        LEFT JOIN {project_table} p ON tr.project_id = p.id
        LEFT JOIN {datasets_table} d ON tr.dataset_id = d.id
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
        table_name = get_table_name('training_runs')
        query = f"""
        SELECT * FROM {table_name}
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