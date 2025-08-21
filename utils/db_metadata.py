"""
Database metadata utilities for fetching catalogs, schemas, tables, and columns.
This module contains functions for exploring the database structure, separate from
application-specific table operations.
"""

import yaml
import pandas as pd
import os

# Load DB config for sqlQuery function
try:
    with open('db_config.yaml', 'r') as _f:
        _db_conf = yaml.safe_load(_f)
    
    # For metadata (catalog/schema/table/columns), always use Databricks since that's where ML data lives
    DB_TYPE = 'databricks'  # Always use Databricks for metadata
    CATALOG_NAME = _db_conf['database']['catalog']
    SCHEMA_NAME = _db_conf['database']['schema']
    print(f"Loaded Databricks config for metadata: catalog={CATALOG_NAME}, schema={SCHEMA_NAME}")
    print("Note: Metadata queries always use Databricks (ML data source) regardless of application DB type")
        
except Exception as e:
    print(f"Error loading DB config for metadata: {e}")
    DB_TYPE = None
    CATALOG_NAME = None
    SCHEMA_NAME = None

def sqlQuery(query: str) -> pd.DataFrame:
    """Execute a SQL query against Databricks (ML data source) and return the result as a pandas DataFrame."""
    print(f"Metadata sqlQuery executing against Databricks: {query}")
    
    try:
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
    except Exception as e:
        print(f"Error executing metadata query against Databricks: {e}")
        return pd.DataFrame()

def get_catalogs() -> list:
    """Fetch all catalogs available in the metastore."""
    print(f"get_catalogs called for {DB_TYPE}")
    try:
        if DB_TYPE == 'databricks':
            df = sqlQuery("SHOW CATALOGS")
            if df.empty:
                return []
            # Pick first column containing catalog names
            cols = [c for c in df.columns if 'catalog' in c.lower() or 'name' in c.lower()]
            col = cols[0] if cols else df.columns[0]
            return [str(v) for v in df[col].tolist()]
        elif DB_TYPE == 'postgres':
            # PostgreSQL doesn't have catalogs, only databases
            # Return current database name
            df = sqlQuery("SELECT current_database() as catalog_name")
            if not df.empty:
                return [str(df.iloc[0]['catalog_name'])]
            return []
    except Exception as e:
        print(f"Error fetching catalogs: {e}")
        return []

def get_schemas(catalog: str) -> list:
    """Fetch all schemas within the specified catalog."""
    print(f"get_schemas called with catalog={catalog} for {DB_TYPE}")
    try:
        if DB_TYPE == 'databricks':
            query = f"SHOW SCHEMAS IN {catalog}"
            df = sqlQuery(query)
            if df.empty:
                return []
            # Pick first column containing schema names
            cols = [c for c in df.columns if 'schema' in c.lower() or 'name' in c.lower()]
            col = cols[0] if cols else df.columns[0]
            return [str(v) for v in df[col].tolist()]
        elif DB_TYPE == 'postgres':
            # Get all schemas in PostgreSQL
            query = """
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
            """
            df = sqlQuery(query)
            if not df.empty:
                return [str(v) for v in df['schema_name'].tolist()]
            return []
    except Exception as e:
        print(f"Error fetching schemas: {e}")
        return []

def get_tables(catalog: str = None, schema: str = None) -> list:
    """Fetch all table names from the specified or configured catalog and schema."""
    sch = schema or SCHEMA_NAME
    print(f"get_tables called with catalog={catalog}, schema={sch} for {DB_TYPE}")
    try:
        if DB_TYPE == 'databricks':
            cat = catalog or CATALOG_NAME
            query = f"SHOW TABLES IN {cat}.{sch}"
            df = sqlQuery(query)
            if df.empty:
                return []
            # Determine the column containing table names
            cols = [c for c in df.columns if 'name' in c.lower()]
            col = cols[0] if cols else df.columns[0]
            return [str(t) for t in df[col].tolist()]
        elif DB_TYPE == 'postgres':
            # Get tables from PostgreSQL information_schema
            query = f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{sch}' AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
            df = sqlQuery(query)
            if not df.empty:
                return [str(v) for v in df['table_name'].tolist()]
            return []
    except Exception as e:
        print(f"Error fetching tables: {e}")
        return []
  
def get_columns(catalog: str = None, schema: str = None, table: str = None) -> list:
    """Fetch all column names from the specified catalog.schema.table."""
    sch = schema or SCHEMA_NAME
    if not table or not sch:
        return []
        
    print(f"get_columns called with catalog={catalog}, schema={sch}, table={table} for {DB_TYPE}")
    try:
        if DB_TYPE == 'databricks':
            cat = catalog or CATALOG_NAME
            if not cat:
                return []
            # Describe table to get column metadata
            query = f"DESCRIBE TABLE {cat}.{sch}.{table}"
            df = sqlQuery(query)
            if df.empty:
                return []
            # Determine column containing column names
            cols = [c for c in df.columns if 'col' in c.lower()]
            col = cols[0] if cols else df.columns[0]
            return [str(v) for v in df[col].tolist()]
        elif DB_TYPE == 'postgres':
            # Get columns from PostgreSQL information_schema
            query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = '{sch}' AND table_name = '{table}'
            ORDER BY ordinal_position
            """
            df = sqlQuery(query)
            if not df.empty:
                return [str(v) for v in df['column_name'].tolist()]
            return []
    except Exception as e:
        print(f"Error fetching columns for {catalog}.{sch}.{table}: {e}")
        return []