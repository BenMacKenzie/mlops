#!/usr/bin/env python3
"""
Parameterized SQL Execution Script

This script reads database configuration from YAML and executes SQL files with parameter substitution.
"""

import os
import sys
import yaml
import re
from databricks import sql
from databricks.sdk.core import Config


def get_sql_connection():
    """Get a connection to the Databricks SQL warehouse."""
    cfg = Config()  # Pull environment variables for auth
    warehouse_id = os.getenv('DATABRICKS_WAREHOUSE_ID')
    if not warehouse_id:
        raise ValueError("DATABRICKS_WAREHOUSE_ID environment variable is required")
    
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: cfg.authenticate
    )


def load_config(config_file: str = "db_config.yaml"):
    """Load database configuration from YAML file."""
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)


def substitute_parameters(sql_content: str, config: dict) -> str:
    """Substitute parameters in SQL content with values from config."""
    catalog = config['database']['catalog']
    schema = config['database']['schema']
    
    # Replace placeholders with actual values
    sql_content = sql_content.replace('{catalog}', catalog)
    sql_content = sql_content.replace('{schema}', schema)
    
    return sql_content


def execute_sql_file(sql_file: str, config_file: str = "db_config.yaml"):
    """Execute SQL statements from a file with parameter substitution."""
    try:
        # Load configuration
        config = load_config(config_file)
        print(f"Using catalog: {config['database']['catalog']}")
        print(f"Using schema: {config['database']['schema']}")
        
        # Read the SQL file
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        # Substitute parameters
        sql_content = substitute_parameters(sql_content, config)
        
        # Split into individual statements and filter out comments
        statements = []
        lines = sql_content.split('\n')
        current_statement = []
        
        for line in lines:
            line = line.strip()
            # Skip empty lines and comment lines
            if not line or line.startswith('--'):
                continue
            
            current_statement.append(line)
            
            # If line ends with semicolon, we have a complete statement
            if line.endswith(';'):
                full_statement = ' '.join(current_statement)
                if full_statement.strip():
                    statements.append(full_statement)
                current_statement = []
        
        # Handle any remaining statement without semicolon
        if current_statement:
            full_statement = ' '.join(current_statement)
            if full_statement.strip():
                statements.append(full_statement)
        
        # Execute each statement
        with get_sql_connection() as connection:
            with connection.cursor() as cursor:
                for i, statement in enumerate(statements, 1):
                    if statement:  # Skip empty statements
                        print(f"Executing statement {i}: {statement[:50]}...")
                        cursor.execute(statement)
                        print(f"✅ Statement {i} executed successfully")
        
        print(f"\n✅ Successfully executed {len(statements)} SQL statements from {sql_file}")
        
    except Exception as e:
        print(f"❌ Error executing SQL file: {e}")
        raise


def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python run_sql_parametrized.py <sql_file> [config_file]")
        print("Example: python run_sql_parametrized.py init_tables.sql")
        print("Example: python run_sql_parametrized.py init_tables.sql custom_config.yaml")
        sys.exit(1)
    
    sql_file = sys.argv[1]
    config_file = sys.argv[2] if len(sys.argv) > 2 else "db_config.yaml"
    
    if not os.path.exists(sql_file):
        print(f"❌ SQL file '{sql_file}' not found")
        sys.exit(1)
    
    if not os.path.exists(config_file):
        print(f"❌ Config file '{config_file}' not found")
        sys.exit(1)
    
    print(f"Executing SQL file: {sql_file}")
    print(f"Using config file: {config_file}")
    execute_sql_file(sql_file, config_file)


if __name__ == "__main__":
    main() 