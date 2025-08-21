#!/bin/bash

# Script to run SQL files against existing PostgreSQL database
# Usage: ./run_sql.sh [sql_file]
# If no sql_file is specified, defaults to init_db.sql

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Source .env file from project root
source "$SCRIPT_DIR/../.env"

# Get SQL file from argument or use default
SQL_FILE=${1:-"$SCRIPT_DIR/init_db.sql"}

# Check if SQL file exists
if [ ! -f "$SQL_FILE" ]; then
    echo "Error: SQL file not found: $SQL_FILE"
    echo ""
    echo "Usage: $0 [sql_file]"
    echo "  sql_file: Path to SQL file to execute (default: init_db.sql)"
    echo ""
    echo "Available SQL files in postgres directory:"
    ls -1 "$SCRIPT_DIR"/*.sql 2>/dev/null || echo "  No .sql files found"
    exit 1
fi

echo "Running SQL file: $SQL_FILE"
echo "Against database: $DB_NAME on $DB_HOST:$DB_PORT"
echo ""

# Run the SQL file
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f "$SQL_FILE"

# Check if the command was successful
if [ $? -eq 0 ]; then
    echo ""
    echo "SQL file executed successfully!"
else
    echo ""
    echo "Error: SQL execution failed!"
    exit 1
fi