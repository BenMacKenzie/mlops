#!/bin/bash

# Script to create PostgreSQL database and initialize tables
# This script creates the database AND runs the init_db.sql file

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Source .env file from project root
source "$SCRIPT_DIR/../.env"

echo "Creating database $DB_NAME if it doesn't exist..."

# Create the database if it doesn't exist
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c "CREATE DATABASE $DB_NAME;" 2>/dev/null

if [ $? -eq 0 ]; then
    echo "Database $DB_NAME created successfully!"
else
    echo "Database $DB_NAME already exists or could not be created."
fi

echo ""
echo "Initializing tables..."

# Run the PostgreSQL init script
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f "$SCRIPT_DIR/init_db.sql"

if [ $? -eq 0 ]; then
    echo ""
    echo "Database initialization complete!"
else
    echo ""
    echo "Error: Table initialization failed!"
    echo "You can re-run table creation with: ./run_sql.sh"
    exit 1
fi 