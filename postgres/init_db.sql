-- Initialize tables for MLOps project (PostgreSQL version)
-- This script creates the necessary schemas and tables for PostgreSQL

-- Note: PostgreSQL doesn't have catalogs like Databricks, only schemas
-- We'll use the schema concept and ignore catalog for PostgreSQL

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS app;

-- Set search path to app schema
SET search_path TO app;

-- Drop existing tables if they exist (for clean reinstall)
DROP TABLE IF EXISTS app.training_runs CASCADE;
DROP TABLE IF EXISTS app.datasets CASCADE;
DROP TABLE IF EXISTS app.feature_lookups CASCADE;
DROP TABLE IF EXISTS app.eol_definition CASCADE;
DROP TABLE IF EXISTS app.project CASCADE;

-- Create project table
CREATE TABLE app.project (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    catalog VARCHAR(255) NOT NULL,  -- Keep for compatibility with Databricks version
    schema VARCHAR(255) NOT NULL,   -- Keep for compatibility with Databricks version
    git_url TEXT NOT NULL,
    training_notebook TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create eol_definition table
CREATE TABLE app.eol_definition (
    id BIGSERIAL PRIMARY KEY,
    project_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    sql_definition TEXT NOT NULL,
    label VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (project_id) REFERENCES app.project(id) ON DELETE CASCADE
);

-- Create feature_lookups table
CREATE TABLE app.feature_lookups (
    id BIGSERIAL PRIMARY KEY,
    project_id BIGINT NOT NULL,
    eol_id BIGINT,
    name VARCHAR(255) NOT NULL,
    features TEXT[],  -- PostgreSQL array of text
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (project_id) REFERENCES app.project(id) ON DELETE CASCADE,
    FOREIGN KEY (eol_id) REFERENCES app.eol_definition(id) ON DELETE SET NULL
);

-- Create datasets table
CREATE TABLE app.datasets (
    id BIGSERIAL PRIMARY KEY,
    project_id BIGINT NOT NULL,
    feature_lookup_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    evaluation_type VARCHAR(50) NOT NULL,
    percentage DECIMAL(10,2),
    status VARCHAR(50) DEFAULT 'NOT_STARTED',
    materialized BOOLEAN NOT NULL DEFAULT FALSE,
    run_id VARCHAR(255),
    run_url TEXT,
    training_table_name VARCHAR(255),
    eval_table_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (project_id) REFERENCES app.project(id) ON DELETE CASCADE,
    FOREIGN KEY (feature_lookup_id) REFERENCES app.feature_lookups(id) ON DELETE CASCADE
);

-- Create training_runs table
CREATE TABLE app.training_runs (
    id BIGSERIAL PRIMARY KEY,
    project_id BIGINT NOT NULL,
    dataset_id BIGINT NOT NULL,
    job_id BIGINT,  -- Databricks job ID
    job_name VARCHAR(255),
    run_id BIGINT,  -- Databricks run ID
    experiment_name VARCHAR(255),
    parameters TEXT,  -- JSON string of parameters used
    status VARCHAR(50),  -- PENDING, RUNNING, SUCCESS, FAILED, TERMINATED, SKIPPED
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    created_by VARCHAR(255),  -- User who initiated the run
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT,  -- Store error details if failed
    metrics TEXT,  -- JSON string of metrics from the run
    model_uri TEXT,  -- MLflow model URI if model was registered
    databricks_run_url TEXT,  -- Direct link to Databricks run
    FOREIGN KEY (project_id) REFERENCES app.project(id) ON DELETE CASCADE,
    FOREIGN KEY (dataset_id) REFERENCES app.datasets(id) ON DELETE CASCADE
);

-- Create indexes for better query performance
CREATE INDEX idx_project_name ON app.project(name);
CREATE INDEX idx_eol_definition_project ON app.eol_definition(project_id);
CREATE INDEX idx_eol_definition_name ON app.eol_definition(name);
CREATE INDEX idx_feature_lookups_project ON app.feature_lookups(project_id);
CREATE INDEX idx_feature_lookups_eol ON app.feature_lookups(eol_id);
CREATE INDEX idx_datasets_project ON app.datasets(project_id);
CREATE INDEX idx_datasets_feature_lookup ON app.datasets(feature_lookup_id);
CREATE INDEX idx_training_runs_project ON app.training_runs(project_id);
CREATE INDEX idx_training_runs_dataset ON app.training_runs(dataset_id);
CREATE INDEX idx_training_runs_status ON app.training_runs(status);

-- Create update timestamp trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply update timestamp triggers to all tables with updated_at column
CREATE TRIGGER update_project_updated_at BEFORE UPDATE ON app.project
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_eol_definition_updated_at BEFORE UPDATE ON app.eol_definition
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_feature_lookups_updated_at BEFORE UPDATE ON app.feature_lookups
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_datasets_updated_at BEFORE UPDATE ON app.datasets
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_training_runs_updated_at BEFORE UPDATE ON app.training_runs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions (adjust as needed for your setup)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA app TO your_app_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA app TO your_app_user;

-- Display created tables
\dt app.*

-- Success message
\echo 'PostgreSQL tables for MLOps project created successfully!'