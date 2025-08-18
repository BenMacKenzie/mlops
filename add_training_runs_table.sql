-- Add training_runs table to track all training jobs and runs
-- This table will store historical information about training runs

-- Drop table if exists (for development - remove in production)
DROP TABLE IF EXISTS {catalog}.{schema}.training_runs;

-- Create training_runs table
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.training_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    project_id BIGINT NOT NULL,
    dataset_id BIGINT NOT NULL,
    job_id BIGINT,  -- Databricks job ID
    job_name STRING,
    run_id BIGINT,  -- Databricks run ID
    experiment_name STRING,
    parameters STRING,  -- JSON string of parameters used
    status STRING,  -- PENDING, RUNNING, SUCCESS, FAILED, TERMINATED, SKIPPED
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    created_by STRING,  -- User who initiated the run
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message STRING,  -- Store error details if failed
    metrics STRING,  -- JSON string of metrics from the run
    model_uri STRING,  -- MLflow model URI if model was registered
    databricks_run_url STRING  -- Direct link to Databricks run
);

-- Add foreign key references (optional, depends on your Databricks setup)
-- ALTER TABLE {catalog}.{schema}.training_runs 
-- ADD CONSTRAINT fk_training_runs_project FOREIGN KEY (project_id) 
-- REFERENCES {catalog}.{schema}.project(id);

-- ALTER TABLE {catalog}.{schema}.training_runs 
-- ADD CONSTRAINT fk_training_runs_dataset FOREIGN KEY (dataset_id) 
-- REFERENCES {catalog}.{schema}.datasets(id);

-- Create indexes for better query performance
-- CREATE INDEX idx_training_runs_project ON {catalog}.{schema}.training_runs(project_id);
-- CREATE INDEX idx_training_runs_dataset ON {catalog}.{schema}.training_runs(dataset_id);
-- CREATE INDEX idx_training_runs_status ON {catalog}.{schema}.training_runs(status);
-- CREATE INDEX idx_training_runs_created_at ON {catalog}.{schema}.training_runs(created_at);