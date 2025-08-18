-- Initialize tables for MLOps project
-- This script creates the necessary catalogs, schemas, and tables
Parameters: {catalog}, {schema}, {catalog}.{schema}

--Create catalog if it doesn't exist
CREATE CATALOG IF NOT EXISTS {catalog};

--Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};

drop table if exists {catalog}.{schema}.project;

--Create project table
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.project (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    name STRING NOT NULL,
    description STRING not null,
    catalog STRING NOT NULL,
    schema STRING NOT NULL,
    git_url string not null,
    training_notebook string not null
);

drop table if exists {catalog}.{schema}.eol_definition;

create table if not exists {catalog}.{schema}.eol_definition (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    project_id BIGINT NOT NULL,
    name STRING NOT NULL,
    sql_definition STRING NOT NULL,
    label  STRING
);

drop table if exists {catalog}.{schema}.feature_lookups;

create table if not exists {catalog}.{schema}.feature_lookups (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    project_id BIGINT NOT NULL,
    eol_id BIGINT,
    name STRING NOT NULL,
    features array<string>
);

-- drop TABLE  {catalog}.{schema}.datasets;

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.datasets (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    project_id BIGINT NOT NULL,
    feature_lookup_id BIGINT NOT NULL,
    name STRING NOT NULL,
    evaluation_type STRING NOT NULL,
    percentage DECIMAL(10,2),
    materialized BOOLEAN NOT NULL,
    run_id STRING,
    run_url STRING,
    training_table_name STRING,
    eval_table_name STRING
);


-- Drop table if exists (for development - remove in production)
-- DROP TABLE IF EXISTS {catalog}.{schema}.training_runs;

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
