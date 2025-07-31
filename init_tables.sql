-- Initialize tables for MLOps project
-- This script creates the necessary catalogs, schemas, and tables
-- Parameters: {catalog}, {schema}, {catalog}.{schema}

-- Create catalog if it doesn't exist
CREATE CATALOG IF NOT EXISTS {catalog};

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};

-- drop table if exists {catalog}.{schema}.project;

-- Create project table
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.project (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    name STRING NOT NULL,
    description STRING not null,
    catalog STRING NOT NULL,
    schema STRING NOT NULL,
    git_url string not null,
    training_notebook string not null
);

-- drop table if exists {catalog}.{schema}.eol_definition;

create table if not exists {catalog}.{schema}.eol_definition (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    project_id BIGINT NOT NULL,
    name STRING NOT NULL,
    sql_definition STRING NOT NULL
);

create table if not exists {catalog}.{schema}.feature_lookups (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    project_id BIGINT NOT NULL,
    eol_id BIGINT NOT NULL,
    features array<string> NOT NULL
);
-- CREATE TABLE IF NOT EXISTS datasets (
--     id BIGINT GENERATED ALWAYS AS IDENTITY,
--     project_id BIGINT NOT NULL,
--     name STRING NOT NULL,
--     eol_id BIGINT NOT NULL,
--     feature_lookup_definition STRING NOT NULL,
--     evaluation_type STRING NOT NULL,
--     percentage DECIMAL(10,2),
--     materialized BOOLEAN NOT NULL DEFAULT FALSE,
--     training_table_name STRING,
--     eval_table_name STRING,
--     target STRING
-- );
