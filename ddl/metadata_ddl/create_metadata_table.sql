-- ============================================================================
-- Metadata Configuration Table DDL (SCD Type 2)
-- ============================================================================
-- Placeholders: {catalog} and {schema} are replaced at runtime
-- ============================================================================

CREATE CATALOG IF NOT EXISTS {catalog};

CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.ingestion_config (
  -- Identity & Grouping
  application_name STRING NOT NULL COMMENT 'Application name',
  load_group_name STRING NOT NULL COMMENT 'Load group for orchestration',
  dataset_name STRING NOT NULL COMMENT 'Dataset identifier',
  dataset_layer STRING COMMENT 'Data layer: raw, base, curated, gold',
  dataset_layer_desc STRING COMMENT 'Layer description',
  dataset_desc STRING COMMENT 'Dataset description',
  
  -- Execution Control
  dataset_load_sequence INT NOT NULL COMMENT 'Execution order (same = parallel)',
  is_active BOOLEAN DEFAULT true COMMENT 'Enable/disable dataset',
  has_custom BOOLEAN DEFAULT false COMMENT 'Uses custom transformation',
  
  -- Source Configuration
  source_load_type STRING COMMENT 'FULL or INCREMENTAL',
  source_table_list STRING COMMENT 'Source paths (comma-separated)',
  backload_history_in_days INT COMMENT 'Days of history for backload',
  
  -- Target Configuration
  target_load_type STRING NOT NULL COMMENT 'UPSERT, OVERWRITE, APPEND, SCD1, SCD2',
  target_uc_catalog STRING NOT NULL COMMENT 'Target catalog',
  target_uc_schema STRING NOT NULL COMMENT 'Target schema',
  target_uc_table STRING NOT NULL COMMENT 'Target table',
  create_target_table_if_not_exists BOOLEAN DEFAULT true COMMENT 'Auto-create target table if it does not exist',
  
  -- Table Attributes
  target_primary_key_columns STRING COMMENT 'Primary keys (comma-separated)',
  merge_key_columns STRING COMMENT 'Merge keys (comma-separated)',
  partition_columns STRING COMMENT 'Partition columns (comma-separated)',
  clustering_columns STRING COMMENT 'Clustering columns (comma-separated)',
  target_table_properties STRING COMMENT 'Additional table properties',
  
  -- SCD Configuration
  scd_columns STRING COMMENT 'SCD tracking columns (comma-separated)',
  
  -- Custom Transformation
  transformation_path STRING COMMENT 'Path to transformation YAML',
  
  -- Processing Timestamps
  processing_start_timestamp TIMESTAMP COMMENT 'Processing window start',
  processing_end_timestamp TIMESTAMP COMMENT 'Processing window end',
  
  -- Notification & Reporting
  notification_email_id STRING COMMENT 'Email addresses (comma-separated)',
  dataset_notification_flag BOOLEAN DEFAULT false COMMENT 'Enable notifications',
  reporting_frequency STRING COMMENT 'DAILY, WEEKLY, MONTHLY',
  reporting_date DATE COMMENT 'Last reporting date',
  
  -- Runtime Metadata
  stage_name STRING COMMENT 'Current stage',
  method_start_time TIMESTAMP COMMENT 'Method start',
  method_end_time TIMESTAMP COMMENT 'Method end',
  
  -- Audit Fields
  created_by STRING DEFAULT current_user() COMMENT 'Created by',
  created_at TIMESTAMP DEFAULT current_timestamp() COMMENT 'Created at',
  updated_by STRING DEFAULT current_user() COMMENT 'Updated by',
  updated_at TIMESTAMP DEFAULT current_timestamp() COMMENT 'Updated at',
  
  -- SCD Type 2 Tracking
  effective_start_date TIMESTAMP DEFAULT current_timestamp() NOT NULL COMMENT 'Version effective date',
  effective_end_date TIMESTAMP DEFAULT timestamp('9999-12-31 23:59:59') NOT NULL COMMENT 'Version expiry date',
  is_current BOOLEAN DEFAULT true NOT NULL COMMENT 'Current version flag',
  config_version INT DEFAULT 1 NOT NULL COMMENT 'Version number',
  
  -- Constraints
  CONSTRAINT pk_ingestion_config PRIMARY KEY (load_group_name, dataset_name, effective_start_date)
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported'
);