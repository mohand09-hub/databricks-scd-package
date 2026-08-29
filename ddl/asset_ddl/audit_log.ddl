-- ============================================================================
-- Audit Log Table DDL
-- ============================================================================
-- Captures metadata about each data load operation (batch-level).
-- Placeholders: {catalog} and {schema} are replaced at runtime.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.audit_log (
  batch_id            STRING        NOT NULL COMMENT 'Unique batch identifier (UUID)',
  load_group_name     STRING        NOT NULL COMMENT 'Load group from ingestion_config',
  dataset_name        STRING        NOT NULL COMMENT 'Dataset name from ingestion_config',
  source_table_list   STRING               COMMENT 'Source table(s) loaded',
  target_uc_catalog   STRING               COMMENT 'Target catalog',
  target_uc_schema    STRING               COMMENT 'Target schema',
  target_uc_table     STRING        NOT NULL COMMENT 'Target table loaded',
  load_type           STRING               COMMENT 'OVERWRITE, APPEND, UPSERT, SCD1, SCD2',
  rows_inserted       BIGINT        DEFAULT 0 COMMENT 'Rows inserted',
  rows_deleted        BIGINT        DEFAULT 0 COMMENT 'Rows deleted',
  rows_updated        BIGINT        DEFAULT 0 COMMENT 'Rows updated',
  rows_total          BIGINT        DEFAULT 0 COMMENT 'Total rows in target after load',
  start_time          TIMESTAMP     NOT NULL COMMENT 'Load start time',
  end_time            TIMESTAMP             COMMENT 'Load end time',
  duration_seconds    BIGINT               COMMENT 'Load duration in seconds',
  status              STRING        NOT NULL COMMENT 'SUCCESS, FAILED, RUNNING',
  error_message       STRING               COMMENT 'Error details if failed',
  
  -- Default audit columns (consistent across all tables)
  record_created_timestamp TIMESTAMP DEFAULT current_timestamp() COMMENT 'Record creation timestamp',
  record_created_by       STRING        DEFAULT current_user()     COMMENT 'Record created by',
  application_name        STRING                                   COMMENT 'Application name from metadata',
  reporting_date          DATE                                     COMMENT 'Reporting date from metadata',
  created_by          STRING        DEFAULT current_user() COMMENT 'User who ran the load',
  created_at          TIMESTAMP     DEFAULT current_timestamp() COMMENT 'Audit record created at',
  
  CONSTRAINT pk_audit_log PRIMARY KEY (batch_id)
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);