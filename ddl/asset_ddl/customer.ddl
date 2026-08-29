-- ============================================================================
-- Target Table DDL: customer (TPCH transformed)
-- ============================================================================
-- Placeholders: {catalog} and {schema} are replaced at runtime
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.customer (
  c_custkey       BIGINT        NOT NULL COMMENT 'Customer key',
  c_name          STRING               COMMENT 'Customer name',
  c_address       STRING               COMMENT 'Customer address',
  c_nationkey     BIGINT               COMMENT 'Nation key',
  c_phone         STRING               COMMENT 'Customer phone',
  c_acctbal       DECIMAL(18,2)        COMMENT 'Account balance',
  c_mktsegment    STRING               COMMENT 'Market segment',
  c_comment       STRING               COMMENT 'Customer comment',
  
  -- Default audit columns (auto-populated by TransformationRunner)
  record_created_timestamp TIMESTAMP DEFAULT current_timestamp() COMMENT 'Record creation timestamp',
  record_created_by       STRING        DEFAULT current_user()     COMMENT 'Record created by',
  batch_id                STRING                                   COMMENT 'Batch identifier from audit log',
  application_name        STRING                                   COMMENT 'Application name from metadata',
  reporting_date          DATE                                     COMMENT 'Reporting date from metadata',
  
  -- SCD2 tracking columns
  effective_start_date    TIMESTAMP     NOT NULL DEFAULT current_timestamp() COMMENT 'SCD2 effective start date',
  effective_end_date      TIMESTAMP     NOT NULL DEFAULT to_timestamp('9999-12-31 23:59:59') COMMENT 'SCD2 effective end date',
  is_current              BOOLEAN       NOT NULL DEFAULT true COMMENT 'SCD2 current flag',
  config_version          INTEGER       NOT NULL DEFAULT 1 COMMENT 'SCD2 version number',
  
  CONSTRAINT pk_customer PRIMARY KEY (c_custkey, effective_start_date)
)
USING DELTA
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);