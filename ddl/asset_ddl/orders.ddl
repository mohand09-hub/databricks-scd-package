-- ============================================================================
-- Target Table DDL: orders (TPCH transformed)
-- ============================================================================
-- Placeholders: {catalog} and {schema} are replaced at runtime
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.orders (
  o_orderkey      BIGINT        NOT NULL COMMENT 'Order key',
  o_custkey       BIGINT               COMMENT 'Customer key',
  o_orderstatus   STRING               COMMENT 'Order status (O, F, P)',
  o_totalprice    DECIMAL(18,2)        COMMENT 'Total order price',
  o_orderdate     DATE                COMMENT 'Order date',
  o_orderpriority STRING               COMMENT 'Order priority',
  o_clerk         STRING               COMMENT 'Clerk who took the order',
  o_shippriority  INT                  COMMENT 'Ship priority',
  o_comment       STRING               COMMENT 'Order comment',
  order_year      INT                  COMMENT 'Derived: year extracted from o_orderdate',
  
  -- Default audit columns (auto-populated by TransformationRunner)
  record_created_timestamp TIMESTAMP DEFAULT current_timestamp() COMMENT 'Record creation timestamp',
  record_created_by       STRING        DEFAULT current_user()     COMMENT 'Record created by',
  batch_id                STRING                                   COMMENT 'Batch identifier from audit log',
  application_name        STRING                                   COMMENT 'Application name from metadata',
  reporting_date          DATE                                     COMMENT 'Reporting date from metadata',
  
  CONSTRAINT pk_orders PRIMARY KEY (o_orderkey)
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);