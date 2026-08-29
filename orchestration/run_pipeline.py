# Databricks notebook source
# DBTITLE 1,Pipeline Overview
# MAGIC %md
# MAGIC # Metadata-Driven Pipeline Runner
# MAGIC
# MAGIC This notebook is the executable entry point for the metadata-driven transformation framework.
# MAGIC
# MAGIC All paths and catalog/schema names are defined as **widget parameters** grouped by module so they can be overridden by a Databricks Job at runtime.
# MAGIC
# MAGIC **Common Parameters:**
# MAGIC | Parameter | Default | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `catalog` | `dev_customer` | Unity Catalog name |
# MAGIC
# MAGIC **Metadata Module Parameters:**
# MAGIC | Parameter | Default | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `metadata_schema` | `metadata` | Schema for ingestion_config table |
# MAGIC | `metadata_ddl_path` | `.../ddl/metadata_ddl/create_metadata_table.sql` | DDL for metadata table |
# MAGIC | `scd_config_path` | `.../ddl/metadata_ddl/scd_config.yaml` | SCD2 YAML config |
# MAGIC | `csv_path` | `.../ddl/metadata_ddl/ingestion_config.csv` | Metadata CSV |
# MAGIC
# MAGIC **Audit Module Parameters:**
# MAGIC | Parameter | Default | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `base_schema` | `base` | Schema for audit log table |
# MAGIC | `audit_ddl_path` | `.../ddl/asset_ddl/audit_log.ddl` | DDL for audit log table |
# MAGIC
# MAGIC **Asset Module Parameters:**
# MAGIC | Parameter | Default | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `asset_ddl_path` | `.../ddl/asset_ddl/orders.ddl` | DDL for orders target table |
# MAGIC
# MAGIC **Flow:**
# MAGIC 1. Step 1 (one-time): Create metadata table from DDL
# MAGIC 2. Step 2 (recurring): Read CSV and apply SCD2 merge
# MAGIC 3. Step 3 (one-time): Create audit table from DDL
# MAGIC 4. Step 4 (one-time): Create target table from DDL
# MAGIC 5. Step 5 (recurring): Run transformation (captures audit log)

# COMMAND ----------

# DBTITLE 1,Define Parameters
# ---------------------------------------------------------------------------
# Widget parameters — override these via Databricks Job base_parameters.
# Organized by module for clarity and modularity.
# ---------------------------------------------------------------------------
import sys

_BASE = "/Workspace/Users/mrdmohu@gmail.com/Metadata_Driven_Transformation_Framework"

# --- Common Parameters ---
dbutils.widgets.text("catalog", "dev_customer")

# --- Metadata Module Parameters ---
dbutils.widgets.text("metadata_schema", "metadata")
dbutils.widgets.text("metadata_ddl_path", f"{_BASE}/ddl/metadata_ddl/create_metadata_table.sql")
dbutils.widgets.text("scd_config_path", f"{_BASE}/ddl/metadata_ddl/scd_config.yaml")
dbutils.widgets.text("csv_path", f"{_BASE}/ddl/metadata_ddl/ingestion_config.csv")

# --- Audit Module Parameters ---
dbutils.widgets.text("base_schema", "base")
dbutils.widgets.text("audit_ddl_path", f"{_BASE}/ddl/asset_ddl/audit_log.ddl")

# --- Asset Module Parameters ---
dbutils.widgets.text("asset_ddl_path", f"{_BASE}/ddl/asset_ddl/orders.ddl")

# --- Retrieve Parameter Values ---
# Common
catalog = dbutils.widgets.get("catalog")

# Metadata module
metadata_schema = dbutils.widgets.get("metadata_schema")
metadata_ddl_path = dbutils.widgets.get("metadata_ddl_path")
scd_config_path = dbutils.widgets.get("scd_config_path")
csv_path = dbutils.widgets.get("csv_path")

# Audit module
base_schema = dbutils.widgets.get("base_schema")
audit_ddl_path = dbutils.widgets.get("audit_ddl_path")

# Asset module
asset_ddl_path = dbutils.widgets.get("asset_ddl_path")

# --- Display Configuration ---
print("=== Pipeline Configuration ===")
print(f"\nCommon:")
print(f"  catalog = {catalog}")
print(f"\nMetadata Module:")
print(f"  metadata_schema = {metadata_schema}")
print(f"\nAudit Module:")
print(f"  base_schema = {base_schema}")
print(f"\nAsset Module:")
print(f"  asset_ddl_path = {asset_ddl_path}")

# COMMAND ----------

# DBTITLE 1,Step 1 Description
# MAGIC %md
# MAGIC ## Step 1: Create Metadata Table (One-Time)
# MAGIC
# MAGIC Runs the metadata DDL to create `{catalog}.{metadata_schema}.ingestion_config`. Skips if the table already exists.

# COMMAND ----------

# DBTITLE 1,Step 1: Run Metadata DDL
sys.path.insert(0, f"{_BASE}/utils")

from master_script import run_ddl

run_ddl(
    spark=spark,
    ddl_path=metadata_ddl_path,
    catalog=catalog,
    schema=metadata_schema,
)

# COMMAND ----------

# DBTITLE 1,Step 2 Description
# MAGIC %md
# MAGIC ## Step 2: Load Metadata from CSV (Recurring)
# MAGIC
# MAGIC Reads the CSV using the table schema and applies SCD Type 2 merge into `{catalog}.{metadata_schema}.ingestion_config`.

# COMMAND ----------

# DBTITLE 1,Step 2: SCD2 Load
from master_script import run_scd2_load

run_scd2_load(
    spark=spark,
    csv_path=csv_path,
    scd_config_path=scd_config_path,
    catalog=catalog,
    schema=metadata_schema,
)

# COMMAND ----------

# DBTITLE 1,Step 3 Description
# MAGIC %md
# MAGIC ## Step 3: Create Audit Table (One-Time)
# MAGIC
# MAGIC Runs the audit DDL to create `{catalog}.{base_schema}.audit_log`. Skips if the table already exists.

# COMMAND ----------

# DBTITLE 1,Step 3: Run Audit DDL
run_ddl(
    spark=spark,
    ddl_path=audit_ddl_path,
    catalog=catalog,
    schema=base_schema,
)

# COMMAND ----------

# DBTITLE 1,Step 4 Description
# MAGIC %md
# MAGIC ## Step 4: Create Target Table (One-Time)
# MAGIC
# MAGIC Runs the asset DDL to create `{catalog}.{base_schema}.orders`. Skips if the table already exists.

# COMMAND ----------

# DBTITLE 1,Step 4: Run Asset DDL
run_ddl(
    spark=spark,
    ddl_path=asset_ddl_path,
    catalog=catalog,
    schema=base_schema,
)

# COMMAND ----------

# DBTITLE 1,Step 5 Description
# MAGIC %md
# MAGIC ## Step 5: Run Transformation (Recurring)
# MAGIC
# MAGIC Reads active metadata from `ingestion_config` and executes the transformation for the `TPCH_TRANSFORMATIONS` load group.

# COMMAND ----------

# DBTITLE 1,Step 5: Run Transformation
from transformation_runner import TransformationRunner

audit_table = f"{catalog}.{base_schema}.audit_log"

runner = TransformationRunner(
    spark=spark,
    metadata_table=f"{catalog}.{metadata_schema}.ingestion_config",
    audit_table=audit_table,
)
runner.run_load_group("TPCH_TRANSFORMATIONS")

print("Pipeline complete.")