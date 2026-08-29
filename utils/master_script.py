"""
Master Script — DDL Utilities
==============================

Provides DDL execution utilities for creating tables from DDL files.

Note: SCD2 merge logic has been moved to TransformationRunner.
Use this module for DDL creation only.

Usage (from a Databricks notebook or job)::

    from master_script import run_ddl

    base = "/Workspace/Users/mrdmohu@gmail.com/Metadata_Driven_Transformation_Framework"

    # Create metadata table from DDL
    run_ddl(
        spark=spark,
        ddl_path=f"{base}/ddl/metadata_ddl/create_metadata_table.sql",
        catalog="dev_customer",
        schema="metadata",
    )

    # Create target table from asset DDL
    run_ddl(
        spark=spark,
        ddl_path=f"{base}/ddl/asset_ddl/orders.ddl",
        catalog="dev_customer",
        schema="base",
    )
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from metadata_loader import MetadataLoader
from scd_utils import SCDUtils

logger = logging.getLogger(__name__)


def run_ddl(spark: SparkSession, ddl_path: str, catalog: str, schema: str) -> None:
    """Read the DDL file, replace {catalog}/{schema} placeholders, and execute."""
    with open(ddl_path, "r") as fh:
        ddl_content = fh.read()

    ddl_content = ddl_content.replace("{catalog}", catalog).replace("{schema}", schema)

    for statement in ddl_content.split(";"):
        statement = statement.strip()
        if statement:
            logger.info("Executing DDL statement...")
            spark.sql(statement)

    logger.info("DDL execution complete.")


def run_scd2_load(
    spark: SparkSession,
    csv_path: str,
    scd_config_path: str,
    catalog: str,
    schema: str,
) -> None:
    """
    Load metadata CSV and apply SCD Type 2 merge to ingestion_config table.

    This function is specifically for loading the metadata table.
    For data transformations using SCD2, use TransformationRunner with target_load_type='SCD2'.

    Prerequisites: The target table must already exist (created by run_ddl).
    """
    table_name = f"{catalog}.{schema}.ingestion_config"

    # Read the CSV using the table schema (no inference)
    loader = MetadataLoader(spark, csv_path, table_name)
    source_df = loader.read()
    logger.info("CSV loaded with %d rows", source_df.count())

    # Apply SCD2 merge
    scd = SCDUtils(spark, scd_config_path, table_name)
    scd.apply_scd2(source_df)

    logger.info("SCD2 load complete for metadata table")


if __name__ == "__main__":
    _spark = SparkSession.builder.getOrCreate()

    _base = "/Workspace/Users/mrdmohu@gmail.com/Metadata_Driven_Transformation_Framework"

    # Create metadata table from DDL
    run_ddl(
        spark=_spark,
        ddl_path=f"{_base}/ddl/metadata_ddl/create_metadata_table.sql",
        catalog="dev_customer",
        schema="metadata",
    )

    # Create target table from asset DDL
    run_ddl(
        spark=_spark,
        ddl_path=f"{_base}/ddl/asset_ddl/orders.ddl",
        catalog="dev_customer",
        schema="base",
    )