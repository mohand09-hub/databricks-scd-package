"""
Metadata CSV Loader
===================

Reads a metadata CSV file into a Spark DataFrame using the schema
from an existing Delta table (created by the DDL).  No schema
inference — types come from the table definition.

Usage (from a Databricks notebook or job)::

    from metadata_loader import MetadataLoader

    loader = MetadataLoader(spark, csv_path, table_name)
    df = loader.read()
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class MetadataLoader:
    """
    Reads a metadata CSV file into a Spark DataFrame.

    The schema is derived from the target Delta table (created by the
    DDL), not inferred from the CSV.  Only columns present in the CSV
    header are read; SCD2 and audit columns are excluded automatically.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    csv_path : str
        Path to the CSV file (UC volume, DBFS, or workspace path).
    table_name : str
        Fully qualified target table name (catalog.schema.table).
        Must already exist (created by the DDL).
    """

    def __init__(self, spark: SparkSession, csv_path: str, table_name: str) -> None:
        self.spark = spark
        self.csv_path = csv_path
        self.table_name = table_name

    def read(self) -> DataFrame:
        """Read the CSV using the schema from the target Delta table."""
        # Get column names from the CSV header (limit 0 for speed).
        header_cols = (
            self.spark.read
            .option("header", True)
            .csv(self.csv_path)
            .limit(0)
            .columns
        )

        # Build a read schema using only CSV columns, typed from the table.
        table_fields = {f.name: f for f in self.spark.table(self.table_name).schema.fields}
        read_schema = StructType([
            table_fields[c] for c in header_cols if c in table_fields
        ])

        df = (
            self.spark.read
            .option("header", True)
            .schema(read_schema)
            .csv(self.csv_path)
        )
        logger.info("Read %d rows from %s", df.count(), self.csv_path)
        return df
