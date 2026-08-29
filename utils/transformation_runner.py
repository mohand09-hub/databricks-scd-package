"""
Transformation Runner
=====================

Reads metadata from the ingestion_config table and executes a simple
transformation for each active dataset in a load group.

For datasets with has_custom = false, the runner performs a direct
select of all source columns.  For has_custom = true, it loads and
applies the transformation logic from the transformation_path.

Usage (from a Databricks notebook or job)::

    from transformation_runner import TransformationRunner

    runner = TransformationRunner(
        spark,
        "dev_customer.metadata.ingestion_config",
        audit_table="dev_customer.base.audit_log",
    )
    runner.run_load_group("TPCH_TRANSFORMATIONS")
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, TimestampType

from scd_utils import SCDUtils

logger = logging.getLogger(__name__)


class TransformationRunner:
    """
    Executes transformations based on ingestion_config metadata.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    metadata_table : str
        Fully qualified name of the ingestion_config table.
    """

    def __init__(
        self,
        spark: SparkSession,
        metadata_table: str,
        audit_table: Optional[str] = None,
    ) -> None:
        self.spark = spark
        self.metadata_table = metadata_table
        self.audit_table = audit_table

    # ------------------------------------------------------------------
    # Metadata helpers
    # ------------------------------------------------------------------

    def _read_metadata(self, load_group: str) -> List[Dict[str, Any]]:
        """Return active metadata rows for the given load group, ordered by sequence."""
        df = (
            self.spark.table(self.metadata_table)
            .filter(F.col("is_current") == True)
            .filter(F.col("is_active") == True)
            .filter(F.col("load_group_name") == load_group)
            .orderBy(F.col("dataset_load_sequence"))
        )
        rows = df.collect()
        if not rows:
            raise ValueError(f"No active datasets found for load group '{load_group}'")
        return [row.asDict() for row in rows]

    def _full_target_name(self, meta: Dict[str, Any]) -> str:
        """Build the fully qualified target table name from metadata."""
        return f"{meta['target_uc_catalog']}.{meta['target_uc_schema']}.{meta['target_uc_table']}"

    # ------------------------------------------------------------------
    # Source & target helpers
    # ------------------------------------------------------------------

    def _read_source(self, meta: Dict[str, Any]) -> DataFrame:
        """Read the source table(s) specified in metadata."""
        source = meta["source_table_list"].strip()
        logger.info("Reading source: %s", source)
        return self.spark.table(source)

    def _create_target_if_needed(self, meta: Dict[str, Any], df: DataFrame) -> None:
        """Create the target table if it doesn't exist and the flag is set."""
        target = self._full_target_name(meta)
        if meta.get("create_target_table_if_not_exists", True):
            if not self.spark.catalog.tableExists(target):
                logger.info("Creating target table: %s", target)
                (
                    df.write
                    .format("delta")
                    .mode("overwrite")
                    .saveAsTable(target)
                )

    def _write_target(self, meta: Dict[str, Any], df: DataFrame) -> None:
        """Write the transformed DataFrame to the target table using the configured load type."""
        target = self._full_target_name(meta)
        load_type = meta["target_load_type"].upper()
        logger.info("Writing to %s with load_type=%s", target, load_type)

        if load_type == "OVERWRITE":
            (df.write.format("delta").mode("overwrite").saveAsTable(target))
        elif load_type == "APPEND":
            (df.write.format("delta").mode("append").saveAsTable(target))
        elif load_type == "MERGE":
            self._write_merge(meta, df)
        elif load_type == "SCD2":
            self._write_scd2(meta, df)
        else:
            raise NotImplementedError(f"Load type '{load_type}' not yet supported")

    def _write_merge(self, meta: Dict[str, Any], df: DataFrame) -> None:
        """Perform a simple MERGE (UPSERT) operation using merge_key_columns."""
        target = self._full_target_name(meta)
        merge_keys = self._parse_column_list(meta.get("merge_key_columns", ""))
        
        if not merge_keys:
            raise ValueError(f"MERGE load type requires merge_key_columns in metadata for '{meta['dataset_name']}'")
        
        logger.info("Performing MERGE on %s using keys: %s", target, merge_keys)
        
        delta_table = DeltaTable.forName(self.spark, target)
        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        
        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition,
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        logger.info("MERGE complete for %s", target)

    def _write_scd2(self, meta: Dict[str, Any], df: DataFrame) -> None:
        """
        Apply SCD Type 2 using the generic SCDUtils class.

        - Business keys: from merge_key_columns in metadata
        - Tracked columns: from scd_columns if specified, else auto-detect
          (all columns except keys + audit columns)
        """
        target = self._full_target_name(meta)

        business_keys = self._parse_column_list(meta.get("merge_key_columns", ""))

        if not business_keys:
            raise ValueError(
                f"SCD2 load type requires merge_key_columns in metadata for '{meta['dataset_name']}'"
            )

        scd_cols_str = meta.get("scd_columns", "") or ""
        if scd_cols_str.strip():
            track_columns = self._parse_column_list(scd_cols_str)
        else:
            track_columns = []  # empty = SCDUtils auto-detects all except keys + audit

        scd_config = {
            "scd2": {
                "business_key": business_keys,
                "track_changes_columns": track_columns,
                "valid_from_column": "effective_start_date",
                "valid_to_column": "effective_end_date",
                "active_flag_column": "is_current",
                "version_column": "config_version",
                "active_flag_value": True,
                "inactive_flag_value": False,
                "end_of_time": "9999-12-31 23:59:59",
            }
        }

        logger.info("Applying SCD2 to %s with keys=%s, track_cols=%s", target, business_keys, track_columns)

        scd = SCDUtils(spark=self.spark, scd_config=scd_config, table_name=target)
        scd.apply_scd2(df)

    @staticmethod
    def _parse_column_list(col_str: str) -> List[str]:
        """Parse comma-separated column list, handling empty/None."""
        if not col_str or col_str.strip() == "":
            return []
        return [c.strip() for c in col_str.split(",") if c.strip()]

    def _parse_scd2_config(self, config_str: str) -> Dict[str, List[str]]:
        """Parse SCD2 config string: 'key:col1,col2;track:col3,col4'."""
        parts = config_str.split(";")
        config = {"business_keys": [], "track_columns": []}
        
        for part in parts:
            if ":" not in part:
                continue
            key, value = part.split(":", 1)
            key = key.strip().lower()
            cols = self._parse_column_list(value)
            if key == "key":
                config["business_keys"] = cols
            elif key == "track":
                config["track_columns"] = cols
        
        if not config["business_keys"]:
            raise ValueError(f"SCD2 config must define business keys: {config_str}")
        
        return config

    def _add_scd2_columns(
        self, df: DataFrame, version: int = 1, version_col: Optional[str] = None
    ) -> DataFrame:
        """Add SCD2 audit columns to DataFrame."""
        df = (
            df
            .withColumn("effective_start_date", F.current_timestamp())
            .withColumn("effective_end_date", F.to_timestamp(F.lit("9999-12-31 23:59:59")))
            .withColumn("is_current", F.lit(True))
        )
        
        if version_col is not None:
            df = df.withColumn("config_version", F.col(version_col)).drop(version_col)
        else:
            df = df.withColumn("config_version", F.lit(version))
        
        return df

    def _detect_scd2_changes(
        self, source_df: DataFrame, target_df: DataFrame,
        business_keys: List[str], track_columns: List[str]
    ) -> DataFrame:
        """Detect rows where tracked columns have changed."""
        # Add missing track columns as NULL
        for col_name in track_columns:
            if col_name not in source_df.columns:
                source_df = source_df.withColumn(col_name, F.lit(None))
        
        join_cond = [source_df[k] == target_df[k] for k in business_keys]
        joined = source_df.alias("src").join(
            target_df.alias("tgt"),
            on=join_cond,
            how="inner",
        )
        
        change_cond = None
        for col_name in track_columns:
            differs = ~F.col(f"src.{col_name}").eqNullSafe(F.col(f"tgt.{col_name}"))
            change_cond = differs if change_cond is None else change_cond | differs
        
        return joined.filter(change_cond).select("src.*")

    def _expire_scd2_rows(
        self, target: str, changed_keys: DataFrame, business_keys: List[str]
    ) -> None:
        """Expire active rows that have changed."""
        delta_table = DeltaTable.forName(self.spark, target)
        merge_condition = " AND ".join(
            [f"target.{k} = src.{k}" for k in business_keys]
        ) + " AND target.is_current = true"
        
        delta_table.alias("target").merge(
            changed_keys.alias("src"),
            merge_condition,
        ).whenMatchedUpdate(set={
            "effective_end_date": F.current_timestamp(),
            "is_current": F.lit(False),
        }).execute()
        
        logger.info("Expired changed SCD2 rows")

    def _get_next_scd2_versions(
        self, target: str, business_keys: List[str]
    ) -> DataFrame:
        """Get next version number for each business key."""
        return (
            self.spark.table(target)
            .groupBy(*business_keys)
            .agg((F.max("config_version") + 1).alias("_next_version"))
        )

    # ------------------------------------------------------------------
    # Transformation logic
    # ------------------------------------------------------------------

    def _apply_transformation(self, meta: Dict[str, Any], source_df: DataFrame) -> DataFrame:
        """
        Apply transformation to the source DataFrame.

        If has_custom is True, load the transformation from transformation_path.
        Otherwise, perform a simple pass-through with a derived order_year column
        if the source has an o_orderdate column.
        """
        if meta.get("has_custom", False):
            path = meta.get("transformation_path", "")
            if not path:
                raise ValueError(f"Dataset '{meta['dataset_name']}' has has_custom=true but no transformation_path")
            return self._apply_custom_transformation(source_df, path)

        # Simple transformation: pass-through with derived column
        df = source_df
        if "o_orderdate" in df.columns:
            df = df.withColumn("order_year", F.year(F.col("o_orderdate")))
        logger.info("Applied simple transformation for dataset '%s'", meta["dataset_name"])
        return df

    def _apply_custom_transformation(self, source_df: DataFrame, path: str) -> DataFrame:
        """Load and apply a custom transformation from a Python file."""
        import importlib.util
        spec = importlib.util.spec_from_file_location("custom_transform", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if not hasattr(module, "transform"):
            raise AttributeError(f"Custom transformation '{path}' must define a transform(df) function")
        return module.transform(source_df)

    def _add_default_columns(
        self,
        df: DataFrame,
        meta: Dict[str, Any],
        batch_id: str,
    ) -> DataFrame:
        """
        Append default audit columns to the transformed DataFrame.

        These columns are added automatically after the business transformation,
        regardless of the custom logic applied:
          - record_created_timestamp
          - record_created_by
          - batch_id
          - application_name
          - reporting_date
        """
        reporting_date = meta.get("reporting_date")

        df = (
            df
            .withColumn("record_created_timestamp", F.current_timestamp())
            .withColumn("record_created_by", F.expr("current_user()"))
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("application_name", F.lit(meta.get("application_name", "")))
            .withColumn(
                "reporting_date",
                F.lit(reporting_date) if reporting_date else F.current_date(),
            )
        )
        logger.info("Added default audit columns for batch_id=%s", batch_id)
        return df

    # ------------------------------------------------------------------
    # Audit helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_row_count(spark: SparkSession, table_name: str) -> int:
        """Return the row count of a table, or 0 if it doesn't exist."""
        try:
            return spark.table(table_name).count()
        except Exception:
            return 0

    def _write_audit_row(self, audit: Dict[str, Any]) -> None:
        """Append a single audit record to the audit table."""
        if not self.audit_table:
            return

        df = self.spark.createDataFrame([audit])
        (
            df.write
            .format("delta")
            .mode("append")
            .saveAsTable(self.audit_table)
        )
        logger.info("Audit row written for batch_id=%s", audit["batch_id"])

    def _build_audit_row(
        self,
        meta: Dict[str, Any],
        batch_id: str,
        start_time: datetime,
        end_time: Optional[datetime],
        rows_before: int,
        rows_after: int,
        source_count: int,
        load_type: str,
        status: str,
        error_message: str = "",
    ) -> Dict[str, Any]:
        """Build an audit dictionary from load results."""
        duration = int((end_time - start_time).total_seconds()) if end_time else 0

        rows_inserted = 0
        rows_deleted = 0
        rows_updated = 0

        if load_type == "OVERWRITE":
            rows_inserted = source_count
            rows_deleted = rows_before
        elif load_type == "APPEND":
            rows_inserted = source_count

        return {
            "batch_id": batch_id,
            "load_group_name": meta["load_group_name"],
            "dataset_name": meta["dataset_name"],
            "source_table_list": meta.get("source_table_list", ""),
            "target_uc_catalog": meta.get("target_uc_catalog", ""),
            "target_uc_schema": meta.get("target_uc_schema", ""),
            "target_uc_table": meta["target_uc_table"],
            "load_type": load_type,
            "rows_inserted": rows_inserted,
            "rows_deleted": rows_deleted,
            "rows_updated": rows_updated,
            "rows_total": rows_after,
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": duration,
            "status": status,
            "error_message": error_message,
            "created_by": "",
            "created_at": datetime.utcnow(),
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_load_group(self, load_group: str) -> None:
        """
        Run all active datasets in a load group, in sequence order.
        Captures audit metadata for each dataset load.
        """
        configs = self._read_metadata(load_group)
        logger.info("Found %d datasets in load group '%s'", len(configs), load_group)

        for meta in configs:
            dataset = meta["dataset_name"]
            batch_id = str(uuid.uuid4())
            target = self._full_target_name(meta)
            load_type = meta["target_load_type"].upper()
            start_time = datetime.utcnow()

            logger.info("Processing dataset: %s (batch_id=%s)", dataset, batch_id)

            rows_before = self._get_row_count(self.spark, target)
            error_message = ""
            status = "SUCCESS"

            try:
                source_df = self._read_source(meta)
                source_count = source_df.count()
                transformed_df = self._apply_transformation(meta, source_df)
                transformed_df = self._add_default_columns(transformed_df, meta, batch_id)

                # SCD2 tables must exist from DDL — don't auto-create from DataFrame
                if load_type != "SCD2":
                    self._create_target_if_needed(meta, transformed_df)

                self._write_target(meta, transformed_df)

            except Exception as e:
                status = "FAILED"
                error_message = str(e)
                source_count = 0
                logger.error("Dataset '%s' failed: %s", dataset, e)
                raise

            finally:
                end_time = datetime.utcnow()
                rows_after = self._get_row_count(self.spark, target) if status == "SUCCESS" else rows_before

                audit = self._build_audit_row(
                    meta=meta,
                    batch_id=batch_id,
                    start_time=start_time,
                    end_time=end_time,
                    rows_before=rows_before,
                    rows_after=rows_after,
                    source_count=source_count,
                    load_type=load_type,
                    status=status,
                    error_message=error_message,
                )
                self._write_audit_row(audit)

                logger.info(
                    "Dataset '%s' %s — inserted=%d, deleted=%d, total=%d, duration=%ds",
                    dataset, status, audit["rows_inserted"], audit["rows_deleted"],
                    audit["rows_total"], audit["duration_seconds"],
                )

        logger.info("Load group '%s' complete", load_group)