"""
SCD Type 2 Utilities
====================

Handles SCD Type 2 merge logic for any Delta target table.

For the **metadata table**: pass a YAML config path (scd_config_path).
For **target/asset tables**: pass a config dict directly (scd_config),
built from the ingestion_config metadata row.

Usage — metadata table (YAML config)::

    from scd_utils import SCDUtils

    scd = SCDUtils(spark, scd_config_path=".../scd_config.yaml", table_name=table_name)
    scd.apply_scd2(source_df)

Usage — target table (config dict from metadata)::

    scd_config = {
        "scd2": {
            "business_key": ["o_orderkey"],
            "track_changes_columns": [],  # empty = auto-detect all except keys + audit cols
            "valid_from_column": "effective_start_date",
            "valid_to_column": "effective_end_date",
            "active_flag_column": "is_current",
            "version_column": "config_version",
            "active_flag_value": True,
            "inactive_flag_value": False,
            "end_of_time": "9999-12-31 23:59:59",
        }
    }
    scd = SCDUtils(spark, scd_config=scd_config, table_name=target_table)
    scd.apply_scd2(source_df)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import yaml
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class SCDUtils:
    """
    Applies SCD Type 2 merge logic to a Delta table.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    scd_config_path : str
        Path to the scd_config.yaml file.
    table_name : str
        Fully qualified target table name (catalog.schema.table).
    """

    # Columns excluded when auto-detecting tracked columns
    DEFAULT_AUDIT_COLUMNS = {
        "record_created_timestamp", "record_created_by", "batch_id",
        "application_name", "reporting_date",
        "effective_start_date", "effective_end_date", "is_current", "config_version",
        "created_by", "created_at",
    }

    def __init__(
        self,
        spark: SparkSession,
        scd_config_path: Optional[str] = None,
        table_name: str = None,
        scd_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.spark = spark

        if scd_config is not None:
            self.config = scd_config
        elif scd_config_path is not None:
            self.config = self._load_config(scd_config_path)
        else:
            raise ValueError("Either scd_config_path or scd_config must be provided.")

        scd = self.config["scd2"]
        self.key_cols: List[str] = scd["business_key"]
        self.track_cols: List[str] = scd.get("track_changes_columns", [])
        self.valid_from_col: str = scd["valid_from_column"]
        self.valid_to_col: str = scd["valid_to_column"]
        self.active_col: str = scd["active_flag_column"]
        self.version_col: str = scd.get("version_column", "config_version")
        self.active_flag_value: Any = scd["active_flag_value"]
        self.inactive_flag_value: Any = scd["inactive_flag_value"]
        self.end_of_time: str = scd["end_of_time"]

        self.metadata_table: str = table_name

    def auto_detect_track_columns(self, source_df: DataFrame) -> List[str]:
        """
        Return all column names from source_df except business keys,
        SCD2 tracking columns, and default audit columns.
        Used when track_changes_columns is empty (track all non-audit columns).
        """
        exclude = set(self.key_cols)
        exclude.update({
            self.valid_from_col, self.valid_to_col,
            self.active_col, self.version_col,
        })
        exclude.update(self.DEFAULT_AUDIT_COLUMNS)
        return [c for c in source_df.columns if c not in exclude]

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------

    @staticmethod
    def _load_config(path: str) -> Dict[str, Any]:
        """Read the SCD2 YAML configuration file."""
        with open(path, "r") as fh:
            cfg = yaml.safe_load(fh)
        if "scd2" not in cfg:
            raise ValueError(f"Config file '{path}' is missing the 'scd2' top-level key.")
        logger.info("Loaded SCD2 config from %s", path)
        return cfg

    def _table_exists(self) -> bool:
        """Return True if the metadata Delta table already exists."""
        return self.spark.catalog.tableExists(self.metadata_table)

    # ------------------------------------------------------------------
    # SCD2 column helpers
    # ------------------------------------------------------------------

    def _add_scd2_columns(
        self,
        df: DataFrame,
        version_col: Optional[str] = None,
    ) -> DataFrame:
        """
        Append the SCD2 audit columns to *df*.

        Parameters
        ----------
        df : DataFrame
            Source DataFrame (business columns only).
        version_col : str, optional
            Name of a temporary column holding the next version number
            for changed rows.  If None, version defaults to 1.
        """
        df = (
            df
            .withColumn(self.valid_from_col, F.current_timestamp())
            .withColumn(self.valid_to_col, F.to_timestamp(F.lit(self.end_of_time)))
            .withColumn(self.active_col, F.lit(self.active_flag_value))
        )
        if version_col is not None:
            df = (
                df
                .withColumn(self.version_col, F.col(version_col))
                .drop(version_col)
            )
        else:
            df = df.withColumn(self.version_col, F.lit(1))
        return df

    # ------------------------------------------------------------------
    # Change detection
    # ------------------------------------------------------------------

    def _detect_changes(
        self, source_df: DataFrame, target_df: DataFrame,
    ) -> DataFrame:
        """
        Join source with active target rows on the business key and return
        source rows whose tracked attributes have changed (null-safe comparison).
        If track_cols is empty, auto-detect all non-audit columns.
        """
        # Auto-detect tracked columns if not specified
        if not self.track_cols:
            self.track_cols = self.auto_detect_track_columns(source_df)
            logger.info("Auto-detected %d tracked columns: %s", len(self.track_cols), self.track_cols)

        # Add any tracked columns missing from the source as NULL so the
        # comparison does not fail with UNRESOLVED_COLUMN.
        for col_name in self.track_cols:
            if col_name not in source_df.columns:
                source_df = source_df.withColumn(col_name, F.lit(None))

        join_cond = [source_df[k] == target_df[k] for k in self.key_cols]
        joined = source_df.alias("src").join(
            target_df.alias("tgt"),
            on=join_cond,
            how="inner",
        )

        change_cond = None
        for col_name in self.track_cols:
            differs = ~F.col(f"src.{col_name}").eqNullSafe(F.col(f"tgt.{col_name}"))
            change_cond = differs if change_cond is None else change_cond | differs

        changed = joined.filter(change_cond).select("src.*")
        return changed

    # ------------------------------------------------------------------
    # Merge operations
    # ------------------------------------------------------------------

    def _expire_changed_rows(self, changed_keys: DataFrame) -> None:
        """MERGE to expire rows whose business key matches a changed source row."""
        delta_table = DeltaTable.forName(self.spark, self.metadata_table)

        merge_condition = " AND ".join(
            [f"target.{k} = src.{k}" for k in self.key_cols]
        ) + f" AND target.{self.active_col} = {self.active_flag_value}"

        delta_table.alias("target").merge(
            changed_keys.alias("src"),
            merge_condition,
        ).whenMatchedUpdate(set={
            self.valid_to_col: F.current_timestamp(),
            self.active_col: F.lit(self.inactive_flag_value),
        }).execute()

        logger.info("Expired changed rows in %s", self.metadata_table)

    def _get_next_versions(self) -> DataFrame:
        """Return max(version) + 1 per business key from the target table."""
        return (
            self.spark.table(self.metadata_table)
            .groupBy(*self.key_cols)
            .agg((F.max(self.version_col) + 1).alias("_next_version"))
        )

    def _insert_rows(self, df: DataFrame) -> None:
        """Append rows to the metadata Delta table."""
        count = df.count()
        (
            df.write
            .format("delta")
            .mode("append")
            .saveAsTable(self.metadata_table)
        )
        logger.info("Inserted %d rows into %s", count, self.metadata_table)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def apply_scd2(self, source_df: DataFrame) -> None:
        """
        Apply SCD Type 2 merge:
        1. First run — table doesn't exist: insert all rows as version 1.
        2. Subsequent runs:
           a. New keys  → INSERT with version 1.
           b. Changed   → expire old version, INSERT new version (max + 1).
           c. Unchanged → no action.
        """
        # --- First run: table doesn't exist yet ---
        if not self._table_exists():
            logger.info("Table %s does not exist — creating with initial load", self.metadata_table)
            staged = self._add_scd2_columns(source_df)
            self._insert_rows(staged)
            return

        # --- Subsequent runs: SCD2 merge ---
        target_active = (
            self.spark.table(self.metadata_table)
            .filter(F.col(self.active_col) == self.active_flag_value)
        )

        # Brand-new rows: business key not present in active target.
        new_rows = source_df.join(
            target_active.select(*self.key_cols),
            on=self.key_cols,
            how="left_anti",
        ).localCheckpoint()

        # Changed rows: same key, different tracked attributes.
        changed_rows = self._detect_changes(source_df, target_active).localCheckpoint()

        new_count = new_rows.count()
        changed_count = changed_rows.count()

        # Step 1: expire old versions of changed rows.
        if changed_count > 0:
            self._expire_changed_rows(changed_rows.select(*self.key_cols))

        # Step 2: stage and insert new + changed rows.
        # changed_rows and new_rows are cached so they are NOT re-evaluated
        # after the expire operation modifies the target table.
        staged_parts: List[DataFrame] = []

        if new_count > 0:
            staged_parts.append(self._add_scd2_columns(new_rows))

        if changed_count > 0:
            next_versions = self._get_next_versions()
            changed_with_ver = changed_rows.join(next_versions, on=self.key_cols, how="inner")
            staged_parts.append(
                self._add_scd2_columns(changed_with_ver, version_col="_next_version")
            )

        if staged_parts:
            final_df = staged_parts[0]
            for part in staged_parts[1:]:
                final_df = final_df.unionByName(part)
            self._insert_rows(final_df)
        else:
            logger.info("No new or changed metadata rows — nothing to insert.")

        logger.info("SCD2 merge complete for %s", self.metadata_table)