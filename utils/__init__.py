"""
Metadata-Driven Transformation Framework
=========================================

A modular framework for metadata-driven data ingestion and transformation.

Modules
-------
master_script.py       — run_ddl(), run_scd2_load() entry points.
metadata_loader.py      — MetadataLoader: reads CSV using table schema.
scd_utils.py            — SCDUtils: SCD Type 2 merge logic.
transformation_runner.py — TransformationRunner: executes transforms from metadata.

Usage
-----
    from utils.master_script import run_ddl, run_scd2_load
    from utils.transformation_runner import TransformationRunner

Or add the utils folder to sys.path and import directly:
    sys.path.insert(0, "/path/to/utils")
    from master_script import run_ddl, run_scd2_load
"""

from master_script import run_ddl, run_scd2_load
from metadata_loader import MetadataLoader
from scd_utils import SCDUtils
from transformation_runner import TransformationRunner

__all__ = [
    "run_ddl",
    "run_scd2_load",
    "MetadataLoader",
    "SCDUtils",
    "TransformationRunner",
]