# Metadata Driven Transformation Framework

A config-driven framework for managing data transformation metadata with SCD Type 2 history.

## Structure

```
Metadata_Driven_Transformation_Framework/
├── volume_content/               ← Deploy to a Unity Catalog volume
│   ├── scd_config.yaml           ← SCD2 column configuration (business keys, tracked columns, audit column names)
│   └── metadata_csv/
│       └── transformation_metadata.csv   ← Input metadata (one row per column mapping)
├── ddl/
│   └── create_metadata_table.sql ← DDL for the metadata Delta table (includes SCD2 audit columns)
└── utils/
    └── metadata_loader.py        ← PySpark script: reads CSV → applies SCD2 merge into metadata table
```

## How It Works

1. **DDL** (`ddl/create_metadata_table.sql`) creates `main.metadata.transformation_metadata` with business columns plus three SCD2 audit columns: `meta_valid_from`, `meta_valid_to`, `meta_is_active`.

2. **SCD Config** (`volume_content/scd_config.yaml`) declares which columns form the business key, which columns are tracked for changes, the audit column names, and the metadata table location — all external to the code.

3. **Loader** (`utils/metadata_loader.py`) reads the CSV, compares it against existing active rows in the metadata table, and applies SCD2 merge logic:
   - New business keys → inserted as active rows.
   - Changed attributes → old version expired (`meta_valid_to = now`, `meta_is_active = false`), new version inserted as active.
   - Unchanged rows → no action.

## Usage

```python
from metadata_loader import MetadataLoader

loader = MetadataLoader(
    spark=spark,
    scd_config_path="/Volumes/main/metadata_framework/config/scd_config.yaml",
    csv_path="/Volumes/main/metadata_framework/metadata_csv/transformation_metadata.csv",
)
loader.run()
```

## Deployment to Unity Catalog Volume

Copy the entire `volume_content/` folder to a UC volume, e.g. `/Volumes/main/metadata_framework/`:

```
/Volumes/main/metadata_framework/
├── scd_config.yaml
└── metadata_csv/
    └── transformation_metadata.csv
```

The loader script reads both the config and the CSV from this volume at runtime.
