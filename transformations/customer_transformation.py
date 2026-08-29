"""Customer Transformation Logic

This module contains the business transformation logic for the customer dataset.
It is called by TransformationRunner when has_custom=true.

The transform() function receives the source DataFrame and returns the transformed DataFrame.
The TransformationRunner will then add audit columns and apply SCD2 logic.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(source_df: DataFrame) -> DataFrame:
    """
    Transform TPCH customer data.
    
    Source columns:
    - c_custkey: Customer key (primary key)
    - c_name: Customer name
    - c_address: Customer address
    - c_nationkey: Nation key
    - c_phone: Phone number
    - c_acctbal: Account balance
    - c_mktsegment: Market segment
    - c_comment: Customer comment
    
    Transformations applied:
    1. Pass-through all source columns
    2. Add derived columns for analytics
    
    Returns:
        Transformed DataFrame ready for SCD2 processing
    """
    # Start with all source columns
    df = source_df
    
    # Add derived columns (examples - customize as needed)
    df = df.withColumn(
        "customer_segment_category",
        F.when(F.col("c_mktsegment") == "AUTOMOBILE", "Vehicle")
         .when(F.col("c_mktsegment") == "BUILDING", "Construction")
         .when(F.col("c_mktsegment") == "FURNITURE", "Home")
         .when(F.col("c_mktsegment") == "MACHINERY", "Industrial")
         .when(F.col("c_mktsegment") == "HOUSEHOLD", "Home")
         .otherwise("Other")
    )
    
    # Add balance tier classification
    df = df.withColumn(
        "balance_tier",
        F.when(F.col("c_acctbal") < 0, "Negative")
         .when(F.col("c_acctbal") < 1000, "Low")
         .when(F.col("c_acctbal") < 5000, "Medium")
         .otherwise("High")
    )
    
    # Extract country code from phone (first 2 digits before the dash)
    df = df.withColumn(
        "country_code",
        F.regexp_extract(F.col("c_phone"), r"^(\d{2})", 1)
    )
    
    return df