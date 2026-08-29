"""Orders Transformation Logic

This module contains the business transformation logic for the orders dataset.
It is called by TransformationRunner when has_custom=true.

The transform() function receives the source DataFrame and returns the transformed DataFrame.
The TransformationRunner will then add audit columns and apply the configured load type.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(source_df: DataFrame) -> DataFrame:
    """
    Transform TPCH orders data.
    
    Source columns:
    - o_orderkey: Order key (primary key)
    - o_custkey: Customer key (foreign key)
    - o_orderstatus: Order status (F, O, P)
    - o_totalprice: Total price
    - o_orderdate: Order date
    - o_orderpriority: Order priority
    - o_clerk: Clerk name
    - o_shippriority: Shipping priority
    - o_comment: Order comment
    
    Transformations applied:
    1. Pass-through all source columns
    2. Extract order_year from o_orderdate
    
    Returns:
        Transformed DataFrame ready for loading
    """
    # Start with all source columns
    df = source_df
    
    # Add derived column: order year
    df = df.withColumn("order_year", F.year(F.col("o_orderdate")))
    
    return df
