# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Orders Transformation (SQL Logic Only)
# MAGIC
# MAGIC This notebook contains only the SQL transformation from source to target.
# MAGIC DDL creation, metadata reading, and orchestration are handled by the master script.
# MAGIC
# MAGIC **Source:** `samples.tpch.orders`
# MAGIC **Target:** `dev_customer.base.orders`
# MAGIC **Load type:** OVERWRITE
# MAGIC
# MAGIC Derived column `order_year` is extracted from `o_orderdate`.

# COMMAND ----------

# DBTITLE 1,Transform and Insert
# MAGIC %sql
# MAGIC -- Transform TPCH orders: select all columns + derive order_year
# MAGIC -- Insert into the target table (structure defined by orders.ddl)
# MAGIC INSERT OVERWRITE dev_customer.base.orders
# MAGIC SELECT
# MAGIC   o_orderkey,
# MAGIC   o_custkey,
# MAGIC   o_orderstatus,
# MAGIC   o_totalprice,
# MAGIC   o_orderdate,
# MAGIC   o_orderpriority,
# MAGIC   o_clerk,
# MAGIC   o_shippriority,
# MAGIC   o_comment,
# MAGIC   YEAR(o_orderdate) AS order_year
# MAGIC FROM samples.tpch.orders;

# COMMAND ----------

# DBTITLE 1,Verify Output
# MAGIC %sql
# MAGIC -- Verify the transformed data
# MAGIC SELECT * FROM dev_customer.base.orders LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Row Count
# MAGIC %sql
# MAGIC -- Total row count
# MAGIC SELECT COUNT(*) AS total_rows FROM dev_customer.base.orders;