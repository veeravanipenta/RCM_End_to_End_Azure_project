# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_provider
# MAGIC (
# MAGIC     ProviderID string,
# MAGIC     FirstName string,
# MAGIC     LastName string,
# MAGIC     DeptID string,
# MAGIC     NPI long,
# MAGIC     datasource string
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE gold.dim_provider

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.dim_provider
# MAGIC SELECT 
# MAGIC     ProviderID,
# MAGIC     FirstName,
# MAGIC     LastName,
# MAGIC     CONCAT(DeptID, '-', datasource) AS DeptID,
# MAGIC     NPI,
# MAGIC     datasource
# MAGIC FROM silver.providers
# MAGIC WHERE is_quarantined = false;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dim_provider
