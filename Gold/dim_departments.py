# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_department
# MAGIC (
# MAGIC     Dept_Id STRING,
# MAGIC     SRC_Dept_Id STRING,
# MAGIC     Name STRING,
# MAGIC     datasource STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE gold.dim_department

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- #Insert distinct, non-quarantined records from Silver into Gold
# MAGIC INSERT INTO gold.dim_department
# MAGIC SELECT DISTINCT
# MAGIC     Dept_Id,
# MAGIC     SRC_Dept_Id,
# MAGIC     Name,
# MAGIC     datasource
# MAGIC FROM silver.departments
# MAGIC WHERE is_quarantined = FALSE;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dim_department;
