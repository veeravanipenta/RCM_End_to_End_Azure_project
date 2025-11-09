# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

#Reading hospital A departments data
df_hosa = spark.read.parquet("abfss://bronze@healthcaresac.dfs.core.windows.net/hosa/departments")

#Reading Hospital B departments data
df_hosb = spark.read.parquet("abfss://bronze@healthcaresac.dfs.core.windows.net/hosb/departments")

#union two departments dataframes
df_merged = df_hosa.unionByName(df_hosb)

# COMMAND ----------

display(df_hosa)

# COMMAND ----------

#CREATE a dept_id column using the existing columns and rename the deptid to src_dept_id
df_merged = df_merged.withColumn("SRC_Dept_id", f.col("DeptID")). \
    withColumn("Dept_id", f.concat(f.col("DeptID"),f.lit("-"),f.col("datasource"))).drop("DeptID")

df_merged.createOrReplaceTempView("departments")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.departments

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.departments(
# MAGIC   Dept_id string,
# MAGIC   SRC_Dept_Id string,
# MAGIC   Name string,
# MAGIC   datasource string,
# MAGIC   is_quarantined boolean
# MAGIC ) using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.departments

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC Dept_id, 
# MAGIC SRC_Dept_Id, 
# MAGIC Name, 
# MAGIC datasource, 
# MAGIC   case 
# MAGIC     when SRC_Dept_id is null or Name is null then True 
# MAGIC     else False 
# MAGIC     end as is_quarantined
# MAGIC from departments

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail silver.departments;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into silver.departments 
# MAGIC select 
# MAGIC Dept_id, 
# MAGIC SRC_Dept_Id, 
# MAGIC Name, 
# MAGIC datasource, 
# MAGIC   case 
# MAGIC     when SRC_Dept_id is null or Name is null then True 
# MAGIC     else False 
# MAGIC     end as is_quarantined
# MAGIC from departments
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.departments

# COMMAND ----------


