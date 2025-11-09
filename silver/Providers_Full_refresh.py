# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

#Reading Hospital A Providers data 
df_hosa=spark.read.parquet("abfss://bronze@healthcaresac.dfs.core.windows.net/hosa/providers")

#Reading Hospital B providers data 
df_hosb=spark.read.parquet("abfss://bronze@healthcaresac.dfs.core.windows.net/hosb/providers")

#union two providers dataframes
df_merged = df_hosa.unionByName(df_hosb)
display(df_merged)

df_merged.createOrReplaceTempView("providers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.providers(
# MAGIC   ProviderID string,
# MAGIC   FirstName string,
# MAGIC   LastName string,
# MAGIC   Specialization string,
# MAGIC   DeptID string,
# MAGIC   NPI long,
# MAGIC   datasource string,
# MAGIC   is_quarantined boolean
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE  TABLE silver.providers

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver.providers
# MAGIC select distinct ProviderID,
# MAGIC FirstName,
# MAGIC LastName,
# MAGIC specialization,
# MAGIC DeptID,
# MAGIC NPI,
# MAGIC datasource,
# MAGIC case
# MAGIC   when ProviderID is null or DeptID is null then TRUE 
# MAGIC   else False
# MAGIC   END AS is_quarantined
# MAGIC from providers;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.providers
