# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

#Reading Cpt_codes data 
cpt_codes_df=spark.read.parquet("abfss://bronze@healthcaresac.dfs.core.windows.net/cptcodes/*")

display(cpt_codes_df)

cpt_codes_df.createOrReplaceTempView("cptcodes")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cptcodes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temp view with quarantine check for CPT codes
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT 
# MAGIC     cpt_codes,
# MAGIC     procedure_code_category,
# MAGIC     procedure_code_descriptions,
# MAGIC     code_status,
# MAGIC     CASE 
# MAGIC         WHEN cpt_codes IS NULL OR procedure_code_descriptions IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM cptcodes;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the silver.cptcodes table using Delta
# MAGIC CREATE TABLE IF NOT EXISTS silver.cptcodes (
# MAGIC     cpt_codes STRING,
# MAGIC     procedure_code_category STRING,
# MAGIC     procedure_code_descriptions STRING,
# MAGIC     code_status STRING,
# MAGIC     is_quarantined BOOLEAN,
# MAGIC     audit_insertdate TIMESTAMP,
# MAGIC     audit_modifieddate TIMESTAMP,
# MAGIC     is_current BOOLEAN
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Truncate table silver.cptcodes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Update old records (mark as historical)
# MAGIC MERGE INTO silver.cptcodes AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.cpt_codes = source.cpt_codes AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC     target.procedure_code_category != source.procedure_code_category OR
# MAGIC     target.procedure_code_descriptions != source.procedure_code_descriptions OR
# MAGIC     target.code_status != source.code_status OR
# MAGIC     target.is_quarantined != source.is_quarantined
# MAGIC ) THEN
# MAGIC   UPDATE SET
# MAGIC     target.is_current = false,
# MAGIC     target.audit_modifieddate = current_timestamp();
# MAGIC
# MAGIC -- Step 2: Insert new records
# MAGIC MERGE INTO silver.cptcodes AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.cpt_codes = source.cpt_codes AND target.is_current = true
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     cpt_codes,
# MAGIC     procedure_code_category,
# MAGIC     procedure_code_descriptions,
# MAGIC     code_status,
# MAGIC     is_quarantined,
# MAGIC     audit_insertdate,
# MAGIC     audit_modifieddate,
# MAGIC     is_current
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.cpt_codes,
# MAGIC     source.procedure_code_category,
# MAGIC     source.procedure_code_descriptions,
# MAGIC     source.code_status,
# MAGIC     source.is_quarantined,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     true
# MAGIC   );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update old records to implement SCD Type 2
# MAGIC MERGE INTO silver.cptcodes AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.cpt_codes = source.cpt_codes 
# MAGIC    AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC     target.procedure_code_category != source.procedure_code_category OR
# MAGIC     target.procedure_code_descriptions != source.procedure_code_descriptions OR
# MAGIC     target.code_status != source.code_status OR
# MAGIC     target.is_quarantined != source.is_quarantined
# MAGIC ) THEN
# MAGIC   UPDATE SET
# MAGIC     target.is_current = false,
# MAGIC     target.audit_modifieddate = current_timestamp();
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify data
# MAGIC SELECT * FROM silver.cptcodes;
