# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_npi (
# MAGIC   npi_id STRING,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   position STRING,
# MAGIC   organisation_name STRING,
# MAGIC   last_updated STRING,
# MAGIC   refreshed_at TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE gold.dim_npi;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into
# MAGIC   gold.dim_npi
# MAGIC select
# MAGIC   npi_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   position,
# MAGIC   organisation_name,
# MAGIC   last_updated,
# MAGIC   current_timestamp()
# MAGIC from
# MAGIC   silver.npi_extract
# MAGIC   where is_current_flag = true

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dim_npi

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Gold dimension table for patients
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_patient
# MAGIC (
# MAGIC     patient_key STRING,
# MAGIC     src_patientid STRING,
# MAGIC     firstname STRING,
# MAGIC     lastname STRING,
# MAGIC     middlename STRING,
# MAGIC     ssn STRING,
# MAGIC     phonenumber STRING,
# MAGIC     gender STRING,
# MAGIC     dob DATE,
# MAGIC     address STRING,
# MAGIC     datasource STRING
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.dim_patient
# MAGIC SELECT 
# MAGIC     patient_key,
# MAGIC     src_patientid,
# MAGIC     firstname,
# MAGIC     lastname,
# MAGIC     middlename,
# MAGIC     ssn,
# MAGIC     phonenumber,
# MAGIC     gender,
# MAGIC     dob,
# MAGIC     address,
# MAGIC     datasource
# MAGIC FROM silver.patients
# MAGIC WHERE is_current = true
# MAGIC   AND is_quarantined = false;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dim_patient;
