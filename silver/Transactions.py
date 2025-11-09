# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

#Reading Hospital A Transactions data 
df_hosa=spark.read.parquet("abfss://bronze@healthcaresac.dfs.core.windows.net/hosa/transactions")

#Reading Hospital B Transactions data 
df_hosb=spark.read.parquet("abfss://bronze@healthcaresac.dfs.core.windows.net/hosb/transactions")

#union two Transactions dataframes
df_merged = df_hosa.unionByName(df_hosb)
display(df_merged)

df_merged.createOrReplaceTempView("transactions")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT 
# MAGIC     concat(TransactionID,'-',datasource) AS TransactionID,
# MAGIC     TransactionID AS SRC_TransactionID,
# MAGIC     EncounterID,
# MAGIC     PatientID,
# MAGIC     ProviderID,
# MAGIC     DeptID,
# MAGIC     VisitDate,
# MAGIC     ServiceDate,
# MAGIC     PaidDate,
# MAGIC     VisitType,
# MAGIC     Amount,
# MAGIC     AmountType,
# MAGIC     PaidAmount,
# MAGIC     ClaimID,
# MAGIC     PayorID,
# MAGIC     ProcedureCode,
# MAGIC     ICDCode,
# MAGIC     LineOfBusiness,
# MAGIC     MedicaidID,
# MAGIC     MedicareID,
# MAGIC     InsertDate AS SRC_InsertDate,
# MAGIC     ModifiedDate AS SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     CASE 
# MAGIC         WHEN EncounterID IS NULL 
# MAGIC              OR PatientID IS NULL 
# MAGIC              OR TransactionID IS NULL 
# MAGIC              OR VisitDate IS NULL 
# MAGIC         THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.transactions (
# MAGIC   TransactionID STRING,
# MAGIC   SRC_TransactionID STRING,
# MAGIC   EncounterID STRING,
# MAGIC   PatientID STRING,
# MAGIC   ProviderID STRING,
# MAGIC   DeptID STRING,
# MAGIC   VisitDate DATE,
# MAGIC   ServiceDate DATE,
# MAGIC   PaidDate DATE,
# MAGIC   VisitType STRING,
# MAGIC   Amount DOUBLE,
# MAGIC   AmountType STRING,
# MAGIC   PaidAmount DOUBLE,
# MAGIC   ClaimID STRING,
# MAGIC   PayorID STRING,
# MAGIC   ProcedureCode INT,
# MAGIC   ICDCode STRING,
# MAGIC   LineOfBusiness STRING,
# MAGIC   MedicaidID STRING,
# MAGIC   MedicareID STRING,
# MAGIC   SRC_InsertDate DATE,
# MAGIC   SRC_ModifiedDate DATE,
# MAGIC   datasource STRING,
# MAGIC   is_quarantined BOOLEAN,
# MAGIC   audit_insertdate TIMESTAMP,
# MAGIC   audit_modifieddate TIMESTAMP,
# MAGIC   is_current BOOLEAN
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step: Implement SCD Type 2 for transactions
# MAGIC MERGE INTO silver.transactions AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.TransactionID = source.TransactionID
# MAGIC AND target.is_current = true
# MAGIC
# MAGIC WHEN MATCHED
# MAGIC AND (
# MAGIC     target.SRC_TransactionID != source.SRC_TransactionID
# MAGIC     OR target.EncounterID != source.EncounterID
# MAGIC     OR target.PatientID != source.PatientID
# MAGIC     OR target.ProviderID != source.ProviderID
# MAGIC     OR target.DeptID != source.DeptID
# MAGIC     OR target.VisitDate != source.VisitDate
# MAGIC     OR target.ServiceDate != source.ServiceDate
# MAGIC     OR target.PaidDate != source.PaidDate
# MAGIC     OR target.VisitType != source.VisitType
# MAGIC     OR target.Amount != source.Amount
# MAGIC     OR target.AmountType != source.AmountType
# MAGIC     OR target.PaidAmount != source.PaidAmount
# MAGIC     OR target.ClaimID != source.ClaimID
# MAGIC     OR target.PayorID != source.PayorID
# MAGIC     OR target.ProcedureCode != source.ProcedureCode
# MAGIC     OR target.ICDCode != source.ICDCode
# MAGIC     OR target.LineOfBusiness != source.LineOfBusiness
# MAGIC     OR target.MedicaidID != source.MedicaidID
# MAGIC     OR target.MedicareID != source.MedicareID
# MAGIC     OR target.SRC_InsertDate != source.SRC_InsertDate
# MAGIC     OR target.SRC_ModifiedDate != source.SRC_ModifiedDate
# MAGIC     OR target.datasource != source.datasource
# MAGIC     OR target.is_quarantined != source.is_quarantined
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.is_current = false,
# MAGIC     target.audit_modifieddate = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC     TransactionID,
# MAGIC     SRC_TransactionID,
# MAGIC     EncounterID,
# MAGIC     PatientID,
# MAGIC     ProviderID,
# MAGIC     DeptID,
# MAGIC     VisitDate,
# MAGIC     ServiceDate,
# MAGIC     PaidDate,
# MAGIC     VisitType,
# MAGIC     Amount,
# MAGIC     AmountType,
# MAGIC     PaidAmount,
# MAGIC     ClaimID,
# MAGIC     PayorID,
# MAGIC     ProcedureCode,
# MAGIC     ICDCode,
# MAGIC     LineOfBusiness,
# MAGIC     MedicaidID,
# MAGIC     MedicareID,
# MAGIC     SRC_InsertDate,
# MAGIC     SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     is_quarantined,
# MAGIC     audit_insertdate,
# MAGIC     audit_modifieddate,
# MAGIC     is_current
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.TransactionID,
# MAGIC     source.SRC_TransactionID,
# MAGIC     source.EncounterID,
# MAGIC     source.PatientID,
# MAGIC     source.ProviderID,
# MAGIC     source.DeptID,
# MAGIC     source.VisitDate,
# MAGIC     source.ServiceDate,
# MAGIC     source.PaidDate,
# MAGIC     source.VisitType,
# MAGIC     source.Amount,
# MAGIC     source.AmountType,
# MAGIC     source.PaidAmount,
# MAGIC     source.ClaimID,
# MAGIC     source.PayorID,
# MAGIC     source.ProcedureCode,
# MAGIC     source.ICDCode,
# MAGIC     source.LineOfBusiness,
# MAGIC     source.MedicaidID,
# MAGIC     source.MedicareID,
# MAGIC     source.SRC_InsertDate,
# MAGIC     source.SRC_ModifiedDate,
# MAGIC     source.datasource,
# MAGIC     source.is_quarantined,
# MAGIC     current_timestamp(), -- audit_insertdate
# MAGIC     current_timestamp(), -- audit_modifieddate
# MAGIC     true                 -- is_current
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert new records into silver.transactions to implement SCD Type 2
# MAGIC MERGE INTO silver.transactions AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.TransactionID = source.TransactionID
# MAGIC AND target.is_current = true
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC     TransactionID,
# MAGIC     SRC_TransactionID,
# MAGIC     EncounterID,
# MAGIC     PatientID,
# MAGIC     ProviderID,
# MAGIC     DeptID,
# MAGIC     VisitDate,
# MAGIC     ServiceDate,
# MAGIC     PaidDate,
# MAGIC     VisitType,
# MAGIC     Amount,
# MAGIC     AmountType,
# MAGIC     PaidAmount,
# MAGIC     ClaimID,
# MAGIC     PayorID,
# MAGIC     ProcedureCode,
# MAGIC     ICDCode,
# MAGIC     LineOfBusiness,
# MAGIC     MedicaidID,
# MAGIC     MedicareID,
# MAGIC     SRC_InsertDate,
# MAGIC     SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     is_quarantined,
# MAGIC     audit_insertdate,
# MAGIC     audit_modifieddate,
# MAGIC     is_current
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.TransactionID,
# MAGIC     source.SRC_TransactionID,
# MAGIC     source.EncounterID,
# MAGIC     source.PatientID,
# MAGIC     source.ProviderID,
# MAGIC     source.DeptID,
# MAGIC     source.VisitDate,
# MAGIC     source.ServiceDate,
# MAGIC     source.PaidDate,
# MAGIC     source.VisitType,
# MAGIC     source.Amount,
# MAGIC     source.AmountType,
# MAGIC     source.PaidAmount,
# MAGIC     source.ClaimID,
# MAGIC     source.PayorID,
# MAGIC     source.ProcedureCode,
# MAGIC     source.ICDCode,
# MAGIC     source.LineOfBusiness,
# MAGIC     source.MedicaidID,
# MAGIC     source.MedicareID,
# MAGIC     source.SRC_InsertDate,
# MAGIC     source.SRC_ModifiedDate,
# MAGIC     source.datasource,
# MAGIC     source.is_quarantined,
# MAGIC     current_timestamp(), -- audit_insertdate
# MAGIC     current_timestamp(), -- audit_modifieddate
# MAGIC     true                 -- is_current
# MAGIC );
# MAGIC
