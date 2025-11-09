# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists healthcare_db_ws.audit

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists healthcare_db_ws.audit.load_logs(
# MAGIC   data_source string,
# MAGIC   tablename string,
# MAGIC   numberofrowscopied int,
# MAGIC   watermarkcolumnname string,
# MAGIC   loaddate timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table audit.load_logs;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from healthcare_db_ws.audit.load_logs;
