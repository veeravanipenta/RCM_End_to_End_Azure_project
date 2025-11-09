# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

# Read the CSV file
cptcodes_df = spark.read.csv("abfss://landing@healthcaresac.dfs.core.windows.net/cptcodes/*.csv", header=True)

# Replace whitespaces in column names with underscores and convert to lowercase
for col in cptcodes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col)
cptcodes_df.write.format("parquet").mode("overwrite").save("abfss://bronze@healthcaresac.dfs.core.windows.net/cptcodes")

display(cptcodes_df)
