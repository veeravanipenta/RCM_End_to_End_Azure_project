# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

claims_df=spark.read.csv("abfss://landing@healthcaresac.dfs.core.windows.net/claimsdata/*.csv",header=True)

claims_df = claims_df.withColumn(
    "datasource",
    f.when(f.col("_metadata.file_path").contains("hospital1"), "hosa")
     .when(f.col("_metadata.file_path").contains("hospital2"), "hosb")
     .otherwise(None)
)

display(claims_df)

# COMMAND ----------

claims_df.write.format("parquet").mode("overwrite").save("abfss://bronze@healthcaresac.dfs.core.windows.net/claims")
