# Databricks notebook source
sample_df = spark.read.options(header=True).csv("/FileStore/SparkCourse/sample.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demo_db

# COMMAND ----------

sample_df.repartition(100).write.format("delta").mode("overwrite").saveAsTable("demo_db.sample")

# COMMAND ----------

# MAGIC %fs ls user/hive/warehouse/demo_db.db/sample

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS demo_db.sample

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled",True)
spark.conf.set("spark.databricks.delta.autocompact.enabled",True)

# COMMAND ----------

display(dbutils.fs.mounts())
