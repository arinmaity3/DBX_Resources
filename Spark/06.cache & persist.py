# Databricks notebook source
from pyspark.sql.functions import expr
df = spark.range(1,1000000).toDF("id")\
            .repartition(10)\
            .withColumn("square",expr("id*id"))\
            .cache()

# COMMAND ----------

df.take(10)

# COMMAND ----------

df.count()
