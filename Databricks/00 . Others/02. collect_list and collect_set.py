# Databricks notebook source
# MAGIC %md
# MAGIC PySpark SQL collect_list() and collect_set() functions are used to create an array (ArrayType) column on DataFrame by merging rows, typically after group by or window partitions.

# COMMAND ----------


# Import
# from pyspark.sql import SparkSession

# Create SparkSession
# spark = SparkSession.builder.appName('SparkByExamples.com') \
#                     .getOrCreate()

# Prepare data
data = [('James','Java'),
  ('James','Python'),
  ('James','Python'),
  ('Anna','PHP'),
  ('Anna','Javascript'),
  ('Maria','Java'),
  ('Maria','C++'),
  ('James','Scala'),
  ('Anna','PHP'),
  ('Anna','HTML')
]

# Create DataFrame
df = spark.createDataFrame(data,schema=["name","languages"])
df.printSchema()
df.show()


# COMMAND ----------

from pyspark.sql.functions import collect_list,collect_set

# COMMAND ----------

df_collected_list = df.groupBy('name').agg(collect_list('languages').alias("Collected_languages"))

# COMMAND ----------

df_collected_list.display()

# COMMAND ----------

from pyspark.sql.functions import explode

# COMMAND ----------

df_collected_list.select('name',explode('collected_languages')).display()

# COMMAND ----------

df_collected_set = df.groupBy('name').agg(collect_set('languages').alias("lang_set"))

# COMMAND ----------

df_collected_set.display()

# COMMAND ----------

df_collected_set.select("name",explode('lang_set')).display()
