# Databricks notebook source
# MAGIC %md
# MAGIC ### Join Strategies in Spark:
# MAGIC ##### 1. Broadcast Hash Join
# MAGIC ##### 2. Shuffle Hash Join
# MAGIC ##### 3. Shuffle Sort Merge Join
# MAGIC ##### 4. Cartesian Join
# MAGIC ##### 5. Broadcasted Nested Loop Join

# COMMAND ----------

spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)

# COMMAND ----------

spark.conf.get("spark.sql.join.preferSortMergeJoin")

# COMMAND ----------

countries_df = spark.read.options(header=True,inferSchema=True).csv("/FileStore/tables/countries.csv")
country_regions_df = spark.read.options(header=True,inferSchema=True).csv("/FileStore/tables/country_regions.csv")

# COMMAND ----------

joined_df = countries_df.join(country_regions_df,countries_df['region_id']==country_regions_df['id'])

# COMMAND ----------

joined_df.explain(extended=True)

# COMMAND ----------

joined_df.explain(mode="formatted")

# COMMAND ----------

joined_df.explain("cost")

# COMMAND ----------


