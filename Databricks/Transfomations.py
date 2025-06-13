# Databricks notebook source
# MAGIC %md
# MAGIC https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.html

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. select(): 
# MAGIC - Narrow transformation
# MAGIC - Selects a subset of columns from the DataFrame. 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. filter(): 
# MAGIC - Narrow transformation
# MAGIC - Filters the DataFrame based on a condition.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.withColumn(): 
# MAGIC - Narrow transformation
# MAGIC - Adds a new column to the DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. drop(): 
# MAGIC - Narrow
# MAGIC - Drops a column from the DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. distinct(): 
# MAGIC - Wide
# MAGIC - Returns a new DataFrame with the distinct rows of the original DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. union(): 
# MAGIC - Narrow
# MAGIC - Returns a new DataFrame with the rows of the original DataFrame and the rows of the specified DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. intersect(): 
# MAGIC - 
# MAGIC Intersect is a wide transformation in PySpark DataFrames.
# MAGIC
# MAGIC A wide transformation is a transformation that requires data to be shuffled across multiple partitions in order to be computed. This is because the intersect operation needs to compare all of the data in one DataFrame to all of the data in the other DataFrame, regardless of which partition it is in.
# MAGIC
# MAGIC To do this, Spark first needs to shuffle the data so that all of the data for a given key is on the same partition. This can be a costly operation, especially for large datasets.
# MAGIC
# MAGIC Once the data has been shuffled, Spark can then perform the intersect operation and return a new DataFrame with the data that is present in both DataFrames.
# MAGIC

# COMMAND ----------


# Create two DataFrames
df1 = spark.createDataFrame([("Alice", 10), ("Bob", 20)], ["name", "age"])
df2 = spark.createDataFrame([("Alice", 10), ("Carol", 30)], ["name", "age"])

# Intersect the two DataFrames
intersected_df = df1.intersect(df2)
df1.show()
df2.show()
# Print the results
intersected_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### 8. exceptAll(): 
# MAGIC
# MAGIC - exceptAll is also a wide transformation in PySpark. It is similar to intersect, but instead of returning the data that is present in both DataFrames, it returns the data that is present in the first DataFrame but not in the second DataFrame.

# COMMAND ----------

# Except the two DataFrames
df1.show()
df2.show()
excepted_df = df1.exceptAll(df2)

# Print the results
excepted_df.show()
df2.exceptAll(df1).show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### 9. join(): 
# MAGIC - Wide
# MAGIC - Joins the original DataFrame with another DataFrame based on a common column.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 10. groupBy().agg(): 
# MAGIC - Wide
# MAGIC - Groups the rows of the DataFrame by a column or a set of columns.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 12. sort() / orderBy(): 
# MAGIC - Wide
# MAGIC - Sorts the rows of the DataFrame by a column or a set of columns.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 13. limit(): 
# MAGIC - Narrow
# MAGIC - Returns a new DataFrame with the first n rows of the original DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 14. sample(): 
# MAGIC - Narrow
# MAGIC - Returns a new DataFrame with a random sample of the rows of the original DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 15. coalesce(): 
# MAGIC Reduces the number of partitions in the DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 16. repartition(): 
# MAGIC Repartions the DataFrame into a specified number of partitions.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 17. pivot() :
# MAGIC - Wide

# COMMAND ----------

# MAGIC %md
# MAGIC #### 18. dropDuplicates() :
# MAGIC - Wide

# COMMAND ----------

# MAGIC %md
# MAGIC #### 19. na.drop() or dropna():
# MAGIC - Narrow

# COMMAND ----------

# MAGIC %md
# MAGIC #### 20. na.fill() or fillna():
# MAGIC - Narrow
