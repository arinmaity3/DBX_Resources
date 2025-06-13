# Databricks notebook source
# MAGIC %md
# MAGIC #### 01. Counting Instances in Text
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Find the number of times the words 'bull' and 'bear' occur in the contents. We're counting the number of times the words occur so words like 'bullish' should not be included in our count.
# MAGIC Output the word 'bull' and 'bear' along with the corresponding number of occurrences.

# COMMAND ----------

from pyspark.sql.functions import split, explode, col

# Create a Spark session
#spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Sample DataFrame with a 'content' column
data = [("1", "bull and bear are not bullish"), ("2", "bulls and bears are not bullish at all")]
df = spark.createDataFrame(data, ["id", "content"])


# COMMAND ----------

df.display()

# COMMAND ----------

# Tokenize the content into words
df.withColumn("words", split(col("content"), " ")).display()

# COMMAND ----------

df = df.withColumn("words", split(col("content"), " "))

# COMMAND ----------

# Explode the words into separate rows
df = df.select("id", explode(col("words")).alias("word"))

# COMMAND ----------

df.display()

# COMMAND ----------

# Filter out words that are 'bull' or 'bear'
filtered_df = df.filter((col("word") == "bull") | (col("word") == "bear"))

# COMMAND ----------

filtered_df.display()

# COMMAND ----------

# Group by word and count occurrences
result_df = filtered_df.groupBy("word").count()

# COMMAND ----------


# Show the result
result_df.show()
