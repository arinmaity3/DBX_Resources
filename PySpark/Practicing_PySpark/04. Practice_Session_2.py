# Databricks notebook source
#from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, when ,first


# COMMAND ----------

# Create a Spark session
#spark = SparkSession.builder.appName("CricketAnalysis").getOrCreate()

# COMMAND ----------

# Sample data
data = [
    (1, "VIRAT", 4),
    (1, "KL", 1),
    (2, "SKY", 6),
    (2, "KL", 4),
    (2, "VIRAT", 6),
    (3, "Rohit", 6),
    (3, "VIRAT", 6),
    (3, "KL", 4),
    (3, "SKY", 6),
    (3, "Hardik", 6)
]

# Create a DataFrame
columns = ["Match No", "Batsman", "Score"]
df = spark.createDataFrame(data, columns)


# COMMAND ----------

df.display()

# COMMAND ----------

# Define a Window specification partitioned by Match No. and ordered by Score
window_spec = Window.partitionBy("Match No").orderBy(col("Score").desc())

# Add a row number column to identify the order of scores within each match
df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))

# COMMAND ----------

df_with_row_num.display()

# COMMAND ----------

# Create conditions for selecting First6, Second6, and Third6
conditions = [
    col("row_num") == 1,
    col("row_num") == 2,
    col("row_num") == 3
]

# Use when() and otherwise() to populate the columns based on conditions
df_with_rank = df_with_row_num.withColumn("First6", when(conditions[0], col("Batsman")).otherwise(None))
df_with_rank = df_with_rank.withColumn("Second6", when(conditions[1], col("Batsman")).otherwise(None))
df_with_rank = df_with_rank.withColumn("Third6", when(conditions[2], col("Batsman")).otherwise(None))

# COMMAND ----------

df_with_rank.display()


# COMMAND ----------

pivot_df = df_with_rank.groupBy("Match No") \
    .agg(
        first("First6").alias("First6"),
        first("Second6").alias("Second6"),
        first("Third6").alias("Third6")
    )

# COMMAND ----------

# Show the result
pivot_df.show()




# COMMAND ----------

# Stop the Spark session
#spark.stop()
