# Databricks notebook source
df= spark.createDataFrame(([1,2,3],[1,2,3],[1,None,3],[1,None,None]),['a','b','c'])

# COMMAND ----------

df.display()

# COMMAND ----------

#Dropping Duplicates
df.dropDuplicates().show()

# COMMAND ----------

df.na.drop().display()

# COMMAND ----------

df.na.drop(subset=['c']).display()

# COMMAND ----------

df.na.fill(value=0,subset=['b']).na.fill(value=-1,subset=['c']).display()

# COMMAND ----------

df.fillna(value=0,subset=['b']).fillna(value=-1,subset=['c']).display()

# COMMAND ----------

df.dropna().dropDuplicates().display()

# COMMAND ----------

simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank,desc

# COMMAND ----------

spec = Window.partitionBy('department').orderBy(desc('salary'))
df1 = df.withColumn('dense_ranking', dense_rank().over(spec))

# COMMAND ----------

df1.display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.functions import row_number

# COMMAND ----------

df1.withColumn('row_number',row_number().over(Window.partitionBy('department').orderBy(desc('salary')))).display()
