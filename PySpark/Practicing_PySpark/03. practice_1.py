# Databricks notebook source
data = [(1,"Arin","arinmaity3@tcs.uk.com"),(2,"Riya","riya123@bt.au.org")]

# COMMAND ----------

column = ["id","Name","email"]

# COMMAND ----------

df= spark.createDataFrame(data,column)

# COMMAND ----------

df.display()

# COMMAND ----------

#df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import split,col

# COMMAND ----------

new_df= df.withColumn("username",split(df['email'],"@")[0])\
    .withColumn("organization",split(split(df['email'],"@")[1],"\.")[0])\
    .withColumn("organization",split(split(df['email'],"@")[1],"\.")[1])\
    .withColumn("domain",split(split(df['email'],"@")[1],"\.")[2])

# COMMAND ----------

new_df.display()

# COMMAND ----------

from pyspark.sql.functions import concat_ws,lit,current_date

# COMMAND ----------

new_df=new_df.withColumn("last_name",lit("placeholder"))\
    .withColumn("date",current_date())

# COMMAND ----------

new_df.display()

# COMMAND ----------

new_df = new_df.withColumn("Full Name",concat_ws(" ",new_df['Name'],new_df['last_name']))

# COMMAND ----------

new_df.display()

# COMMAND ----------

from pyspark.sql.functions import substring

# COMMAND ----------

new_df = new_df.withColumn("last",substring(new_df['last_name'],1,5))

# COMMAND ----------

new_df.display()

# COMMAND ----------

from pyspark.sql.functions import initcap

# COMMAND ----------

new_df = new_df.withColumn("last",initcap(new_df['last']))

# COMMAND ----------

new_df.display()

# COMMAND ----------

spark.version
