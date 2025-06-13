# Databricks notebook source
# MAGIC %run "/Users/arinmaity1@gmail.com/Practicing_PySpark/Unit Test/transformation"

# COMMAND ----------

#%run ./transformation

# COMMAND ----------

survey_df  = load_survey_df(spark,"dbfs:/FileStore/SparkCourse/sample.csv")
count_df =count_by_country(survey_df)
count_df.show()

# COMMAND ----------


