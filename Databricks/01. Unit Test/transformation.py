# Databricks notebook source
def load_survey_df(spark,data_file):
    return spark.read.options(header=True,inferschema=True).csv(data_file)

# COMMAND ----------

def count_by_country(survey_df):
    return survey_df.filter("Age<40")\
                    .select("Age","Gender","Country","state")\
                    .groupBy("country")\
                    .count()
