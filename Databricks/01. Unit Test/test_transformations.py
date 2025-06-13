# Databricks notebook source
# MAGIC %run ./transformation

# COMMAND ----------

from unittest import TextTestRunner,TestCase,TestSuite
from pyspark.sql import SparkSession

# COMMAND ----------

class TransformationTestCase(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder\
                                .appName("test-case-pyspark")\
                                .getOrCreate()
    
    def test_datafile_loading(self):
        sample_df =load_survey_df(self.spark,"dbfs:/FileStore/SparkCourse/sample.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count,9,"Record count should be 9")

    def test_country_count(self):
        sample_df =load_survey_df(self.spark,"dbfs:/FileStore/SparkCourse/sample.csv")
        count_list =count_by_country(sample_df).collect()

        count_dict  = dict()
        for row in count_list:
            count_dict[row['country']] = row['count']
        

        self.assertEqual(count_dict['United States'],4,"Count for US should be 4")
        self.assertEqual(count_dict['Canada'],2,"Count for Canada should be 9")
        self.assertEqual(count_dict['United Kingdom'],1,"Count for UK should be 1")

# COMMAND ----------

def suite():
    suite = TestSuite()
    suite.addTest(TransformationTestCase('test_datafile_loading'))
    suite.addTest(TransformationTestCase('test_country_count'))
    return suite

# COMMAND ----------

runner = TextTestRunner()
runner.run(suite())
