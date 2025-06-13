# Databricks notebook source
from pyspark.sql.functions import row_number,expr,trim
from pyspark.sql.window import Window
import sys
import traceback

def gen_L2_DIM_Currency_populate():
    try:
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)

        global gen_L2_DIM_Currency
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()

        gl_lstOfCurrencyCollect.extend(gen_L2_DIM_Organization.\
                       select(col("companyCode"),col("reportingCurrency")\
                       ,lit('orgUnit'))\
                       .distinct().rdd.map(lambda row : [row[0],row[1],row[2]]).collect())

        gl_lstOfCurrencyCollect.extend(gen_L2_DIM_Organization.\
                       select(col("companyCode"),col("localCurrency")\
                       ,lit('orgUnit'))\
                       .distinct().rdd.map(lambda row : [row[0],row[1],row[2]]).collect())

        gen_L1_TMP_Currency = spark.createDataFrame(gl_lstOfCurrencyCollect,currencySchemaCollect).\
                         select(col("companyCode"),col("currencyCode")).distinct()

        gen_L1_TMP_DistinctCurrency = gen_L1_TMP_Currency.join(gen_L2_DIM_Organization,\
                         [gen_L1_TMP_Currency.companyCode == gen_L2_DIM_Organization.companyCode],how='inner').\
                         select(gen_L1_TMP_Currency["currencyCode"])\
                         .filter( ~(col("currencyCode").isin('')) & (col("currencyCode").isNotNull()) )\
                         .distinct()
        
        DummyCurrency = spark.sql("select  \
                                                '#NA'   AS  currencyCode")

        gen_L1_TMP_DistinctCurrency = gen_L1_TMP_DistinctCurrency.union(DummyCurrency)
        w = Window().orderBy(lit('currencySurrogateKey'))
        gen_L2_DIM_Currency = gen_L1_TMP_DistinctCurrency.select(F.row_number().over(w).alias("currencySurrogateKey"),\
                                            col('currencyCode').alias("currencyISOCode"),\
                                            ).distinct()
        gen_L2_DIM_Currency = objDataTransformation.gen_convertToCDMandCache \
            (gen_L2_DIM_Currency,'gen','L2_DIM_Currency',False)
        
        executionStatus = "L2_DIM_Currency populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as e:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


