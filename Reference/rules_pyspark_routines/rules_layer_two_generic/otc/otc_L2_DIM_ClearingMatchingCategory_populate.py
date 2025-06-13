# Databricks notebook source
from pyspark.sql.functions import  lit,col,row_number
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback

def otc_L2_DIM_ClearingMatchingCategory_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global otc_L2_DIM_ClearingMatchingCategory

    w = Window.orderBy(F.lit(''))
    otc_L2_DIM_ClearingMatchingCategory = knw_LK_GD_ClearingBoxLookupMatchingCategory.select(col('matchingCategory'),col('matchingCategoryText'))\
                                                  .withColumn("clearingMatchingCategorySurrogateKey", F.row_number().over(w))

    
    otc_L2_DIM_ClearingMatchingCategory = objDataTransformation.gen_convertToCDMandCache(otc_L2_DIM_ClearingMatchingCategory,\
                                          'otc','L2_DIM_ClearingMatchingCategory',False,targetPath = gl_CDMLayer2Path)
    
    
    otc_vw_DIM_ClearingMatchingCategory = otc_L2_DIM_ClearingMatchingCategory.select(col('clearingMatchingCategorySurrogateKey'),col('matchingCategory')\
                                                                                    ,col('matchingCategoryText'))
       
    default_List =[[0,'NONE','NONE']]
    default_df = spark.createDataFrame(default_List)
    otc_vw_DIM_ClearingMatchingCategory = otc_vw_DIM_ClearingMatchingCategory.union(default_df)     
    
    objGenHelper.gen_writeToFile_perfom(otc_vw_DIM_ClearingMatchingCategory\
                ,gl_CDMLayer2Path +"otc_vw_DIM_ClearingMatchingCategory.parquet") 
    
    
    executionStatus = "otc_L2_DIM_ClearingMatchingCategory populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
