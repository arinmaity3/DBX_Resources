# Databricks notebook source
from pyspark.sql.functions import row_number,expr,trim,col,lit
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback

def knw_L2_DIM_BifurcationRuleType_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global knw_L2_DIM_BifurcationRuleType

    knw_L2_DIM_BifurcationRuleType = objGenHelper.gen_readFromFile_perform\
                          (gl_knowledgePath  + 'knw_L2_DIM_BifurcationRuleType.delta')
    
    dwh_L2_DIM_BifurcationRuleType = objGenHelper.gen_readFromFile_perform\
                         (gl_knowledgePath  + 'knw_L2_DIM_BifurcationRuleType.parquet')
    
    dwh_vw_DIM_BifurcationRuleType = objDataTransformation.gen_convertToCDMStructure_generate\
                         (dwh_L2_DIM_BifurcationRuleType,'dwh','vw_DIM_BifurcationRuleType',isIncludeAnalysisID = True)[0]
    
    objGenHelper.gen_writeToFile_perfom\
                          (dwh_vw_DIM_BifurcationRuleType,gl_CDMLayer2Path +"knw_L2_DIM_BifurcationRuleType.parquet")

    executionStatus = "knw_L2_DIM_BifurcationRuleType populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


