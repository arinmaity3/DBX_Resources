# Databricks notebook source
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number,expr,trim,col,lit,when
from pyspark.sql.window import Window

def fin_L2_DIM_JEBifurcation_Transaction_Base_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    
    global fin_L2_DIM_JEBifurcation_Transaction_Base
    
    w = Window().orderBy(lit(''))
    fin_L2_DIM_JEBifurcation_Transaction_Base = fin_L2_DIM_JEBifurcation_AllDimAttributes.alias('jed1')\
      .select(col('transactionNo')\
             ).distinct()\
      .withColumn("transactionLinkID", row_number().over(w))

    fin_L2_DIM_JEBifurcation_Transaction_Base = objDataTransformation.gen_convertToCDMandCache(\
                         fin_L2_DIM_JEBifurcation_Transaction_Base,'fin','L2_DIM_JEBifurcation_Transaction_Base',True)
    
  
    objGenHelper.gen_writeToFile_perfom(fin_L2_DIM_JEBifurcation_Transaction_Base,gl_CDMLayer2Path + "fin_L2_DIM_JEBifurcation_Transaction_Base.parquet")

    executionStatus = "fin_L2_DIM_JEBifurcation_Transaction_Base completed successfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
