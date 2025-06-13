# Databricks notebook source
import uuid
import sys
import traceback
def gen_L2_DIM_Parameter_populate():
  """Populate gen_L2_DIM_Parameter"""
  try:
    
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      
      analysisid = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ANALYSISID'))
      dfParameter=spark.sql("SELECT routineId"\
                            " ,name parameterName"\
                            " ,parameterValue"\
                            " from knw_LK_CD_Parameter")
  
      gen_L2_DIM_Parameter = objDataTransformation.gen_convertToCDMStructure_generate(dfParameter,'dwh','vw_DIM_Parameter',True)[0] 
      objGenHelper.gen_writeToFile_perfom(gen_L2_DIM_Parameter,gl_CDMLayer2Path +"gen_L2_DIM_Parameter.parquet")  
      executionStatus = "gen_L2_DIM_Parameter populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)

      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
 
