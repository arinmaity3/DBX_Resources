# Databricks notebook source
from pyspark.sql.types import StructType
import sys
import traceback

def gen_placeHolderFiles_G0001_populate():
  try:
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    
    lstOfTables = [['dwh','vw_FACT_JEBifurcationBalance','fin_L2_FACT_JEBifurcationBalance']
                   ,['dwh','vw_FACT_JEBifurcation','fin_L2_FACT_GLBifurcation']
                   ,['dwh','vw_DIM_GLAccountCombination','fin_L2_DIM_GLAccountCombination']
                   ,['dwh','vw_FACT_JEBifurcation_Raw','fin_L2_FACT_GLBifurcation_Raw']
                   ,['dwh','vw_DIM_JEBifurcation_LineItem','fin_L2_DIM_JEBifurcation_LineItem']
                   ,['dwh','vw_DIM_JEBifurcation_Base','fin_L2_DIM_JEBifurcation_Base']
                   ,['dwh','vw_DIM_BifurcationDataType','fin_L2_DIM_BifurcationDataType']
                   ,['dwh','vw_DIM_BifurcationPatternSource','knw_L2_DIM_BifurcationPatternSource']
                   ,['dwh','vw_DIM_BifurcationRuleType','knw_L2_DIM_BifurcationRuleType']
                   ,['fin','L2_FACT_GLBifurcation_Agg','fin_L2_FACT_GLBifurcation_Agg']
                   ,['fin','L2_DIM_JEBifurcation_Link','fin_L2_DIM_JEBifurcation_Link']
                   ,['fin','L2_DIM_JEBifurcation_Transaction_Base','fin_L2_DIM_JEBifurcation_Transaction_Base']
                  ]
    
    for tables in lstOfTables:
      fileName = tables[2]   
      query = "{fileName} = spark.createDataFrame([], StructType([]))".format(fileName = fileName)
      exec(query)
      query = "{dfName} = objDataTransformation.gen_convertToCDMStructure_generate({fileName},\
                      '{schemaName}','{tableName}',{includeAnalysisID})[0]"\
                      .format(dfName = fileName,fileName = fileName,\
                      schemaName = tables[0],tableName = tables[1],includeAnalysisID = True)  
      exec(query)
      if (fileName == 'fin_L2_DIM_GLAccountCombination'):
          targetFilePath = gl_CDMLayer2Path + fileName + ".csv"
          query = "objGenHelper.gen_writeSingleCsvFile_perform({fileName},'{targetFilePath}')".\
              format(fileName = fileName,targetFilePath= targetFilePath)
      else:
          targetFilePath = gl_CDMLayer2Path + fileName + ".parquet"
          query = "objGenHelper.gen_writeToFile_perfom({fileName},'{targetFilePath}')".\
              format(fileName = fileName,targetFilePath= targetFilePath)  
      exec(query)
    executionStatus = "Place holder files needed for G0001 has been populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
