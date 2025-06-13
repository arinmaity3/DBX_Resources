# Databricks notebook source
import sys
import traceback
import pandas as pd
from pandas import DataFrame

def _zeroRowCount_validate(fileFullPath):
  #app_zeroRowCount_validate
  try:
    objGenHelper = gen_genericHelper()   
    
    fileExtension = pathlib.Path(fileFullPath).suffix.lower()
    if (fileExtension == '.xls') or (fileExtension == '.xlsx'):
      dfFile = objGenHelper.gen_readFromFile_perform(fileFullPath,0,None,True)
      if (StructField("blankData",StringType(),True) in dfFile.schema):
        executionStatus = "Zero row count validation failed '" + pathlib.Path(fileFullPath).name + "'"
        executionStatusID = LOG_EXECUTION_STATUS.FAILED
      else:
        executionStatus = "Zero row count file validation succeeded '" + pathlib.Path(fileFullPath).name + "'"
        executionStatusID =  LOG_EXECUTION_STATUS.SUCCESS
    else:
      executionStatus = "Zero row count file validation is applicable for excel files"
      executionStatusID =  LOG_EXECUTION_STATUS.NOT_APPLICABLE
      
    return [executionStatusID,executionStatus]
  except Exception as err:
    raise
    
def app_zeroRowCount_validate(fileName,fileID,fileType,executionID):
  try:
    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,fileID,fileName,fileType,VALIDATION_ID.ZERO_ROW_COUNT,executionID = executionID)
    filePath = gl_rawFilesPath + fileName  
    lstOfStatus=_zeroRowCount_validate(filePath)

    if(lstOfStatus[0] == LOG_EXECUTION_STATUS.SUCCESS):            
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,lstOfStatus[1])
      return [LOG_EXECUTION_STATUS.SUCCESS,lstOfStatus[1]]
    else:            
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,lstOfStatus[1])  
      return [LOG_EXECUTION_STATUS.FAILED,lstOfStatus[1]]
        
  except Exception as err:        
     executionStatus = objGenHelper.gen_exceptionDetails_log()       
     executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
     return [LOG_EXECUTION_STATUS.FAILED,executionStatus]   
  

