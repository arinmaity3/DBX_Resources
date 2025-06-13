# Databricks notebook source
import sys
import traceback
import pandas as pd
from pandas import DataFrame

def _zeroKBFile_validate(fileFullPath):
  #Zero KB - business logic
  try:
    objGenHelper = gen_genericHelper()
        
    fileExtension = pathlib.Path(fileFullPath).suffix.lower()
    if (fileExtension == '.xls') or (fileExtension == '.xlsx'):
        dfRFile = objGenHelper.gen_readFromFile_perform(filePath,0,None,True)
        if (StructField("blankData",StringType(),True) in dfRFile.schema):
            executionStatus = "Zero KB file validation failed for the file '" + pathlib.Path(fileFullPath).name + "'"
            executionStatusID = LOG_EXECUTION_STATUS.FAILED
        else:
            executionStatus = "Zero KB file validation succeeded for the file '" + pathlib.Path(fileFullPath).name + "'"
            executionStatusID =  LOG_EXECUTION_STATUS.SUCCESS
    else:
        fileContent = dbutils.fs.head(fileFullPath,1000)
        if len(fileContent.strip()) != 0:
            executionStatus = "Zero KB file validation succeeded for the file '" + pathlib.Path(fileFullPath).name + "'"
            executionStatusID =  LOG_EXECUTION_STATUS.SUCCESS
        else:
            executionStatus = "Zero KB file validation failed for the file '" + pathlib.Path(fileFullPath).name + "'"
            executionStatusID = LOG_EXECUTION_STATUS.FAILED
    
    return [executionStatusID,executionStatus]
  except Exception as err:
    raise

def app_zeroKBFile_validate(fileName,fileID,fileType,executionID,filePath):
    #Zero KB - execution logic
    try:
                        
        logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,fileID,fileName,fileType,VALIDATION_ID.ZERO_KB,executionID = executionID)
        objGenHelper = gen_genericHelper()

        lstOfStatus = _zeroKBFile_validate(filePath)
        
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
