# Databricks notebook source
import sys
import traceback


def _columnDelimiter_validate(fileFullPath,fileEncoding,columnDelimiter,fileName):
  #validation Column delimiter
  try:
      objGenHelper = gen_genericHelper()
      with open("/dbfs/"+fileFullPath, mode='r', encoding=fileEncoding) as f:
          header = f.readline()
          if header.find(columnDelimiter) != -1:
              executionStatus = "Column delimiter validation succeeded for the file '" + fileName + "'"
              executionStatusID =  LOG_EXECUTION_STATUS.SUCCESS
          else:
              executionStatus = "Column delimiter validation failed '" + fileName + "'"
              executionStatusID = LOG_EXECUTION_STATUS.FAILED
      return [executionStatusID,executionStatus]		
  except Exception as err:
    raise

def app_columnDelimiter_validate(fileName,fileID,fileType,columnDelimiter,executionID,filePath,fileEncoding):
  try:
      
      objGenHelper = gen_genericHelper()
      logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,fileID,fileName,fileType,VALIDATION_ID.COLUMN_DELIMITER,executionID = executionID)
      
      lstOfStatus = _columnDelimiter_validate(filePath,fileEncoding,columnDelimiter,fileName)
      if(lstOfStatus[0] == LOG_EXECUTION_STATUS.SUCCESS):
          executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,lstOfStatus[1])
          return [LOG_EXECUTION_STATUS.SUCCESS,lstOfStatus[1]]
      else:
          if gl_ERPSystemID == 10:
              executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,lstOfStatus[1])
              return [LOG_EXECUTION_STATUS.FAILED,lstOfStatus[1]]
          else:
              executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,lstOfStatus[1])
              return [LOG_EXECUTION_STATUS.WARNING,lstOfStatus[1]]

  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
