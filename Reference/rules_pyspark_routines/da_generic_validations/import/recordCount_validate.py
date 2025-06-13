# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import abs,col, asc
from pyspark.sql.types import Row
import traceback  
import datetime
from datetime import datetime
import os

def _recordCountERP_validate(fileFullPath,fileEncoding,fileID,fileName):
  #validateRecordCount
  try:
    objGenHelper = gen_genericHelper()
     
    importedRows = int(gl_parameterDictionary['sourceFiles'].filter(col("fileID") == fileID)\
    .select(col("rowsExtracted")).collect()[0][0])
  
    uploadedRows = 0
    fp = "/dbfs" + fileFullPath
    with open(fp, "r",encoding = fileEncoding) as f:
      for line in f:
        if(line.strip()!= ""):
          uploadedRows = uploadedRows + 1

    diffrows = importedRows - (uploadedRows -1)            
    if diffrows == 0 :
      executionStatus = "The imported rows are reconciled with the uploaded rows '" + fileName + "'"
      executionStatusID =  LOG_EXECUTION_STATUS.SUCCESS
    else:
      executionStatus = "The imported rows are not reconciled with the uploaded rows '" + fileName + "'"
      executionStatusID = LOG_EXECUTION_STATUS.FAILED      
    
    return   [executionStatusID,executionStatus,importedRows,uploadedRows -1]
  except Exception as err:
    raise

def _recordCount_validate(dfSource,fileFullPath,fileEncoding,fileName):
  #validateRecordCount
  try:
    objGenHelper = gen_genericHelper()
   
    importedRows = dfSource.count()
    uploadedRows = 0
    fp = "/dbfs" + fileFullPath
    with open(fp, "r",encoding = fileEncoding) as f:
      for line in f:
        if(line.strip()!= ""):
          uploadedRows = uploadedRows + 1

    
    diffrows = importedRows - (uploadedRows -1)
    if diffrows == 0 :
      executionStatus = "The imported rows are reconciled with the uploaded rows '" + fileName + "'"
      executionStatusID =  LOG_EXECUTION_STATUS.SUCCESS
    else:
      executionStatus = "The imported rows are not reconciled with the uploaded rows '" + fileName + "'"
      executionStatusID = LOG_EXECUTION_STATUS.WARNING      
    
    return   [executionStatusID,executionStatus,importedRows,uploadedRows -1]
  except Exception as err:
    raise


def app_recordCount_validate(dfSource,fileName,fileID,fileType,executionID,filePath,fileEncoding,ERPSystemID=10): 
  try:
    
        
    logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,fileID,fileName,fileType,VALIDATION_ID.UPLOAD_VS_IMPORT,executionID = executionID)
    
    objGenHelper = gen_genericHelper()
    
    if ERPSystemID == 10:        
        lstOfStatus = _recordCount_validate(dfSource,filePath,fileEncoding,fileName)
    else:                
        lstOfStatus = _recordCountERP_validate(filePath,fileEncoding,fileID,fileName)

    if(lstOfStatus[0] == LOG_EXECUTION_STATUS.SUCCESS):            
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,lstOfStatus[1])  
      return [LOG_EXECUTION_STATUS.SUCCESS,lstOfStatus[1],lstOfStatus[3],None,VALIDATION_ID.UPLOAD_VS_IMPORT.value]
    else:
      importedRows = lstOfStatus[2]
      uploadedRows = lstOfStatus[3]
      
      validationResult = [[1,VALIDATION_ID.UPLOAD_VS_IMPORT.value,fileType,fileType,"TotalRecordsInFile",importedRows],
                          [1,VALIDATION_ID.UPLOAD_VS_IMPORT.value,fileType,fileType,"TotalImportedRecords",uploadedRows]]                  
      
      dfResult_Detail = spark.createDataFrame(data= validationResult,schema = gl_ValidationResultDetailSchema) 
      
      executionLog.add(LOG_EXECUTION_STATUS.WARNING,logID,lstOfStatus[1],dfDetail = dfResult_Detail )  
      return [LOG_EXECUTION_STATUS.WARNING,lstOfStatus[1],uploadedRows,None,VALIDATION_ID.UPLOAD_VS_IMPORT.value]
        
  except Exception as err:        
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus,None)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus,0,None,VALIDATION_ID.UPLOAD_VS_IMPORT.value] 

