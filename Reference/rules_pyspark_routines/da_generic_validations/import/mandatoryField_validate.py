# Databricks notebook source
import sys
import traceback
import pandas as pd


def _mandatoryField_validate(dfSource,fileName,fileID,gl_metadataDictionary,schemaName,tableName):
  try:
          
    lstOfKeyColumns = list()
    lstOfInputColumns = list()
    
    [lstOfKeyColumns.append(c.columnName.upper()) for c in  gl_metadataDictionary['dic_ddic_column'].\
                    filter((upper(col("schemaName")) == schemaName.upper()) & \
                    (upper(col("tableName")) == tableName.upper()) & \
                    (col("isKey") == 'X')).\
                    select(col("columnName")).distinct().collect()]
    
    [lstOfInputColumns.append(c.upper()) for c in dfSource.columns]
    lstOfMissingCols = list(set(lstOfKeyColumns)-set(lstOfInputColumns))
    
    if(len(lstOfMissingCols) == 0):
      executionStatus = "Mandatory field validation succeeded for the table '" + fileName + "'"
      executionStatusID =  LOG_EXECUTION_STATUS.SUCCESS
    else:
      executionStatus = "Mandatory fields [" + ','.join(lstOfMissingCols) +"] are missing for the file '" + fileName + "'"
      executionStatusID =  LOG_EXECUTION_STATUS.FAILED
      
    return [executionStatusID,executionStatus,lstOfMissingCols]
  
  except Exception as err:
     raise
 

def app_mandatoryField_validate(dfSource,fileName,fileID,fileType,gl_metadataDictionary,schemaName,tableName,executionID):
  try:
                        
    logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,fileID,fileName,fileType,VALIDATION_ID.MANDATORY_FIELD,executionID = executionID)
    objGenHelper = gen_genericHelper()
    
    lstOfStatus = _mandatoryField_validate(dfSource,fileName,fileID,gl_metadataDictionary,schemaName,tableName)
    
    if(lstOfStatus[0] == LOG_EXECUTION_STATUS.SUCCESS):
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,lstOfStatus[1])  
      return [LOG_EXECUTION_STATUS.SUCCESS,lstOfStatus[1]]
    else:
      data = lstOfStatus[2]
      df_missingColumns = spark.createDataFrame(pd.DataFrame(data,columns =['columns']))
      df_missingColumns = df_missingColumns.withColumn("fileName",lit(fileName))
      
      w = Window().orderBy(lit('groupSlno'))
      dfmandatoryFielddtl = df_missingColumns.withColumn("groupSlno", row_number().over(w))
      dfmandatoryFielddtls= objGenHelper.gen_dynamicPivot(fileType =fileType,
                                      validationID =VALIDATION_ID.MANDATORY_FIELD.value,
                                      pivotColumn1='columns',
                                      pivotColumn2='fileName',                                      
                                      dfToPivot=dfmandatoryFielddtl)
     
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,lstOfStatus[1],dfDetail = dfmandatoryFielddtls)  
      return [LOG_EXECUTION_STATUS.FAILED,lstOfStatus[1],df_missingColumns]

  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)    
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus] 
  
