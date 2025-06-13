# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import trim

def app_mandatoryParameter_validate(executionID = ""):
  try:
    
    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = VALIDATION_ID.MANDATORY_PARAMS,
                              executionID = executionID)
    
    lstOfMandatoryParams = ['START_DATE','END_DATE','PERIOD_START','PERIOD_END',
                        'FIN_YEAR','ERP_SYSTEM_ID','ERP_SYSTEM_ID_GENERIC']
    
    
    #check some key populated with null or empty
    if (gl_parameterDictionary['knw_LK_CD_Parameter'].\
        filter((col("name").isNull() == True) | (trim(col("name")) == "" )).count() != 0):              
        executionStatus = "Name column is empty in Parameter file 'knw_LK_CD_Parameter'"
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    
    
    #check missing mandatory parameters
    lstOfMissingParams = list()
    [lstOfMissingParams.append(p.name)
      for p in gl_parameterDictionary['knw_LK_CD_Parameter'].\
      filter((col("name").isin(lstOfMandatoryParams)) & \
             (col("parameterValue").isNull()) | (trim(col("parameterValue")) == "")).\
      select(col("name")).collect()]
    
    if(len(lstOfMissingParams)!= 0):        
      executionStatus = "Mandatory parameters '" + ",".join(lstOfMissingParams) + "''"\
                        "are missing in parameter file 'knw_LK_CD_Parameter'"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    
    executionStatus = 'Mandatory parameter validation completed successfully.'
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    print(err)
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]   

