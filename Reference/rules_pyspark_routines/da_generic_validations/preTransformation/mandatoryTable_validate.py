# Databricks notebook source
import sys
import traceback

def app_mandatoryTable_validate(executionID = ""):
  try:

    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = VALIDATION_ID.MANDATORY_TABLE,
                              executionID = executionID)
    
    scopedAnalytics = objGenHelper.gen_lk_cd_parameter_get('GLOBAL','SCOPED_ANALYTICS').split(",")              
    lstOfMissingFiles = list()
    
    [lstOfMissingFiles.append(t.sparkTableName[4:]) \
         for t in gl_metadataDictionary['dic_ddic_tablesRequiredForERPPackages'].alias("M")\
        .join(gl_parameterDictionary[FILES.KPMG_ERP_STAGING_RECORD_COUNT].alias("R"),\
              [upper(col("M.sparkTableName")) == upper(col("R.tableName"))],how = 'left')\
              .filter(col("M.auditProcedureName").isin(scopedAnalytics) &\
                (col("M.ERPSystemID") == gl_ERPSystemID)& \
                (col("M.isMandatory") == 1) &
                ((col("R.netRecordCount") == 0) |(col("R.netRecordCount").isNull())))\
         .select(col("M.sparkTableName")).distinct().collect()]
        
    if(len(lstOfMissingFiles) != 0):
      executionStatus = "Mandatory files '" + ",".join(lstOfMissingFiles) + "' are missing."
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,",".join(lstOfMissingFiles))
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]  
    else:
      executionStatus = "Mandatory files validation succeeded."
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    print(err)
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]   

