# Databricks notebook source
import sys
import traceback

def app_ORA_GLMultipleChartOfAccount_validate(executionID = ""):
  try:
    
    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = VALIDATION_ID.GL_MULTIPLE_CHARTOFACCOUNT,
                              executionID = executionID)
    
    
    nullValue_count = knw_LK_CD_ReportingSetup.alias('rps').filter(when(col('rps.chartofAccount').isNull(),lit(''))\
                        .otherwise(col('rps.chartofAccount')).cast("string")!="").count()
    if nullValue_count == 0:
      executionStatus = "Chart of account column is empty in ''[knw].[LK_CD_ReportingSetup].[chartofAccount]'' "       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    
    
    knw_LK_CD_ReportingSetup_distinct = knw_LK_CD_ReportingSetup.select(col('chartOfAccount')).distinct() 
    missingparameter = knw_LK_CD_ReportingSetup_distinct.select('chartOfAccount').rdd.map(lambda x: x[0]).collect()
    
    if (len(missingparameter) > 1):
      Joinstring = ','.join(missingparameter)
      executionStatus = "Multiple chart of account exists in ('[knw].[LK_CD_ReportingSetup].[chartofAccount]')" + Joinstring  + "'"       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
      

    executionStatus = "GLMultiple chartOfAccounts validation completed successfully."  
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]  
  
  except Exception as err:
    print(err)
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

