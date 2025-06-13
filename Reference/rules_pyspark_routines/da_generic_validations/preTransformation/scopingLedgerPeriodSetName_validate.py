# Databricks notebook source
import sys
import traceback

def app_ORA_scopingLedgerPeriodSetName_validate(executionID = ""):
  try:
    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = VALIDATION_ID.SCOPING_LEDGER_PERIOD_SET_NAME,
                              executionID = executionID)
    executionStatusID = LOG_EXECUTION_STATUS.STARTED
    executionStatus = ""
  
    recd_count = erp_GL_LEDGERS.alias('ldgr').filter(when(col('ldgr.LEDGER_ID').isNull(),lit(''))\
                        .otherwise(col('ldgr.LEDGER_ID')).cast("string")!="").count()
    if (recd_count == 0):
      executionStatus = "LedgerID column is empty in '[erp].[GL_LEDGERS].[LEDGER_ID]'"  
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
       
    recd_count = erp_GL_LEDGERS.alias('ldgr').filter(when(col('ldgr.PERIOD_SET_NAME').isNull(),lit(''))\
                        .otherwise(col('ldgr.PERIOD_SET_NAME')).cast("string")!="").count()
    if (recd_count == 0):
      executionStatus = "Period set name column is empty in '[erp].[GL_LEDGERS].[PERIOD_SET_NAME]'"       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
      
      
    recd_count = knw_LK_CD_ReportingSetup.alias('rps').filter(when(col('rps.ledgerID').isNull(),lit(''))\
                        .otherwise(col('rps.ledgerID')).cast("string")!="").count()
    if (recd_count == 0):
      executionStatus = "LedgerID column is empty in '[knw].[LK_CD_ReportingSetup].[ledgerID]'"       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]  
    
      
    recd_count = erp_GL_PERIODS.alias('glp').filter(when(col('glp.PERIOD_SET_NAME').isNull(),lit(''))\
                        .otherwise(col('glp.PERIOD_SET_NAME')).cast("string")!="").count()
    if (recd_count == 0):
      executionStatus = "Period set name column is empty in '[erp].[GL_PERIODS].[PERIOD_SET_NAME]'"       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus] 
    
    
    df_duplicate_PERIOD_SET_NAME = erp_GL_LEDGERS.alias('GL')\
                                    .join(erp_GL_PERIODS.alias('GP'),\
                                         (col('GP.PERIOD_SET_NAME')==(col('GL.PERIOD_SET_NAME'))),how='inner')\
                                    .join(knw_LK_CD_ReportingSetup.alias('RS'),\
                                         (col('RS.LEDGERID')==(col('GL.LEDGER_ID').cast("string"))),how='inner')\
                                    .select(col('GL.PERIOD_SET_NAME').alias('PERIOD_SET_NAME')).distinct()

    missingparameter = df_duplicate_PERIOD_SET_NAME.select("PERIOD_SET_NAME").rdd.flatMap(lambda x: x).collect()

    if (len(missingparameter)>1):
      Joinstring = ','.join(missingparameter)
      executionStatus = "The scoped ledger IDs are associated with more than one PERIOD_SET_NAME in the GL_PERIODS Table ("  + str(Joinstring) +""  \
                        ") Please create a separate analysis for each ledger group(s) per PERIOD_SET_NAME"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus] 
    
    executionStatus="ScopingLedgerPeriodSetName Validation successful"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    print(err)
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
