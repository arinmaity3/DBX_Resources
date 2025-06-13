# Databricks notebook source
import sys
import traceback

def app_ORA_ScopingLedger_validate(executionID = ""):
  try:
    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = VALIDATION_ID.SCOPING_LEDGER,
                              executionID = executionID)
    
                                                                       
    recd_count = knw_LK_CD_ReportingSetup.alias('rps').filter(when(col('rps.ledgerID').isNull(),lit(''))\
                        .otherwise(col('rps.ledgerID')).cast("string")!="").count()
    if (recd_count == 0):
      executionStatus = "LedgerID column is empty in '[knw].[LK_CD_ReportingSetup].[ledgerID]'"  
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
      
    recd_count = erp_GL_LEDGERS.alias('ldgr').filter(when(col('ldgr.LEDGER_ID').isNull(),lit(''))\
                        .otherwise(col('ldgr.LEDGER_ID')).cast("string")!="").count()
    if (recd_count == 0):
      executionStatus = "LedgerID column is empty in '[erp].[GL_LEDGERS].[LEDGER_ID]'"       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
      
      
    df_missing_ScopedledgerID = df = knw_LK_CD_ReportingSetup.alias('DR')\
        .join(erp_GL_LEDGERS.alias('GLG'),(col('DR.ledgerID')==(col('GLG.LEDGER_ID'))),how='left_anti')\
        .select(col('DR.ledgerID').alias("columnList")).distinct()
    missingparameter = df_missing_ScopedledgerID.select("columnList").rdd.flatMap(lambda x: x).collect()

    if len(missingparameter)>0:
      Joinstring = ','.join(missingparameter)
      executionStatus = "Scoped ledger ID in Organization Setup ('knw.LK_CD_ReportingSetup.ledgerID') does not matched with "\
                       " the source data loaded in('[erp].[GL_LEDGERS].LEDGER_ID]')" + str(Joinstring) +"'"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus] 
    
    executionStatus="ScopingLedger Validation successful"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    print(err)
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

