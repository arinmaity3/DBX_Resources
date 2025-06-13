# Databricks notebook source
import sys
import traceback

def app_ORA_ScopingLedgerCOA_validate(executionID = ""):
  try:
    
    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = VALIDATION_ID.SCOPING_LEDGER_COA,
                              executionID = executionID)
    
    executionStatusID = LOG_EXECUTION_STATUS.STARTED
    executionStatus = ""
    
    recd_count = knw_LK_CD_ReportingSetup.alias('rps').filter(when(col('rps.ledgerID').isNull(),lit(''))\
                        .otherwise(col('rps.ledgerID')).cast("string")!="").count()
    if (recd_count == 0):       
      executionStatus = "LedgerID column is empty in [knw].[LK_CD_ReportingSetup].[ledgerID]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
   
    recd_count = knw_LK_CD_ReportingSetup.alias('rps').filter(when(col('rps.chartofAccount').isNull(),lit(''))\
                        .otherwise(col('rps.chartofAccount')).cast("string")!="").count()
    if (recd_count == 0):
      executionStatus = "Chart of account column is empty in '[knw].[LK_CD_ReportingSetup].[chartofAccount]'" 
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    
    recd_count = erp_GL_LEDGERS.alias('ldg').filter(when(col('ldg.LEDGER_ID').isNull(),lit(''))\
                        .otherwise(col('ldg.LEDGER_ID')).cast("string")!="").count()
    if (recd_count == 0):
      executionStatus = "Ledger ID column is empty in '[erp].[GL_LEDGERS].[LEDGER_ID]'"        
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    
    recd_count = erp_GL_LEDGERS.alias('ldg').filter(when(col('ldg.CHART_OF_ACCOUNTS_ID').isNull(),lit(''))\
                        .otherwise(col('ldg.CHART_OF_ACCOUNTS_ID')).cast("string")!="").count()
    if (recd_count == 0):
      executionStatus = "Chart of accounts column is empty in '[erp].[GL_LEDGERS].[CHART_OF_ACCOUNTS_ID]'"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)

    missing_ScopedLedger_DF = knw_LK_CD_ReportingSetup.alias('DR')\
                           .join(erp_GL_LEDGERS.alias('GLG')\
                               ,(col('DR.ledgerID')==col('GLG.LEDGER_ID'))\
                               &(col('DR.chartofAccount')==col('GLG.CHART_OF_ACCOUNTS_ID')),how="left_anti")\
                          .select(concat(col('DR.ledgerID'),lit("|"),col('DR.chartOfAccount'),lit("|")\
                                        ).alias("columnList")).distinct()
    missingparameter = missing_ScopedLedger_DF.select("columnList").rdd.flatMap(lambda x: x).collect()

    if (len(missingparameter)> 0):
      Joinstring = ','.join(missingparameter)
      executionStatus = "Scoped Ledger ('[knw].[LK_CD_ReportingSetup].[ledgerID]') and COA ('[knw].[LK_CD_ReportingSetup].[chartOfAccount]')"\
                         "in Organization Setup  does not match with the source data loaded in"\
                         "('[erp].[GL_LEDGERS].[LEDGER_ID]') and ('[erp].[GL_LEDGERS].[CHART_OF_ACCOUNTS_ID]')'" + Joinstring +"'"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    
    executionStatus = "ScopingledgerCOA_ validated successfully"  
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as err:
    print(err)
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

