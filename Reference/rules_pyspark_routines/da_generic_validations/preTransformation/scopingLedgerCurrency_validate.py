# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import lit,col,coalesce

def app_ORA_scopingLedgerCurrency_validate(executionID = ""):
  try:
    
    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = VALIDATION_ID.SCOPING_LEDGER_CURRENCY,
                              executionID = executionID)
    
    executionStatusID = LOG_EXECUTION_STATUS.STARTED
    executionStatus = ""

    rec_count = knw_LK_CD_ReportingSetup.alias("base").filter(when(col("base.ledgerID").isNull(),lit(""))\
    .otherwise(col("base.ledgerID")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "LedgerID column is empty in [knw].[LK_CD_ReportingSetup].[ledgerID]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    rec_count = erp_GL_LEDGERS.alias("base").filter(when(col("base.LEDGER_ID").isNull(),lit(""))\
    .otherwise(col("base.LEDGER_ID")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "LedgerID column is empty in [erp].[GL_LEDGERS].[LEDGER_ID]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    df_scopingledgerCurrency_missing = knw_LK_CD_ReportingSetup.alias("dr").join(erp_GL_LEDGERS.alias("glg"),(col("dr.ledgerID")==col("glg.LEDGER_ID")),how="left")\
    .filter(col("dr.localCurrencyCode")!=coalesce(col("glg.CURRENCY_CODE"),lit("")))\
    .select(concat(col("dr.ledgerID"),lit("|"),col("dr.companyCode"),lit("|"),col("dr.localCurrencyCode"),lit("|"),col("glg.CURRENCY_CODE"))\
    .alias("columnList")).distinct()
    error_msg = df_scopingledgerCurrency_missing.select("columnList").rdd.flatMap(lambda x: x).collect()

    if len(error_msg)>0:
        executionStatus ="Scoped currency in Organization Setup ([knw].[LK_CD_ReportingSetup].[localCurrencyCode])  does not match with"
        executionStatus = executionStatus + "the source data loaded in ([erp].[GL_LEDGERS].[CURRENCY_CODE]) " + str(error_msg)
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    else:
        executionStatus = "Scoping Ledger Currency Validation successful"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
      
  except Exception as err: 
    print(err)
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
