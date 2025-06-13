# Databricks notebook source
import sys
import traceback

def app_ORA_scopingLedgerCalendar_validate(executionID = ""):
  try:
  
    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = VALIDATION_ID.SCOPING_LEDGER_CALENDAR,
                              executionID = executionID)
    
    executionStatusID = LOG_EXECUTION_STATUS.STARTED
    executionStatus = ""
    
    rec_count = erp_GL_LEDGERS.alias("base").filter(when(col("base.LEDGER_ID").isNull(),lit(""))\
    .otherwise(col("base.LEDGER_ID")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "LedgerID column is empty in [erp].[GL_LEDGERS].[LEDGER_ID]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    rec_count = erp_GL_LEDGERS.alias("base").filter(when(col("base.PERIOD_SET_NAME").isNull(),lit(""))\
    .otherwise(col("base.PERIOD_SET_NAME")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "Period set name column is empty in [erp].[GL_LEDGERS].[PERIOD_SET_NAME]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    rec_count = erp_GL_LEDGERS.alias("base").filter(when(col("base.ACCOUNTED_PERIOD_TYPE").isNull(),lit(""))\
    .otherwise(col("base.ACCOUNTED_PERIOD_TYPE")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "Accounted period type column is empty in [erp].[GL_LEDGERS].[ACCOUNTED_PERIOD_TYPE]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    rec_count = knw_LK_CD_ReportingSetup.alias("base").filter(when(col("base.ledgerID").isNull(),lit(""))\
    .otherwise(col("base.ledgerID")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "LedgerID column is empty in [knw].[LK_CD_ReportingSetup].[ledgerID]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
        
    rec_count = erp_GL_PERIODS.alias("base").filter(when(col("base.PERIOD_SET_NAME").isNull(),lit(""))\
    .otherwise(col("base.PERIOD_SET_NAME")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "Period set name column is empty in [erp].[GL_PERIODS].[PERIOD_SET_NAME]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    
    rec_count = erp_GL_PERIODS.alias("base").filter(when(col("base.PERIOD_TYPE").isNull(),lit(""))\
    .otherwise(col("base.PERIOD_TYPE")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "Period type column is empty in [erp].[GL_PERIODS].[PERIOD_TYPE]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    
    df_LEDGER_ID_Distinct = erp_GL_LEDGERS.alias("gl")\
    .join(knw_LK_CD_ReportingSetup.alias("lrs"),col("gl.LEDGER_ID")==col("lrs.ledgerID"),how="inner")\
    .select(col("gl.LEDGER_ID").alias("LEDGER_ID")).distinct()

    period_accounted_count = erp_GL_LEDGERS.alias("gl")\
    .join(df_LEDGER_ID_Distinct.alias("ldg"),col("gl.LEDGER_ID")==col("ldg.LEDGER_ID"),how="inner")\
    .select(concat(col("gl.PERIOD_SET_NAME"),col("gl.ACCOUNTED_PERIOD_TYPE")).alias("period_accounted")).distinct().count()

    if period_accounted_count>1:
      executionStatus ="Multiple calendar exists, create different analysis for each calendar"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    df_missing_scoped_ledgerid = erp_GL_LEDGERS.alias("gl")\
    .join(erp_GL_PERIODS.alias("gp"),(col("gp.PERIOD_SET_NAME")==col("gl.PERIOD_SET_NAME"))\
                                &(col("gp.PERIOD_TYPE")==col("gl.ACCOUNTED_PERIOD_TYPE")),how="left_anti")\
    .join(knw_LK_CD_ReportingSetup.alias("lrs"),col("lrs.ledgerID")==col("gl.LEDGER_ID").cast("string"),how="inner")\
    .select(col("gl.LEDGER_ID").cast("string").alias("columnList")).distinct()

    error_msg = df_missing_scoped_ledgerid.select("columnList").rdd.flatMap(lambda x: x).collect()
    
    if len(error_msg)>0:      
        executionStatus = "Ledger '" + ",".join(error_msg) + "' does not match with source data"        
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    else:
        executionStatus = "Scoping Ledger Calendar Validation successful"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
  except Exception as err: 
    print(err)
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus] 
