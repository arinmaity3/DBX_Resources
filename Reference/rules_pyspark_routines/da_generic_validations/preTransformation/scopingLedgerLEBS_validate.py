# Databricks notebook source
import sys
import traceback

def app_ORA_scopingLedgerLEBS_validate(executionID = ""):
  try:
    
    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = VALIDATION_ID.SCOPING_LEDGER_LEBS,
                              executionID = executionID)
    
    executionStatusID = LOG_EXECUTION_STATUS.STARTED
    executionStatus = ""
    
    rec_count = knw_LK_CD_ReportingSetup.alias("base").filter(when(col("base.ledgerID").isNull(),lit(""))\
    .otherwise(col("base.ledgerID")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "LedgerID column is empty in [knw].[LK_CD_ReportingSetup].[ledgerID]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    rec_count = knw_LK_CD_ReportingSetup.alias("base").filter(when(col("base.balancingSegment").isNull(),lit(""))\
    .otherwise(col("base.balancingSegment")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "Balancing segment column is empty in [knw].[LK_CD_ReportingSetup].[balancingSegment]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    rec_count = erp_GL_LEDGER_NORM_SEG_VALS.alias("base").filter(when(col("base.LEDGER_ID").isNull(),lit(""))\
    .otherwise(col("base.LEDGER_ID")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "LedgerID column is empty in [erp].[GL_LEDGER_NORM_SEG_VALS].[LEDGER_ID]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    rec_count = erp_GL_LEDGER_NORM_SEG_VALS.alias("base").filter(when(col("base.SEGMENT_VALUE").isNull(),lit(""))\
    .otherwise(col("base.SEGMENT_VALUE")).cast("string")!="").count()
    if rec_count==0:
      executionStatus = "Segment value column is empty in [erp].[GL_LEDGER_NORM_SEG_VALS].[SEGMENT_VALUE]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    df_Missing_Scoped_ledger = knw_LK_CD_ReportingSetup.alias("rpt").join(erp_GL_LEDGER_NORM_SEG_VALS.alias("glsv")\
    ,(col("rpt.ledgerID")==col("glsv.LEDGER_ID"))&(col("rpt.balancingSegment")==col("glsv.SEGMENT_VALUE").cast("string")),how="left_anti")\
    .select(concat(col("rpt.ledgerID").cast("string"),lit("/"),col("rpt.balancingSegment").cast("string")).alias("columnList")).distinct()

    error_msg = df_Missing_Scoped_ledger.select("columnList").rdd.flatMap(lambda x: x).collect()
    
    if len(error_msg)>0:
        executionStatus = "Scoped ledger ([knw].[LK_CD_ReportingSetup].[ledgerID]) and associated balancing segment ([knw].[LK_CD_ReportingSetup]"
        executionStatus = executionStatus + ".[balancingSegment]) in Organization Setup  does not match with source data loaded in ([erp]" 
        executionStatus = executionStatus + ".[GL_LEDGER_NORM_SEG_VALS].[LEDGER_ID]) and ([erp].[GL_LEDGER_NORM_SEG_VALS].[SEGMENT_VALUE] " 
        executionStatus = executionStatus + str(error_msg)
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    else:
        executionStatus="Scoping Ledger LEBS Validation successful"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as err: 
    print(err)
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus] 
