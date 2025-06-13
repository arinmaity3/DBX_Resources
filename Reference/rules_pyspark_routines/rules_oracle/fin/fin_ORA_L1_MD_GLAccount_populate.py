# Databricks notebook source
from pyspark.sql.functions import concat,coalesce
import sys
import traceback

def fin_ORA_L1_MD_GLAccount_populate(): 
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    
    global fin_L1_MD_GLAccount

    finYear = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR')

    df_lrs = knw_LK_CD_ReportingSetup.alias("rpt")\
              .join(erp_GL_LEDGERS.alias("gll"),col("rpt.ledgerID")==col("gll.LEDGER_ID"),how="inner")\
              .select(col("rpt.balancingSegment").alias("balancingSegment")\
              ,col("gll.CHART_OF_ACCOUNTS_ID").alias("chartofaccount")\
              ,col("rpt.companyCode").alias("companyCode")\
              ,col("rpt.reportingLanguageCode").alias("reportingLanguageCode")).distinct()

    df_glaccauto = erp_GL_JE_HEADERS.alias("glh")\
              .join(erp_GL_LEDGERS.alias("gll"),col("glh.LEDGER_ID")==col("gll.LEDGER_ID"))\
              .join(erp_GL_PERIODS.alias("glp"),(col("gll.PERIOD_SET_NAME")==col("glp.PERIOD_SET_NAME"))\
              &(col("gll.ACCOUNTED_PERIOD_TYPE")==col("glp.PERIOD_TYPE"))\
              &(col("glh.PERIOD_NAME")==col("glp.PERIOD_NAME")),how="inner")\
              .join(erp_GL_JE_LINES.alias("gjl"),(col("glh.LEDGER_ID")==col("gjl.LEDGER_ID"))\
              &(col("glh.JE_HEADER_ID")==col("gjl.JE_HEADER_ID")),how="inner")\
              .join(erp_GL_CODE_COMBINATIONS.alias("gcc"),(col("gll.CHART_OF_ACCOUNTS_ID")==col("gcc.CHART_OF_ACCOUNTS_ID"))\
              &(col("gjl.CODE_COMBINATION_ID")==col("gcc.CODE_COMBINATION_ID")),how="inner")\
              .join(gen_ORA_L0_STG_ClientCCIDAccount.alias("lsc"),col("gcc.CODE_COMBINATION_ID")==col("lsc.codeCombinationId"),how="inner")\
              .filter((~col("glh.JE_SOURCE").isin(["Manual","AutoCopy","Other","Spreadsheet"])) & (col("glp.PERIOD_YEAR") == finYear))\
              .select(concat(col("gll.LEDGER_ID"),lit("-"),col("lsc.balancingSegmentValue")).alias("companyCode")\
              ,col("lsc.naturalAccountNumber").alias("naturalAccountNumber")\
              ,lit(1).alias("isOnlyAutomatedPostingAllowed")).distinct()   
        
    
    fin_L1_MD_GLAccount = gen_ORA_L0_STG_ClientCCIDAccount.alias("lsc")\
              .join(df_lrs.alias("lrs"),(col("lrs.chartofaccount")==col("lsc.chartOfAccountsID"))\
              &(col("lrs.balancingSegment")==col("lsc.balancingSegmentValue")),how="inner")\
              .join(erp_FND_ID_FLEX_STRUCTURES_VL.alias("fifsvl")\
              ,(col("fifsvl.ID_FLEX_CODE")==lit("GL#"))\
              &(col("fifsvl.APPLICATION_ID")==lit(101))\
              &(col("fifsvl.ID_FLEX_NUM")==col("lsc.chartOfAccountsID")),how="left")\
              .join(df_glaccauto.alias("glaccauto"),(col("lrs.companyCode")==col("glaccauto.companyCode"))\
              &(col("lsc.naturalAccountNumber")==	col("glaccauto.naturalAccountNumber")),how="left")\
              .select(col("lrs.companyCode").alias("companyCode")\
              ,col("lsc.naturalAccountNumber").alias("AccountNumber")\
              ,col("lsc.naturalAccountDesc").alias("AccountName")\
              ,col("lsc.naturalAccountDesc").alias("accountDescription")\
              ,col("lrs.reportingLanguageCode").alias("LanguageCode")\
              ,col("lsc.chartOfAccountsID").alias("ChartOfAccountId")\
              ,col("fifsvl.ID_FLEX_STRUCTURE_NAME").alias("ChartOfAccountText")\
              ,when(col("lsc.accountType").isin(["A","L","O"]),lit("BS"))\
              .when(col("lsc.accountType").isin(["R","E"]),lit("PL"))\
              .otherwise(col("lsc.accountType")).alias("accountType")\
              ,coalesce(col("glaccauto.isOnlyAutomatedPostingAllowed"),lit(0)).alias("isOnlyAutomatedPostingAllowed")\
              ,lit(3).alias("isInterCoFromAccountMapping")\
              ,lit(3).alias("isInterCoFromMasterData")\
              ,lit(3).alias("isInterCoFromTransactionData")\
              ,lit("#NA#").alias("segment01")
              ,lit("#NA#").alias("segment02")
              ,lit("#NA#").alias("segment03")
              ,lit("#NA#").alias("segment04")
              ,lit("#NA#").alias("segment05")).distinct()

    fin_L1_MD_GLAccount = objDataTransformation.gen_convertToCDMandCache \
        (fin_L1_MD_GLAccount,'fin','L1_MD_GLAccount',targetPath=gl_CDMLayer1Path)
    
    executionStatus = "L1_MD_GLAccount populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
