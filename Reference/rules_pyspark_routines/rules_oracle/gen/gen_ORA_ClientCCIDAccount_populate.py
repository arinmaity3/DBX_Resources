# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import expr,min,sum,col,lit,when,lag
from pyspark.sql.window import Window

def gen_ORA_ClientCCIDAccount_populate(): 
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
        global gen_ORA_L0_STG_ClientCCIDAccount        
        global gen_L1_STG_BalanceSegmentDetermination

        lcr=knw_LK_CD_ReportingSetup.alias("a")\
        .join(erp_GL_LEDGERS.alias("b"),\
        expr("a.ledgerID=b.LEDGER_ID"),"inner")\
        .selectExpr("b.CHART_OF_ACCOUNTS_ID as chartofaccount")\
        .distinct()

        gen_ORA_L0_TMP_NaturalAccount=\
        erp_GL_CODE_COMBINATIONS.alias("gcc")\
        .join(erp_FND_ID_FLEX_SEGMENTS.alias("fs"),\
        expr("( fs.ID_FLEX_NUM = gcc.CHART_OF_ACCOUNTS_ID ) "),"inner")\
        .join(erp_FND_SEGMENT_ATTRIBUTE_VALUES.alias("fv"),\
        expr("( fs.ID_FLEX_CODE = fv.ID_FLEX_CODE\
         AND fs.APPLICATION_ID = fv.APPLICATION_ID\
         AND fs.ID_FLEX_NUM = fv.ID_FLEX_NUM\
         AND fs.APPLICATION_COLUMN_NAME = fv.APPLICATION_COLUMN_NAME\
         AND fv.APPLICATION_ID = 101\
         AND fv.ID_FLEX_CODE = 'GL#'\
         AND fv.SEGMENT_ATTRIBUTE_TYPE = 'GL_ACCOUNT'\
         AND fv.ATTRIBUTE_VALUE ='Y' ) "),"inner")\
        .join(lcr.alias("lcr"),\
        expr("gcc.CHART_OF_ACCOUNTS_ID = lcr.chartofAccount"),"inner")\
        .filter(expr("gcc.SUMMARY_FLAG='N'"))\
        .selectExpr("gcc.CODE_COMBINATION_ID as codeCombinationId"\
  	     		 ,"CASE fv.APPLICATION_COLUMN_NAME        \
  	     			 WHEN 'SEGMENT1' THEN GCC.SEGMENT1   \
  	     			 WHEN 'SEGMENT2' THEN GCC.SEGMENT2   \
  	     			 WHEN 'SEGMENT3' THEN GCC.SEGMENT3   \
  	     			 WHEN 'SEGMENT4' THEN GCC.SEGMENT4   \
  	     			 WHEN 'SEGMENT5' THEN GCC.SEGMENT5   \
  	     			 WHEN 'SEGMENT6' THEN GCC.SEGMENT6   \
  	     			 WHEN 'SEGMENT7' THEN GCC.SEGMENT7   \
  	     			 WHEN 'SEGMENT8' THEN GCC.SEGMENT8   \
  	     			 WHEN 'SEGMENT9' THEN GCC.SEGMENT9   \
  	     			 WHEN 'SEGMENT10' THEN GCC.SEGMENT10 \
  	     			 WHEN 'SEGMENT11' THEN GCC.SEGMENT11 \
  	     			 WHEN 'SEGMENT12' THEN GCC.SEGMENT12 \
  	     			 WHEN 'SEGMENT13' THEN GCC.SEGMENT13 \
  	     			 WHEN 'SEGMENT14' THEN GCC.SEGMENT14 \
  	     			 WHEN 'SEGMENT15' THEN GCC.SEGMENT15 \
  	     			 WHEN 'SEGMENT16' THEN GCC.SEGMENT16 \
  	     			 WHEN 'SEGMENT17' THEN GCC.SEGMENT17 \
  	     			 WHEN 'SEGMENT18' THEN GCC.SEGMENT18 \
  	     			 WHEN 'SEGMENT19' THEN GCC.SEGMENT19 \
  	     			 WHEN 'SEGMENT20' THEN GCC.SEGMENT20 \
  	     			 WHEN 'SEGMENT21' THEN GCC.SEGMENT21 \
  	     			 WHEN 'SEGMENT22' THEN GCC.SEGMENT22 \
  	     			 WHEN 'SEGMENT23' THEN GCC.SEGMENT23 \
  	     			 WHEN 'SEGMENT24' THEN GCC.SEGMENT24 \
  	     			 WHEN 'SEGMENT25' THEN GCC.SEGMENT25 \
  	     			 WHEN 'SEGMENT26' THEN GCC.SEGMENT26 \
  	     			 WHEN 'SEGMENT27' THEN GCC.SEGMENT27 \
  	     			 WHEN 'SEGMENT28' THEN GCC.SEGMENT28 \
  	     			 WHEN 'SEGMENT29' THEN GCC.SEGMENT29 \
  	     			 WHEN 'SEGMENT30' THEN GCC.SEGMENT30 \
  	     			 ELSE NULL  END as accountNumber"\
  	     		 ,"fs.SEGMENT_NAME    as   naturalAccountName"\
  	     		 ,"fs.FLEX_VALUE_SET_ID  as   FLEX_VALUE_SET_ID"\
  	     		 ,"gcc.CHART_OF_ACCOUNTS_ID as   CHART_OF_ACCOUNTS_ID"\
  	     		 ,"gcc.ACCOUNT_TYPE         as  ACCOUNT_TYPE")

        gen_L1_STG_BalanceSegmentDetermination=\
        erp_GL_CODE_COMBINATIONS.alias("gcc")\
        .join(erp_GL_LEDGERS.alias("gl"),\
        expr("( gl.CHART_OF_ACCOUNTS_ID= gcc.CHART_OF_ACCOUNTS_ID ) "),"inner")\
        .join(knw_LK_CD_ReportingSetup.alias("lcr"),\
        expr("( gl.LEDGER_ID = lcr.ledgerID )"),"inner")\
        .selectExpr("gcc.CODE_COMBINATION_ID as codeCombinationId",\
  	     		 "CASE gl.BAL_SEG_COLUMN_NAME             \
  	     		 WHEN 'SEGMENT1' THEN GCC.SEGMENT1   \
  	     		 WHEN 'SEGMENT2' THEN GCC.SEGMENT2   \
  	     		 WHEN 'SEGMENT3' THEN GCC.SEGMENT3   \
  	     		 WHEN 'SEGMENT4' THEN GCC.SEGMENT4   \
  	     		 WHEN 'SEGMENT5' THEN GCC.SEGMENT5   \
  	     		 WHEN 'SEGMENT6' THEN GCC.SEGMENT6   \
  	     		 WHEN 'SEGMENT7' THEN GCC.SEGMENT7   \
  	     		 WHEN 'SEGMENT8' THEN GCC.SEGMENT8   \
  	     		 WHEN 'SEGMENT9' THEN GCC.SEGMENT9   \
  	     		 WHEN 'SEGMENT10' THEN GCC.SEGMENT10 \
  	     		 WHEN 'SEGMENT11' THEN GCC.SEGMENT11 \
  	     		 WHEN 'SEGMENT12' THEN GCC.SEGMENT12 \
  	     		 WHEN 'SEGMENT13' THEN GCC.SEGMENT13 \
  	     		 WHEN 'SEGMENT14' THEN GCC.SEGMENT14 \
  	     		 WHEN 'SEGMENT15' THEN GCC.SEGMENT15 \
  	     		 WHEN 'SEGMENT16' THEN GCC.SEGMENT16 \
  	     		 WHEN 'SEGMENT17' THEN GCC.SEGMENT17 \
  	     		 WHEN 'SEGMENT18' THEN GCC.SEGMENT18 \
  	     		 WHEN 'SEGMENT19' THEN GCC.SEGMENT19 \
  	     		 WHEN 'SEGMENT20' THEN GCC.SEGMENT20 \
  	     		 WHEN 'SEGMENT21' THEN GCC.SEGMENT21 \
  	     		 WHEN 'SEGMENT22' THEN GCC.SEGMENT22 \
  	     		 WHEN 'SEGMENT23' THEN GCC.SEGMENT23 \
  	     		 WHEN 'SEGMENT24' THEN GCC.SEGMENT24 \
  	     		 WHEN 'SEGMENT25' THEN GCC.SEGMENT25 \
  	     		 WHEN 'SEGMENT26' THEN GCC.SEGMENT26 \
  	     		 WHEN 'SEGMENT27' THEN GCC.SEGMENT27 \
  	     		 WHEN 'SEGMENT28' THEN GCC.SEGMENT28 \
  	     		 WHEN 'SEGMENT29' THEN GCC.SEGMENT29 \
  	     		 WHEN 'SEGMENT30' THEN GCC.SEGMENT30 \
  	     		 ELSE NULL END as balancingSegment" ).distinct()

        lcr=knw_LK_CD_ReportingSetup.alias("a")\
        .join(erp_GL_LEDGERS.alias("b"),\
        expr("a.ledgerID=b.LEDGER_ID"),"inner")\
        .selectExpr("a.balancingSegment","b.CHART_OF_ACCOUNTS_ID as chartofaccount")\
        .distinct()

        gen_ORA_L0_STG_ClientCCIDAccount=\
        gen_ORA_L0_TMP_NaturalAccount.alias("gcc")\
        .join(gen_L1_STG_BalanceSegmentDetermination.alias("bs"),\
        expr("( bs.codeCombinationId = gcc.codeCombinationId ) "),"inner")\
        .join(erp_FND_FLEX_VALUES_VL.alias("fl"),\
        expr("( fl.FLEX_VALUE_SET_ID = gcc.FLEX_VALUE_SET_ID\
         AND fl.FLEX_VALUE = gcc.accountnumber ) "),"inner")\
        .join(lcr.alias("lcr"),\
        expr("( lcr.chartofAccount=gcc.CHART_OF_ACCOUNTS_ID\
        and lcr.balancingSegment=bs.balancingSegment )"),"inner")\
         .selectExpr("gcc.codeCombinationId    as codeCombinationId"\
        ,"gcc.accountnumber      as naturalAccountNumber"\
        ,"gcc.naturalAccountName  as naturalAccountName"\
        ,"fl.DESCRIPTION       as naturalAccountDesc"\
        ,"bs.balancingSegment      as balancingSegmentValue"\
        ,"gcc.CHART_OF_ACCOUNTS_ID as chartOfAccountsID"\
        ,"gcc.ACCOUNT_TYPE         as accountType")

        gen_ORA_L0_STG_ClientCCIDAccount =  objDataTransformation.gen_convertToCDMandCache \
         (gen_ORA_L0_STG_ClientCCIDAccount,'gen','ORA_L0_STG_ClientCCIDAccount',targetPath=gl_layer0Temp_gen)

        gen_L1_STG_BalanceSegmentDetermination =  objDataTransformation.gen_convertToCDMandCache \
         (gen_L1_STG_BalanceSegmentDetermination,'gen','L1_STG_BalanceSegmentDetermination',targetPath=gl_layer0Temp_gen)
        
        executionStatus = "gen_ORA_L0_STG_ClientCCIDAccount \
  	     			    and gen_L1_STG_BalanceSegmentDetermination populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

