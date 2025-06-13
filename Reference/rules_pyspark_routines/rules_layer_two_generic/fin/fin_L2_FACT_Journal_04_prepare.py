# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,regexp_replace,abs

def fin_L2_FACT_Journal_04_prepare():
  try:
    
    global fin_L2_STG_Journal_04_prepare
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
 
    fin_L2_STG_Journal_04_prepare= fin_L1_TD_Journal.alias('jou1')\
       .join(gen_L2_DIM_Organization.alias('dou2'),\
       ((col('jou1.companyCode')==col('dou2.companyCode'))\
       & (col('dou2.companyCode')!=lit('#NA#'))),how='inner')\
       .join(fin_L1_STG_JEWithCriticalComment.alias('jrnlCmnt')\
       ,((col('jrnlCmnt.journalSurrogateKey').eqNullSafe(col('jou1.journalSurrogateKey')))),how='left')\
       .select(col('dou2.organizationUnitSurrogateKey'),\
               col('jou1.journalSurrogateKey')\
       ,when(col('jou1.amountDC') < 0,substring(floor(abs(col("jou1.amountDC")))*-1,-1,1))\
               .otherwise(substring(floor(col("jou1.amountDC")),-1,1)).cast('int')\
               .alias("firstDigitOfAmount")\
       ,when(col('jou1.amountDC') < 0,\
            expr("(length(floor(abs(amountDC))*-1)-length(rtrim(regexp_replace(floor(abs(amountDC))*-1,\
                         RIGHT(floor(abs(amountDC))*-1,1),' '))))" ))\
       .otherwise(expr("(length(floor(amountDC))-length(rtrim(regexp_replace(floor(amountDC)\
               ,RIGHT(floor(amountDC),1),' '))))" ))\
               .alias('consistentEndingNumberOfDigit')\
       ,when(col("jou1.debitCreditIndicator") == "C",col("jou1.quantity") *(-1)).otherwise(col("jou1.quantity"))\
                    .alias("quantityInSign")\
       ,when(col("jou1.debitCreditIndicator") == "C",col("jou1.amountLC") *(-1)).otherwise(col("jou1.amountLC"))\
                    .alias("amountLCInSign")\
       ,when(col("jrnlCmnt.jeCriticalCommentSurrogateKey").isNull(),lit("-1"))\
                 .otherwise(col("jrnlCmnt.jeCriticalCommentSurrogateKey")).alias("jeCriticalCommentSurrogateKey"),\
       col("lineItemCount"))        
   
    fin_L2_STG_Journal_04_prepare = objDataTransformation.gen_convertToCDMandCache \
        (fin_L2_STG_Journal_04_prepare,'fin','L2_STG_Journal_04_prepare',True)

    recordCount2 = fin_L2_STG_Journal_04_prepare.count()
      
    if gl_countJET != recordCount2:
      executionStatus="Number of records in fin_L2_FACT_Journal_04_prepare are \
      not reconciled with number of records in [fin].[L1_TD_Journal]"
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)  
      warnings.warn(executionStatus)
      return [False,"fin_L2_STG_Journal_04_prepare population failed"] 
                
    executionStatus = "fin_L2_STG_Journal_04_prepare  population sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
       
  except Exception as e:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
      
