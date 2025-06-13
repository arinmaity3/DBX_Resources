# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col

def fin_L1_STG_Journal_00_Initialize_populate():     
  """Populate L1_STG_Journal_00_Initialize,L1_STG_Journal_DocumentHeaderLevelDistinctIDs"""
  
  try:
    global fin_L1_STG_Journal_00_Initialize, fin_L1_STG_Journal_DocumentHeaderLevelDistinctIDs
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    
    dr_wrw= Window.orderBy(lit('A'))
    dr_window = Window.partitionBy().orderBy("companyCode","fiscalYear","documentNumber","financialPeriod")
    
    dr_window1 = Window.partitionBy().orderBy("companyCode","fiscalYear","documentNumber","financialPeriod"\
                                              ,"lineItem","debitCreditIndicator")
    
    dr_wcnt= Window.partitionBy("companyCode","fiscalYear","documentNumber","financialPeriod")\
            .orderBy("companyCode","fiscalYear","documentNumber","financialPeriod")
                      
    fin_L1_STG_Journal_00_Initialize = fin_L1_TD_Journal.select(F.row_number().over(dr_wrw).alias("journalSurrogateKey"),\
                                 F.dense_rank().over(dr_window).alias("transactionIDbyJEDocumnetNumber"),\
                                 F.dense_rank().over(dr_window1).alias("transactionIDbyPrimaryKey"),\
                                "companyCode","fiscalYear","documentNumber","financialPeriod",\
                                 "lineItem","debitCreditIndicator",lit('1').alias("partitionKeyForGLACube"))       
    
   
    fin_L1_STG_Journal_00_Initialize=fin_L1_STG_Journal_00_Initialize.withColumn("lineItemCount", F.count('*').over(dr_wcnt))

    fin_L1_STG_Journal_DocumentHeaderLevelDistinctIDs=fin_L1_STG_Journal_00_Initialize.select((['companyCode','fiscalYear',\
                                                                     'financialPeriod','documentNumber'\
                                                                    ,'transactionIDbyJEDocumnetNumber'])).distinct()
    fin_L1_STG_Journal_00_Initialize  = objDataTransformation.gen_convertToCDMandCache \
        (fin_L1_STG_Journal_00_Initialize,'fin','L1_STG_Journal_00_Initialize',True)
    
    fin_L1_STG_Journal_DocumentHeaderLevelDistinctIDs  = objDataTransformation.gen_convertToCDMandCache \
        (fin_L1_STG_Journal_DocumentHeaderLevelDistinctIDs,'fin','L1_STG_Journal_DocumentHeaderLevelDistinctIDs',True)
    
    recordCount2 = fin_L1_STG_Journal_00_Initialize.count()      
    if recordCount2 != gl_countJET:  
      warnings.warn("Record counts are not reconciled [fin].[L1_TD_Journal]")
      return [False,"fin_L1_STG_Journal_00_Initialize population failed"]
  
    executionStatus = "L1_STG_Journal_00_Initialize populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)

    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)

    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

