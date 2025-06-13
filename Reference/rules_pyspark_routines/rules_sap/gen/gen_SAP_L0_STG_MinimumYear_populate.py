# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import col,year,lit

def gen_SAP_L0_STG_MinimumYear_populate(): 
    try:
      
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()

      logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)

      global gen_L0_STG_MinimumYear
      
      gen_L0_STG_MinimumYear = erp_BSID\
                         .filter(~((col("GJAHR") == lit('')) & (col("GJAHR").isNotNull()) & (col('GJAHR') == '1900')))\
                         .agg(min("GJAHR").alias("minYear"))\
                         .union(erp_BSAD\
                         .filter(~((col("GJAHR") == lit('')) & (col("GJAHR").isNotNull()) & (col('GJAHR') == '1900')))\
                         .agg(min("GJAHR").alias("minYear")))\
                         .union(erp_BSIK\
                         .filter(~((col("GJAHR") == lit('')) & (col("GJAHR").isNotNull()) & (col('GJAHR') == '1900')))\
                         .agg(min("GJAHR").alias("minYear")))\
                         .union(erp_BSAK\
                         .filter(~((col("GJAHR") == lit('')) & (col("GJAHR").isNotNull()) & (col('GJAHR') == '1900')))\
                         .agg(min("GJAHR").alias("minYear")))\
                         .union(erp_BSIS\
                         .filter(~((col("GJAHR") == lit('')) & (col("GJAHR").isNotNull()) & (col('GJAHR') == '1900')))\
                         .agg(min("GJAHR").alias("minYear")))\
                         .union(erp_BSAS\
                         .filter(~((col("GJAHR") == lit('')) & (col("GJAHR").isNotNull()) & (col('GJAHR') == '1900')))\
                         .agg(min("GJAHR").alias("minYear")))\
                         .union(erp_FEBEP\
                         .filter(~((col("GJAHR") == lit('')) & (col("GJAHR").isNotNull()) & (col('GJAHR') == '1900')))\
                         .agg(min("GJAHR").alias("minYear")))\
                         .union(erp_FEBKO\
                         .filter(~((col("AZDAT") == lit('')) & (col("AZDAT").isNotNull()) & (col('AZDAT') == '1900')))\
                         .agg(min(year("AZDAT")).alias("minYear")))
              
      gen_L0_STG_MinimumYear =  objDataTransformation.gen_convertToCDMandCache \
         (gen_L0_STG_MinimumYear,'gen','L0_STG_MinimumYear',targetPath=gl_CDMLayer1Path)
       
      executionStatus = "gen_L0_STG_MinimumYear populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
     
        

