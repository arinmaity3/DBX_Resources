# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import col,countDistinct

def otc_SAP_L0_STG_DocumentTypeBackfilling_populate(): 
    try:
      
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()

      logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)

      global otc_L0_STG_DocumentTypeBackfilling
      
      otc_L0_TMP_DocumentTypeBackfilling = erp_VBFA.alias('vbfa_erp')\
                                     .select(col('vbfa_erp.VBELN').alias('documentNumber'),col('vbfa_erp.VBTYP_N').alias('documentType'))\
                                     .union(erp_VBFA.alias('vbfb_erp')\
                                     .select(col('vbfb_erp.VBELV').alias('documentNumber'),col('vbfb_erp.VBTYP_V').alias('documentType')))\
                                     .distinct()

      otc_L0_TMP_BackfillingAmbiguous = otc_L0_TMP_DocumentTypeBackfilling\
                                    .select(col('documentNumber').alias('documentNumber'),col('documentType'))\
                                    .groupby('documentNumber')\
                                    .agg(countDistinct("documentType").alias('countDocumentType'))\
                                    .where(col('countDocumentType') > 1)

      otc_L0_STG_DocumentTypeBackfilling = otc_L0_TMP_DocumentTypeBackfilling.alias('dtbaf')\
                                    .join(otc_L0_TMP_BackfillingAmbiguous.alias('dtbam'), \
                                        col("dtbaf.documentNumber") == col("dtbam.documentNumber"),"left") \
                                    .select(col('dtbaf.documentNumber').alias('documentNumber'),col('dtbaf.documentType').alias('documentType'))\
                                    .filter(col('dtbam.documentNumber').isNull())
              
      otc_L0_STG_DocumentTypeBackfilling =  objDataTransformation.gen_convertToCDMandCache \
         (otc_L0_STG_DocumentTypeBackfilling,'otc','L0_STG_DocumentTypeBackfilling',targetPath=gl_CDMLayer1Path)
       
      executionStatus = "L0_STG_DocumentTypeBackfilling populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
     
        


