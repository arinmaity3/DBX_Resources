# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,floor

def fin_L1_JEDocumentClassification_01_prepare():
  try:
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global fin_L1_STG_JEDocumentClassification_01_prepare
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()

    dfL1_STG_JEDocumentClassification_01_prepare = fin_L1_TD_Journal.alias("Journal").\
            select(col("Journal.transactionIDbyJEDocumnetNumber"),\
            lit(gl_analysisID).alias("analysisID")).filter(
                                (
                                  (floor(col("Journal.amountDC")) >0) \
                                & (
                                    (floor(col("Journal.amountDC"))%1000 == 0)
                                    | (floor(col("Journal.amountDC"))%1000 >= 999 )
                                  )
                                )
                              |
                                (
                                  (floor(col("Journal.amountDC")) == 0) & (floor(col("Journal.amountLC")) > 0) \
                                & (
                                    (floor(col("Journal.amountLC"))%1000 == 0)
                                    | (floor(col("Journal.amountLC"))%1000 >= 999 )
                                  )
                                )

                               ).distinct()


    fin_L1_STG_JEDocumentClassification_01_prepare = objDataTransformation.gen_convertToCDMandCache \
    (dfL1_STG_JEDocumentClassification_01_prepare,'fin','L1_STG_JEDocumentClassification_01_prepare',True)

    executionStatus = "fin_L1_STG_JEDocumentClassification_01_prepare populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
      
#fin_L1_JEDocumentClassification_01_prepare_populate()
#Validation Script
# df=fin_L1_STG_JEDocumentClassification_01_prepare.alias("j1").join \
#                                           (fin_L1_STG_Journal_DocumentHeaderLevelDistinctIDs.alias("j2"), \
#                                           (col("j1.transactionIDbyJEDocumnetNumber")      == col("j2.transactionIDbyJEDocumnetNumber"))\
#                                            ,"inner")\
#       .select(col("j1.transactionIDbyJEDocumnetNumber").alias("transactionIDbyJEDocumnetNumber") \
#              ,col("j2.documentNumber").alias("documentNumber")\
#             )
# df.display()
