# Databricks notebook source
#otc_L1_STG_30_OrderingDataChange_populate
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType    
from datetime import datetime
from pyspark.sql.functions import *
def otc_L1_STG_30_OrderingDataChange_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    
    global otc_L1_STG_30_OrderingDataChange
    
    v_orderingChangeparentTransactionID =\
        when(col('sftp1.childID').isNull(),lit(0))\
        .otherwise(col('sftp1.salesParentTransactionID'))
    
    v_orderingChangetransactionID =\
        when(col('sftp1.childID').isNull(),lit(0))\
        .otherwise(col('sftp1.salesTransactionID'))
    
    v_orderingChangeorderSubTransactionID =\
        when(col('sftp1.childID').isNull(),lit(0))\
        .otherwise(col('sftp1.salesOrderSubTransactionID'))
    
    df_L1_STG_30_OrderingDataChange=otc_L1_TD_ChangeOrder.alias("ordr")\
        .join(otc_L1_STG_26_SalesFlowTreeParent.alias("sftp1")\
            ,((col('ordr.orderingChangedocumentnumber').eqNullSafe(col('sftp1.salesDocumentNumber')))\
             & (col('ordr.orderingchangeDocumentLineItem').eqNullSafe(col('sftp1.salesdocumentLineItem')))\
               ),how="left")\
        .select(lit(v_orderingChangeparentTransactionID).alias("orderingChangeparentTransactionID")\
               ,lit(v_orderingChangetransactionID).alias("orderingChangetransactionID")\
               ,lit(v_orderingChangeorderSubTransactionID).alias("orderingChangeorderSubTransactionID")\
               ,col('ordr.orderingChangedocumentnumber').alias("orderingChangedocumentnumber")\
               ,col('ordr.orderingchangeDocumentLineItem').alias("orderingchangeDocumentLineItem")\
               ,col('ordr.orderingChangeDocumentChangeNumber').alias("orderingChangeDocumentChangeNumber")\
               ,col('ordr.orderingChangeDocumentChangeUser').alias("orderingChangeDocumentChangeUser")\
               ,col('ordr.orderingChangeDocumentChangedate').alias("orderingChangeDocumentChangedate")\
               ,col('ordr.orderingChangeDocumentChangeTime').alias("orderingChangeDocumentChangeTime")\
               ,col('ordr.orderingChangeTransactionCode').alias("orderingChangeTransactionCode")\
               ,col('ordr.orderingDocumentChangeIndicator').alias("orderingDocumentChangeIndicator")\
               ,col('ordr.orderingDocumentChangeField').alias("orderingDocumentChangeField")\
               ,col('ordr.orderingChangeValueNew').alias("orderingChangeValueNew")\
               ,col('ordr.orderingdocumentChangeValueOld').alias("orderingdocumentChangeValueOld")\
               )
    
    otc_L1_STG_30_OrderingDataChange  = objDataTransformation.gen_convertToCDMandCache \
			(df_L1_STG_30_OrderingDataChange,'otc','L1_STG_30_OrderingDataChange',False)
    otc_L1_STG_30_OrderingDataChange.display()
    executionStatus = "L1_STG_30_OrderingDataChange populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
