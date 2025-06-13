# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  col,expr,substring,lit


def otc_L2_DIM_SalesTypeParentBranchTransactionChain_populate():
 
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)

    global otc_L2_DIM_SalesTypeParentBranchTransactionChain
    
    vExpr_ = expr("LEFT(documentTypeKPMG, CHARINDEX('|', documentTypeKPMG,3))")

    otc_L2_DIM_SalesTypeParentBranchTransactionChain = otc_L1_STG_22_InitTreeParentBranchString.alias('ITPBS')\
       .select(col('ITPBS.branchDataID').alias('branchDataID')\
          ,col('ITPBS.hasBranchReturn').alias('hasBranchReturn')\
          ,col('ITPBS.hasBranchInvoiceCancellation').alias('hasBranchInvoiceCancellation')\
          ,substring(col('ITPBS.documentTypeClient'),1,1000).alias('documentTypeClient')\
          ,substring(col('ITPBS.documentTypeClientReal'),1,4000).alias('documentTypeClientReal')\
          ,substring(col('ITPBS.documentTypeKPMG'),1,1000).alias('documentTypeKPMG')\
          ,substring(col('ITPBS.documentTypeKPMGReal'),1,4000).alias('documentTypeKPMGReal')\
          ,substring(col('ITPBS.documentTypeKPMGShort'),1,1000).alias('documentTypeKPMGShort')\
          ,substring(col('ITPBS.documentTypeKPMGShortReal'),1,4000).alias('documentTypeKPMGShortReal')\
          ,substring(col('ITPBS.documentTypeKPMGGroup'),1,1000).alias('documentTypeKPMGGroup')\
          ,substring(col('ITPBS.documentTypeKPMGGroupReal'),1,4000).alias('documentTypeKPMGGroupReal')\
          ,substring(col('ITPBS.documentTypeKPMGInterpreted'),1,4000).alias('documentTypeKPMGInterpreted')\
          ,substring(col('ITPBS.documentTypeKPMGInterpretedReal'),1,4000).alias('documentTypeKPMGInterpretedReal'))\
       .filter(col('ITPBS.branchDataID') > 0)\
       .distinct()
    
    otc_L2_DIM_SalesTypeParentBranchTransactionChain = objDataTransformation.gen_convertToCDMandCache\
          (otc_L2_DIM_SalesTypeParentBranchTransactionChain,'otc','L2_DIM_SalesTypeParentBranchTransactionChain',False)
    
    default_List =[[0,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE']]
    default_df = spark.createDataFrame(default_List)
  
    otc_vw_DIM_salesTypeParentBranchTransactionChain = otc_L2_DIM_SalesTypeParentBranchTransactionChain.\
       select(col('branchDataID').alias('branchDataID')\
           ,when(col('hasBranchReturn')==1,lit('Yes - Branch has Return Doc'))\
                .otherwise(lit('No - Branch has no Return Doc')).alias('hasBranchReturn')\
           ,when(col('hasBranchInvoiceCancellation')==1,lit('Yes - Branch has Invoice Cancellation Doc'))\
                .otherwise(lit('No - Branch has no Invoice Cancellation Doc'))\
                .alias('hasBranchInvoiceCancellation')\
           ,col('documentTypeClient').alias('documentTypeClient')\
           ,col('documentTypeClientReal').alias('documentTypeClientReal')\
           ,col('documentTypeKPMG').alias('documentTypeKPMG')\
           ,lit(vExpr_).alias('documentTypeKPMGChainStart')\
           ,col('documentTypeKPMGReal').alias('documentTypeKPMGReal')\
           ,col('documentTypeKPMGShort').alias('documentTypeKPMGShort')\
           ,col('documentTypeKPMGShortReal').alias('documentTypeKPMGShortReal')\
           ,col('documentTypeKPMGGroup').alias('documentTypeKPMGGroup')\
           ,col('documentTypeKPMGGroupReal').alias('documentTypeKPMGGroupReal')\
           ,col('documentTypeKPMGInterpreted').alias('documentTypeKPMGInterpreted')\
           ,col('documentTypeKPMGInterpretedReal').alias('documentTypeKPMGInterpretedReal'))\
       .union(default_df)
    
    objGenHelper.gen_writeToFile_perfom(otc_vw_DIM_salesTypeParentBranchTransactionChain\
                ,gl_CDMLayer2Path +"otc_vw_DIM_salesTypeParentBranchTransactionChain.parquet")  
    
    executionStatus = "L2_DIM_SalesTypeParentBranchTransactionChain populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

 

