# Databricks notebook source
from pyspark.sql.functions import  lit,col,row_number
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback

def otc_L2_DIM_SalesTypeParentSubBranchTransactionChain_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global otc_L2_DIM_SalesTypeParentSubBranchTransactionChain

    otc_L2_DIM_SalesTypeParentSubBranchTransactionChain  =  otc_L1_STG_salesTypeParentSubBranchTransactionChain.\
                                                            select(col('SubBranchId'),col('SubBranchString'),col('SubBranchRank'),col('SubBranchDocumentSetClassificationId')).\
                                                            distinct()
    
    otc_L2_DIM_SalesTypeParentSubBranchTransactionChain = objDataTransformation.gen_convertToCDMandCache\
          (otc_L2_DIM_SalesTypeParentSubBranchTransactionChain,'otc','L2_DIM_SalesTypeParentSubBranchTransactionChain',False,targetPath = gl_CDMLayer2Path)
    
    default_List =[[0,'NONE','NONE','NONE','NONE']]
    default_df = spark.createDataFrame(default_List)
    
    otc_vw_DIM_salesTypeParentSubBranchTransactionChain = otc_L2_DIM_SalesTypeParentSubBranchTransactionChain.alias('PSBTC')\
                                                          .join(knw_LK_GD_SubBranchDocumentSetClassification.alias('SBDSC'),\
                                                                  (col("PSBTC.subBranchDocumentSetClassificationId")==col("SBDSC.subBranchDocumentSetClassificationId")),how="inner")\
                                                          .select(col('PSBTC.subBranchID'),col('PSBTC.subBranchString'),\
                                                                 when(col('PSBTC.subBranchRank') == 1,"Single Cut Off Item")\
                                                                 .otherwise("Multiple Cut Off Items").alias('subBranchRank'),\
                                                                 col('SBDSC.subBranchDocumentSetClassification'),col('SBDSC.subBranchDocumentSetClassificationCategory'))\
                                                          .union(default_df)
    
    objGenHelper.gen_writeToFile_perfom(otc_vw_DIM_salesTypeParentSubBranchTransactionChain\
                                                    ,gl_CDMLayer2Path +"otc_vw_DIM_salesTypeParentSubBranchTransactionChain.parquet") 
    
    executionStatus = "otc_L2_DIM_SalesTypeParentSubBranchTransactionChain populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
