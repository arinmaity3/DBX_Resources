# Databricks notebook source
import sys
import traceback
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

def otc_L2_DIM_SalesTypeTransactionChain_populate():
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        
        global otc_L2_DIM_SalesTypeTransactionChain
        
        otc_L2_DIM_SalesTypeTransactionChain = otc_L1_STG_SalesTypeTransactionChain\
          .select(col('salesTypeTransactionChainID').alias('salesTypeTransactionChainSurrogateKey')\
	    	 ,col('salesTypeTransactionSalesStart').alias('salesTypeTransactionChainStart')\
	    	 ,col('salesTypeTransactionChainString').alias('salesTypeTransactionChainString')\
	    	 ,col('SalesTypeTransactionChainString2').alias('salesTypeTransactionChainString2')\
	    	 ,col('SalesTypeTransactionChainString3').alias('salesTypeTransactionChainString3'))
        
        otc_L2_DIM_SalesTypeTransactionChain = objDataTransformation.gen_convertToCDMandCache\
              (otc_L2_DIM_SalesTypeTransactionChain,'otc','L2_DIM_SalesTypeTransactionChain',False)
        
        default_List =[[0,'NONE','NONE','NONE','NONE']]
        default_df = spark.createDataFrame(default_List)
        
        otc_vw_DIM_salesTypeTransactionChain = otc_L2_DIM_SalesTypeTransactionChain\
          .select(col('salesTypeTransactionChainSurrogateKey').alias('salesTypeTransactionChainSurrogateKey')\
             ,col('salesTypeTransactionChainStart').alias('salesTypeTransactionChainStart')\
             ,col('salesTypeTransactionChainString').alias('salesTypeTransactionChainString')\
             ,col('salesTypeTransactionChainString2').alias('salesTypeTransactionChainString2')\
             ,col('salesTypeTransactionChainString3').alias('salesTypeTransactionChainString3'))\
          .union(default_df)
    
        objGenHelper.gen_writeToFile_perfom(otc_vw_DIM_salesTypeTransactionChain\
                                            ,gl_CDMLayer2Path +"otc_vw_DIM_salesTypeTransactionChain.parquet")        
        
        executionStatus = "otc_L2_DIM_SalesTypeTransactionChain populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as e:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


  
  

