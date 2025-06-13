# Databricks notebook source
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number,col,lit
from pyspark.sql.window import Window

def otc_L2_FACT_SalesSLToGLReconciliationGLAccountList_populate():
    try:
            
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)

      w= Window.orderBy(lit(''))

      otc_L2_FACT_SalesSLToGLReconciliationGLAccountList = otc_L2_DIM_SalesSLToGLReconciliation.alias('slgl')\
         .join(otc_L1_STG_SLToGLReconciliationBillingToFILines.alias('fil1')\
               ,(col('slgl.combinedPopulationID')==(col('fil1.combinedPopulationID'))),how='inner')\
         .select(col('slgl.salesSLToGLReconciliationSurrogateKey').alias('salesSLToGLReconciliationSurrogateKey')\
                ,col('fil1.glAccountSurrogateKey').alias('glAccountSurrogateKey')\
                ,col('fil1.AccountNumberName').alias('AccountNumberName')\
                ,col('fil1.FIAmountLC').alias('FIAmountLC')\
                ,col('fil1.isRevenue').alias('isRevenue'))

      otc_L2_FACT_SalesSLToGLReconciliationGLAccountList = otc_L2_FACT_SalesSLToGLReconciliationGLAccountList\
          .withColumn("salesSLToGLReconciliationGLAccountListSurrogateKey",F.row_number().over(w))

      otc_L2_FACT_SalesSLToGLReconciliationGLAccountList  = objDataTransformation.gen_convertToCDMandCache\
          (otc_L2_FACT_SalesSLToGLReconciliationGLAccountList,'otc','L2_FACT_SalesSLToGLReconciliationGLAccountList',False)

      otc_vw_FACT_SalesSLToGLReconciliationGLAccountList = otc_L2_FACT_SalesSLToGLReconciliationGLAccountList.alias('fp2')\
          .select(col('fp2.salesSLToGLReconciliationGLAccountListSurrogateKey')\
              .alias('salesSLToGLReconciliationGLAccountListSurrogateKey')\
          ,col('fp2.salesSLToGLReconciliationSurrogateKey').alias('salesSLToGLReconciliationSurrogateKey')\
          ,col('fp2.glAccountSurrogateKey').alias('glAccountSurrogateKey')\
          ,col('fp2.FIAmountLC').alias('FIAmountLC'))
      
      objGenHelper.gen_writeToFile_perfom(otc_vw_FACT_SalesSLToGLReconciliationGLAccountList,\
          gl_CDMLayer2Path +"otc_vw_FACT_SalesSLToGLReconciliationGLAccountList.parquet")
      
      default_List =[[0,'NONE',0]]
      default_df = spark.createDataFrame(default_List)
      
      otc_vw_DIM_SalesSLToGLReconciliationGLAccountList = otc_L2_FACT_SalesSLToGLReconciliationGLAccountList.alias('fp2')\
            .select(col('fp2.glAccountSurrogateKey').alias('glAccountSurrogateKey')\
                   ,col('fp2.AccountNumberName').alias('AccountNumberName')\
                   ,col('fp2.isRevenue').alias('isRevenue')).distinct()\
           .union(default_df)

      objGenHelper.gen_writeToFile_perfom(otc_vw_DIM_SalesSLToGLReconciliationGLAccountList,\
          gl_CDMLayer2Path +"otc_vw_DIM_SalesSLToGLReconciliationGLAccountList.parquet")


      executionStatus = "otc_L2_FACT_SalesSLToGLReconciliationGLAccountList populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log() 
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)

        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
 
   
  


