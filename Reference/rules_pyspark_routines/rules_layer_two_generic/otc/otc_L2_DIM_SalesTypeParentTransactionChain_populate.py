# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import row_number,expr

def otc_L2_DIM_SalesTypeParentTransactionChain_populate():
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        
        global otc_L2_DIM_SalesTypeParentTransactionChain
        global otc_L1_STG_SalesTypeParentTransactionChainBridge
        
        w = Window.orderBy(F.lit(''))
        otc_L1_TMP_SalesTypeParentTransactionChainSource = otc_L1_STG_26_SalesFlowInitTreeParentShortString\
                                                           .select(col('salesParentTransactionID'),expr('LEFT(salesDocumentCategoryString,1000)as documentTypeClient'),\
                                                                  expr('LEFT(salesDocumentCategoryString2,1000)as documentTypeClientReal'),\
                                                                  expr('LEFT(salesTypeLineItemString,1000)as documentTypeKPMG'),\
                                                                  expr('LEFT(salesTypeLineItemString2,1000)as documentTypeKPMGReal'),\
                                                                  expr('LEFT(salesTypeTextString,1000)as documentTypeKPMGShort'),\
                                                                  expr('LEFT(salesTypeTextString2,1000)as documentTypeKPMGShortReal'),\
                                                                  expr('LEFT(salesTypeGroupString,1000)as documentTypeKPMGGroup'),\
                                                                  expr('LEFT(salesTypeGroupString2,1000)as documentTypeKPMGGroupReal'),\
                                                                  expr('LEFT(salesTypeTextStringInterpreted,1000)as documentTypeKPMGShortInterpreted'),\
                                                                  expr('LEFT(salesTypeTextStringInterpreted2,1000)as documentTypeKPMGShortInterpreted2'),\
                                                                  col('documentTypeClientIdDistinctCount'))

        otc_L2_DIM_SalesTypeParentTransactionChain = otc_L1_TMP_SalesTypeParentTransactionChainSource.select(otc_L1_TMP_SalesTypeParentTransactionChainSource.columns[1:])\
                                                     .distinct()\
                                                     .withColumn("parentTransactionChainSurrogateKey",row_number().over(w))

        otc_L2_DIM_SalesTypeParentTransactionChain = objDataTransformation.gen_convertToCDMandCache\
                      (otc_L2_DIM_SalesTypeParentTransactionChain,'otc','L2_DIM_SalesTypeParentTransactionChain',False,targetPath = gl_CDMLayer2Path)

        otc_L1_STG_SalesTypeParentTransactionChainBridge = otc_L1_TMP_SalesTypeParentTransactionChainSource.alias('S')\
                                                            .join(otc_L2_DIM_SalesTypeParentTransactionChain.alias('T'),\
                                                                  (col("S.documentTypeClient")==col("T.documentTypeClient"))\
                                                                 &(col("S.documentTypeClientReal")==col("T.documentTypeClientReal"))\
                                                                 &(col("S.documentTypeKPMG")==col("T.documentTypeKPMG"))\
                                                                 &(col("S.documentTypeKPMGReal")==col("T.documentTypeKPMGReal"))\
                                                                 &(col("S.documentTypeKPMGShort")==col("T.documentTypeKPMGShort"))\
                                                                 &(col("S.documentTypeKPMGShortReal")==col("T.documentTypeKPMGShortReal"))\
                                                                 &(col("S.documentTypeKPMGGroup")==col("T.documentTypeKPMGGroup"))\
                                                                 &(col("S.documentTypeKPMGGroupReal")==col("T.documentTypeKPMGGroupReal"))\
                                                                 &(col("S.documentTypeKPMGShortInterpreted")==col("T.documentTypeKPMGShortInterpreted"))\
                                                                 &(col("S.documentTypeKPMGShortInterpreted2")==col("T.documentTypeKPMGShortInterpreted2")),how="inner")\
                                                            .select(col('salesParentTransactionID'),col('parentTransactionChainSurrogateKey'))

        otc_L1_STG_SalesTypeParentTransactionChainBridge = objDataTransformation.gen_convertToCDMandCache\
                      (otc_L1_STG_SalesTypeParentTransactionChainBridge,'otc','L1_STG_SalesTypeParentTransactionChainBridge',False,targetPath = gl_Layer2Staging)

        default_List =[[0,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE',0]]
        default_df = spark.createDataFrame(default_List)

        vKPMGChainStart = expr("LEFT(documentTypeKPMG, CHARINDEX('|', documentTypeKPMG,3))")
        vKPMGRealChainStart = expr("LEFT(documentTypeKPMGReal, CHARINDEX('|', documentTypeKPMGReal,3))")

        otc_vw_DIM_salesTypeParentTransactionChain = otc_L2_DIM_SalesTypeParentTransactionChain\
                                                     .select(col('parentTransactionChainSurrogateKey'),col('documentTypeClient'),col('documentTypeClientReal'),col('documentTypeKPMG')\
                                                            ,lit(vKPMGChainStart).alias('documentTypeKPMGChainStart'),col('documentTypeKPMGReal')\
                                                            ,lit(vKPMGRealChainStart).alias('documentTypeKPMGRealChainStart'),col('documentTypeKPMGShort')\
                                                            ,col('documentTypeKPMGShortReal'),col('documentTypeKPMGGroup')\
                                                            ,col('documentTypeKPMGGroupReal'),col('documentTypeKPMGShortInterpreted')\
                                                            ,col('documentTypeKPMGShortInterpreted2'),col('documentTypeClientIdDistinctCount'))\
                                                     .union(default_df)

        objGenHelper.gen_writeToFile_perfom(otc_vw_DIM_salesTypeParentTransactionChain\
                                                    ,gl_CDMLayer2Path +"otc_vw_DIM_salesTypeParentTransactionChain.parquet")  
        
        executionStatus = "otc_L2_DIM_SalesTypeParentTransactionChain populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
      
    except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    


 
