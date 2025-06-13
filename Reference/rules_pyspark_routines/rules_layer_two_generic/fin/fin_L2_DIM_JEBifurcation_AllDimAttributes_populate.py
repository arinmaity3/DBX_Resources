# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,floor,trim

def fin_L2_DIM_JEBifurcation_AllDimAttributes_populate():
    try:
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global fin_L2_DIM_JEBifurcation_AllDimAttributes
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()

      L2_DIM_JEBifurcation_AllDimAttributes = fin_L2_FACT_GLBifurcation.alias('glbf')\
	                                .select(col('glbf.glJEBifurcationId').alias('bifurcationIdDerived')\
									    ,col('glbf.glBifurcationblockID').alias('bifurcationBlockID')\
										,when(col('glbf.direction').isNull(),lit('')) \
										   .otherwise(col('glbf.direction')).alias('bifurcationDirection')\
										,col('glbf.glBifurcationResultCategory').alias('bifurcationResultCategory')\
										,col('glbf.enhancedAttribute').alias('bifurcationEnhancedAttribute')\
										,lit(0).alias('isMultiBPViewAssignment')\
										,when(col('glbf.glJELineItem').isNull(),lit(0)) \
										   .otherwise(col('glbf.glJELineItem')).alias('lineItem')\
										,col('glbf.glJELineItemCalc').alias('lineItemCalculated')\
										,col('glbf.glJEFixedLineSort').alias('glJEFixedLineSort')\
										,col('glbf.glJEFixedDrCrIndicator').alias('fixedDebitCreditTypeID')\
										,when(col('glbf.glJELineItemProcOrder').isNull(),lit(0)) \
										   .otherwise(col('glbf.glJELineItemProcOrder')).alias('lineItemProcessingOrder')\
										,col('glbf.loopCount').alias('bifurcationLoopCount')\
										,when(col('glbf.glJERelatedLineItem').isNull(),lit(0)) \
										   .otherwise(col('glbf.glJERelatedLineItem')).alias('relatedLineItem')\
										,col('glbf.glJERelatedLineItemCalc').alias('relatedLineItemCalculated')\
										,col('glbf.glJERelatedLineSort').alias('glJERelatedLineSort')\
										,col('glbf.glJERelatedDrCrIndicator').alias('relatedDebitCreditTypeID')\
										,col('glbf.glJERuleSequence').alias('ruleSequence')\
										,col('glbf.glJETransactionNo').alias('transactionNo'))
      
      dummy_list = [[0,0,'NONE','6_NONE','NONE',1,'NONE','NONE','NONE','NONE',0,'NONE','NONE','NONE','NONE','NONE',0,'NONE']]

      dummy_df = spark.createDataFrame(dummy_list)
      fin_L2_DIM_JEBifurcation_AllDimAttributes = L2_DIM_JEBifurcation_AllDimAttributes.union(dummy_df)
      
      fin_L2_DIM_JEBifurcation_AllDimAttributes  = objDataTransformation.gen_convertToCDMandCache \
		  (fin_L2_DIM_JEBifurcation_AllDimAttributes,'fin','L2_DIM_JEBifurcation_AllDimAttributes',False)

      executionStatus = "fin_L2_DIM_JEBifurcation_AllDimAttributes populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
        
    except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]



