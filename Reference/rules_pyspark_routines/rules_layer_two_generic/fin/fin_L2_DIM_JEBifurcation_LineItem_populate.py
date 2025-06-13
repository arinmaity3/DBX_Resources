# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,floor,trim

def fin_L2_DIM_JEBifurcation_LineItem_populate():
    try:
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global fin_L2_DIM_JEBifurcation_LineItem
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      w = Window().orderBy(lit(''))
      fin_L2_DIM_JEBifurcation_LineItem = fin_L2_DIM_JEBifurcation_AllDimAttributes.alias('glbf')\
	                                .select(col('glbf.lineItem').alias('lineItem') \
									    ,col('glbf.lineItemCalculated').alias('lineItemCalculated') \
                                        ,col('glbf.glJEFixedLineSort').alias('glJEFixedLineSort') \
                                        ,col('glbf.lineItemProcessingOrder').alias('lineItemProcessingOrder') \
                                        ,col('glbf.relatedLineItem').alias('relatedLineItem') \
                                        ,col('glbf.relatedLineItemCalculated').alias('relatedLineItemCalculated') \
                                        ,col('glbf.glJERelatedLineSort').alias('glJERelatedLineSort') \
                                        ).distinct()

      fin_L2_DIM_JEBifurcation_LineItem = fin_L2_DIM_JEBifurcation_LineItem.withColumn("lineItemLinkID", row_number().over(w))
      
      fin_L2_DIM_JEBifurcation_LineItem  = objDataTransformation.gen_convertToCDMandCache \
          (fin_L2_DIM_JEBifurcation_LineItem,'fin','L2_DIM_JEBifurcation_LineItem',False)
      
      dwh_vw_DIM_JEBifurcation_LineItem = objDataTransformation.gen_convertToCDMStructure_generate \
          (fin_L2_DIM_JEBifurcation_LineItem,'dwh','vw_DIM_JEBifurcation_LineItem',True)[0]

      keysAndValues = {'lineItemLinkID':-1,'lineItem':'NONE','lineItemCalculated':'NONE',\
          'glJEFixedLineSort':'NONE','lineItemProcessingOrder':0,'relatedLineItem':'NONE',\
          'relatedLineItemCalculated':'NONE','glJERelatedLineSort':'NONE'}

      dwh_vw_DIM_JEBifurcation_LineItem = objGenHelper.gen_placeHolderRecords_populate \
          ('dwh.vw_DIM_JEBifurcation_LineItem',keysAndValues,dwh_vw_DIM_JEBifurcation_LineItem)
      objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_JEBifurcation_LineItem,gl_CDMLayer2Path + "fin_L2_DIM_JEBifurcation_LineItem.parquet" )

      executionStatus = "fin_L2_DIM_JEBifurcation_LineItem populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
        
    except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]




