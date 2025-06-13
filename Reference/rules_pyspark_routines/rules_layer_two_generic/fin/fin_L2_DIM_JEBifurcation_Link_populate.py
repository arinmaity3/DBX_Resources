# Databricks notebook source
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number,expr,trim,col,lit,when
from pyspark.sql.window import Window

def fin_L2_DIM_JEBifurcation_Link_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global fin_L2_DIM_JEBifurcation_Link

    join1 = when(col('a.bifurcationEnhancedAttribute').isNull(), lit('') ) \
                                  .otherwise(col('a.bifurcationEnhancedAttribute'))

    join2 = when(col('L2.bifurcationEnhancedAttribute').isNull(), lit('') ) \
                                  .otherwise(col('L2.bifurcationEnhancedAttribute'))

    fin_L2_DIM_JEBifurcation_Link  = fin_L2_DIM_JEBifurcation_AllDimAttributes.alias('a')\
      .join(fin_L2_DIM_JEBifurcation_LineItem.alias('L1'),(col('a.lineItem')	==	col('L1.lineItem')) \
                           & (col('a.lineItemCalculated') == col('L1.lineItemCalculated')) \
                           & (col('a.glJEFixedLineSort') == col('L1.glJEFixedLineSort')) \
                           & (col('a.lineItemProcessingOrder') == col('L1.lineItemProcessingOrder')) \
                           & (col('a.relatedLineItem') == col('L1.relatedLineItem')) \
                           & (col('a.relatedLineItemCalculated') == col('L1.relatedLineItemCalculated')) \
                           & (col('a.glJERelatedLineSort') == col('L1.glJERelatedLineSort')),how =('inner'))\
      .join(fin_L2_DIM_JEBifurcation_Base.alias('L2'),(col('a.bifurcationBlockID')	==	col('L2.bifurcationBlockID')) \
                           & (col('a.bifurcationDirection') == col('L2.bifurcationDirection')) \
                           & (col('a.bifurcationResultCategory') == col('L2.bifurcationResultCategory')) \
                           & (lit(join1) == lit(join2)) \
                           & (col('a.isMultiBPViewAssignment') == col('L2.isMultiBPViewAssignment')) \
                           & (col('a.fixedDebitCreditTypeID') == col('L2.fixedDebitCreditTypeID')) \
                           & (col('a.bifurcationLoopCount') == col('L2.bifurcationLoopCount'))
                           & (col('a.relatedDebitCreditTypeID') == col('L2.relatedDebitCreditTypeID')) \
                           & (col('a.ruleSequence') == col('L2.ruleSequence')) \
                           ,how =('inner'))\
      .join(fin_L2_DIM_JEBifurcation_Transaction_Base.alias('L3'),(col('a.transactionNo')==col('L3.transactionNo')),how =('inner'))\
      .select(col('a.bifurcationIdDerived')\
              ,col('L1.lineItemLinkID').alias('lineItemLinkID')\
              ,col('L2.exceptLineItemLinkID').alias('exceptLineItemLinkID')\
              ,col('L3.transactionLinkID').alias('transactionLinkID'))

    
    fin_L2_DIM_JEBifurcation_Link = objDataTransformation.gen_convertToCDMandCache \
                                            (fin_L2_DIM_JEBifurcation_Link,'fin','L2_DIM_JEBifurcation_Link',False)

    objGenHelper.gen_writeToFile_perfom(fin_L2_DIM_JEBifurcation_Link,gl_CDMLayer2Path + "fin_L2_DIM_JEBifurcation_Link.parquet" )

    executionStatus = "fin_L2_DIM_JEBifurcation_Link populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

