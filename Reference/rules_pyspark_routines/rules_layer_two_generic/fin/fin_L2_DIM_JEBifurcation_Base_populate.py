# Databricks notebook source
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number,expr,trim,col,lit,when
from pyspark.sql.window import Window

def fin_L2_DIM_JEBifurcation_Base_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global fin_L2_DIM_JEBifurcation_Base

    w = Window().orderBy(lit(''))
    fin_L2_DIM_JEBifurcation_Base = fin_L2_DIM_JEBifurcation_AllDimAttributes.alias('jed1')\
      .select(col('bifurcationBlockID')\
             ,col('bifurcationDirection')\
             ,col('bifurcationResultCategory')\
             ,col('bifurcationEnhancedAttribute')\
             ,col('isMultiBPViewAssignment')\
             ,col('fixedDebitCreditTypeID')\
             ,col('bifurcationLoopCount')\
             ,col('relatedDebitCreditTypeID')\
             ,col('ruleSequence')\
             ).distinct()\
      .withColumn("exceptLineItemLinkID", row_number().over(w))

    fin_L2_DIM_JEBifurcation_Base = objDataTransformation.gen_convertToCDMandCache(\
                         fin_L2_DIM_JEBifurcation_Base,'fin','L2_DIM_JEBifurcation_Base',False)
    
    jebf = fin_L2_DIM_JEBifurcation_Base.alias('jebf1')\
        .select(col('exceptLineItemLinkID')\
                ,col('bifurcationBlockID')\
                ,col('bifurcationDirection')\
                ,col('bifurcationResultCategory')\
                ,col('bifurcationEnhancedAttribute')\
                ,col('isMultiBPViewAssignment')\
                ,col('fixedDebitCreditTypeID')\
                ,col('bifurcationLoopCount')\
                ,col('relatedDebitCreditTypeID')\
                ,col('ruleSequence')\
                )

    chklist = [[-1,0,'NONE','6_NONE','NONE',1,'NONE','NONE','NONE',0]]
    df1 = spark.createDataFrame(chklist)                                                                                       
    dwh_vw_DIM_JEBifurcation_Base = jebf.unionAll(df1)

    dwh_vw_DIM_JEBifurcation_Base = objDataTransformation.gen_convertToCDMStructure_generate(\
                          dwh_vw_DIM_JEBifurcation_Base, 'dwh','vw_DIM_JEBifurcation_Base',True)[0]
    
    objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_JEBifurcation_Base,gl_CDMLayer2Path + "fin_L2_DIM_JEBifurcation_Base.parquet" )

    executionStatus = "fin_L2_DIM_JEBifurcation_Base completed successfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
