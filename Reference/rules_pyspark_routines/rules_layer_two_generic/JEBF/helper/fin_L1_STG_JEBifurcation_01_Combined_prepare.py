# Databricks notebook source
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import  col,when,dense_rank,concat

def fin_L1_STG_JEBifurcation_01_Combined_prepare():

    try:

        objGenHelper = gen_genericHelper() 
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        global fin_L1_STG_JEBifurcation_01_Combined

        fin_L1_STG_JEBifurcation_Link = spark.read.format("delta")\
                                       .load(gl_postBifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta")
        input_dataTypeDF = fin_L1_STG_JEBifurcation_Link.filter(col("dataType")=='I')\
                         .select(col('transactionID'),col('blockSource'),col('patternSource')).distinct()
        output_dataTypeDF = fin_L1_STG_JEBifurcation_Link.filter(col("dataType")=='O')\
                         .select(col('transactionID'),col('lineItemNumber'),col('lineItem')).distinct()
        dr_window = Window.partitionBy("link.journalID").orderBy("link.transactionID")
        ruleSequenceCase = when(col('pat.patternSource') == '1-Transaction Based Pattern',lit(1))\
                             .when(col('pat.patternSource') == '3-Block Building Pattern',lit(2))\
                             .when(col('pat.patternSource') == '4-Shifted Block Building Pattern',lit(3))\
                             .when(col('pat.patternSource') == '5-Combined Pattern',lit(4))\
                             .when(col('pat.patternSource') == '6-No Pattern',lit(4))\
                             .otherwise(lit(0))

        fin_L1_STG_JEBifurcation_01_Combined = fin_L1_STG_JEBifurcation_Link.alias('link')\
               .join(input_dataTypeDF.alias('pat'),(col('pat.transactionID')==col('link.transactionID')),how='inner')\
               .join(output_dataTypeDF.alias('lrel'),((col('link.transactionID')==col('lrel.transactionID'))\
                          & (col('link.relatedLineItemNumber')==col('lrel.lineItemNumber'))),how='left')\
               .select(col('link.journalSurrogateKey').cast(LongType())\
                       ,col('link.journalID').alias('transactionID').cast(StringType())\
                       ,col('link.lineItemNumber').cast(StringType())\
                       ,col('link.fixedAccount').cast(StringType())\
                       ,col('link.fixedLIType').cast(StringType())\
                       ,col('link.amount').cast(DecimalType(32,6))\
                       ,col('link.dataType').cast(StringType())\
                       ,col('link.loopCount').cast(StringType())\
                       ,col('link.direction').cast(StringType())\
                       ,col('link.rule').cast(StringType())\
                       ,col('link.bifurcationType').cast(StringType())\
                       ,col('link.relatedLineItemNumber').cast(StringType())\
                       ,col('link.aggregatedIntoLineItem').cast(ShortType())\
                       ,col('link.relatedAccount').cast(StringType())\
                       ,col('link.relatedLIType').cast(StringType())\
                       ,col('link.journalID').cast(StringType())\
                       ,col('link.companyCode').cast(StringType()).alias('businessUnitCode')\
                       ,col('link.lineItem').cast(StringType()).alias('fixedOriginalLineItem')\
                       ,col('link.patternID').cast(ShortType())\
                       ,col('pat.patternSource').cast(StringType())\
                       ,col('pat.blockSource').cast(StringType())\
                       ,(col('link.rulesequence') + lit(ruleSequenceCase).cast(ShortType())).alias('RuleSequence')\
                       ,when((col('lrel.lineItem').isNull()),'')\
                           .otherwise(col('lrel.lineItem')).cast(StringType()).alias('relatedOrginalLineItem')\
                       ,F.dense_rank().over(dr_window).alias("blockID"))\
                       .filter(col('link.dataType')=='O')

        fin_L1_STG_JEBifurcation_01_Combined  = objDataTransformation.gen_convertToCDMandCache(fin_L1_STG_JEBifurcation_01_Combined\
                                                       ,'fin','L1_STG_JEBifurcation_01_Combined',False)
        
        executionStatus = "L1_STG_JEBifurcation_01_Combined_prepare completed successfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
