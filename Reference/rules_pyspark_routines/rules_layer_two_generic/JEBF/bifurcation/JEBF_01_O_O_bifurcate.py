# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  col,concat,lit,when,lead,lag
from pyspark.sql.window import Window

def JEBF_01_O_O_bifurcate():
  
  try:
    objGenHelper = gen_genericHelper() 
   
    patternID=1
    ruleSequence=1
    windowSpec  = Window.partitionBy("transactionID").orderBy("transactionLineIDKey")
    
    fin_L1_TMP_JEBifurcation_Filtered=fin_L1_TMP_JEBifurcation.filter(col("patternID") == 1)
    fin_L1_STG_JEBifurcation_Link = fin_L1_TMP_JEBifurcation_Filtered\
                                    .select(col('journalSurrogateKey')\
                                            ,col('transactionIDbyPrimaryKey')\
                                            ,col('transactionID')\
                                            ,col('lineItem')\
                                            ,col('transactionLineID').alias('lineItemNumber')\
                                            ,col('accountID').alias('fixedAccount')\
                                            ,col('debitCreditCode').alias('fixedLIType')\
                                            ,when((col('debitCreditCode') == lit('C')) , col('creditAmount'))\
                                                    .otherwise(col('debitAmount')).alias('amount')\
                                            ,lit('O').alias('dataType'),lit('Unambiguous').alias('rule')\
                                            ,lit('01-O-O').alias('bifurcationType'),col('transactionLine')\
                                            ,col('journalId')\
                                            ,when(lead("transactionLineID",1).over(windowSpec).isNull(),lag("transactionLineID",1).over(windowSpec))\
                                                .otherwise(lead("transactionLineID",1).over(windowSpec)).alias('relatedLineItemNumber')\
                                            ,when(lead("accountID",1).over(windowSpec).isNull(),lag("accountID",1).over(windowSpec))\
                                                .otherwise(lead("accountID",1).over(windowSpec)).alias('relatedAccount')\
                                            ,when(lead("debitCreditCode",1).over(windowSpec).isNull(),lag("debitCreditCode",1).over(windowSpec))\
                                                .otherwise(lead("debitCreditCode",1).over(windowSpec)).alias('relatedLIType')\
                                            ,lit(None).alias('patternSource')\
                                            ,lit(None).alias('blockSource')\
                                            ,lit(ruleSequence).alias('ruleSequence')\
                                            ,lit('').alias('loopCount')\
                                            ,lit('').alias('direction')\
                                            ,lit('').alias('aggregatedInToLineItem')\
                                            ,lit(patternID).alias('patternID'))
    
    fileName = gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta"
    objGenHelper.gen_writeToFile_perfom(df = fin_L1_STG_JEBifurcation_Link,filenamepath = fileName,mode="append")
    

  except Exception as err:
     raise

