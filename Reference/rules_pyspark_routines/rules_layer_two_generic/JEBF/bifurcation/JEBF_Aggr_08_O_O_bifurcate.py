# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import lit,col,row_number,when,concat,col,substring,count,sum,lead,lag
import pyspark.sql.functions as F
import inspect

def JEBF_Aggr_08_O_O_bifurcate(lsPatterns):
        try:
            objGenHelper = gen_genericHelper()
            
            ruleSequence = 7
            
            windowSpec  = Window.partitionBy("trans.transactionID","trans.patternID").orderBy("trans.transactionLineIDKey")
    
    
            fin_L1_TMP_JEBifurcation_Filtered=fin_L1_TMP_JEBifurcation_Accountwise_Agg.filter(col("patternID").isin(*lsPatterns))


            fin_L1_STG_JEBifurcation_Link = fin_L1_TMP_JEBifurcation_Filtered.alias('trans')\
                                            .join(fin_L1_TMP_JEBifurcation_Accountwise_Agg_Docbased.alias('dw'),\
                                            col('dw.transactionID') == col('trans.transactionID'),'inner')\
                                               .filter((col('trans.patternID').isin(*lsPatterns))&(col('dw.isOnetoOne') == lit(1)))\
                                            .select(col('journalSurrogateKey'),col('trans.transactionIDbyPrimaryKey')\
                                                    ,col('trans.transactionID'),col('lineItem')\
                                                    ,col('transactionLineID').alias('lineItemNumber')\
                                                    ,col('accountID').alias('fixedAccount')\
                                                    ,col('debitCreditCode').alias('fixedLIType')\
                                                    ,(when(col('trans.debitCreditCode') == lit('D'),col('debitAmount'))\
                                                            .otherwise(col('creditAmount'))).cast(DecimalType(32,6)).alias('amount')\
                                                    ,lit('O').alias('dataType')\
                                                    ,lit('Agg Unambiguous').alias('rule')\
                                                    ,lit('08-Agg-O-O').alias('bifurcationType')\
                                                    ,col('transactionLine'),col('journalId')\
                                                    ,when(lead("transactionLineID",1).over(windowSpec).isNull()\
                                                           ,lag("transactionLineID",1).over(windowSpec))\
                                                               .otherwise(lead("transactionLineID",1).over(windowSpec)).alias('relatedLineItemNumber')\
                                                    ,when(lead("accountID",1).over(windowSpec).isNull(),lag("accountID",1).over(windowSpec))\
                                                           .otherwise(lead("accountID",1).over(windowSpec)).alias('relatedAccount')\
                                                    ,when(lead("debitCreditCode",1).over(windowSpec).isNull(),lag("debitCreditCode",1).over(windowSpec))\
                                                       .otherwise(lead("debitCreditCode",1).over(windowSpec)).alias('relatedLIType')\
                                                    ,lit(ruleSequence).alias('ruleSequence')\
                                                    ,lit('').alias('loopCount')\
                                                    ,lit('').alias('direction')\
                                                    ,lit('').alias('aggregatedInToLineItem')\
                                                    ,lit(None).alias('aggregatedInToLine')\
                                                    ,col('patternID').alias('patternID'))


            fileName = gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta"
            objGenHelper.gen_writeToFile_perfom(df = fin_L1_STG_JEBifurcation_Link,filenamepath = fileName,mode="append")
            
        except Exception as err:
           raise
