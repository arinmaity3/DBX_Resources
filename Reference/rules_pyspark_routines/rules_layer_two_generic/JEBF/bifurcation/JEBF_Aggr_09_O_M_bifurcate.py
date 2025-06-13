# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,row_number,when,concat,col
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import inspect

def JEBF_Aggr_09_O_M_bifurcate(lsPatterns):
    try:

      objGenHelper = gen_genericHelper() 
      ruleSequence = 8
      df_input = fin_L1_TMP_JEBifurcation_Accountwise_Agg.filter(col('patternID').isin(*lsPatterns))\
                   .select(col('journalSurrogateKey')\
                   ,col('transactionID')\
                   ,col('transactionIDbyPrimaryKey')\
                   ,col('transactionLineIDKey')\
                   ,col('transactionLineID')\
                   ,col('accountID')\
                   ,col('patternID')\
                   ,col('debitCreditCode')\
                   ,col('patternID')\
                   ,expr("case when debitCreditCode = 'C' then creditAmount else debitAmount end").alias("amount"))

      credit_RowNum = Window.partitionBy("transactionID","patternID").orderBy("transactionLineIDKey") 
      df_credit = df_input.filter(col('debitCreditCode') == 'C')\
                   .select(col('journalSurrogateKey')\
                    ,col('transactionLineID')\
                    ,col('transactionIDbyPrimaryKey')\
                    ,col('transactionID')\
                    ,col('debitCreditCode').alias('credit_code')\
                    ,col('transactionLineID').alias('credit_Line')\
                    ,col('accountID'),col('debitCreditCode')\
                    ,col('patternID')\
                    ,F.row_number().over(credit_RowNum).alias('credit_RowNum')\
                    ,col('accountID').alias('credit_accountID')\
                    ,col('amount'))

      df_debit = df_input.filter(col('debitCreditCode') == 'D')\
                  .select(col('journalSurrogateKey')\
                    ,col('transactionLineID')\
                    ,col('transactionID')\
                    ,col('transactionIDbyPrimaryKey')\
                    ,col('debitCreditCode').alias('debit_code')\
                    ,col('transactionLineID').alias('debit_Line')\
                    ,col('accountID').alias('debit_accountID')\
                    ,col('patternID')\
                    ,col('debitCreditCode'))

      df1 = df_credit.alias('main')\
                    .join(df_debit.alias('sub'),\
                    (col('main.transactionID') == col('sub.transactionID'))\
                   &(col('main.patternID') ==col('sub.patternID')),'inner')\
                    .select(col('main.journalSurrogateKey')\
                      ,col('main.transactionIDbyPrimaryKey')\
                      ,col('main.transactionID')\
                      ,col('credit_code').alias('fixedLIType')\
                      ,col('debit_code').alias('relatedLIType')\
                      ,col('amount')\
                      ,col('credit_accountID').alias('fixedAccount')\
                      ,col('debit_accountID').alias('relatedAccount')\
                      ,col('main.patternID')\
                      ,concat(col('credit_Line'),lit('.1')).alias('lineItemNumber')\
                      ,concat(col('debit_Line'),lit('.'),col('credit_RowNum')).alias('relatedLineItemNumber'))

      df2 = df_credit.alias('main')\
                    .join(df_debit.alias('sub'),\
                    (col('main.transactionID') == col('sub.transactionID'))\
                    &(col('main.patternID') ==col('sub.patternID')),'inner')\
                    .select(col('sub.journalSurrogateKey')\
                     ,col('sub.transactionIDbyPrimaryKey')\
                     ,col('sub.transactionID')\
                     ,col('debit_code').alias('fixedLIType')\
                     ,col('credit_code').alias('relatedLIType')\
                     ,col('amount')\
                     ,col('debit_accountID').alias('fixedAccount')\
                     ,col('credit_accountID').alias('relatedAccount')\
                     ,col('main.patternID')\
                     ,concat(col('debit_Line'),lit('.'),col('credit_RowNum')).alias('lineItemNumber')\
                     ,concat(col('credit_Line'),lit('.1')).alias('relatedLineItemNumber'))
      
      df2.sort(col("transactionID"))
      dfoutput = df1.unionAll(df2)
      
      fin_L1_STG_JEBifurcation_Link = dfoutput.alias('tTab')\
                    .join(fin_L1_TMP_JEBifurcation_Accountwise_Agg.alias('jeb1'),\
                     (col('jeb1.journalSurrogateKey') == col('tTab.journalSurrogateKey')),'inner')\
                    .join(fin_L1_TMP_JEBifurcation_Accountwise_Agg_Docbased.alias('dw')\
                     ,col('dw.transactionID') == col('jeb1.transactionID'),'inner')\
                    .filter((col('jeb1.patternID').isin(*lsPatterns))&(col('dw.debitCount') == lit(1))&(col('dw.creditCount') > lit(1)) )\
                    .select(col('tTab.journalSurrogateKey').alias('journalSurrogateKey')\
                     ,col('jeb1.transactionIDbyPrimaryKey')\
                     ,col('jeb1.transactionLine')\
                     ,col('tTab.amount')\
                     ,lit(None).alias('patternSource')\
                     ,lit(None).alias('blockSource')\
                     ,col('jeb1.transactionID')\
                     ,col('jeb1.lineItem')\
                     ,col('tTab.lineItemNumber')\
                     ,col('tTab.fixedAccount')\
                     ,col('tTab.fixedLIType')\
                     ,lit('O').alias('dataType')\
                     ,lit('Agg Unambiguous').alias('rule')\
                     ,lit('09-Agg-O-M').alias('bifurcationType')\
                     ,col('jeb1.journalId')\
                     ,col('tTab.relatedLineItemNumber')\
                     ,col('tTab.relatedAccount')\
                     ,col('tTab.relatedLIType')\
                     ,lit(ruleSequence).alias('ruleSequence')\
                     ,lit('').alias('loopCount')\
                     ,lit('').alias('direction')\
                     ,lit('').alias('aggregatedInToLineItem')\
                     ,lit(None).alias('aggregatedInToLine')\
                     ,col('jeb1.patternID').alias('patternID'))

      fileName=gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta"
      objGenHelper.gen_writeToFile_perfom(df=fin_L1_STG_JEBifurcation_Link,filenamepath=fileName,mode="append")
        
    except Exception as err:
       raise
