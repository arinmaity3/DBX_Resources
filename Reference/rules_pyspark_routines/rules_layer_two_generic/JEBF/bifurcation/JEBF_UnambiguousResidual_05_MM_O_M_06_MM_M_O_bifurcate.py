# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import lit,col,row_number,when,concat,col,substring,count,sum
import pyspark.sql.functions as F
import inspect

def JEBF_UnambiguousResidual_05_MM_O_M_06_MM_M_O_bifurcate(lsPatterns,ruleSequence):
    try:
      objGenHelper = gen_genericHelper() 
      ommocte = fin_L1_TMP_JEBifurcation.groupBy("transactionID","patternID")\
              .agg(
                sum(F.when((F.col("debitCreditCode") == 'C'),1)).alias("creditCount"), \
                sum(F.when((F.col("debitCreditCode") == 'D'),1)).alias("debitCount")
                  )

      fin_L1_TMP_JEBifurcation_OMMO = ommocte.alias('cte')\
              .filter( ((col('cte.debitCount') == 1) & (col('cte.creditCount') > 1)  \
                        | (col('cte.creditCount') == 1) & (col('cte.debitCount') > 1))\
                       & (col('cte.patternID').isin(*lsPatterns)))\
              .select(col('cte.transactionID')\
               ,col('cte.creditCount')\
               ,col('cte.debitCount')\
               ,col('cte.patternID')\
               ,when ((col('cte.debitCount') == 1) & (col('cte.creditCount') > 1) \
                      ,lit('05-MM-O-M'))\
                .otherwise(lit('06-MM-M-O')).alias('bifurcationtype'))

      df_cte = fin_L1_TMP_JEBifurcation.alias('oneside')\
              .join(fin_L1_TMP_JEBifurcation_OMMO.alias('ommo'),\
               (col('oneside.transactionID') ==	col('ommo.transactionID')) \
                 & (col('oneside.patternID') == (col('ommo.patternID'))) \
                 & (((col('oneside.debitCreditCode') == lit('D')) & (col('ommo.debitCount') == 1)) \
                    | (col('oneside.debitCreditCode') == lit('C')) & (col('ommo.creditCount') == 1))\
                 ,how =('inner'))\
              .select(col('oneside.transactionID')\
               ,col('oneside.transactionLineID')\
               ,col('oneside.transactionLineIDKey')\
               ,col('oneside.debitCreditCode')\
               ,col('oneside.accountID')\
               ,col('oneside.patternID')\
               ,col('ommo.bifurcationType'))

      dr_line= Window.partitionBy("jeb1.transactionID","jeb1.patternID").orderBy("jeb1.transactionLineIDKey")

      fin_L1_TMP_JEBifurcation_UnambiguousBifurcated = df_cte.alias('oneside')\
              .join(fin_L1_TMP_JEBifurcation.alias('jeb1')\
                 ,(col('oneside.transactionID')	==	col('jeb1.transactionID')) \
                   & (col('oneside.patternID') == col('jeb1.patternID')) \
                   & (col('jeb1.debitCreditCode') != col('oneside.debitCreditCode')),how =('inner'))\
              .select(col('oneside.transactionID')\
               ,col('oneside.transactionLineID')\
               ,col('oneside.transactionLineIDKey')\
               ,concat(col('oneside.transactionLineID'),lit('.')\
                       ,F.row_number().over(dr_line)).alias("lineItemNumber")\
               ,when(col('oneside.debitCreditCode') == lit('D'),col('creditAmount'))\
                .when(col('oneside.debitCreditCode') == lit('C'),col('debitAmount')).alias('amount')\
               ,col('oneside.debitCreditCode')\
               ,col('oneside.accountID')\
               ,col('oneside.patternID')\
               ,concat(col('jeb1.transactionLineID'),lit('.1')).alias('relatedLineItemNumber')\
               ,col('jeb1.accountID').alias('relatedAccountID')\
               ,col('jeb1.transactionLineID').alias('relatedtransactionLineID')\
               ,col('jeb1.transactionLineIDKey').alias('relatedtransactionLineIDKey')\
               ,col('oneside.bifurcationType').alias('bifurcationType'))

      relateditems = fin_L1_TMP_JEBifurcation_UnambiguousBifurcated.alias('ubf')\
              .select(col('ubf.transactionID')\
               ,col('ubf.relatedtransactionLineIDKey').alias('transactionLineID')\
               ,col('ubf.relatedtransactionLineIDKey').alias('transactionLineIDKey')\
               ,col('ubf.relatedLineItemNumber').alias('lineItemNumber')\
               ,col('ubf.amount').alias('amount')\
               ,when(col('ubf.debitCreditCode') == lit('D'),lit('C'))\
                 .otherwise(lit('D')).alias('debitCreditCode')\
               ,col('ubf.relatedAccountID').alias('accountID')\
               ,col('patternID')\
               ,col('lineItemNumber').alias('relatedLineItemNumber')\
               ,col('accountID').alias('relatedAccountID')\
               ,col('transactionLineID').alias('relatedtransactionLineID')\
               ,col('transactionLineIDKey').alias('relatedtransactionLineIDKey')\
               ,col('bifurcationType').alias('bifurcationType'))

      fin_L1_TMP_JEBifurcation_UnambiguousBifurcated = fin_L1_TMP_JEBifurcation_UnambiguousBifurcated.union(relateditems)


      fin_L1_STG_JEBifurcation_Link_01 = fin_L1_TMP_JEBifurcation.alias('jeb1')\
              .join(fin_L1_TMP_JEBifurcation_UnambiguousBifurcated.alias('unamb')\
                      ,(col('unamb.transactionID')	==	col('jeb1.transactionID')) \
                      & (col('unamb.transactionLineIDKey') == col('jeb1.transactionLineIDKey')) \
                      & (col('jeb1.patternID') == col('unamb.patternID')),how =('inner'))\
              .select(col('jeb1.journalSurrogateKey')\
               ,col('jeb1.transactionIDbyPrimaryKey')\
               ,col('jeb1.transactionID')\
               ,col('jeb1.lineItem')\
               ,col('unamb.lineItemNumber')\
               ,col('jeb1.accountID').alias('fixedAccount')\
               ,col('jeb1.debitCreditCode').alias('fixedLIType')\
               ,col('unamb.amount').alias('amount')\
               ,lit('O').alias('dataType')\
               ,lit('Unambiguous Residual').alias('rule')\
               ,col('unamb.bifurcationType').alias('bifurcationType')\
               ,col('jeb1.transactionLine').alias('transactionLine')\
               ,col('jeb1.journalId').alias('journalId')\
               ,col('unamb.relatedLineItemNumber').alias('relatedLineItemNumber')\
               ,col('unamb.relatedAccountID').alias('relatedAccount')\
               ,when(col('jeb1.debitCreditCode') == lit('D'),lit('C'))\
                .otherwise(lit('D')).alias('relatedLIType')\
               ,lit(ruleSequence).alias('ruleSequence')\
               ,lit('').alias('loopCount')\
               ,lit('').alias('direction')\
               ,lit('').alias('aggregatedInToLineItem')\
               ,col('unamb.patternID').alias('patternID'))

      fileName=gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta"
      objGenHelper.gen_writeToFile_perfom(df=fin_L1_STG_JEBifurcation_Link_01,filenamepath=fileName,mode="append")
  
    except Exception as err:
       raise
