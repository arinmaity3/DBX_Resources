# Databricks notebook source
from pyspark.sql.functions import when,lit,col
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sys
import traceback
import uuid
from pyspark.sql import Window
import inspect

def JEBF_AggNonAdjacentExactMatches_11_Agg_M_M_O_O_bifurcate(lsPatterns,ruleSequence):
   
    try: 

          objGenHelper = gen_genericHelper() 
                  
          Row1= Window.partitionBy("transactionId","patternID","debitAmount").orderBy('transactionLineIDKey')
          Row2= Window.partitionBy("transactionId","patternID","creditAmount").orderBy('transactionLineIDKey')
          
          Fordebit = fin_L1_TMP_JEBifurcation_Accountwise_Agg\
                                     .filter((col('debitCreditCode') == 'D') & (col('patternID').isin(*lsPatterns)))\
                                     .select(col('transactionId').alias('transactionId')\
                                             ,col('transactionLineIDKey')\
                                             ,col('debitAmount')\
                                             ,col('patternID')\
                                             ,col('debitCreditCode')\
                                             ,col('accountID')\
                                             ,F.row_number().over(Row1).alias("drRow"))
          
          Forcredit = fin_L1_TMP_JEBifurcation_Accountwise_Agg\
                                     .filter((col('debitCreditCode') == 'C') & (col('patternID').isin(*lsPatterns)))\
                                     .select(col('transactionId').alias('transactionId')\
                                             ,col('transactionLineIDKey')\
                                             ,col('creditAmount')\
                                             ,col('patternID')\
                                             ,col('debitCreditCode')\
                                             ,col('accountID')\
                                             ,F.row_number().over(Row2).alias("crRow"))
          
          L1_TMP_JEBifurcation_NonAdjacentExactMatch = Fordebit.alias('Dr')\
                                                    .join(Forcredit.alias('Cr'),\
                                                    (col('Dr.transactionID') == col('Cr.transactionID'))\
                                                    & (col('Dr.debitAmount') ==  col('Cr.creditAmount'))\
                                                    & (col('Dr.patternID') ==  col('Cr.patternID'))\
                                                    & (col('Dr.drRow') ==  col('Cr.crRow')),'inner')\
                                                    .select(col('Dr.transactionID')\
                                                    ,col('Dr.patternID')\
                                                            ,col('Dr.transactionLineIDKey').alias('DrLineIDKey')\
                                                            ,col('Cr.transactionLineIDKey').alias('CrLineIDKey')\
                                                            ,col('Dr.debitAmount').alias('debitAmount')\
                                                            ,col('Cr.creditAmount').alias('creditAmount')\
                                                            ,col('Dr.drRow').alias('rowNo')\
                                                            ,col('Cr.debitCreditCode').alias('CrrelatedLIType')\
                                                            ,col('Cr.accountID').alias('CrrelatedAccount')\
                                                            ,col('Dr.debitCreditCode').alias('DrrelatedLIType')\
                                                            ,col('Dr.accountID').alias('DrrelatedAccount')\
                                                            )
          
          un1 = L1_TMP_JEBifurcation_NonAdjacentExactMatch.alias('trans')\
                .join(fin_L1_TMP_JEBifurcation_Accountwise_Agg.alias('drRow'),\
                     (col('trans.transactionID') == col('drRow.transactionID'))\
                                                    & (col('trans.DrLineIDKey') ==  col('drRow.transactionLineIDKey'))\
                                                    & (col('trans.patternID') ==  col('drRow.patternID')),'inner')\
                .select(col('drRow.journalSurrogateKey')\
                        ,col('drRow.transactionIDbyPrimaryKey')\
                        ,col('drRow.transactionID')\
                        ,col('drRow.lineItem')\
                        ,col('transactionLineID')\
                        ,col('debitCreditCode')\
                        ,col('accountID')\
                        ,col('drRow.debitAmount')\
                        ,col('drRow.creditAmount')\
                        ,col('transactionLine')\
                        ,col('trans.patternID')\
                        ,col('journalId')\
                        ,col('trans.debitAmount').alias('amount')\
                        ,col('trans.CrLineIDKey').alias('RelRow')\
                        ,col('CrrelatedAccount').alias('relatedAccount')\
                        ,col('CrrelatedLIType').alias('relatedLIType'))
          
          un2 = L1_TMP_JEBifurcation_NonAdjacentExactMatch.alias('trans')\
                .join(fin_L1_TMP_JEBifurcation_Accountwise_Agg.alias('crRow'),\
                     (col('trans.transactionID') == col('crRow.transactionID'))\
                                                    & (col('trans.CrLineIDKey') ==  col('crRow.transactionLineIDKey'))\
                                                    & (col('trans.patternID') ==  col('crRow.patternID')),'inner')\
                .select(col('crRow.journalSurrogateKey'),col('crRow.transactionIDbyPrimaryKey')\
                        ,col('crRow.transactionID'),col('crRow.lineItem'),col('transactionLineID')\
                        ,col('debitCreditCode'),col('accountID')\
                        ,col('crRow.debitAmount'),col('crRow.creditAmount')\
                        ,col('transactionLine'),col('trans.patternID'),col('journalId')\
                        ,col('trans.debitAmount').alias('amount')\
                        ,col('trans.DrLineIDKey').alias('RelRow')\
                        ,col('DrrelatedAccount').alias('relatedAccount')\
                        ,col('DrrelatedLIType').alias('relatedLIType'))
          
          unionresult = un1.unionAll(un2)
          
          fin_L1_STG_JEBifurcation_Link = unionresult\
                                          .select(col('journalSurrogateKey')\
                                                   ,col('transactionIDbyPrimaryKey')\
                                                   ,col('transactionID'),col('lineItem')\
                                                   ,col('transactionLineID').alias('lineItemNumber')\
                                                   ,col('accountID').alias('fixedAccount')\
                                                   ,col('debitCreditCode').alias('fixedLIType')\
                                                   ,when((col('debitCreditCode') == 'D'), col('debitAmount'))\
                                                          .otherwise(col('creditAmount')).alias('amount')\
                                                   ,lit('O').alias('dataType')\
                                                   ,lit('33 Agg NonAdjacentExactMatch').alias('rule')\
                                                   ,lit('11-Agg-MM-O-O').alias('bifurcationType')\
                                                   ,col('transactionLine'),col('journalId')\
                                                   ,col('RelRow').cast(StringType()).alias('relatedLineItemNumber')\
                                                   ,col('relatedAccount'),col('relatedLIType')\
                                                   ,lit(ruleSequence).alias('ruleSequence')\
                                                   ,lit('').alias('loopCount')\
                                                   ,lit('').alias('direction')\
                                                   ,lit('').alias('aggregatedInToLineItem')\
                                                   ,lit(None).alias('aggregatedInToLine')\
                                                   ,col('patternID').alias('patternID'))
          
          fileName = gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta"
          objGenHelper.gen_writeToFile_perfom(df = fin_L1_STG_JEBifurcation_Link ,filenamepath = fileName,mode="append") 

    except Exception as err:
        raise
