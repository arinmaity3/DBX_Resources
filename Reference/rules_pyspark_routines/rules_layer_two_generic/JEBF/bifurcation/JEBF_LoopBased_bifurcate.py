# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,row_number,when,concat,col,expr
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def  JEBF_LoopBased_bifurcate(lsPatterns,ruleSequence):
 
    try:
      objGenHelper = gen_genericHelper() 
      df_Input = fin_L1_TMP_JEBifurcation.filter(col('patternID').isin(*lsPatterns))\
                 .select(col('journalSurrogateKey') \
                 ,col('transactionID') \
                 ,col('transactionLineIDKey') \
                 ,col('debitCreditCode') \
                 ,col('patternID') \
                 ,expr("cast(case when debitCreditCode = 'C' then creditAmount \
                       else debitAmount end as varchar(50))").alias("amount") \
                 ,col('accountID'))

      df_Input=df_Input.alias('inp')\
               .join(JEBF_LargeTransaction.alias('large'),\
                    (col('large.transactionID') == col('inp.transactionID'))\
                     &(col('large.patternID') ==  col('inp.patternID')),'leftanti')\
               .select(col('inp.*'))

      dfOutputLoop = df_Input.groupby("transactionID","patternID").applyInPandas(ApplyLoopingStrategy,gl_LoopingSchema)

      dfOutputLoop=dfOutputLoop.alias("out")\
          .join(df_Input.alias("inp")\
          ,(col("out.transactionID")==col("inp.transactionID"))\
          &(col("out.oneSideTransactionLineIDKey")==col("inp.transactionLineIDKey")),"inner")\
          .select(col('out.*'),col("inp.patternID"))

      dfOutputLoop = dfOutputLoop\
                .withColumn("amount",col("amount").cast(DecimalType(32,6)))\
                .withColumn('lineItemNumber',col("oneSideTransactionLineIDKey").cast(StringType()))\
                .withColumn('relatedLineItemNumber',col("manySideTransactionLineIDKey").cast(StringType()))\
                .withColumn('bifurcationType'\
                    ,expr("case when (count(*) over(partition by transactionID,patternID,oneSideTransactionLineIDKey))=1 \
                      then '04-MM-O-O' when oneSideDebitCreditCode = 'D' \
                      then '05-MM-O-M' else '06-MM-M-O' end"))
      
      
      dfOutputLoop = dfOutputLoop.withColumn("lineItemNumber"\
                    ,expr("case when bifurcationType <> '04-MM-O-O' \
                        then concat(lineItemNumber,'.',\
                              cast(row_number() over (partition by transactionID,patternID,oneSideTransactionLineIDKey \
                              order by manySideTransactionLineIDKey) as varchar(40)))\
                        else lineItemNumber end"))\
              .withColumn("relatedLineItemNumber"\
                    ,expr("case when bifurcationType <> '04-MM-O-O' \
                        then concat(relatedLineItemNumber,'.1')\
                        else relatedLineItemNumber end"))
      
      df_Input_oneSide = df_Input.alias("inp")\
             .join(dfOutputLoop.alias("out"),\
             (col('out.transactionID') == col('inp.transactionID'))\
               & (col('out.oneSideTransactionLineIDKey') ==  col('inp.transactionLineIDKey'))\
             ,'inner')\
             .select(col('inp.transactionID')\
              ,col('transactionLineIDKey')\
              ,col('accountID')).distinct()

      df_Input_manySide = df_Input.alias("inp")\
             .join(dfOutputLoop.alias("out"),\
             (col('out.transactionID') == col('inp.transactionID'))\
               & (col('out.manySideTransactionLineIDKey') ==  col('inp.transactionLineIDKey'))\
               ,'inner')\
             .select(col('inp.transactionID')\
              ,col('transactionLineIDKey')\
              ,col('accountID'))

      dfOutputLoop = dfOutputLoop.alias('trans')\
              .join(df_Input_oneSide.alias('oneside'),\
                 (col('trans.transactionID') == col('oneside.transactionID'))\
                   & (col('trans.oneSideTransactionLineIDKey') ==  col('oneside.transactionLineIDKey')))\
              .join(df_Input_manySide.alias('manyside'),\
                 (col('trans.transactionID') == col('manyside.transactionID'))\
                   & (col('trans.manySideTransactionLineIDKey') ==  col('manyside.transactionLineIDKey')))\
               .select(col("trans.*")\
                ,col("oneside.accountID").alias("oneSideAccount")\
                ,col("manyside.accountID").alias("manySideAccount"))

      df_link = dfOutputLoop.alias('trans') \
                .join(fin_L1_TMP_JEBifurcation.alias('inp'),\
                    (col('trans.transactionID') == col('inp.transactionID'))\
                  & (col('trans.oneSideTransactionLineIDKey') ==  col('inp.transactionLineIDKey')),'inner')\
                .select(col('inp.journalSurrogateKey') \
                       ,col('inp.transactionIDbyPrimaryKey') \
                       ,col('inp.transactionID') \
                       ,col('trans.lineItemNumber') \
                       ,col('inp.accountID').alias('fixedAccount') \
                       ,col('inp.debitCreditCode').alias('fixedLIType') \
                       ,col('trans.amount') \
                       ,lit('O').alias('dataType') \
                       ,lit('Looping Strategy').alias('rule') \
                       ,col('trans.bifurcationType').alias('bifurcationType') \
                       ,col('inp.journalId') \
                       ,col('trans.relatedLineItemNumber') \
                       ,col('trans.manySideAccount').alias('relatedAccount') \
                       ,expr("case when trans.oneSideDebitCreditCode = 'D' then 'C' else 'D' end").alias('relatedLIType') \
                       ,lit(ruleSequence).alias('ruleSequence') \
                       ,col('trans.loopCount').alias('loopCount') \
                       ,lit('1').alias('direction') \
                       ,lit('').alias('aggregatedInToLineItem') \
                       ,col('inp.lineItem') \
                       ,col('inp.patternID').alias('patternID') \
                       ,col('inp.transactionLine') \
                       ,lit(None).alias('patternSource') \
                       ,lit(None).alias('blockSource') \
                      )

      #df_link.write.format("delta").mode("append").save(gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta")
      
      fileName=gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta"
      objGenHelper.gen_writeToFile_perfom(df=df_link,filenamepath=fileName,mode="append")
      
      df_link_02 = dfOutputLoop.alias('trans') \
                   .join(fin_L1_TMP_JEBifurcation.alias('inp'), \
                      (col('trans.transactionID') == col('inp.transactionID')) \
                    & (col('trans.manySideTransactionLineIDKey') ==  col('inp.transactionLineIDKey')),'inner') \
                  .select(col('inp.journalSurrogateKey') \
                       ,col('inp.transactionIDbyPrimaryKey') \
                       ,col('inp.transactionID') \
                       ,col('trans.relatedLineItemNumber').alias('lineItemNumber') \
                       ,col('inp.accountID').alias('fixedAccount') \
                       ,col('inp.debitCreditCode').alias('fixedLIType') \
                       ,col('trans.amount') \
                       ,lit('O').alias('dataType') \
                       ,lit('Looping Strategy').alias('rule') \
                       ,col('trans.bifurcationType').alias('bifurcationType') \
                       ,col('inp.journalId') \
                       ,col('trans.lineItemNumber').alias('relatedLineItemNumber') \
                       ,col('trans.oneSideAccount').alias('relatedAccount') \
                       ,col('trans.oneSideDebitCreditCode').alias('relatedLIType') \
                       ,lit(ruleSequence).alias('ruleSequence') \
                       ,col('trans.loopCount').alias('loopCount') \
                       ,lit('1').alias('direction') \
                       ,lit('').alias('aggregatedInToLineItem') \
                       ,col('inp.lineItem') \
                       ,col('inp.patternID').alias('patternID') \
                       ,col('inp.transactionLine') \
                       ,lit(None).alias('patternSource') \
                       ,lit(None).alias('blockSource') \
                      )

      fileName=gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta"
      objGenHelper.gen_writeToFile_perfom(df=df_link_02,filenamepath=fileName,mode="append")
  
    except Exception as err:
      raise
