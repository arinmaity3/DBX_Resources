# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,when,concat,col,substring

def JEBF_PrebifurcationInput_prepare():
  try:

    global fin_L1_TMP_JEBifurcation
    
    objGenHelper = gen_genericHelper()

    fin_L1_STG_JEBifurcation_00_Initialize = spark.read.format("delta")\
                                       .load(gl_preBifurcationPath+"fin_L1_STG_JEBifurcation_00_Initialize.delta")

    prebifurcationInput = fin_L1_STG_JEBifurcation_00_Initialize\
              .select(col('journalSurrogateKey')\
                      ,col('transactionIDbyPrimaryKey')\
                      ,col('transactionIDbyJEDocumnetNumber').cast(StringType()).alias('transactionID')\
                      ,col('lineItem')\
                      ,col('transactionLineIDKey')\
                      ,col('transactionLineIDKey').cast(StringType()).alias('transactionLineID')\
                      ,col('transactionLineIDKey').alias('transactionLine')\
                      ,col('journalID')\
                      ,col('accountID')\
                      ,col('debitAmount')\
                      ,col('creditAmount')\
                      ,col('debitCreditCode')\
                      ,col('postingAmount'),lit(0).alias('patternID')\
                      ,lit('6-No Pattern').alias('patternSource')\
                      ,lit('0_No_Block').alias('blockSource'))
    
    prebifurcationInput.createOrReplaceTempView("prebifurcationInput")
    sqlContext.cacheTable("prebifurcationInput")
    
    up1 = prebifurcationInput.alias('up')\
          .filter( ((col('up.postingAmount') < 0) & (col('up.debitCreditCode') == 'D')\
                     | (col('up.postingAmount') > 0) & (col('up.debitCreditCode') == 'C')))\
          .select(col('up.transactionID').alias('transactionID')).distinct()
    
    up2 = prebifurcationInput.alias('up')\
          .filter( ((col('up.postingAmount') > 0) & (col('up.debitCreditCode') == 'D') \
                    | (col('up.postingAmount') < 0) & (col('up.debitCreditCode') == 'C')))\
          .select(col('up.transactionID').alias('transactionID')).distinct()
      
    uptate = up1.alias('up1')\
            .join(up2.alias("up2"),col('up1.transactionID') == col('up2.transactionID'),'inner')\
            .select(col('up1.transactionID'))

    chk = prebifurcationInput.alias('nn')\
          .join(uptate.alias('nnn'),col("nn.transactionID") == col("nnn.transactionID"),"leftsemi")\
          .select(col('nn.transactionID').alias('transactionID')\
                  ,lit(1).alias('IrregularTransaction'))\
          .distinct()

    
    Irregular = when(col('ck.IrregularTransaction').isNull(),lit(0))\
                      .otherwise(col('ck.IrregularTransaction'))

    prebifurcationInput = prebifurcationInput.alias('bf1')\
                    .join(chk.alias('ck'),(col('bf1.transactionID') == col('ck.transactionID')),"left")\
                    .withColumn("isIrregularTransaction",lit(Irregular))\
                    .select(col('bf1.journalSurrogateKey')\
                            ,col('bf1.transactionIDbyPrimaryKey')\
                            ,col('bf1.transactionID')\
                            ,col('bf1.lineItem')\
                            ,col('bf1.transactionLineIDKey')\
                            ,col('bf1.transactionLineID')\
                            ,col('bf1.transactionLine')\
                            ,col('bf1.journalId')\
                            ,col('bf1.accountID')\
                            ,col('bf1.debitAmount')\
                            ,col('bf1.creditAmount')\
                            ,col('bf1.debitCreditCode')\
                            ,col('bf1.postingAmount')\
                            ,col('bf1.patternSource')\
                            ,col('bf1.blockSource')\
                            ,col('isIrregularTransaction')\
                            ,col('patternID'))
    
    prebifurcationInput.createOrReplaceTempView("prebifurcationInput")
    sqlContext.cacheTable("prebifurcationInput")
    
    df1 = prebifurcationInput.groupBy("transactionID")\
                                .agg(sum("postingAmount").alias("pa"))\
                                .where(abs(col('pa')) > 0.001)
    

    amount = when(col('li.debitCreditCode') == 'D',col('li.debitAmount'))\
             .otherwise(col('li.creditAmount'))
  
    
    prebifurcationInput_Link = prebifurcationInput.alias('li')\
                  .join(df1.alias('src'),col("li.transactionID") == col("src.transactionID"),"leftsemi")\
                  .select(col('li.journalSurrogateKey')\
                          ,col('li.transactionIDbyPrimaryKey')\
                          ,col('li.transactionID')\
                          ,col('li.lineItem')\
                          ,col('li.transactionLineID').alias('lineItemNumber')\
                          ,col('li.accountID').alias('fixedAccount')\
                          ,col('li.debitCreditCode').alias('fixedLIType')\
                          ,lit(amount).alias('amount')\
                          ,lit('I').alias('dataType')\
                          ,lit('').alias('rule')\
                          ,lit('').alias('bifurcationType')\
                          ,col('li.journalId')\
                          ,col('li.transactionLine')\
                          ,lit('').alias('relatedLineItemNumber')\
                          ,lit('').alias('relatedAccount')\
                          ,lit('').alias('relatedLIType')\
                          ,lit(0).alias('ruleSequence')\
                          ,lit('').alias('loopCount')\
                          ,lit('').alias('direction')\
                          ,lit('').alias('aggregatedInToLineItem')\
                          ,col('li.patternID')\
                          ,col('li.patternSource')\
                          ,when(col("li.blockSource").isNull(),lit("0_No_Block")).\
                                  otherwise(col("li.blockSource")).alias("blockSource"))
    
    fileName = gl_preBifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta"
    objGenHelper.gen_writeToFile_perfom(df = prebifurcationInput_Link,filenamepath = fileName)
    
    fin_L1_STG_JEBifurcation_Link = objGenHelper.gen_readFromFile_perform(gl_preBifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta")
                                           
    
    to_del = prebifurcationInput.alias('src')\
            .join(fin_L1_STG_JEBifurcation_Link.alias('li')\
                  ,(col('li.transactionID') == col('src.transactionID')) \
                  & (col('li.dataType') == lit('I')),'leftsemi')\
            .select(col('src.transactionID'))
    
    prebifurcationInput = prebifurcationInput.alias('sr')\
                                  .join(to_del.alias('srr')\
                                   ,(col('sr.transactionID') == col('srr.transactionID')),'leftanti')
  
    TransactionIDnew = when(((col('postingAmount') < 0) & (col('debitCreditCode') == 'D') \
                             | (col('postingAmount') > 0) & (col('debitCreditCode') == 'C'))\
                                 ,concat(col("transactionID"),lit("-NG")))\
                      .otherwise(col('transactionID'))
    blocksourcenew = when(((col('postingAmount') < 0) & (col('debitCreditCode') == 'D') \
                           | (col('postingAmount') > 0) & (col('debitCreditCode') == 'C'))\
                                ,lit('1_Block_Building_NG'))\
                    .otherwise(col('blockSource'))
    
    prebifurcationInput = prebifurcationInput\
                                  .withColumn("TransactionID",lit(TransactionIDnew))\
                                  .withColumn("blockSource",lit(blocksourcenew))
    
    bksrc = when((col('isIrregularTransaction') == '1') & ((when(col("blockSource").isNull(),lit(""))\
                     .otherwise(col("blockSource"))) != lit('1_Block_Building_NG') ),lit('1_Block_Building'))\
            .otherwise(col('blockSource'))
    prebifurcationInput = prebifurcationInput\
                                  .withColumn("blockSource",lit(bksrc))

    df2 = prebifurcationInput.groupBy("transactionID")\
                                .agg(sum("postingAmount").alias("pa"))\
                                .where(abs(col('pa')) > 0.001)
  
    amount1 = when(col('li.debitCreditCode') == 'D',col('li.debitAmount'))\
             .otherwise(col('li.creditAmount'))
    
    prebifurcationInput_Link = prebifurcationInput.alias('li')\
                .join(df2.alias('src'),col("li.transactionID") == col("src.transactionID"),"leftsemi")\
                .select(col('li.journalSurrogateKey')\
                  ,col('li.transactionIDbyPrimaryKey')\
                  ,col('li.transactionID')\
                  ,col('li.lineItem')\
                  ,col('li.transactionLineID').alias('lineItemNumber')\
                  ,col('li.accountID').alias('fixedAccount')\
                  ,col('li.debitCreditCode').alias('fixedLIType')\
                  ,lit(amount1).alias('amount')\
                  ,lit('I').alias('dataType')\
                  ,lit('').alias('rule')\
                  ,lit('').alias('bifurcationType')\
                  ,col('li.journalId')\
                  ,col('li.transactionLine')\
                  ,lit('').alias('relatedLineItemNumber')\
                  ,lit('').alias('relatedAccount')\
                  ,lit('').alias('relatedLIType')\
                  ,lit(0).alias('ruleSequence')\
                  ,lit('').alias('loopCount')\
                  ,lit('').alias('direction')\
                  ,lit('').alias('aggregatedInToLineItem')\
                  ,col('li.patternID')\
                  ,col('li.patternSource')\
                  ,when(col("li.blockSource").isNull(),lit("0_No_Block")).\
                                otherwise(col("li.blockSource")).alias("blockSource"))

    
    fileName = gl_preBifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta"
    objGenHelper.gen_writeToFile_perfom(df = prebifurcationInput_Link,filenamepath = fileName ,mode = "append")

    fin_L1_STG_JEBifurcation_Link = objGenHelper.gen_readFromFile_perform(\
                          gl_preBifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta")
    prebifurcationInput_Link = fin_L1_STG_JEBifurcation_Link\
                                   .filter(col('dataType') == lit('I'))\
                                   .select(col('journalSurrogateKey')\
                                           ,col('transactionIDbyPrimaryKey')\
                                           ,col('transactionID')\
                                           ,col('lineItem')\
                                           ,col('lineItemNumber')\
                                           ,col('fixedAccount')\
                                           ,col('fixedLIType')\
                                           ,col('amount').alias('amount')\
                                           ,lit('O').alias('dataType')\
                                           ,lit('unbifurcated').alias('rule')\
                                           ,lit('99-unbifurcated').alias('bifurcationType')\
                                           ,col('journalId')\
                                           ,col('transactionLine')\
                                           ,lit('').alias('relatedLineItemNumber')\
                                           ,lit('#NA#').alias('relatedAccount')\
                                           ,lit('').alias('relatedLIType')\
                                           ,lit(0).alias('ruleSequence')\
                                           ,lit('').alias('loopCount')\
                                           ,lit('').alias('direction')\
                                           ,lit('').alias('aggregatedInToLineItem')\
                                           ,col('patternID')\
                                           ,lit('').alias('patternSource')\
                                           ,lit('').alias("blockSource"))
  

    
    fileName = gl_preBifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta"
    objGenHelper.gen_writeToFile_perfom(df = prebifurcationInput_Link,filenamepath = fileName ,mode = "append")

    to_del1 = prebifurcationInput.alias('src')\
            .join(prebifurcationInput_Link.alias('li')\
                  ,(col('li.transactionID') == col('src.transactionID')) \
                  & (col('li.dataType') == lit('O')),'leftsemi')\
            .select(col('src.transactionID'))
    
    prebifurcationInput = prebifurcationInput.alias('sr')\
                                  .join(to_del1.alias('srr')\
                                        ,(col('sr.transactionID') == col('srr.transactionID')),'leftanti')
    prebifurcationInput = prebifurcationInput\
                          .selectExpr("journalSurrogateKey"\
                                      ,"transactionIDbyPrimaryKey"\
                                      ,"transactionID"\
                                      ,"lineItem"\
                                      ,"transactionLineIDKey"\
                                      ,"transactionLineID"\
                                      ,"journalId"\
                                      ,"accountID"\
                                      ,"debitAmount"\
                                      ,"creditAmount"\
                                      ,"debitCreditCode"\
                                      ,"cast(postingAmount as decimal(32,6))"\
                                      ,"patternID"\
                                      ,"patternSource"\
                                      ,"transactionLine"\
                                      ,"blockSource"\
                                      ,"isIrregularTransaction")
    
    
    fileName = gl_preBifurcationPath+"fin_L1_TMP_JEBifurcation.delta"
    objGenHelper.gen_writeToFile_perfom(df = prebifurcationInput,filenamepath = fileName)
    
    fin_L1_TMP_JEBifurcation = objGenHelper.gen_readFromFile_perform(\
                               gl_preBifurcationPath+"fin_L1_TMP_JEBifurcation.delta")
  
  except Exception as err:
    raise
