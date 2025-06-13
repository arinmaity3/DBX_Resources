# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, lead, first, last, desc
import pandas as pd

def JEBF_32_AdjMatch_bifurcate(lsPatterns,ruleSequence):
    try:
            objGenHelper = gen_genericHelper() 
            
            dfInputADJ1=fin_L1_TMP_JEBifurcation.filter(col("patternID").isin(*lsPatterns))\
                            .select("transactionID","transactionLineIDKey"\
                                    ,"debitCreditCode"\
                                    ,expr("CASE debitCreditCode WHEN 'D' THEN postingAmount ELSE -1* postingAmount END").alias("amount")\
                                    ,"accountID"\
                                    ,expr("postingAmount"),col("accountID").alias("fixedAccount")\
                                    ,"patternID", "lineItem")

            dfInputADJ1 = dfInputADJ1.withColumn('leadLineIDKey', lead('transactionLineIDKey')\
                                    .over(Window.partitionBy('transactionID','patternID').orderBy('transactionLineIDKey')))

            dfInputADJ1 = dfInputADJ1.withColumn('relatedAccount', lead('accountID')\
                                    .over(Window.partitionBy('transactionID','patternID').orderBy('transactionLineIDKey')))
           
                                                
            dfInputADJ1 = dfInputADJ1.withColumn('relatedLIType',lead('debitCreditCode')\
                                    .over(Window.partitionBy('transactionID','patternID').orderBy('transactionLineIDKey')))
           
            dfInputADJ1 = dfInputADJ1.withColumn('isAdjacentMatch',when((lead('transactionLineIDKey')\
                                                                       .over(Window.partitionBy('transactionID','patternID')\
                                                                       .orderBy('transactionLineIDKey')) - col("transactionLineIDKey") == 1)\
                                                                   &(col("postingAmount") +lead('postingAmount')\
                                                                       .over(Window.partitionBy('transactionID','patternID')\
                                                                       .orderBy('transactionLineIDKey'))==0)\
                                                                   &(col("debitCreditCode") !=lead('debitCreditCode')\
                                                                   .over(Window.partitionBy('transactionID','patternID')\
                                                                   .orderBy('transactionLineIDKey'))),1).otherwise(0))
          
            fin_L1_TMP_JEBifurcation_All_AdjacentMatch_01Delta = dfInputADJ1.filter("isAdjacentMatch=1")
            objGenHelper.gen_writeToFile_perfom(df = fin_L1_TMP_JEBifurcation_All_AdjacentMatch_01Delta,\
                filenamepath = gl_bifurcationPath+"fin_L1_TMP_JEBifurcation_All_AdjacentMatch_01.delta")
                                            
        
            fin_L1_TMP_JEBifurcation_All_AdjacentMatch_01Delta = spark.read.format("delta")\
                .load(gl_bifurcationPath+"fin_L1_TMP_JEBifurcation_All_AdjacentMatch_01.delta")
            
          
            
            fin_L1_TMP_JEBifurcation_All_AdjacentMatch_01 = fin_L1_TMP_JEBifurcation_All_AdjacentMatch_01Delta\
                                                                .withColumn('lagOfLeadLine', lag('leadLineIDKey')\
                                                                .over(Window.partitionBy('transactionID','patternID')\
                                                                    .orderBy('transactionLineIDKey')))
                        
            fin_L1_TMP_JEBifurcation_OverlappingLine = fin_L1_TMP_JEBifurcation_All_AdjacentMatch_01\
							  .select("transactionID"\
									  ,"transactionLineIDKey"\
									  ,"leadLineIDKey"\
									  ,"patternID"\
                                      ,"debitCreditCode"\
                                      ,"amount"\
									  ,"lagOfLeadLine")\
							  .filter("transactionLineIDKey = lagOfLeadLine")

            L1_TMP_JEBifurcation_OverlappingLine_Lags=\
            fin_L1_TMP_JEBifurcation_OverlappingLine\
            .selectExpr("transactionID"\
                    ,"transactionLineIDKey"\
                    ,"debitCreditCode"\
                    ,"amount"\
                    ,"patternID"\
                    ,"leadLineIDKey"\
                    ,"lagOfLeadLine"\
                    ,"lag(transactionLineIDKey) OVER (PARTITION BY transactionID,patternID \
                                            ORDER BY transactionLineIDKey) as lagOverLapLine"\
                    ,"lag(debitCreditCode) OVER (PARTITION BY transactionID,patternID \
                                            ORDER BY transactionLineIDKey) as lagOverLapDebitCreditCode"\
                    ,"lag(amount) OVER (PARTITION BY transactionID,patternID \
                                            ORDER BY transactionLineIDKey) as lagOverLapAmount")
            
            L1_TMP_JEBifurcation_OverlappingBlockStart=\
            L1_TMP_JEBifurcation_OverlappingLine_Lags\
            .selectExpr("*","case when transactionLineIDKey-lagOverLapLine <>1 \
            or debitCreditCode=lagOverLapDebitCreditCode \
            or amount<>lagOverLapAmount then 1 else 0 end as isNewOverLapBlockStart")
            
            L1_TMP_JEBifurcation_OverlappingBlocks=\
            L1_TMP_JEBifurcation_OverlappingBlockStart\
            .selectExpr("*","SUM(isNewOverLapBlockStart) OVER (PARTITION BY transactionID,patternID \
                                            ORDER BY transactionLineIDKey ROWS UNBOUNDED PRECEDING) as BlockID")
            
            L1_TMP_JEBifurcation_OverlappingBlocks_Delete=\
            L1_TMP_JEBifurcation_OverlappingBlocks\
            .groupBy("transactionID","patternID","blockID")\
            .agg(expr("count(*) as cnt"))\
            .filter("cnt==1")
            
            dfDeletedLine=L1_TMP_JEBifurcation_OverlappingBlocks.alias("ovr")\
            .join(L1_TMP_JEBifurcation_OverlappingBlocks_Delete.alias("del"),\
                 expr("ovr.transactionID=del.transactionID and ovr.BlockID=del.BlockID\
                 and ovr.patternID=del.patternID"),"inner")\
            .select("ovr.transactionID","ovr.transactionLineIDKey","ovr.patternID")

            df_input = DeltaTable.forPath(spark, gl_bifurcationPath+"fin_L1_TMP_JEBifurcation_All_AdjacentMatch_01.delta")
            df_input.alias("ADJ")\
            .merge(source=dfDeletedLine.alias("de")\
                 ,condition="ADJ.transactionID = de.transactionID\
                             and ADJ.transactionLineIDKey=de.transactionLineIDKey\
                             and ADJ.patternID=de.patternID")\
            .whenMatchedDelete(condition = lit(True)).execute() 
            
            L1_TMP_JEBifurcation_OverlappingBlocks_MultiAdj=\
            L1_TMP_JEBifurcation_OverlappingBlocks\
            .groupBy("transactionID","patternID","blockID").agg(expr("count(*) as cnt"))\
            .filter("cnt>1")
            
            L1_TMP_JEBifurcation_OverlappingBlocks_MultiAdjInp=\
            L1_TMP_JEBifurcation_OverlappingBlocks_MultiAdj.alias("mul")\
            .join(L1_TMP_JEBifurcation_OverlappingBlocks.alias("ovr"),\
                 expr("mul.transactionID=ovr.transactionID and mul.BlockID=ovr.BlockID"),"inner")\
            .select("ovr.transactionID","ovr.patternID","ovr.transactionLineIDKey","ovr.BlockID")
            
            df_ApplyAdjDel=L1_TMP_JEBifurcation_OverlappingBlocks_MultiAdjInp\
              .groupby("transactionID","BlockID","patternID").applyInPandas(ApplyADJ,gl_ADJSchema)
            
            df_input = DeltaTable.forPath(spark, gl_bifurcationPath+"fin_L1_TMP_JEBifurcation_All_AdjacentMatch_01.delta")
            df_input.alias("ADJ")\
            .merge(source=df_ApplyAdjDel.alias("de")\
                 ,condition="ADJ.transactionID = de.transactionID\
                             and ADJ.transactionLineIDKey=de.transactionLineIDKey")\
            .whenMatchedDelete(condition = lit(True)).execute() 
            
            dfADJSet1 = fin_L1_TMP_JEBifurcation_All_AdjacentMatch_01\
                                       .select("transactionID" ,"lineItem"\
                                                ,col("transactionLineIDKey").alias("lineItemNumber")\
                                                ,"debitCreditCode"\
                                                ,"amount"\
                                                ,"fixedAccount"\
                                                ,col("debitCreditCode").alias("fixedLIType")\
                                                ,col("leadLineIDKey").alias("relatedLineItemNumber")\
                                                ,"relatedAccount"\
                                                ,"relatedLIType"\
                                                ,"patternID"\
                                               )
                      
            dfADJSet2=fin_L1_TMP_JEBifurcation_All_AdjacentMatch_01\
                          .select("transactionID"\
                                   ,"lineItem"\
                                   ,col("leadLineIDKey").alias("lineItemNumber")\
                                   ,expr("CASE WHEN debitCreditCode ='C' THEN 'D' ELSE 'C' END").alias("debitCreditCode")\
                                   ,"amount"\
                                   ,col("relatedAccount").alias("fixedAccount")\
                                   ,col("relatedLIType").alias("fixedLIType")\
                                   ,col("transactionLineIDKey").alias("relatedLineItemNumber")\
                                   ,col("fixedAccount").alias("relatedAccount")\
                                   ,col("debitCreditCode").alias("relatedLIType")\
                                   ,"patternID"\
                                  )
            dfADJall=dfADJSet1.unionAll(dfADJSet2)
            fin_L1_STG_JEBifurcation_Link = dfADJall.alias("adj")\
                                .join(fin_L1_TMP_JEBifurcation.alias("trans")\
                                    ,(col("adj.transactionID") == col("trans.transactionID"))\
                                    &(col("adj.patternID") == col("trans.patternID"))
                                    &(col("adj.lineItemNumber") == col("trans.transactionLineID")),how='inner')\
                                .select(col("trans.journalSurrogateKey").alias("journalSurrogateKey")\
                                ,col("trans.transactionIDbyPrimaryKey").alias("transactionIDbyPrimaryKey")\
                                ,col("trans.transactionID").alias("transactionID")\
                                ,col("trans.lineItem").alias("lineItem")\
                                ,col("trans.transactionLineID").alias("lineItemNumber")\
                                ,col("trans.accountID").alias("fixedAccount")\
                                ,col("trans.debitCreditCode").alias("fixedLIType")\
                                ,expr("CASE trans.debitCreditCode WHEN 'D' THEN trans.postingAmount\
                                        ELSE -1* trans.postingAmount END" ).cast(DecimalType(32,6)).alias("amount")\
                                ,lit('O').alias("dataType")\
                                ,lit('Adjacent Matches').alias("rule")\
                                ,lit('32-AdjMatch').alias("bifurcationType")\
                                ,col("trans.transactionLine")\
                                ,col("trans.journalId").alias("journalId")\
                                ,col("adj.relatedLineItemNumber").cast(StringType()).alias("relatedLineItemNumber")\
                                ,col("adj.relatedAccount").alias("relatedAccount")\
                                ,col("adj.relatedLIType").alias("relatedLIType")\
                                ,lit(ruleSequence).alias("ruleSequence")\
                                ,lit('').alias("loopCount")\
                                ,lit('').alias("direction")\
                                ,lit('').alias("aggregatedInToLineItem")\
                                ,col("adj.patternID").alias("patternID"))
           
            fileName=gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta"
            objGenHelper.gen_writeToFile_perfom(df = fin_L1_STG_JEBifurcation_Link, filenamepath = fileName,mode="append")
          
    except Exception as err:
           raise
