# Databricks notebook source
def aggregateInputAndUpdateLink():
     try:
      objGenHelper = gen_genericHelper()
      
      global fin_L1_TMP_JEBifurcation_Accountwise_Agg_Docbased
      global fin_L1_TMP_JEBifurcation_Accountwise_Agg

      fin_L1_STG_JEBifurcation_Link=objGenHelper.gen_readFromFile_perform\
                                       (gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta")
      fin_L1_TMP_JEBifurcation_Accountwise_Agg=fin_L1_TMP_JEBifurcation\
                .groupBy("transactionID","accountID","debitCreditCode") \
                .agg(min("transactionIDbyPrimaryKey").alias("transactionIDbyPrimaryKey"),
                     when(count(expr("*"))>1,concat(lit("A"),min("transactionLineIDKey").cast(StringType())))\
                      .otherwise(min("transactionLineIDKey").cast(StringType())).alias("transactionLineID"),
                     when(count(expr("*"))>1,concat(lit("A"),min("transactionLineIDKey").cast(StringType())))\
                      .otherwise(min("transactionLineIDKey").cast(StringType())).alias("transactionLineIDKey")\
                     ,min("transactionLineIDKey").cast(StringType()).alias("aggregatedInToLineItem")\
                     ,min("transactionLine").cast(IntegerType()).alias("transactionLine")\
                     ,sum("debitAmount").cast(DecimalType(32,6)).alias("debitAmount")\
                     ,sum("creditAmount").cast(DecimalType(32,6)).alias("creditAmount")\
                     ,sum("postingAmount").cast(DecimalType(32,6)).alias("postingAmount")\
                     ,min("journalId").alias("journalId")\
                     ,min("patternID").alias("patternID")
                     )
      fin_L1_TMP_JEBifurcation_Accountwise_Agg = fin_L1_TMP_JEBifurcation_Accountwise_Agg.alias("AGG")\
                        .join(fin_L1_TMP_JEBifurcation.alias("LTJ")\
                              ,(col("AGG.transactionID")==col("LTJ.transactionID"))\
                              &(col("AGG.accountID")==col("LTJ.accountID"))
                              &(col("AGG.debitCreditCode")==col("LTJ.debitCreditCode"))
                              &(col("AGG.transactionLine")==col("LTJ.transactionLine")),"inner")\
                        .select(col("AGG.*"),col("LTJ.journalSurrogateKey"),col("LTJ.lineItem"))
      fin_L1_TMP_JEBifurcationWithAgg_cte = fin_L1_TMP_JEBifurcation\
                  .selectExpr("transactionID","transactionLineIDKey"\
 	   		       ,"CASE	WHEN	COUNT(transactionLineIDKey) OVER (PARTITION BY transactionID,accountID,debitCreditCode )> 1 \
                                    THEN concat('A', CAST(MIN(transactionLineIDKey) \
                                                OVER (PARTITION BY transactionID,accountID,debitCreditCode)  AS VARCHAR(10))) \
 	   				        ELSE	CAST(MIN(transactionLineIDKey) \
                                                OVER (PARTITION BY transactionID,accountID,debitCreditCode) AS VARCHAR(10)) \
                                    END AS aggregatedInToLineItem ")
      fin_L1_TMP_JEBifurcationWithAgg_cte = fin_L1_TMP_JEBifurcationWithAgg_cte.alias("cte")\
                        .join(fin_L1_TMP_JEBifurcation_Accountwise_Agg.alias("jeaa1")\
                               ,(col("cte.transactionID")==col("jeaa1.transactionID"))\
                               &(col("cte.aggregatedInToLineItem")==col("jeaa1.transactionLineID")),"inner")\
                        .select(col("cte.transactionID")\
                               ,col("cte.transactionLineIDKey")\
                               ,col("jeaa1.aggregatedInToLineItem")\
                               ,col("jeaa1.transactionLine"))
      
      fin_L1_STG_JEBifurcation_Link = fin_L1_STG_JEBifurcation_Link\
                                      .withColumn("aggregatedInToLine",lit('').cast(IntegerType()))
      fin_L1_STG_JEBifurcation_Link.write.format("delta").mode("overwrite")\
                                   .option("mergeSchema", "true").save(gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta")
      fin_L1_STG_JEBifurcation_Link = DeltaTable.forPath(spark, gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta")
      fin_L1_STG_JEBifurcation_Link.alias("jel1")\
                .merge(source=fin_L1_TMP_JEBifurcationWithAgg_cte.alias("cte")\
                ,condition="jel1.transactionID=cte.transactionID AND jel1.lineItemNumber=cte.transactionLineIDKey AND jel1.dataType='I'") \
                .whenMatchedUpdate(set = {"aggregatedInToLineItem": "cte.aggregatedInToLineItem"\
                                   ,"aggregatedInToLine": "cte.transactionLine"}).execute()
      
      
      
      fileName = gl_bifurcationPath+"fin_L1_TMP_JEBifurcation_Accountwise_Agg.delta"
      objGenHelper.gen_writeToFile_perfom(df = fin_L1_TMP_JEBifurcation_Accountwise_Agg,filenamepath = fileName\
                                          ,isCreatePartitions = True,partitionColumns = "patternID")
      
      fin_L1_TMP_JEBifurcation_Accountwise_Agg_Docbased=fin_L1_TMP_JEBifurcation_Accountwise_Agg.groupBy("transactionID") \
                         .agg(expr("CASE WHEN(ABS(SUM(debitAmount) - SUM(creditAmount)) > 0.001) THEN 1 ELSE 0 END").alias("isMismatched"),\
                              expr("SUM(CASE WHEN debitCreditCode = 'C' THEN 1 ELSE 0 END)").alias("creditCount"),\
                              expr("SUM(CASE WHEN debitCreditCode = 'D' THEN 1 ELSE 0 END)").alias("debitCount"),\
                              when(count(expr("*"))==2,lit(1)).otherwise(lit(0)).alias("isOnetoOne")\
                              ).filter(expr("isMismatched=0"))
      
     
      fin_L1_TMP_JEBifurcation_Accountwise_Agg = objGenHelper.gen_readFromFile_perform\
                               (gl_bifurcationPath+"fin_L1_TMP_JEBifurcation_Accountwise_Agg.delta")
      
     except Exception as err:
        raise
