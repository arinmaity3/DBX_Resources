# Databricks notebook source
import sys
import traceback


def JEBF_BlockBuilding_perform():
  try:
    objGenHelper = gen_genericHelper()
    
    fin_L1_TMP_JEBifurcation.createOrReplaceTempView("fin_L1_TMP_JEBifurcation")
    BB_input = spark.sql("Select transactionID\
                           ,transactionLineIDKey\
                           ,case when sum((-(creditAmount)) + debitAmount) \
                                    OVER(PARTITION BY transactionID ORDER BY transactionLineIDKey ROWS \
			                             BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) = 0 THEN 1 \
                                 ELSE 0 end AS cumSumInd \
                           from fin_L1_TMP_JEBifurcation where patternID = 0 ")
    BB_input.createOrReplaceTempView("BB_input")
    BB_input_block = spark.sql("select transactionID\
                              ,transactionLineIDKey\
                              ,cumSumInd \
                              ,case when cumSumInd = 1 \
                                    then sum(cumSumInd)\
                                       OVER(PARTITION BY transactionID ORDER BY transactionLineIDKey ROWS \
                                            BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - 1 \
                                    else SUM(cumSumInd)\
                                         OVER(PARTITION BY transactionID ORDER BY transactionLineIDKey ROWS \
                                            BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
                               end +1 AS blockInd \
                               from BB_input")
    window_transactionLineIDKey= Window.partitionBy("transactionId","blockInd").orderBy('transactionLineIDKey')
    window_block	=	Window.partitionBy(col('transactionId'))

    BB_output = BB_input_block\
                .select(col('transactionId').alias('transactionId')\
                        ,concat(col('transactionId'),lit('-'),col('blockInd')).alias('transactionID_new')\
                        ,col('transactionLineIDKey'),col('blockInd'),col('cumSumInd')\
                        ,F.row_number().over(window_transactionLineIDKey).alias("transactionLineIDKey_new")\
                        ,F.max(col('blockInd')).over(window_block).alias('BlockCount'))
    df_input = DeltaTable.forPath(spark, gl_preBifurcationPath+"fin_L1_TMP_JEBifurcation.delta")
    df_input.alias("inp")\
           .merge(source=BB_output.alias("bb")\
           ,condition="inp.patternID=0 and bb.transactionID = inp.transactionID \
                      and bb.transactionLineIDKey=inp.transactionLineIDKey") \
           .whenMatchedUpdate(set = { "transactionID": "bb.transactionID_New"\
                             ,"blockSource": "case when bb.BlockCount=1 then inp.blockSource else '1_Block_Building' end"\
                             ,"transactionLineIDKey":"case when isIrregularTransaction=0 then bb.transactionLineIDKey_new \
                                   else inp.transactionLineIDKey end"\
                             ,"transactionLineID":"case when isIrregularTransaction=0 then bb.transactionLineIDKey_new \
                                   else inp.transactionLineID end"}).execute()
    
  except Exception as err:
       raise
    
    
