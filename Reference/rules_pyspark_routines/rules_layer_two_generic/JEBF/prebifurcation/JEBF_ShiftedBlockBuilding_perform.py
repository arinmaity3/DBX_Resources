# Databricks notebook source
import sys
import traceback
import pandas as pd
import inspect

def JEBF_ShiftedBlockBuilding_perform(shiftedBlockCall):
    
        try:
            objGenHelper = gen_genericHelper()
            
            blockSource="2_Shifted_Block_Building" if shiftedBlockCall ==1 else "3_Shifted_Block_Building"
            
            dfTransactionColumns=fin_L1_TMP_JEBifurcation.filter(col("patternID") == 0)\
                                 .select(col("transactionID")\
                                         ,col("transactionLineIDKey")\
                                         ,col("debitCreditCode")\
                                         ,col("postingAmount").cast(StringType()).alias("postingAmount"))
            if shiftedBlockCall==2:
                dfTransactionColumns=dfTransactionColumns\
                  .filter(expr("blockSource='2_Shifted_Block_Building'"))
            dfSmallTransactions=dfTransactionColumns.groupBy("transactionID")\
                .agg(expr("count(*) as lineCount"))\
                .filter(expr("lineCount<="+str(gl_SBBMaxLineCount)))
            dfTransactionColumns=dfTransactionColumns.alias("inp")\
                .join(dfSmallTransactions.alias("small")\
                ,expr("inp.transactionID=small.transactionID"),"inner")\
                .selectExpr("inp.*")
            if dfTransactionColumns.rdd.isEmpty():
                #No records with patternID 0. So SBB not executed.
                return
            else:
                dfSBBOutput = dfTransactionColumns.groupby("transactionID").applyInPandas(ApplySBB,gl_SBBSchema)
                df_input = DeltaTable.forPath(spark, gl_preBifurcationPath+"fin_L1_TMP_JEBifurcation.delta")
                df_input.alias("inp")\
               .merge(source=dfSBBOutput.alias("sbbout")\
               ,condition="inp.patternID=0 and sbbout.transactionID = inp.transactionID \
                          and sbbout.transactionLineIDKey=inp.transactionLineIDKey") \
               .whenMatchedUpdate(set = { "transactionID": "sbbout.transactionID_New","blockSource": lit(blockSource)}).execute()
    
        except Exception as err:
                raise
