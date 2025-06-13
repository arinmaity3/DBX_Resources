# Databricks notebook source
def JEBF_LargeTransaction_prepare(maxLineCount=1000):
        try:
           global JEBF_LargeTransaction

           JEBF_LargeTransaction = fin_L1_TMP_JEBifurcation\
                    .groupBy("transactionID","patternid")\
                    .agg(count(expr("*")).alias("transactionCount"))\
                    .filter(col("transactionCount")>lit(maxLineCount))
        except Exception as err:
             raise
