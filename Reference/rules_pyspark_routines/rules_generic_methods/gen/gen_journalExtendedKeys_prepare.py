# Databricks notebook source
import sys
import traceback
from pyspark.sql.window import Window

def _maxJournalSurrogateKey_get():

      global  gl_CDMLayer1Path
      maxSurrogateKey = 0
      try:
          dbutils.fs.ls(gl_CDMLayer1Path + 'fin_L1_TD_Journal.delta')
          maxSurrogateKey = objGenHelper.gen_readFromFile_perform(gl_CDMLayer1Path + "fin_L1_TD_Journal.delta").\
                agg({"journalSurrogateKey":"max"}).collect()[0][0]
          return maxSurrogateKey
      except AnalysisException as err:
          pass
      finally:
          return maxSurrogateKey

def gen_journalExtendedKeys_prepare():    
      try:
          global fin_L1_TD_Journal
          global gl_layer0Temp


          objGenHelper = gen_genericHelper()

          lstOfAddedColumns = ["transactionIDbyJEDocumnetNumber","documentAmountLC",
                     "lineItemCount","transactionIDbyPrimaryKey","journalSurrogateKey"]

          fin_L1_TD_Journal = fin_L1_TD_Journal.drop(*lstOfAddedColumns)
          
          objGenHelper.gen_writeToFile_perfom(fin_L1_TD_Journal, gl_layer0Temp + "fin_L1_TD_Journal_Tmp.delta")          
          fin_L1_TD_Journal_Tmp = objGenHelper.gen_readFromFile_perform(gl_layer0Temp + "fin_L1_TD_Journal_Tmp.delta")

          w = Window.orderBy(F.lit(''))
          df_JrnlIDbyPrimaryKey=fin_L1_TD_Journal_Tmp.select("companyCode","fiscalYear","documentNumber","financialPeriod"\
                                                                    ,"lineItem","debitCreditIndicator").distinct()\
              .withColumn("transactionIDbyPrimaryKey", F.row_number().over(w))
          
          objGenHelper.gen_writeToFile_perfom(df_JrnlIDbyPrimaryKey,gl_layer0Temp + "JrnlIDbyPrimaryKey.delta")
          df_JrnlIDbyPrimaryKey = objGenHelper.gen_readFromFile_perform(gl_layer0Temp + "JrnlIDbyPrimaryKey.delta")
                   
          
          df_JrnlIDbyJEDocumnetNumber=fin_L1_TD_Journal_Tmp.groupBy("companyCode","fiscalYear","documentNumber","financialPeriod")\
            .agg(sum(when(col("debitCreditIndicator")==lit("D") ,col("amountLC")).otherwise(0.00)).alias('documentAmountLC')\
            ,count("lineItem").alias('lineItemCount'))\
            .withColumn("transactionIDbyJEDocumnetNumber", F.row_number().over(w))
          
          objGenHelper.gen_writeToFile_perfom(df_JrnlIDbyJEDocumnetNumber,gl_layer0Temp + "df_JrnlIDbyJEDocumnetNumber.delta")          
          df_JrnlIDbyJEDocumnetNumber = objGenHelper.gen_readFromFile_perform(gl_layer0Temp + "df_JrnlIDbyJEDocumnetNumber.delta")
           

          JEExtended =fin_L1_TD_Journal_Tmp.alias('jrnl')\
              .join(df_JrnlIDbyJEDocumnetNumber.alias('jrnl1')\
                    ,["companyCode","fiscalYear","documentNumber","financialPeriod"],'inner')\
              .join(df_JrnlIDbyPrimaryKey.alias('jrnl2')\
                    ,["companyCode","fiscalYear","documentNumber","financialPeriod","lineItem","debitCreditIndicator"],'inner')\
              .select("jrnl.*","transactionIDbyJEDocumnetNumber","documentAmountLC","lineItemCount","transactionIDbyPrimaryKey")\
              .withColumn("journalSurrogateKey",F.row_number().over(w))
                         
          return JEExtended

      except Exception as err:
          raise
