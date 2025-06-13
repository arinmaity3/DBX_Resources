# Databricks notebook source
import pyspark.sql.functions as F  
def JEBF_31_Zero_Amount_bifurcate():
  
    """Populate fin_L1_JEBifurcation_31_Zero_Amount_populate"""
    try:
        
        objGenHelper = gen_genericHelper() 
        
        fin_L1_TMP_JEBifurcation_Filtered = fin_L1_TMP_JEBifurcation.filter(col("postingAmount") == 0)
        fin_L1_STG_JEBifurcation_Link = fin_L1_TMP_JEBifurcation_Filtered.selectExpr( 
                                        "journalSurrogateKey                       AS  journalSurrogateKey"\
                                       ,"transactionIDbyPrimaryKey                 AS  transactionIDbyPrimaryKey"\
                                       ,"transactionID                             AS    transactionID"\
                                       ,"lineItem                                  AS  lineItem"\
                                       ,"transactionLineID                         AS    lineItemNumber"\
                                       ,"accountID                                 AS  fixedAccount"\
                                       ,"debitCreditCode                           AS  fixedLIType"\
                                       ,"CASE WHEN debitCreditCode = 'D' THEN debitAmount ELSE creditAmount END AS amount"\
                                       ,"'O'                                       AS  dataType"\
                                       ,"'Zero Amount'                             AS  rule"\
                                       ,"'31-ZeroAmount'                           AS  bifurcationType"\
                                       ,"transactionLine                           AS  transactionLine"\
                                       ,"journalId                                 AS  journalId"\
                                       ,"transactionLineID                         AS relatedLineItemNumber"\
                                       ,"accountID                                 AS relatedAccount"\
                                       ,"debitCreditCode                           AS relatedLIType"\
                                       ,"0                                         AS ruleSequence"\
                                       ,"''                                        AS loopCount"\
                                       ,"''                                        AS direction"\
                                       ,"''                                        AS aggregatedInToLineItem"\
                                       ,"patternID                                 AS  patternID"\
                                       )
        
        fileName = gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta"
        objGenHelper.gen_writeToFile_perfom(df = fin_L1_STG_JEBifurcation_Link,filenamepath = fileName,mode="append")
        
        
    except Exception as err:
       raise


