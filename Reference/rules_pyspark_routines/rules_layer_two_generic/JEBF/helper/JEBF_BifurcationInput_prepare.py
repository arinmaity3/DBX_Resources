# Databricks notebook source
import sys
import traceback
def JEBF_BifurcationInput_prepare():
      try:
        objGenHelper = gen_genericHelper()
        global fin_L1_TMP_JEBifurcation
        objGenHelper.gen_Directory_clear(gl_bifurcationPath)
        fin_L1_TMP_JEBifurcation_prebifurcation = objGenHelper.gen_readFromFile_perform(\
                                              gl_preBifurcationPath+"fin_L1_TMP_JEBifurcation.delta")
        fin_L1_STG_JEBifurcation_Link_prebifurcation=objGenHelper.gen_readFromFile_perform(\
                                              gl_preBifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta")
        objGenHelper.gen_writeToFile_perfom(df = fin_L1_STG_JEBifurcation_Link_prebifurcation\
            ,filenamepath=gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta")
        #Write the prebifurcation output as input into Link
        fin_L1_STG_JEBifurcation_Link = fin_L1_TMP_JEBifurcation_prebifurcation.selectExpr(
                     "journalSurrogateKey                       AS  journalSurrogateKey"\
                    ,"transactionIDbyPrimaryKey                 AS  transactionIDbyPrimaryKey"\
                    ,"transactionID                             AS  transactionID"\
                    ,"lineItem                                  AS  lineItem"\
                    ,"transactionLineID                         AS  lineItemNumber"\
                    ,"accountID                                 AS  fixedAccount"\
                    ,"debitCreditCode                           AS  fixedLIType"\
                    ,"CASE WHEN debitCreditCode = 'D' THEN debitAmount ELSE creditAmount END AS amount"\
                    ,"'I'                                       AS  dataType"\
                    ,"''                                        AS  rule"\
                    ,"''                                        AS  bifurcationType"\
                    ,"transactionLine                           AS  transactionLine"\
                    ,"journalId                                 AS  journalId"\
                    ,"''                                        AS relatedLineItemNumber"\
                    ,"''                                        AS relatedAccount"\
                    ,"''                                        AS relatedLIType"\
                    ,"0                                         AS ruleSequence"\
                    ,"''                                        AS loopCount"\
                    ,"''                                        AS direction"\
                    ,"''                                        AS aggregatedInToLineItem"\
                    ,"patternID                                 AS  patternID"\
                    ,"patternSource                             AS  patternSource"\
                    ,"CASE WHEN blockSource IS NULL THEN '0_No_Block' ELSE blockSource END	AS blockSource"
                    )
        
        fileName = gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta"
        objGenHelper.gen_writeToFile_perfom(df = fin_L1_STG_JEBifurcation_Link,filenamepath = fileName ,mode = "append")
        #Write the bifurcation input after partitioning by patternID
        
        fileName=gl_bifurcationPath+"fin_L1_TMP_JEBifurcation.delta"
        objGenHelper.gen_writeToFile_perfom(df = fin_L1_TMP_JEBifurcation_prebifurcation,filenamepath=fileName,\
                                   isCreatePartitions = True,partitionColumns = "patternID",isOverWriteSchema = True)
        fin_L1_TMP_JEBifurcation=objGenHelper.gen_readFromFile_perform(gl_bifurcationPath+"fin_L1_TMP_JEBifurcation.delta")
        return
      except Exception as err:
        raise
