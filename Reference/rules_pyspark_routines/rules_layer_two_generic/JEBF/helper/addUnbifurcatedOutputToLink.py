# Databricks notebook source
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import  col

def addUnbifurcatedOutputToLink():

    try:
        objGenHelper = gen_genericHelper() 
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        objGenHelper.gen_Directory_clear(gl_postBifurcationPath)
        fin_L1_STG_JEBifurcation_00_Initialize = spark.read.format("delta")\
                                       .load(gl_preBifurcationPath+"fin_L1_STG_JEBifurcation_00_Initialize.delta")
        fin_L1_STG_JEBifurcation_Link=objGenHelper.gen_readFromFile_perform\
                                           (gl_bifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta")
        df_journal=fin_L1_STG_JEBifurcation_00_Initialize.select("journalSurrogateKey","companyCode")
        fin_L1_STG_JEBifurcation_Link_with_CC=fin_L1_STG_JEBifurcation_Link.alias("link")\
                                              .join(df_journal.alias("jrnl")\
                                                   ,expr("link.journalSurrogateKey=jrnl.journalSurrogateKey"))\
                                              .selectExpr("link.*","jrnl.companyCode")
        objGenHelper.gen_writeToFile_perfom(df = fin_L1_STG_JEBifurcation_Link_with_CC\
                                            ,filenamepath = gl_postBifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta")
        fin_L1_STG_JEBifurcation_Link_output =fin_L1_STG_JEBifurcation_Link\
                                          .filter("dataType=='O'")\
                                          .selectExpr("journalSurrogateKey"\
                                                      ,"transactionID"\
                                                      ,"transactionLine")
        fin_L1_STG_JEBifurcation_Link_input=fin_L1_STG_JEBifurcation_Link\
                                          .filter("dataType=='I'")
        BifOut=fin_L1_STG_JEBifurcation_Link_output.selectExpr("journalSurrogateKey")
        objGenHelper.gen_writeToFile_perfom(df = BifOut,filenamepath = gl_postBifurcationPath+"BifLines.delta")
        AggBifOut=fin_L1_STG_JEBifurcation_Link_output.alias("out")\
              .join(fin_L1_STG_JEBifurcation_Link_input.alias("inp")\
                    ,expr("out.transactionID=inp.transactionID and \
                         out.transactionLine=inp.aggregatedIntoLine"))\
              .selectExpr("inp.journalSurrogateKey")
        objGenHelper.gen_writeToFile_perfom(df = AggBifOut,filenamepath = gl_postBifurcationPath+"BifLines.delta",mode="append")
        BifLines = objGenHelper.gen_readFromFile_perform(gl_postBifurcationPath+"BifLines.delta")
        UnBifLines=fin_L1_STG_JEBifurcation_Link_input.selectExpr("journalSurrogateKey").subtract(BifLines.select("journalSurrogateKey"))
        objGenHelper.gen_writeToFile_perfom(df = UnBifLines,filenamepath = gl_postBifurcationPath+"UnBifLines.delta")
        UnBifLines = objGenHelper.gen_readFromFile_perform(gl_postBifurcationPath+"UnBifLines.delta")
        fin_L1_STG_JEBifurcation_LinkToAppend = \
        fin_L1_STG_JEBifurcation_Link_input.alias('li')\
            .join(UnBifLines.alias("unb")\
                    ,expr("li.journalSurrogateKey=unb.journalSurrogateKey"))\
            .join(df_journal.alias("jrnl")\
                    ,expr("li.journalSurrogateKey=jrnl.journalSurrogateKey"))\
            .selectExpr("li.journalSurrogateKey AS  journalSurrogateKey"\
                        ,"li.transactionIDbyPrimaryKey AS  transactionIDbyPrimaryKey"\
                        ,"li.transactionID AS transactionID"\
                        ,"li.lineItem AS	lineItem"\
                        ,"li.lineItemNumber AS lineItemNumber"\
                        ,"li.fixedAccount AS  fixedAccount"\
                        ,"li.fixedLIType	AS  fixedLIType"\
                        ,"li.amount AS amount"\
                        ,"'O' AS  dataType"\
                        ,"'unbifurcated' AS  rule"\
                        ,"'99-unbifurcated' AS  bifurcationType"\
                        ,"li.transactionLine"\
                        ,"li.journalId AS  journalId"\
                        ,"'' AS relatedLineItemNumber"\
                        ,"'#NA#' AS relatedAccount"\
                        ,"'' AS relatedLIType"\
                        ,"0 AS ruleSequence"\
                        ,"'' AS loopCount"\
                        ,"'' AS direction"\
                        ,"'' AS aggregatedIntoLineItem"\
                        ,"NULL as aggregatedIntoLine"\
                        ,"li.patternID	AS patternID"\
                        ,"NULL as patternSource"\
                        ,"NULL as blockSource"\
                        ,"jrnl.companyCode as companyCode")
        objGenHelper.gen_writeToFile_perfom(df = fin_L1_STG_JEBifurcation_LinkToAppend,\
                                  filenamepath = gl_postBifurcationPath+"fin_L1_STG_JEBifurcation_Link.delta" \
                                        ,mode = "append")
        
        executionStatus = "addUnbifurcatedOutputToLink completed successfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
