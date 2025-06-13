# Databricks notebook source
import sys
import traceback

def knw_LK_GD_DAFlagKnowledgeAccountMappingDetail_populate():
  """ Populate global dataframe and SQL temp view knw_LK_GD_DAFlagKnowledgeAccountMappingDetail"""
  try:

    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global knw_LK_GD_DAFlagKnowledgeAccountMappingDetail

    spark.sql("UNCACHE TABLE  IF EXISTS knw_LK_GD_DAFlag")
    spark.sql("UNCACHE TABLE  IF EXISTS knw_LK_GD_DAFlagKnowledgeAccount")
    spark.sql("UNCACHE TABLE  IF EXISTS knw_LK_GD_DAFlagKnowledgeAccountMapping")
    
    knw_LK_GD_DAFlag=objGenHelper.gen_readFromFile_perform(gl_commonParameterPath+"knw_LK_GD_DAFlag.csv")
    knw_LK_GD_DAFlagKnowledgeAccount=objGenHelper.gen_readFromFile_perform(gl_commonParameterPath+"knw_LK_GD_DAFlagKnowledgeAccount.csv")
    knw_LK_GD_DAFlagKnowledgeAccountMapping=objGenHelper.gen_readFromFile_perform(gl_commonParameterPath+"knw_LK_GD_DAFlagKnowledgeAccountMapping.csv")
    
    
    knw_LK_GD_DAFlag.createOrReplaceTempView("knw_LK_GD_DAFlag")
    knw_LK_GD_DAFlagKnowledgeAccount.createOrReplaceTempView("knw_LK_GD_DAFlagKnowledgeAccount")
    knw_LK_GD_DAFlagKnowledgeAccountMapping.createOrReplaceTempView("knw_LK_GD_DAFlagKnowledgeAccountMapping")
    
    knw_LK_GD_DAFlagKnowledgeAccountMappingDetail = spark.sql("SELECT DISTINCT \
                     daflg.flagName \
                    ,dakac.knowledgeID \
                    ,dakac.knowledgeAccountName \
                    ,dakac.accountCategory \
                    ,dakac.accountSubcategory \
            FROM knw_LK_GD_DAFlagKnowledgeAccountMapping flgmap \
            INNER JOIN knw_LK_GD_DAFlag daflg  \
            ON flgmap.flagID=daflg.flagID \
            INNER JOIN knw_LK_GD_DAFlagKnowledgeAccount dakac  \
            ON flgmap.knowledgeID = dakac.knowledgeID")

    knw_LK_GD_DAFlagKnowledgeAccountMappingDetail = objDataTransformation.gen_convertToCDMandCache \
        (knw_LK_GD_DAFlagKnowledgeAccountMappingDetail,'knw','LK_GD_DAFlagKnowledgeAccountMappingDetail',False)

    executionStatus = "knw_LK_GD_DAFlagKnowledgeAccountMappingDetail populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)

    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)

    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

