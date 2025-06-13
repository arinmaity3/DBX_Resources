# Databricks notebook source
import sys
import traceback
def knw_LK_GD_DAFlagFiles_populate(processID=PROCESS_ID.L1_TRANSFORMATION):
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()

        if (processID==PROCESS_ID.TRANSFORMATION_VALIDATION):
            validationID=VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION
        else:
            validationID=-1
        logID = executionLog.init(processID,None,None,None,validationID)

        df_knw_LK_GD_DAFlag = objGenHelper.gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_DAFlag.delta")
        df_knw_LK_GD_DAFlagKnowledgeAccount = objGenHelper.gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_DAFlagKnowledgeAccount.delta")
        df_knw_LK_GD_DAFlagKnowledgeAccountMapping = objGenHelper.\
                              gen_readFromFile_perform(gl_knowledgePath + "knw_LK_GD_DAFlagKnowledgeAccountMapping.delta")

        if(objGenHelper.file_exist(gl_commonParameterPath + "knw_LK_GD_DAFlag.csv") == False):
            objGenHelper.gen_writeSingleCsvFile_perform(df = df_knw_LK_GD_DAFlag,\
                                                        targetFile = gl_commonParameterPath + "knw_LK_GD_DAFlag.csv")

        if(objGenHelper.file_exist(gl_commonParameterPath + "knw_LK_GD_DAFlagKnowledgeAccount.csv") == False):
            objGenHelper.gen_writeSingleCsvFile_perform(df = df_knw_LK_GD_DAFlagKnowledgeAccount,
                                                        targetFile = gl_commonParameterPath + "knw_LK_GD_DAFlagKnowledgeAccount.csv")

        if(objGenHelper.file_exist(gl_commonParameterPath + "knw_LK_GD_DAFlagKnowledgeAccountMapping.csv") == False):
            objGenHelper.gen_writeSingleCsvFile_perform(df = df_knw_LK_GD_DAFlagKnowledgeAccountMapping,\
                                                        targetFile = gl_commonParameterPath + "knw_LK_GD_DAFlagKnowledgeAccountMapping.csv")


        executionStatus = "LK_GD_DAFlagFiles populated successfully."
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
