# Databricks notebook source
def fin_L2_DIM_BenfordAnalysis_populate():
    try:
      objGenHelper = gen_genericHelper()  
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)         
       
      fin_L2_DIM_BenfordAnalysis = objGenHelper.gen_readFromFile_perform(gl_knowledgePath  + 'fin_L2_DIM_BenfordAnalysis.parquet')

      dwh_vw_DIM_BenfordAnalysis = objDataTransformation.gen_convertToCDMStructure_generate(\
                fin_L2_DIM_BenfordAnalysis,'dwh','vw_DIM_BenfordAnalysis',isIncludeAnalysisID = True)[0]      
      objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_BenfordAnalysis,\
                gl_CDMLayer2Path +"fin_L2_DIM_BenfordAnalysis.parquet")

      executionStatus = "fin_L2_DIM_BenfordAnalysis populated sucessfully."
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)      
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as err:  
      
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)       
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
 
 
