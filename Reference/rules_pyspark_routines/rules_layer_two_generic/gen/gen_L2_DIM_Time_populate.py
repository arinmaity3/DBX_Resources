# Databricks notebook source
def gen_L2_DIM_Time_populate():
  try:
    
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global gen_L2_DIM_Time

      gen_L2_DIM_Time = objGenHelper.gen_readFromFile_perform(gl_knowledgePath  + 'gen_L2_DIM_Time.delta')
      gen_L2_DIM_Time.createOrReplaceTempView('gen_L2_DIM_Time')
      dwh_vw_DIM_Time = objDataTransformation.gen_convertToCDMStructure_generate(gen_L2_DIM_Time,\
          'dwh','vw_DIM_Time',isIncludeAnalysisID = True, isSqlFormat = True)[0]      
      objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_Time,gl_CDMLayer2Path +"gen_L2_DIM_Time.parquet")
                  
      executionStatus = "gen_L2_DIM_Time populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
  except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log() 
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)

        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
 
   
  
