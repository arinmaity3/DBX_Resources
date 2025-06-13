# Databricks notebook source
import sys
import traceback

def knw_LK_GD_MaterialityKnowledge_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)         

    knw_L2_FACT_MaterialityRange = objGenHelper\
        .gen_readFromFile_perform(gl_knowledgePath  + 'knw_L2_FACT_MaterialityRange.parquet')
    
    dwh_vw_FACT_MaterialityRange = objDataTransformation\
        .gen_convertToCDMStructure_generate(knw_L2_FACT_MaterialityRange\
                                  ,'dwh','vw_FACT_MaterialityRange',isIncludeAnalysisID = True)[0]
    
    objGenHelper.gen_writeToFile_perfom(dwh_vw_FACT_MaterialityRange\
                                ,gl_CDMLayer2Path +"knw_L2_FACT_MaterialityRange.parquet")

    knw_L2_DIM_MaterialityMetricType = objGenHelper\
        .gen_readFromFile_perform(gl_knowledgePath  + 'knw_L2_DIM_MaterialityMetricType.parquet')
      
    dwh_vw_DIM_MaterialityMetricType = objDataTransformation\
        .gen_convertToCDMStructure_generate(knw_L2_DIM_MaterialityMetricType\
                                  ,'dwh','vw_DIM_MaterialityMetricType',isIncludeAnalysisID = True)[0]
    
    objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_MaterialityMetricType\
                                ,gl_CDMLayer2Path +"knw_L2_DIM_MaterialityMetricType.parquet")

    knw_L2_DIM_MaterialityMetric = objGenHelper\
        .gen_readFromFile_perform(gl_knowledgePath  + 'knw_L2_DIM_MaterialityMetric.parquet')
    
    dwh_vw_DIM_MaterialityMetric = objDataTransformation\
        .gen_convertToCDMStructure_generate(knw_L2_DIM_MaterialityMetric\
                                  ,'dwh','vw_DIM_MaterialityMetric',isIncludeAnalysisID = True)[0]
    
    objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_MaterialityMetric\
                                ,gl_CDMLayer2Path +"knw_L2_DIM_MaterialityMetric.parquet")

    knw_L2_FACT_MaterialityMBTScenarioBenchmark = objGenHelper\
        .gen_readFromFile_perform(gl_knowledgePath  + 'knw_L2_FACT_MaterialityMBTScenarioBenchmark.parquet')
    
    dwh_vw_FACT_MaterialityMBTScenarioBenchmark = objDataTransformation\
        .gen_convertToCDMStructure_generate(knw_L2_FACT_MaterialityMBTScenarioBenchmark\
                                  ,'dwh','vw_FACT_MaterialityMBTScenarioBenchmark',isIncludeAnalysisID = True)[0]
    
    objGenHelper.gen_writeToFile_perfom(dwh_vw_FACT_MaterialityMBTScenarioBenchmark\
                                ,gl_CDMLayer2Path +"knw_L2_FACT_MaterialityMBTScenarioBenchmark.parquet")

    knw_L2_DIM_MaterialityMBTScenario = objGenHelper\
        .gen_readFromFile_perform(gl_knowledgePath  + 'knw_L2_DIM_MaterialityMBTScenario.parquet')
    
    dwh_vw_DIM_MaterialityMBTScenario = objDataTransformation\
        .gen_convertToCDMStructure_generate(knw_L2_DIM_MaterialityMBTScenario\
                                  ,'dwh','vw_DIM_MaterialityMBTScenario',isIncludeAnalysisID = True)[0]
    
    objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_MaterialityMBTScenario\
                                ,gl_CDMLayer2Path +"knw_L2_DIM_MaterialityMBTScenario.parquet")

    knw_L2_FACT_MaterialityMBTIndustryBenchmark = objGenHelper\
          .gen_readFromFile_perform(gl_knowledgePath  + 'knw_L2_FACT_MaterialityMBTIndustryBenchmark.parquet')
    
    dwh_vw_FACT_MaterialityMBTScenarioBenchmark = objDataTransformation\
          .gen_convertToCDMStructure_generate(knw_L2_FACT_MaterialityMBTIndustryBenchmark\
                                    ,'dwh','vw_FACT_MaterialityMBTIndustryBenchmark',isIncludeAnalysisID = True)[0]
    
    objGenHelper.gen_writeToFile_perfom(dwh_vw_FACT_MaterialityMBTScenarioBenchmark\
                                ,gl_CDMLayer2Path +"knw_L2_FACT_MaterialityMBTIndustryBenchmark.parquet")

    knw_L2_DIM_MaterialityMBTIndustry = objGenHelper\
          .gen_readFromFile_perform(gl_knowledgePath  + 'knw_L2_DIM_MaterialityMBTIndustry.parquet')
    
    dwh_vw_DIM_MaterialityMBTIndustry = objDataTransformation\
          .gen_convertToCDMStructure_generate(knw_L2_DIM_MaterialityMBTIndustry\
                                    ,'dwh','vw_DIM_MaterialityMBTIndustry',isIncludeAnalysisID = True)[0]
    
    objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_MaterialityMBTIndustry\
                                ,gl_CDMLayer2Path +"knw_L2_DIM_MaterialityMBTIndustry.parquet")

    knw_L2_DIM_MaterialityEntityType = objGenHelper\
          .gen_readFromFile_perform(gl_knowledgePath  + 'knw_L2_DIM_MaterialityEntityType.parquet')
    
    dwh_vw_DIM_MaterialityEntityType = objDataTransformation\
          .gen_convertToCDMStructure_generate(knw_L2_DIM_MaterialityEntityType\
                                    ,'dwh','vw_DIM_MaterialityEntityType',isIncludeAnalysisID = True)[0]
    
    objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_MaterialityEntityType\
                                ,gl_CDMLayer2Path +"knw_L2_DIM_MaterialityEntityType.parquet")

    knw_L2_DIM_MaterialityBenchmarkType = objGenHelper\
          .gen_readFromFile_perform(gl_knowledgePath  + 'knw_L2_DIM_MaterialityBenchmarkType.parquet')
    
    dwh_vw_DIM_MaterialityBenchmarkType = objDataTransformation\
          .gen_convertToCDMStructure_generate(knw_L2_DIM_MaterialityBenchmarkType\
                                    ,'dwh','vw_DIM_MaterialityBenchmarkType',isIncludeAnalysisID = True)[0]
    
    objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_MaterialityBenchmarkType\
                                ,gl_CDMLayer2Path +"knw_L2_DIM_MaterialityBenchmarkType.parquet")

    executionStatus = "knw_LK_GD_MaterialityKnowledge_populate populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)      
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:  
    executionStatus =  objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)       
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
