# Databricks notebook source
def otc_L2_DIM_ClearingTypeChain_populate():
  try:
    
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)

      analysisid = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ANALYSISID'))
      global otc_L2_DIM_ClearingTypeChain
      w = Window().orderBy(lit('clearingTypeChainSurrogateKey'))

      knw_LK_GD_ClearingBoxLookupTypeChain = objGenHelper.gen_readFromFile_perform\
          (gl_knowledgePath  + 'knw_LK_GD_ClearingBoxLookupTypeChain.delta')

      otc_L2_DIM_ClearingTypeChain = knw_LK_GD_ClearingBoxLookupTypeChain.alias('cb')\
          .select(col('cb.typeGroup').alias('clearingTypeGroup')\
          ,col('cb.typeChain').alias('clearingTypeChain')\
          ,col('cb.reportCategory').alias('clearingTypeReportCategory')\
          ,col('cb.reportSubcategory').alias('clearingTypeReportSubCategory')\
          ,col('cb.typeDescription').alias('clearingTypeDescription')\
          ,col('cb.typeRule').alias('clearingTypeRule')\
          ,col('cb.typeText').alias('clearingTypeChainType')) \
          .withColumn("clearingTypeChainSurrogateKey", row_number().over(w))

      otc_L2_DIM_ClearingTypeChain  = objDataTransformation.gen_convertToCDMandCache\
          (otc_L2_DIM_ClearingTypeChain,'otc','L2_DIM_ClearingTypeChain',False)

      otc_vw_DIM_ClearingTypeChain = otc_L2_DIM_ClearingTypeChain.select( \
          'clearingTypeChainSurrogateKey' \
          ,expr('LEFT(clearingTypeGroup,11) as clearingTypeGroup') \
          ,expr('LEFT(clearingTypeChain,55) as clearingTypeChain') \
          ,expr('LEFT(clearingTypeReportCategory,50) as clearingTypeReportCategory') \
          ,expr('LEFT(clearingTypeReportSubCategory,50) as clearingTypeReportSubCategory') \
          ,expr('LEFT(clearingTypeDescription,108) as clearingTypeDescription') \
          ,expr('LEFT(clearingTypeRule,4) as clearingTypeRule') \
          ,expr('LEFT(clearingTypeChainType,95) as clearingTypeChainType'))
      
      otc_vw_DIM_ClearingTypeChain = objDataTransformation.gen_convertToCDMStructure_generate\
          (otc_vw_DIM_ClearingTypeChain,'otc','vw_DIM_ClearingTypeChain',isIncludeAnalysisID = True)[0]

      dummy_list = [[analysisid,0,'NONE','NONE','NONE','NONE','NONE','NONE','NONE']]
      dummy_df = spark.createDataFrame(dummy_list)
      otc_vw_DIM_ClearingTypeChain = otc_vw_DIM_ClearingTypeChain.union(dummy_df)

      objGenHelper.gen_writeToFile_perfom(otc_vw_DIM_ClearingTypeChain,\
          gl_CDMLayer2Path +"otc_L2_DIM_ClearingTypeChain.parquet")


      executionStatus = "otc_L2_DIM_ClearingTypeChain populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
  except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log() 
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)

        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
 
   
  

