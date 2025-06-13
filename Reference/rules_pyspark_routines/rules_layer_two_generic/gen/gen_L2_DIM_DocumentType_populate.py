# Databricks notebook source
def gen_L2_DIM_DocumentType_populate():
  try:
    
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      erpSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID')
      analysisid = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ANALYSISID'))
      global gen_L2_DIM_DocumentType

      knw_LK_GD_ClientKPMGDocumentType = objGenHelper.gen_readFromFile_perform\
          (gl_knowledgePath  + 'knw_LK_GD_ClientKPMGDocumentType.delta')

      documentTypeKPMGDescription = when((col('dt.documentTypeKPMGDescription').isNull() | (col('dt.documentTypeKPMGDescription') == lit(''))), lit('#NA#') ) \
                              .otherwise(col('dt.documentTypeKPMGDescription'))

      documentTypeKPMGShortDescription = when((col('dt.documentTypeKPMGShortDescription').isNull() | (col('dt.documentTypeKPMGShortDescription') == lit(''))), lit('#NA#') ) \
                              .otherwise(col('dt.documentTypeKPMGShortDescription'))

      documentTypeKPMGGroup = when((col('dt.documentTypeKPMGGroup').isNull() | (col('dt.documentTypeKPMGGroup') == lit(''))), lit('#NA#') ) \
                              .otherwise(col('dt.documentTypeKPMGGroup'))

      gen_L2_DIM_DocumentType = knw_LK_GD_ClientKPMGDocumentType.alias('dt')\
          .filter(col('dt.ERPSystemID') == lit(erpSystemID))\
          .select(col('dt.documentTypeClientID').alias('documentTypeSurrogateKey')\
          ,col('dt.documentTypeClientProcessID').alias('documentTypeBusinessProcessID')\
          ,col('dt.documentTypeClient').alias('documentTypeClient')\
          ,col('dt.documentTypeClientDescription').alias('documentTypeClientDescription')\
          ,col('dt.documentTypeKPMGOrder').alias('documentTypeKPMGOrder')\
          ,col('dt.documentTypeKPMG').alias('documentType')\
          ,lit(documentTypeKPMGDescription).alias('documentTypeDescription')\
          ,col('dt.documentTypeKPMGOrderOTC').alias('documentTypeOrderOTC')\
          ,col('dt.documentTypeKPMGOrderPTP').alias('documentTypeOrderPTP')\
          ,col('dt.documentTypeKPMGShort').alias('documentTypeShort')\
          ,lit(documentTypeKPMGShortDescription).alias('documentTypeShortDescription')\
          ,col('dt.documentTypeKPMGShortOrderOTC').alias('documentTypeShortOrderOTC')\
          ,col('dt.documentTypeKPMGShortOrderPTP').alias('documentTypeShortOrderPTP')\
          ,col('dt.documentTypeKPMGGroup').alias('documentTypeGroup')\
          ,lit(documentTypeKPMGGroup).alias('documentTypeGroupDescription')\
          ,col('dt.documentTypeKPMGGroupOrder').alias('documentTypeGroupOrder')\
          ,col('dt.documentTypeKPMGInterpreted').alias('documentTypeKPMGInterpreted')\
          ,col('dt.interpretedDocumentTypeClientID').alias('interpretedDocumentTypeClientID')\
          ,col('dt.isReversed').alias('isReversed')\
          ,col('dt.KPMGInterpretedCalculationRule').alias('KPMGInterpretedCalculationRule')\
          ,col('dt.isSoDRelevant').alias('isSoDRelevant'))

      gen_L2_DIM_DocumentType  = objDataTransformation.gen_convertToCDMandCache\
          (gen_L2_DIM_DocumentType,'gen','L2_DIM_DocumentType',False)

      isSoDRelevant = when(((col('dt.isSoDRelevant')== lit(1)) & (~col('dt.documentTypeSurrogateKey').isin(87,26))) \
                           ,lit('Is Document SoD Relevant - Yes') ) \
                           .otherwise(lit('Is Document SoD Relevant - No'))

      gen_vw_DIM_DocumentType = gen_L2_DIM_DocumentType.alias('dt') \
          .select(col('dt.documentTypeSurrogateKey').alias('documentTypeSurrogateKey') \
          ,col('dt.documentTypeBusinessProcessID').alias('documentTypeBusinessProcessID') \
          ,col('dt.documentTypeClient').alias('documentTypeClient') \
          ,col('dt.documentTypeClientDescription').alias('documentTypeClientDescription') \
          ,concat(col('dt.documentTypeClient')+lit(' (')+col('dt.documentTypeClientDescription')+lit(')')).alias('documentTypeClientDesc') \
          ,when(col('dt.documentTypeKPMGOrder').isNull(),lit(0)).otherwise(col('dt.documentTypeKPMGOrder')).alias('documentTypeKPMGOrder') \
          ,col('dt.documentType').alias('documentType') \
          ,when(col('dt.documentTypeOrderOTC').isNull(),lit(0)).otherwise(col('dt.documentTypeOrderOTC')).alias('documentTypeOrderOTC') \
          ,when(col('dt.documentTypeOrderPTP').isNull(),lit(0)).otherwise(col('dt.documentTypeOrderPTP')).alias('documentTypeOrderPTP') \
          ,col('dt.documentTypeGroup').alias('documentTypeGroup') \
          ,col('dt.documentTypeShort').alias('documentTypeShort') \
          ,col('dt.documentTypeKPMGInterpreted').alias('documentTypeKPMGInterpreted') \
          ,col('dt.interpretedDocumentTypeClientID').alias('interpretedDocumentTypeClientID') \
          ,col('dt.isReversed').alias('isReversed') \
          ,col('dt.KPMGInterpretedCalculationRule').alias('KPMGInterpretedCalculationRule') \
          ,lit(isSoDRelevant).alias('isSoDRelevant'))
      
      gen_vw_DIM_DocumentType = objDataTransformation.gen_convertToCDMStructure_generate\
          (gen_vw_DIM_DocumentType,'gen','vw_DIM_DocumentType',isIncludeAnalysisID = True)[0]

      dummy_list = [[analysisid,0,0,'NONE','NONE','NONE',0,'NONE',0,0,'NONE','NONE','NONE',0,False,0,'Is Document SoD Relevant - No']]
      dummy_df = spark.createDataFrame(dummy_list)
      gen_vw_DIM_DocumentType = gen_vw_DIM_DocumentType.union(dummy_df)

      objGenHelper.gen_writeToFile_perfom(gen_vw_DIM_DocumentType,\
          gl_CDMLayer2Path +"gen_L2_DIM_DocumentType.parquet")


      executionStatus = "gen_L2_DIM_DocumentType populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
  except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log() 
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)

        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
 
   
  
