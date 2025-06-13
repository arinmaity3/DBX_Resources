# Databricks notebook source
#gen.usp_L2_DIM_PostingKey_populate-->gen_L2_DIM_PostingKey_populate
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col,when,coalesce

def gen_L2_DIM_PostingKey_populate(): 
  try:
    
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global gen_L2_DIM_PostingKey
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()

  
    df_PostingKey = spark.createDataFrame(data = gl_lstOfPostingKeyCollect,schema = postingKeySchemaCollect).\
                         select(col("companyCode"),col("postingKey")).distinct()
 
    dr_wrw= Window.orderBy(lit('A'))

    new_column_languageCode = when(col("clientDataReportingLanguage").isNotNull(), col("clientDataReportingLanguage") ) \
                                    .otherwise(col("clientDataSystemReportingLanguage"))

    gen_L2_DIM_TMP_PostingKey =df_PostingKey.alias('posKey')\
                            .join(knw_LK_CD_ReportingSetup.alias('RptSetup'), on='companyCode', how='inner')\
                            .join(gen_L2_DIM_Organization.alias('org'), on='companyCode', how='inner')\
                            .select(col("org.organizationUnitSurrogateKey"),\
                                    col("posKey.postingKey"),\
                                    col("RptSetup.clientDataReportingLanguage"),\
                                    col("RptSetup.clientDataSystemReportingLanguage"),\
                                    lit("#NA#").alias("postingKeyDescription"),\
                                    lit("#NA#").alias("postingKeyKPMG"),\
                                    lit(new_column_languageCode).alias("languageCode"),\
                                   )
    gen_L2_DIM_TMP_PostingKey,status = gen_L2_DIM_PostingKey_AddedDetails(gen_L2_DIM_TMP_PostingKey,gl_CDMLayer1Path)
    
    gen_L2_DIM_PostingKey = gen_L2_DIM_TMP_PostingKey.withColumn("postingSurrogateKey", row_number().over(dr_wrw))

    gen_L2_DIM_PostingKey = objDataTransformation.gen_convertToCDMandCache \
        (gen_L2_DIM_PostingKey,'gen','L2_DIM_PostingKey',False)
    
    executionStatus = "L2_DIM_PostingKey populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

def gen_L2_DIM_PostingKey_AddedDetails(gen_L2_DIM_TMP_PostingKey,fileLocation):
  try:
    
    objGenHelper = gen_genericHelper()
    fileFullName=fileLocation+"gen_L1_MD_PostingKey.delta"
    
    if objGenHelper.file_exist(fileFullName) == True:
      gen_L1_MD_PostingKey = objGenHelper.gen_readFromFile_perform(fileFullName)
      gen_L2_DIM_TMP_PostingKey = gen_L2_DIM_TMP_PostingKey.alias('posKey')\
                       .join(gen_L1_MD_PostingKey.alias('pos1')\
                            ,((col('pos1.postingKey').eqNullSafe(col('posKey.postingKey')))\
                            &(col('posKey.clientDataReportingLanguage').eqNullSafe(col('pos1.languageCode')))),how='left')\
                       .join(gen_L1_MD_PostingKey.alias('pos2')\
                            ,((col('pos2.postingKey').eqNullSafe(col('posKey.postingKey')))\
                            &(col('posKey.clientDataSystemReportingLanguage').eqNullSafe(col('pos2.languageCode')))),how='left')\
                       .select(col("posKey.organizationUnitSurrogateKey").alias('organizationUnitSurrogateKey')\
                              ,col("posKey.postingKey").alias('postingKey')\
                              ,coalesce(col('pos1.postingKeyDescription'),col('pos2.postingKeyDescription')\
                                      ,lit('#NA#')).alias('postingKeyDescription')\
                              ,coalesce(col('pos1.postingKeyKPMG'),col('pos2.postingKeyKPMG')\
                                      ,lit('#NA#')).alias('postingKeyKPMG')\
                              ,coalesce(col('pos1.languageCode'),col('pos2.languageCode'),col('posKey.clientDataReportingLanguage')\
                                      ,col('posKey.clientDataSystemReportingLanguage'),lit('##')).alias('languageCode'))
          
      status="Successfully update L2 PostingKey additional details"
    else:
      status="The file L1 PostingKey does not exists in specified location: "+fileLocation
      gen_L2_DIM_TMP_PostingKey = gen_L2_DIM_TMP_PostingKey.select(col('organizationUnitSurrogateKey'),col('postingKey'),col('postingKeyDescription')\
                                              ,col('postingKeyKPMG'),col('languageCode'))
      
    return gen_L2_DIM_TMP_PostingKey,status
  except Exception as err:
    raise
