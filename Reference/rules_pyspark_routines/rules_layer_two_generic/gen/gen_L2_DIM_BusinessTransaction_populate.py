# Databricks notebook source
from pyspark.sql.functions import row_number,coalesce
import sys
import traceback
def gen_L2_DIM_BusinessTransaction_populate():
      """Populate L2_DIM_BusinessTransaction"""
      try:
          
          logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
          global gen_L2_DIM_BusinessTransaction
          w = Window().orderBy(lit('organizationUnitSurrogateKey'))
          objGenHelper = gen_genericHelper()
          objDataTransformation = gen_dataTransformation()

          gen_L1_TMP_BusinessTransaction = spark.createDataFrame(gl_lstOfBusinessTransactionCollect\
                                       ,businessTransactionSchemaCollect)\
                                       .select(col("companyCode"),col("businessTransactionCode")).distinct()
          gen_L1_TMP_BusinessTransaction.createOrReplaceTempView("gen_L1_TMP_BusinessTransaction")

          L2_DIM_TMP_BusinessTransaction=spark.sql("select "\
                       " org2.organizationUnitSurrogateKey"\
                        ",btr.businessTransactionCode"\
                       ",rpt.clientDataReportingLanguage"\
				       ",rpt.clientDataSystemReportingLanguage"\
				       ",rpt.KPMGDataReportingLanguage"\
                       " from gen_L1_TMP_BusinessTransaction btr"\
                       " INNER JOIN gen_L2_DIM_Organization org2 ON btr.companyCode = org2.companyCode"\
                       " INNER JOIN knw_LK_CD_ReportingSetup rpt ON rpt.companyCode = org2.companyCode")
          
          L2_DIM_TMP_BusinessTransaction=L2_DIM_TMP_BusinessTransaction.withColumn("businessTransactionDescription",lit("#NA#"))
          L2_DIM_TMP_BusinessTransaction=L2_DIM_TMP_BusinessTransaction.withColumn("languageCode",lit("NA"))

          L2_DIM_TMP_BusinessTransaction,status = gen_L2_DIM_BusinessTransaction_AddedDetails(L2_DIM_TMP_BusinessTransaction,gl_CDMLayer1Path)
          
          gen_L2_DIM_BusinessTransaction = L2_DIM_TMP_BusinessTransaction.withColumn("businessTransactionSurrogateKey", row_number().over(w))
          
          gen_L2_DIM_BusinessTransaction = objDataTransformation.gen_convertToCDMandCache \
              (gen_L2_DIM_BusinessTransaction,'gen','L2_DIM_BusinessTransaction',False)
        
          executionStatus = "L2_DIM_BusinessTransaction populated sucessfully"
          executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
          return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

      except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

def gen_L2_DIM_BusinessTransaction_AddedDetails(L2_DIM_TMP_BusinessTransaction,fileLocation):
  try:
      objGenHelper = gen_genericHelper()
      fileFullName=fileLocation+"gen_L1_MD_BusinessTransaction.delta"
      
      if objGenHelper.file_exist(fileFullName) == True:

          gen_L1_MD_BusinessTransaction = objGenHelper.gen_readFromFile_perform(fileFullName)
          L2_DIM_TMP_BusinessTransaction = L2_DIM_TMP_BusinessTransaction.alias('df2')\
                       .join(gen_L1_MD_BusinessTransaction.alias('btr1')\
                              ,(((col('df2.businessTransactionCode')).eqNullSafe(col('btr1.businessTransactionCode')))\
                               &((col('df2.clientDataReportingLanguage')).eqNullSafe(col('btr1.languageCode')))),how='left')\
                       .join(gen_L1_MD_BusinessTransaction.alias('btr2')\
                              ,(((col('df2.businessTransactionCode')).eqNullSafe(col('btr2.businessTransactionCode')))\
                               &((col('df2.clientDataSystemReportingLanguage')).eqNullSafe(col('btr2.languageCode')))),how='left')\
                       .join(gen_L1_MD_BusinessTransaction.alias('btr3')\
                              ,(((col('df2.businessTransactionCode')).eqNullSafe(col('btr3.businessTransactionCode')))\
                               &((col('df2.KPMGDataReportingLanguage')).eqNullSafe(col('btr3.languageCode')))),how='left')\
                       .select(col('df2.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
                             ,col('df2.businessTransactionCode').alias('businessTransactionCode')\
                             ,coalesce(col('btr1.businessTransactionDescription'),col('btr2.businessTransactionDescription')\
                                      ,col('btr3.businessTransactionDescription'),lit('#NA#')).alias('businessTransactionDescription')\
                             ,coalesce(col('btr1.languageCode'),col('btr2.languageCode')\
                                      ,col('btr3.languageCode'),lit('NA')).alias('languageCode'))
                       
          status="Successfully update L2 BusinessTransaction additional details"
      else:
          L2_DIM_TMP_BusinessTransaction = L2_DIM_TMP_BusinessTransaction.select(col('organizationUnitSurrogateKey')\
                                         ,col('businessTransactionCode'),col('businessTransactionDescription')\
                                         ,col('languageCode'))
      
          status="The file L1 BusinessTransaction does not exists in specified location: "+fileLocation
      return L2_DIM_TMP_BusinessTransaction,status
  except Exception as err:
    raise
    
