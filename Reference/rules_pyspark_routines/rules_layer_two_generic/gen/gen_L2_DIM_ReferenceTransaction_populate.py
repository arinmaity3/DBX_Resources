# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,coalesce

def gen_L2_DIM_ReferenceTransaction_populate():
    try:
      
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global gen_L2_DIM_ReferenceTransaction
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()

      L1_TMP_ReferenceTransaction = spark.createDataFrame(gl_lstOfReferenceTransactionCollect,referenceTransactionSchemaCollect).distinct()
      

      gen_L2_DIM_ReferenceTransaction = L1_TMP_ReferenceTransaction\
              .join(gen_L2_DIM_Organization,(L1_TMP_ReferenceTransaction.companyCode==gen_L2_DIM_Organization.companyCode)\
                    ,how='inner')\
              .join(knw_LK_CD_ReportingSetup,(knw_LK_CD_ReportingSetup.companyCode == gen_L2_DIM_Organization.companyCode)\
                    ,how='inner')\
              .select(gen_L2_DIM_Organization["organizationUnitSurrogateKey"]\
                      ,L1_TMP_ReferenceTransaction["referenceTransactionCode"].alias("referenceTransactionCode")\
                      ,lit("#NA#").alias("referenceTransactionDescription")\
                      ,coalesce(knw_LK_CD_ReportingSetup["clientDataReportingLanguage"]\
                                 ,knw_LK_CD_ReportingSetup["clientDataSystemReportingLanguage"]\
                      ,lit("NA")).alias("languageCode"),lit("").alias("phaseID")\
                      ,knw_LK_CD_ReportingSetup["clientDataReportingLanguage"].alias("clientDataReportingLanguage")\
                      ,knw_LK_CD_ReportingSetup["clientDataSystemReportingLanguage"].alias("clientDataSystemReportingLanguage"))

      w = Window().orderBy(lit('referenceTransactionSurrogateKey'))

      gen_L2_DIM_ReferenceTransaction,status = gen_L2_DIM_ReferenceTransaction_AddedDetails(gen_L2_DIM_ReferenceTransaction,gl_CDMLayer1Path)

      gen_L2_DIM_ReferenceTransaction = gen_L2_DIM_ReferenceTransaction.withColumn("referenceTransactionSurrogateKey", row_number().over(w))
      
      gen_L2_DIM_ReferenceTransaction = objDataTransformation.gen_convertToCDMandCache \
          (gen_L2_DIM_ReferenceTransaction,'gen','L2_DIM_ReferenceTransaction',True)
     
      executionStatus = "L2_DIM_ReferenceTransaction populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
      
    except Exception as e:
         executionStatus = objGenHelper.gen_exceptionDetails_log()       
         executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
         return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

def gen_L2_DIM_ReferenceTransaction_AddedDetails(gen_L2_DIM_ReferenceTransaction,fileLocation):
    try:
        objGenHelper = gen_genericHelper()
        
        fileFullName = fileLocation+"gen_L1_MD_ReferenceTransaction.delta"
        if objGenHelper.file_exist(fileFullName) == True:
            gen_L1_MD_ReferenceTransaction = objGenHelper.gen_readFromFile_perform(fileFullName)
            gen_L2_DIM_ReferenceTransaction = gen_L2_DIM_ReferenceTransaction.alias('rf1')\
                                      .join(gen_L1_MD_ReferenceTransaction.alias('rf2')\
                                             ,((col('rf2.referenceTransactionCode').eqNullSafe(col('rf1.referenceTransactionCode')))\
                                             &(col('rf2.languageCode').eqNullSafe(col('rf1.clientDataReportingLanguage'))))\
                                               ,how='left')\
                                      .join(gen_L1_MD_ReferenceTransaction.alias('rf3')\
                                             ,((col('rf3.referenceTransactionCode').eqNullSafe(col('rf1.referenceTransactionCode')))\
                                             &(col('rf3.languageCode').eqNullSafe(col('rf1.languageCode'))))\
                                               ,how='left')\
                                     .select(col('rf1.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
                                            ,col('rf1.referenceTransactionCode').alias('referenceTransactionCode')\
                                            ,coalesce(col('rf2.referenceTransactionDescription'),col('rf3.referenceTransactionDescription')\
                                                     ,lit('#NA#')).alias('referenceTransactionDescription')\
                                            ,coalesce(col('rf1.clientDataReportingLanguage'),col('rf1.clientDataSystemReportingLanguage')\
                                                     ,lit('NA')).alias('languageCode'))
      
      
            status="Successfully update L2 ReferenceTransaction additional details"   
      
        else:
            status="The file L1 ReferenceTransaction does not exists in specified location: "+fileLocation
            gen_L2_DIM_ReferenceTransaction = gen_L2_DIM_ReferenceTransaction.select(col('organizationUnitSurrogateKey')\
                                           ,col('referenceTransactionCode'),col('referenceTransactionDescription')\
                                           ,col('languageCode'),col('phaseID'))
        
        return gen_L2_DIM_ReferenceTransaction,status
    except Exception as err:
        raise
       


       
                                                                                                        
                                                                                                          

