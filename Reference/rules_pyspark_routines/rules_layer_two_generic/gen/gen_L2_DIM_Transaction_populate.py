# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,coalesce

def gen_L2_DIM_Transaction_populate():
    try:
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global gen_L2_DIM_Transaction
      
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      w = Window().orderBy(lit(''))
      gen_L1_TMP_Transaction = spark.createDataFrame(gl_lstOfTransactionCollect,transactionSchemaCollect).\
                               select(col("companyCode"),col("transactionType")).distinct()
          
      gen_L2_DIM_TMP_Transaction = gen_L1_TMP_Transaction.alias("trans")\
                              .join(gen_L2_DIM_Organization.alias("org")\
                                    ,(col("trans.companyCode")    == col("org.companyCode")),"inner")\
                             .select(col("org.organizationUnitSurrogateKey").alias("organizationUnitSurrogateKey")\
                             ,col("trans.transactionType").alias("transactionType")\
                             ,col("org.companyCode").alias("companyCode")\
                             ,lit("#NA#").alias("transactionTypeDescription"))

      gen_L2_DIM_TMP_Transaction,status = gen_L2_DIM_Transaction_AddedDetails(gen_L2_DIM_TMP_Transaction,gl_CDMLayer1Path)
      
      gen_L2_DIM_Transaction = gen_L2_DIM_TMP_Transaction.withColumn("transactionTypeSurrogateKey", row_number().over(w))
                       
      gen_L2_DIM_Transaction  = objDataTransformation.gen_convertToCDMandCache \
          (gen_L2_DIM_Transaction,'gen','L2_DIM_Transaction',True)
      
      executionStatus = "gen_L2_DIM_Transaction populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]     
    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
      
def gen_L2_DIM_Transaction_AddedDetails(gen_L2_DIM_TMP_Transaction,fileLocation):
    try:
        objGenHelper = gen_genericHelper()

        fileFullName = fileLocation+"gen_L1_MD_Transaction.delta"
        if objGenHelper.file_exist(fileFullName) == True:
            gen_L1_MD_Transaction = objGenHelper.gen_readFromFile_perform(fileFullName)
            gen_L2_DIM_TMP_Transaction = gen_L2_DIM_TMP_Transaction.alias('tr')\
	                  .join(knw_LK_CD_ReportingSetup.alias('rpt'),(col('rpt.companyCode')==(col('tr.companyCode'))),how='inner')\
		              .join(gen_L1_MD_Transaction.alias('trn1')\
		                   ,((col('trn1.transactionType').eqNullSafe(col('tr.transactionType')))\
			               &(col('rpt.clientDataReportingLanguage').eqNullSafe(col('trn1.languageCode')))),how='left')\
		              .join(gen_L1_MD_Transaction.alias('trn2')\
		                   ,((col('trn2.transactionType').eqNullSafe(col('tr.transactionType')))\
			                &(col('rpt.clientDataSystemReportingLanguage').eqNullSafe(col('trn2.languageCode')))),how='left')\
		              .select(col("tr.organizationUnitSurrogateKey").alias("organizationUnitSurrogateKey")\
                           ,col("tr.transactionType").alias("transactionType")\
                           ,coalesce(col('trn1.transactionTypeDescription'),col('trn2.transactionTypeDescription')\
			                     ,lit('#NA#')).alias('transactionTypeDescription'))
                      
            status="Successfully update L2 Transaction additional details"
        else:
            gen_L2_DIM_TMP_Transaction = gen_L2_DIM_TMP_Transaction.select(col('organizationUnitSurrogateKey'),col('transactionType')\
                                               ,col('transactionTypeDescription'))
            
            status="The file L1 Transaction does not exists in specified location: "+fileLocation
        return gen_L2_DIM_TMP_Transaction,status
    except Exception as err:
        raise

#gen_L2_DIM_Transaction_populate()
      




