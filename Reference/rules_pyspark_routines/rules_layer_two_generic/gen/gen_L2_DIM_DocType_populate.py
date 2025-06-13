# Databricks notebook source
from pyspark.sql.functions import trim
from pyspark.sql.window import Window
import sys
import traceback
from delta.tables import *

def L2_DIM_DocType_AddedDetails(dfDocumentType,fileLocation):
  try:
    
    objGenHelper = gen_genericHelper()
    status="Update L2 Dim Doc Type details"
    fileFullName=fileLocation+"gen_L1_MD_DocType.delta"
    
    if objGenHelper.file_exist(fileFullName) == True:
      
      gen_L1_MD_DocType = objGenHelper.gen_readFromFile_perform(fileFullName)

      dfDocumentType = dfDocumentType.alias("doc").join(knw_LK_CD_ReportingSetup.alias("rpt")\
      ,(col("doc.companyCode") == col("rpt.companyCode")),how="inner")\
      .join(gen_L1_MD_DocType.alias("doc1"),(col("doc1.documentType")==col("doc.documentType"))\
      &(col("rpt.clientDataReportingLanguage")== col("doc1.languageCode")),how="left")\
      .join(gen_L1_MD_DocType.alias("doc2"),(col("doc2.documentType")==col("doc.documentType"))\
      &(col("rpt.clientDataSystemReportingLanguage")== col("doc2.languageCode")),how="left")\
      .select(col("doc.companyCode").alias("companyCode")\
      ,col("doc.documentType").alias("documentType")\
      ,col("doc.organizationUnitSurrogateKey").alias("organizationUnitSurrogateKey")\
      ,coalesce(col("doc1.documentTypeDescription"),col("doc2.documentTypeDescription")\
      ,col("doc.documentTypeDescription")).alias("documentTypeDescription"))
      
    else:
      dfDocumentType = dfDocumentType
      status="The file L1 Doc Type does not exists in the specified location: "+fileLocation
      
    return dfDocumentType,status
      
  except Exception as err:
    raise



def gen_L2_DIM_DocType_populate():
    """Populate document type"""

    try:

        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        global gen_L2_DIM_DocType
        objGenHelper = gen_genericHelper() 
        objDataTransformation = gen_dataTransformation()
 
        dfDocumentType = spark.createDataFrame(data = gl_lstOfDocTypeCollect, schema = docTypeSchemaCollect).\
                         select(col("companyCode"),col("documentType")).distinct()


        w = Window().orderBy(lit('documentTypeSurrogateKey'))
        dfDocumentType = dfDocumentType.join(gen_L2_DIM_Organization,\
                         [dfDocumentType.companyCode == gen_L2_DIM_Organization.companyCode],how='inner').\
                         select(dfDocumentType["*"],gen_L2_DIM_Organization["organizationUnitSurrogateKey"]).\
                         withColumn("documentTypeDescription",lit("#NA#"))
        
        dfDocumentType,status = L2_DIM_DocType_AddedDetails(dfDocumentType,gl_CDMLayer1Path)
        dfDocumentType = dfDocumentType.withColumn("documentTypeSurrogateKey", row_number().over(w))
                         
        gen_L2_DIM_DocType = objDataTransformation.gen_convertToCDMandCache \
            (dfDocumentType,'gen','L2_DIM_DocType',True)

        executionStatus = "L2_DIM_DocType populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


