# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number
from delta.tables import *

def L2_DIM_Product_AddedDetails(gen_L2_DIM_Product,fileLocation):
  try:
    
    objGenHelper = gen_genericHelper()

    status="Update L2 Dim Product additional details"
    fileFullName=fileLocation+"gen_L1_MD_Product.delta"
    
    if objGenHelper.file_exist(fileFullName) == True:
      
      gen_L1_MD_Product = objGenHelper.gen_readFromFile_perform(fileFullName)

      gen_L2_DIM_Product = gen_L2_DIM_Product.alias("prd1")\
      .join(gen_L2_DIM_Organization.alias("org2")\
      ,col("prd1.organizationUnitSurrogateKey")==col("org2.organizationUnitSurrogateKey"),how="inner")\
      .join(knw_LK_CD_ReportingSetup.alias("rpt"),col("rpt.companyCode") == col("org2.companyCode"),how="left")\
      .join(gen_L1_MD_Product.alias("prd2"),(col("prd2.productArtificialID") == col("prd1.productArtificialID"))\
      &(col("prd2.productLanguageCode") == col("rpt.clientDataReportingLanguage")),how="left")\
      .join(gen_L1_MD_Product.alias("prd3"),(col("prd3.productArtificialID") == col("prd1.productArtificialID"))\
      &(col("prd3.productLanguageCode") == col("rpt.clientDataSystemReportingLanguage")),how="left")\
      .select(col("prd1.productSurrogateKey").alias("productSurrogateKey")\
      ,coalesce(col("prd2.productInternationalArticleNumber"),col("prd3.productInternationalArticleNumber")\
        ,col("prd1.productInternationalArticleNumber")).alias("productInternationalArticleNumber")\
      ,coalesce(col("prd2.productCreationDate"),col("prd3.productCreationDate")\
        ,col("prd1.productCreationDate")).alias("productCreationDate")
      ,coalesce(col("prd2.productBaseUnitOfMeasureClient"),col("prd3.productBaseUnitOfMeasureClient")\
        ,col("prd1.productBaseUnitOfMeasureClient")).alias("productBaseUnitOfMeasureClient")\
      ,coalesce(col("prd2.productBaseUnitOfMeasureKPMG"),col("prd3.productBaseUnitOfMeasureKPMG")\
        ,col("prd1.productBaseUnitOfMeasureKPMG")).alias("productBaseUnitOfMeasureKPMG")\
      ,coalesce(col("prd2.productTypeClient"),col("prd3.productTypeClient")\
        ,col("prd1.productTypeClient")).alias("productTypeClient")\
      ,coalesce(col("prd2.productTypeKPMG"),col("prd3.productTypeKPMG"),col("prd1.productTypeKPMG")).alias("productTypeKPMG")\
      ,coalesce(col("prd2.productGroup"),col("prd3.productGroup"),col("prd1.productGroup")).alias("productGroup")\
      ,coalesce(col("prd2.productProductionType"),col("prd3.productProductionType")\
        ,col("prd1.productProductionType")).alias("productProductionType")\
      ,coalesce(col("prd2.productIndustrySector"),col("prd3.productIndustrySector")\
        ,col("prd1.productIndustrySector")).alias("productIndustrySector")\
      ,coalesce(col("prd2.productLanguageCode"),col("prd3.productLanguageCode")\
        ,lit("#NA#")).alias("productTextLanguageCode")\
      ,coalesce(col("prd2.productName"),col("prd3.productName"),lit("#NA#")).alias("productName")\
      ,coalesce(col("prd2.productBaseUnitOfMeasureClientDescription"),col("prd3.productBaseUnitOfMeasureClientDescription")\
        ,lit("#NA#")).alias("productBaseUnitOfMeasureClientDescription")\
      ,coalesce(col("prd2.productBaseUnitOfMeasureKPMGDescription"),col("prd3.productBaseUnitOfMeasureKPMGDescription")\
        ,lit("#NA#")).alias("productBaseUnitOfMeasureKPMGDescription")\
      ,coalesce(col("prd2.productTypeClientDescription"),col("prd3.productTypeClientDescription")\
        ,lit("#NA#")).alias("productTypeClientDescription")\
      ,coalesce(col("prd2.productTypeKPMGDescription"),col("prd3.productTypeKPMGDescription")\
        ,lit("#NA#")).alias("productTypeKPMGDescription")\
      ,coalesce(col("prd2.productGroupDescription"),col("prd3.productGroupDescription")\
        ,lit("#NA#")).alias("productGroupDescription")\
      ,coalesce(col("prd2.productProductionTypeDescription"),col("prd3.productProductionTypeDescription")\
        ,lit("#NA#")).alias("productProductionTypeDescription")\
      ,coalesce(col("prd2.productIndustrySectorDescription"),col("prd3.productIndustrySectorDescription")\
        ,lit("#NA#")).alias("productIndustrySectorDescription"))

      if gen_L2_DIM_Product.count() >0:
        deltaL2ProductPath=gl_Layer2Staging+"gen_L2_DIM_Product.delta"
        deltaL2Product = DeltaTable.forPath(spark, deltaL2ProductPath)
        
        deltaL2Product.alias("L2_Product_O")\
        .merge(gen_L2_DIM_Product.alias("L2_Product_N"),"L2_Product_N.productSurrogateKey=L2_Product_O.productSurrogateKey")\
        .whenMatchedUpdate(set =
        {
          "productInternationalArticleNumber":"L2_Product_N.productInternationalArticleNumber",
          "productCreationDate":"L2_Product_N.productCreationDate",
          "productBaseUnitOfMeasureClient":"L2_Product_N.productBaseUnitOfMeasureClient",
          "productBaseUnitOfMeasureKPMG":"L2_Product_N.productBaseUnitOfMeasureKPMG",
          "productTypeClient":"L2_Product_N.productTypeClient",
          "productTypeKPMG":"L2_Product_N.productTypeKPMG",
          "productGroup":"L2_Product_N.productGroup",
          "productProductionType":"L2_Product_N.productProductionType",
          "productIndustrySector":"L2_Product_N.productIndustrySector"
        }).execute()

        deltaL2ProductTextPath=gl_Layer2Staging+"gen_L2_DIM_ProductText.delta"
        deltaL2ProductText = DeltaTable.forPath(spark, deltaL2ProductTextPath)
        
        deltaL2ProductText.alias("L2_Product_O")\
        .merge(gen_L2_DIM_Product.alias("L2_Product_N"),"L2_Product_N.productSurrogateKey=L2_Product_O.productSurrogateKey")\
        .whenMatchedUpdate(set =
        {
          "productTextLanguageCode":"L2_Product_N.productTextLanguageCode",
          "productName":"L2_Product_N.productName",
          "productBaseUnitOfMeasureClientDescription":"L2_Product_N.productBaseUnitOfMeasureClientDescription",
          "productBaseUnitOfMeasureKPMGDescription":"L2_Product_N.productBaseUnitOfMeasureKPMGDescription",
          "productTypeClientDescription":"L2_Product_N.productTypeClientDescription",
          "productTypeKPMGDescription":"L2_Product_N.productTypeKPMGDescription",
          "productGroupDescription":"L2_Product_N.productGroupDescription",
          "productProductionTypeDescription":"L2_Product_N.productProductionTypeDescription",
          "productIndustrySectorDescription":"L2_Product_N.productIndustrySectorDescription"
        }).execute()
    else:
      status="The file L1 Product does not exist in the specified location: "+fileLocation
  except Exception as err:
    raise
  


def gen_L2_DIM_Product_populate():
    try:
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global gen_L2_DIM_Product, gen_L2_DIM_ProductText
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
   
      gen_L1_TMP_Product = spark.createDataFrame(gl_lstOfProductCollect,productSchemaCollect).distinct()

      dfL2_DIM_Product = \
                  gen_L1_TMP_Product.alias("prod").join \
                  (gen_L2_DIM_Organization.alias("org"), \
                  (col("prod.companyCode")    == col("org.companyCode")),      \
                  "inner").select(
                                lit(gl_analysisID).alias("analysisID"),\
                                
                                col("organizationUnitSurrogateKey").alias("organizationUnitSurrogateKey"),  \
                                col("productNumber").alias("productNumber"),  \
                                col("productArtificialID").alias("productArtificialID"),  \
                                lit("#NA#").alias("productInternationalArticleNumber"),  \
                                lit('1900-01-01').alias("productCreationDate"),  \
                                lit("#NA#").alias("productBaseUnitOfMeasureClient"),  \
                                lit("#NA#").alias("productBaseUnitOfMeasureKPMG"),  \
                                lit("#NA#").alias("productTypeClient"),  \
                                lit("#NA#").alias("productTypeKPMG"),  \
                                lit("#NA#").alias("productGroup"),  \
                                lit("#NA#").alias("isInterCoFromMasterData"),  \
                                lit("#NA#").alias("productProductionType"),  \
                                lit("#NA#").alias("productIndustrySector"),  \
                                lit("#NA#").alias("productHierarchyLevel1"),  \
                                lit("#NA#").alias("productHierarchyLevel2"),  \
                                lit("#NA#").alias("productHierarchyLevel3"),  \
                                lit("#NA#").alias("productHierarchyLevel4"),  \
                                lit("#NA#").alias("productHierarchyLevel5"),  \
                                lit("#NA#").alias("productHierarchyLevel6"),  \
                                lit("#NA#").alias("productHierarchyLevel7"),  \
                                lit("#NA#").alias("productHierarchyLevel8"),  \
                                lit("#NA#").alias("productHierarchyLevel9")   \
                               ).distinct()
      w = Window().orderBy(lit(''))
      dfL2_DIM_Product = dfL2_DIM_Product.withColumn("productSurrogateKey", row_number().over(w))
      
      gen_L2_DIM_Product = objDataTransformation.gen_convertToCDMandCache(dfL2_DIM_Product,'gen','L2_DIM_Product',True)
      
      dfL2_DIM_ProductText = \
                       gen_L2_DIM_Product.select(
                                     lit(gl_analysisID).alias("analysisID"),\
                                     col("productSurrogateKey").alias("productSurrogateKey"),  \
                                     lit("#NA#").alias("productTextLanguageCode"),  \
                                     lit("#NA#").alias("productName"),  \
                                     lit("#NA#").alias("productBaseUnitOfMeasureClientDescription"),  \
                                     lit("#NA#").alias("productBaseUnitOfMeasureKPMGDescription"),  \
                                     lit("#NA#").alias("productTypeClientDescription"),  \
                                     lit("#NA#").alias("productTypeKPMGDescription"),  \
                                     lit("#NA#").alias("productGroupDescription"),  \
                                     lit("#NA#").alias("productProductionTypeDescription"),  \
                                     lit("#NA#").alias("productIndustrySectorDescription"),  \
                                     lit("#NA#").alias("productHierarchyDescriptionLevel1"),  \
                                     lit("#NA#").alias("productHierarchyDescriptionLevel2"),  \
                                     lit("#NA#").alias("productHierarchyDescriptionLevel3"),  \
                                     lit("#NA#").alias("productHierarchyDescriptionLevel4"),  \
                                     lit("#NA#").alias("productHierarchyDescriptionLevel5"),  \
                                     lit("#NA#").alias("productHierarchyDescriptionLevel6"),  \
                                     lit("#NA#").alias("productHierarchyDescriptionLevel7"),  \
                                     lit("#NA#").alias("productHierarchyDescriptionLevel8"),  \
                                     lit("#NA#").alias("productHierarchyDescriptionLevel9"),  \
                                    ).distinct()
      
      dfL2_DIM_ProductText = dfL2_DIM_ProductText.withColumn("productTextSurrogateKey", row_number().over(w))
      

      gen_L2_DIM_ProductText = objDataTransformation.gen_convertToCDMandCache \
          (dfL2_DIM_ProductText,'gen','L2_DIM_ProductText',True)


      L2_DIM_Product_AddedDetails(gen_L2_DIM_Product,gl_CDMLayer1Path)
     
      executionStatus = "gen_L2_DIM_Product populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


#gen_L2_DIM_Product_populate()
      


