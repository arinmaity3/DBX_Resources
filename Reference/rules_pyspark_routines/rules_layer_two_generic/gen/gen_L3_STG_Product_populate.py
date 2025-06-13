# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,sum,avg,max,when,concat,coalesce
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F
from pyspark.sql.window import Window

#gen_L3_STG_Product_populate
def gen_L3_STG_Product_populate():
    try:
      
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global gen_L3_STG_Product
      #Step2
      new_col_ArticleNumber = when(col('prd2.productInternationalArticleNumber').isNull(), lit('#NA#') ) \
                            .when(col('prd2.productInternationalArticleNumber')=='', lit('#NA#') ) \
                            .otherwise(col('prd2.productInternationalArticleNumber'))

      new_col_productCreationDate= when(col('prd2.productCreationDate').isNull(), lit('1900-01-01') ) \
                                  .otherwise(col('prd2.productCreationDate'))

      new_col_productName=concat(col("prd2.productNumber").cast("String"),lit(' '),col("pdt2.productName"))
      #[productBaseUnitOfMeasureClient(Desc)]
      new_col_MeasureClientDesc=concat((when(col('prd2.productBaseUnitOfMeasureClient').isNull(), lit('#NA#') ) \
                                    .otherwise(col('prd2.productBaseUnitOfMeasureClient')))\
                                    ,lit('(')
                                     ,(when(col('pdt2.productBaseUnitOfMeasureClientDescription').isNull(), lit('#NA#') ) \
                                    .otherwise(col('pdt2.productBaseUnitOfMeasureClientDescription')))\
                                    ,lit(')')
                                    )
      #productBaseUnitOfMeasureKPMG(Desc)
      new_col_MeasureKPMGDesc=concat((when(col('prd2.productBaseUnitOfMeasureKPMG').isNull(), lit('#NA#') ) \
                                    .otherwise(col('prd2.productBaseUnitOfMeasureKPMG')))\
                                    ,lit('(')
                                     ,(when(col('pdt2.productBaseUnitOfMeasureKPMGDescription').isNull(), lit('#NA#') ) \
                                    .otherwise(col('pdt2.productBaseUnitOfMeasureKPMGDescription')))\
                                    ,lit(')')
                                    )
      #productTypeClient(Desc)
      new_col_productTypeClientDesc=concat((when(col('prd2.productTypeClient').isNull(), lit('#NA#') ) \
                                    .otherwise(col('prd2.productTypeClient')))\
                                    ,lit('(')
                                     ,(when(col('pdt2.productTypeClientDescription').isNull(), lit('#NA#') ) \
                                    .otherwise(col('pdt2.productTypeClientDescription')))\
                                    ,lit(')')
                                    )
      #productTypeClient
      new_col_productTypeClient=when(col('prd2.productTypeClient').isNull(), lit('#NA#') ) \
                                    .otherwise(col('prd2.productTypeClient'))
      #productTypeClientDescription
      new_col_productTypeClientDescription=when(col('pdt2.productTypeClientDescription').isNull(), lit('#NA#') ) \
                                    .otherwise(col('pdt2.productTypeClientDescription'))
      #productTypeKPMG(Desc)
      new_col_productTypeKPMGDesc=concat((when(col('prd2.productTypeKPMG').isNull(), lit('#NA#') ) \
                                    .otherwise(col('prd2.productTypeKPMG')))\
                                    ,lit('(')
                                     ,(when(col('pdt2.productTypeKPMGDescription').isNull(), lit('#NA#') ) \
                                    .otherwise(col('pdt2.productTypeKPMGDescription')))\
                                    ,lit(')')
                                    )
      #productGroup
      new_col_productGroup=when(col('prd2.productGroup').isNull(), lit('#NA#') ) \
                                    .otherwise(col('prd2.productGroup'))
      #productGroupDescription
      new_col_productGroupDescription=when(col('pdt2.productGroupDescription').isNull(), lit('#NA#') ) \
                                    .otherwise(col('pdt2.productGroupDescription'))
      #productGroup(Desc)
      new_col_productGroupDesc=concat((when(col('prd2.productGroup').isNull(), lit('#NA#') ) \
                                    .otherwise(col('prd2.productGroup')))\
                                    ,lit('(')
                                     ,(when(col('pdt2.productGroupDescription').isNull(), lit('#NA#') ) \
                                    .otherwise(col('pdt2.productGroupDescription')))\
                                    ,lit(')')
                                    )
      #productProductionType(Desc)
      new_col_productProductionTypeDesc=concat((when(col('prd2.productProductionType').isNull(), lit('#NA#') ) \
                                    .otherwise(col('prd2.productProductionType')))\
                                    ,lit('(')
                                     ,(when(col('pdt2.productProductionTypeDescription').isNull(), lit('#NA#') ) \
                                    .otherwise(col('pdt2.productProductionTypeDescription')))\
                                    ,lit(')')
                                    )
      #productIndustrySector(Desc)
      new_col_productIndustrySectorDesc=concat((when(col('prd2.productIndustrySector').isNull(), lit('#NA#') ) \
                                    .otherwise(col('prd2.productIndustrySector')))\
                                    ,lit('(')
                                     ,(when(col('pdt2.productIndustrySectorDescription').isNull(), lit('#NA#') ) \
                                    .otherwise(col('pdt2.productIndustrySectorDescription')))\
                                    ,lit(')')
                                    )				
      #productHierarchyLevel1(Desc)
      new_col_productHierarchyLevel1Desc=when(col('prd2.productHierarchyLevel1').isNull(), lit('#NA# (#NA#)') ) \
                                        .when(col('prd2.productHierarchyLevel1')=='#NA#',lit('#NA# (#NA#)'))\
                                        .otherwise(concat(col('prd2.productHierarchyLevel1')\
                                           ,lit('(')
                                           ,col('pdt2.productHierarchyDescriptionLevel1')\
                                           ,lit(')')))
      #productHierarchyLevel2(Desc)
      new_col_productHierarchyLevel2Desc=when(col('prd2.productHierarchyLevel2').isNull(),\
                                         lit(concat(col('prd2.productNumber'),lit(' '),col('pdt2.productName'))) ) \
                                        .when(col('prd2.productHierarchyLevel2')=='#NA#',
                                         lit(concat(col('prd2.productNumber'),lit(' '),col('pdt2.productName'))))\
                                        .otherwise(concat(col('prd2.productHierarchyLevel2')\
                                           ,lit('(')
                                           ,col('pdt2.productHierarchyDescriptionLevel2')\
                                           ,lit(')')))
      #productHierarchyLevel3(Desc)
      new_col_productHierarchyLevel3Desc=when(col('prd2.productHierarchyLevel3').isNull(),\
                                         lit(concat(col('prd2.productNumber'),lit(' '),col('pdt2.productName'))) ) \
                                        .when(col('prd2.productHierarchyLevel3')=='#NA#',
                                         lit(concat(col('prd2.productNumber'),lit(' '),col('pdt2.productName'))))\
                                        .otherwise(concat(col('prd2.productHierarchyLevel3')\
                                           ,lit('(')
                                           ,col('pdt2.productHierarchyDescriptionLevel3')\
                                           ,lit(')')))
      #productHierarchyLevel4(Desc)
      new_col_productHierarchyLevel4Desc=when(col('prd2.productHierarchyLevel4').isNull(),\
                                         lit(concat(col('prd2.productNumber'),lit(' '),col('pdt2.productName'))) ) \
                                        .when(col('prd2.productHierarchyLevel4')=='#NA#',
                                         lit(concat(col('prd2.productNumber'),lit(' '),col('pdt2.productName'))))\
                                        .otherwise(concat(col('prd2.productHierarchyLevel4')\
                                           ,lit('(')
                                           ,col('pdt2.productHierarchyDescriptionLevel4')\
                                           ,lit(')')))
      #productHierarchyLevel5(Desc)
      new_col_productHierarchyLevel5Desc=when(col('prd2.productHierarchyLevel5').isNull(),\
                                        lit(concat(col('prd2.productNumber'),lit(' '),col('pdt2.productName'))) ) \
                                        .when(col('prd2.productHierarchyLevel5')=='#NA#',
                                         lit(concat(col('prd2.productNumber'),lit(' '),col('pdt2.productName'))))\
                                        .otherwise(concat(col('prd2.productHierarchyLevel5')\
                                           ,lit('(')
                                           ,col('pdt2.productHierarchyDescriptionLevel5')\
                                           ,lit(')')))
      #productHierarchyLevel6(Desc)
      new_col_productHierarchyLevel6Desc=when(col('prd2.productHierarchyLevel6').isNull(),\
                                         lit(concat(col('prd2.productNumber'),lit(' '),col('pdt2.productName'))) ) \
                                        .when(col('prd2.productHierarchyLevel6')=='#NA#',
                                         lit(concat(col('prd2.productNumber'),lit(' '),col('pdt2.productName'))))\
                                        .otherwise(concat(col('prd2.productHierarchyLevel6')\
                                           ,lit('(')
                                           ,col('pdt2.productHierarchyDescriptionLevel6')\
                                           ,lit(')')))
      #productHierarchyLevel7(Desc)
      new_col_productHierarchyLevel7Desc=when(col('prd2.productHierarchyLevel7').isNull(),\
                                         lit(concat(col('prd2.productNumber'),lit(' '),col('pdt2.productName'))) ) \
                                        .when(col('prd2.productHierarchyLevel7')=='#NA#',
                                         lit(concat(col('prd2.productNumber'),lit(' '),col('pdt2.productName'))))\
                                        .otherwise(concat(col('prd2.productHierarchyLevel7')\
                                           ,lit('(')
                                           ,col('pdt2.productHierarchyDescriptionLevel7')\
                                           ,lit(')')))
      #productHierarchyLevel8(Desc)
      new_col_productHierarchyLevel8Desc=when(col('prd2.productHierarchyLevel8').isNull(),\
                                         lit(concat(col('prd2.productNumber'),lit(' '),col('pdt2.productName'))) ) \
                                        .when(col('prd2.productHierarchyLevel8')=='#NA#',
                                         lit(concat(col('prd2.productNumber'),lit(' '),col('pdt2.productName'))))\
                                        .otherwise(concat(col('prd2.productHierarchyLevel8')\
                                           ,lit('(')
                                           ,col('pdt2.productHierarchyDescriptionLevel8')\
                                           ,lit(')')))
      #productHierarchyLevel9(Desc)
      new_col_productHierarchyLevel9Desc=when(col('prd2.productHierarchyLevel9').isNull(),\
                                        lit(concat(col('prd2.productNumber'),lit(' '),col('pdt2.productName'))) ) \
                                       .when(col('prd2.productHierarchyLevel9')=='#NA#',
                                        lit(concat(col('prd2.productNumber'),lit(' '),col('pdt2.productName'))))\
                                       .otherwise(concat(col('prd2.productHierarchyLevel9')\
                                          ,lit('(')
                                          ,col('pdt2.productHierarchyDescriptionLevel9')\
                                          ,lit(')')))

      gen_L3_TMP_Product= gen_L2_DIM_Product.alias('prd2')\
                 .join(gen_L2_DIM_ProductText.alias('pdt2')\
                       ,(col('prd2.productSurrogateKey')==col('pdt2.productSurrogateKey')),how='inner')\
                 .select(col('prd2.productSurrogateKey').alias('productSurrogateKey')\
                       ,col('prd2.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
                       ,col('prd2.productNumber').alias('productNumber')\
                       ,col('prd2.productArtificialID').alias('productArtificialID')\
                       ,lit(new_col_ArticleNumber).alias('productInternationalArticleNumber')\
                       ,lit(new_col_productCreationDate).alias('productCreationDate')\
                       ,col('pdt2.productTextLanguageCode').alias('productLanguageCode')\
                       ,lit(new_col_productName).alias('productName')\
                       ,coalesce(col('pdt2.productName'),lit('#NA#')).alias('productNameWithoutNumber')\
                       ,lit(new_col_MeasureClientDesc).alias('productBaseUnitOfMeasureClient(Desc)')\
                       ,lit(new_col_MeasureKPMGDesc).alias('productBaseUnitOfMeasureKPMG(Desc)')\
                       ,lit(new_col_productTypeClientDesc).alias('productTypeClient(Desc)')\
                       ,lit(new_col_productTypeClient).alias('productTypeClient')\
                       ,lit(new_col_productTypeClientDescription).alias('productTypeClientDescription')\
                       ,lit(new_col_productTypeKPMGDesc).alias('productTypeKPMG(Desc)')\
                       ,lit(new_col_productGroup).alias('productGroup')\
                       ,lit(new_col_productGroupDescription).alias('productGroupDescription')\
                       ,lit(new_col_productGroupDesc).alias('productGroup(Desc)')\
                       ,lit(new_col_productProductionTypeDesc).alias('productProductionType(Desc)')\
                       ,lit(new_col_productIndustrySectorDesc).alias('productIndustrySector(Desc)')\
                       ,lit(new_col_productHierarchyLevel1Desc).alias('productHierarchyLevel1(Desc)')\
                       ,lit(new_col_productHierarchyLevel2Desc).alias('productHierarchyLevel2(Desc)')\
                       ,lit(new_col_productHierarchyLevel3Desc).alias('productHierarchyLevel3(Desc)')\
                       ,lit(new_col_productHierarchyLevel4Desc).alias('productHierarchyLevel4(Desc)')\
                       ,lit(new_col_productHierarchyLevel5Desc).alias('productHierarchyLevel5(Desc)')\
                       ,lit(new_col_productHierarchyLevel6Desc).alias('productHierarchyLevel6(Desc)')\
                       ,lit(new_col_productHierarchyLevel7Desc).alias('productHierarchyLevel7(Desc)')\
                       ,lit(new_col_productHierarchyLevel8Desc).alias('productHierarchyLevel8(Desc)')\
                       ,lit(new_col_productHierarchyLevel9Desc).alias('productHierarchyLevel9(Desc)')\

                   )
      unknown_list = [[0, 0,'NONE','NONE','NONE','1900-01-01','NONE','NONE','NONE','NONE','NONE','NONE'\
                    ,'NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE','NONE'\
                    ,'NONE','NONE','NONE','NONE','NONE','NONE']]

      unknown_df = spark.createDataFrame(unknown_list)
      gen_L3_TMP_Product = gen_L3_TMP_Product.union(unknown_df)
      #Step2
      

      #productHierarchyDescriptionLevel1Key
      dr_windowLevel1Key = Window.partitionBy().orderBy("productHierarchyLevel1(Desc)")
      #productHierarchyDescriptionLevel2Key
      dr_windowLevel2Key = Window.partitionBy().orderBy("productHierarchyLevel1(Desc)","productHierarchyLevel2(Desc)")
      #productHierarchyDescriptionLevel3Key
      dr_windowLevel3Key = Window.partitionBy().orderBy("productHierarchyLevel1(Desc)","productHierarchyLevel2(Desc)"\
                                                      ,"productHierarchyLevel3(Desc)")
      #productHierarchyDescriptionLevel4Key
      dr_windowLevel4Key = Window.partitionBy().orderBy("productHierarchyLevel1(Desc)","productHierarchyLevel2(Desc)"\
                                                      ,"productHierarchyLevel3(Desc)","productHierarchyLevel4(Desc)")			
      #productHierarchyDescriptionLevel5Key
      dr_windowLevel5Key = Window.partitionBy().orderBy("productHierarchyLevel1(Desc)","productHierarchyLevel2(Desc)"\
                                                      ,"productHierarchyLevel3(Desc)","productHierarchyLevel4(Desc)"\
                                                      ,"productHierarchyLevel5(Desc)")
      #productHierarchyDescriptionLevel6Key
      dr_windowLevel6Key = Window.partitionBy().orderBy("productHierarchyLevel1(Desc)","productHierarchyLevel2(Desc)"\
                                                      ,"productHierarchyLevel3(Desc)","productHierarchyLevel4(Desc)"\
                                                      ,"productHierarchyLevel5(Desc)","productHierarchyLevel6(Desc)")
      #productHierarchyDescriptionLevel7Key
      dr_windowLevel7Key = Window.partitionBy().orderBy("productHierarchyLevel1(Desc)","productHierarchyLevel2(Desc)"\
                                                      ,"productHierarchyLevel3(Desc)","productHierarchyLevel4(Desc)"\
                                                      ,"productHierarchyLevel5(Desc)","productHierarchyLevel6(Desc)"\
                                                      ,"productHierarchyLevel7(Desc)")
      #productHierarchyDescriptionLevel8Key
      dr_windowLevel8Key = Window.partitionBy().orderBy("productHierarchyLevel1(Desc)","productHierarchyLevel2(Desc)"\
                                                      ,"productHierarchyLevel3(Desc)","productHierarchyLevel4(Desc)"\
                                                      ,"productHierarchyLevel5(Desc)","productHierarchyLevel6(Desc)"\
                                                      ,"productHierarchyLevel7(Desc)","productHierarchyLevel8(Desc)")
      #productHierarchyDescriptionLevel8Key
      dr_windowLevel9Key = Window.partitionBy().orderBy("productHierarchyLevel1(Desc)","productHierarchyLevel2(Desc)"\
                                                      ,"productHierarchyLevel3(Desc)","productHierarchyLevel4(Desc)"\
                                                      ,"productHierarchyLevel5(Desc)","productHierarchyLevel6(Desc)"\
                                                      ,"productHierarchyLevel7(Desc)","productHierarchyLevel8(Desc)"\
                                                      ,"productHierarchyLevel9(Desc)") 

      gen_L3_STG_Product=gen_L3_TMP_Product.select(col('productSurrogateKey').alias('productSurrogateKey')\
                 ,col('organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
                 ,col('productNumber').alias('productNumber')\
                 ,col('productArtificialID').alias('productArtificialID')\
                 ,col('productInternationalArticleNumber').alias('productInternationalArticleNumber')\
                 ,col('productCreationDate').alias('productCreationDate')\
                 ,col('productLanguageCode').alias('productLanguageCode')\
                 ,col('productName').alias('productName')\
                 ,col('productNameWithoutNumber').alias('productNameWithoutNumber')\
                 ,col('productBaseUnitOfMeasureClient(Desc)').alias('productBaseUnitOfMeasureClientDesc')\
                 ,col('productBaseUnitOfMeasureKPMG(Desc)').alias('productBaseUnitOfMeasureKPMGDesc')\
                 ,col('productTypeClient(Desc)').alias('productTypeClientDesc')\
                 ,col('productTypeClient').alias('productTypeClient')\
                 ,col('productTypeClientDescription').alias('productTypeClientDescription')\
                 ,col('productTypeKPMG(Desc)').alias('productTypeKPMGDesc')\
                 ,col('productGroup').alias('productGroup')\
                 ,col('productGroupDescription').alias('productGroupDescription')\
                 ,col('productGroup(Desc)').alias('productGroupDesc')\
                 ,col('productProductionType(Desc)').alias('productProductionTypeDesc')\
                 ,col('productIndustrySector(Desc)').alias('productIndustrySectorDesc')\
                 ,col('productHierarchyLevel1(Desc)').alias('productHierarchyLevel1Desc')\
                 ,col('productHierarchyLevel2(Desc)').alias('productHierarchyLevel2Desc')\
                 ,col('productHierarchyLevel3(Desc)').alias('productHierarchyLevel3Desc')\
                 ,col('productHierarchyLevel4(Desc)').alias('productHierarchyLevel4Desc')\
                 ,col('productHierarchyLevel5(Desc)').alias('productHierarchyLevel5Desc')\
                 ,col('productHierarchyLevel6(Desc)').alias('productHierarchyLevel6Desc')\
                 ,col('productHierarchyLevel7(Desc)').alias('productHierarchyLevel7Desc')\
                 ,col('productHierarchyLevel8(Desc)').alias('productHierarchyLevel8Desc')\
                 ,col('productHierarchyLevel9(Desc)').alias('productHierarchyLevel9Desc')\
                 ,F.dense_rank().over(dr_windowLevel1Key).alias("productHierarchyDescriptionLevel1Key")\
                 ,F.dense_rank().over(dr_windowLevel2Key).alias("productHierarchyDescriptionLevel2Key")\
                 ,F.dense_rank().over(dr_windowLevel3Key).alias("productHierarchyDescriptionLevel3Key")\
                 ,F.dense_rank().over(dr_windowLevel4Key).alias("productHierarchyDescriptionLevel4Key")\
                 ,F.dense_rank().over(dr_windowLevel5Key).alias("productHierarchyDescriptionLevel5Key")\
                 ,F.dense_rank().over(dr_windowLevel6Key).alias("productHierarchyDescriptionLevel6Key")\
                 ,F.dense_rank().over(dr_windowLevel7Key).alias("productHierarchyDescriptionLevel7Key")\
                 ,F.dense_rank().over(dr_windowLevel8Key).alias("productHierarchyDescriptionLevel8Key")\
                 ,F.dense_rank().over(dr_windowLevel9Key).alias("productHierarchyDescriptionLevel9Key")\
                                     )

      gen_L3_STG_Product = objDataTransformation.gen_convertToCDMStructure_generate(gen_L3_STG_Product,'gen','L3_STG_Product',True)[0]
      
      dwh_vw_DIM_Product=gen_L3_STG_Product.alias('prd').select(col('prd.analysisID').alias('analysisID')\
                     ,col('prd.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
                     ,col('prd.productSurrogateKey').alias('productSurrogateKey')\
                     ,col('prd.productNumber').alias('productNumber')\
                     ,col('prd.productLanguageCode').alias('productLanguageCode')\
                     ,col('prd.productName').alias('productName')\
                     ,col('prd.productTypeClient').alias('productTypeClient')\
                     ,col('prd.productTypeClientDescription').alias('productTypeClientDescription')\
                     ,col('prd.productGroup').alias('productGroup')\
                     ,col('prd.productGroupDescription').alias('productGroupDescription')\
                     ,col('prd.productHierarchyLevel1Desc').alias('productHierarchyLevel1Description')\
                     ,col('prd.productHierarchyLevel2Desc').alias('productHierarchyLevel2Description')\
                     ,col('prd.productHierarchyLevel3Desc').alias('productHierarchyLevel3Description')\
                     ,col('prd.productHierarchyLevel4Desc').alias('productHierarchyLevel4Description')\
                     ,col('prd.productHierarchyLevel5Desc').alias('productHierarchyLevel5Description')\
                     ,col('prd.productHierarchyLevel6Desc').alias('productHierarchyLevel6Description')\
                     ,col('prd.productHierarchyLevel7Desc').alias('productHierarchyLevel7Description')\
                     ,col('prd.productHierarchyLevel8Desc').alias('productHierarchyLevel8Description')\
                     ,col('prd.productHierarchyLevel9Desc').alias('productHierarchyLevel9Description')\
                    )
      dwh_vw_DIM_Product = objDataTransformation.gen_convertToCDMStructure_generate(dwh_vw_DIM_Product,'dwh','vw_DIM_Product',False)[0]
      objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_Product,gl_CDMLayer2Path + 'gen_L2_DIM_Product.parquet' )
      
      executionStatus = "L3_STG_Product population sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
    except Exception as e:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
      
      
      
