# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import lit,col,when,coalesce


def gen_SAP_L0_STG_ProductHierarchy_populate(): 
  try:
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    global gen_L0_STG_ProductHierarchy
    global gen_L0_STG_ProductHierarchyLevel
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)

    clientLanguageCode_get = knw_LK_CD_ReportingSetup\
                        .select(col('clientCode'),col('clientDataReportingLanguage').alias('languageCode'))\
                        .union(\
                         knw_LK_CD_ReportingSetup.select(col('clientCode'),col('clientDataSystemReportingLanguage').alias('languageCode'))\
                              ).distinct()

    startLevel1 = erp_DD03L.filter((col('TABNAME') == 'PRODHS') & (col('COMPTYPE') == 'E') & (col('ROLLNAME').like('%PRODH1')))\
                           .select(col('LENG')).collect()

    startLevel2 = erp_DD03L.filter((col('TABNAME') == 'PRODHS') & (col('COMPTYPE') == 'E') & (col('ROLLNAME').like('%PRODH2')))\
                           .select(col('LENG')).collect()

    startLevel3 = erp_DD03L.filter((col('TABNAME') == 'PRODHS') & (col('COMPTYPE') == 'E') & (col('ROLLNAME').like('%PRODH3')))\
                           .select(col('LENG')).collect()

    startLevel4 = erp_DD03L.filter((col('TABNAME') == 'PRODHS') & (col('COMPTYPE') == 'E') & (col('ROLLNAME').like('%PRODH4')))\
                           .select(col('LENG')).collect()

    startLevel5 = erp_DD03L.filter((col('TABNAME') == 'PRODHS') & (col('COMPTYPE') == 'E') & (col('ROLLNAME').like('%PRODH5')))\
                           .select(col('LENG')).collect()

    startLevel6 = erp_DD03L.filter((col('TABNAME') == 'PRODHS') & (col('COMPTYPE') == 'E') & (col('ROLLNAME').like('%PRODH6')))\
                           .select(col('LENG')).collect()

    startLevel7 = erp_DD03L.filter((col('TABNAME') == 'PRODHS') & (col('COMPTYPE') == 'E') & (col('ROLLNAME').like('%PRODH7')))\
                           .select(col('LENG')).collect()

    startLevel8 = erp_DD03L.filter((col('TABNAME') == 'PRODHS') & (col('COMPTYPE') == 'E') & (col('ROLLNAME').like('%PRODH8')))\
                           .select(col('LENG')).collect()

    startLevel9 = erp_DD03L.filter((col('TABNAME') == 'PRODHS') & (col('COMPTYPE') == 'E') & (col('ROLLNAME').like('%PRODH9')))\
                           .select(col('LENG')).collect()

    if len(startLevel1) == 0:
      startLevel1 = 0
    else:
      startLevel1 = startLevel1[0][0]

    if len(startLevel2) == 0:
      startLevel2 = 0
    else:
      startLevel2 = startLevel2[0][0]

    if len(startLevel3) == 0:
      startLevel3 = 0
    else:
      startLevel3 = startLevel3[0][0]

    if len(startLevel4) == 0:
      startLevel4 = 0
    else:
      startLevel4 = startLevel4[0][0]

    if len(startLevel5) == 0:
      startLevel5 = 0
    else:
      startLevel5 = startLevel5[0][0]

    if len(startLevel6) == 0:
      startLevel6 = 0
    else:
      startLevel6 = startLevel6[0][0]

    if len(startLevel7) == 0:
      startLevel7 = 0
    else:
      startLevel7 = startLevel7[0][0]

    if len(startLevel8) == 0:
      startLevel8 = 0
    else:
      startLevel8 = startLevel8[0][0]

    if len(startLevel9) == 0:
      startLevel9 = 0
    else:
      startLevel9 = startLevel9[0][0]

    lengthLevel1 = int(startLevel1)
    lengthLevel2 = lengthLevel1 + int(startLevel2) 
    lengthLevel3 = lengthLevel2 + int(startLevel3)
    lengthLevel4 = lengthLevel3 + int(startLevel4)
    lengthLevel5 = lengthLevel4 + int(startLevel5)
    lengthLevel6 = lengthLevel5 + int(startLevel6)
    lengthLevel7 = lengthLevel6 + int(startLevel7)
    lengthLevel8 = lengthLevel7 + int(startLevel8)
    lengthLevel9 = lengthLevel8 + int(startLevel9)

    gen_SAP_L0_TMP_LanguageCodeForT179T = erp_T179T.alias('t179t')\
                                          .join(knw_LK_ISOLanguageKey.alias('lngkey'),\
                                                        ((col('lngkey.sourceSystemValue'))==(col('t179t.SPRAS'))),how="inner")\
                                           .join(knw_clientDataReportingLanguage.alias('rlg0')\
                                                   ,((col('rlg0.clientCode')==(col('t179t.MANDT')))\
                                                    &(col('rlg0.languageCode')==(col('lngkey.targetSystemValue')))),how='inner')\
                                          .select(col('t179t.MANDT').alias('clientCode'),col('rlg0.languageCode').alias('languageCode'),col('t179t.PRODH').alias('productHierarchy')\
                                                  ,col('t179t.VTEXT').alias('productGroupDescription')).distinct()


    productHierarchyDescription1 = erp_T179T.alias('t179t')\
                                          .join(knw_LK_ISOLanguageKey.alias('lngkey'),\
                                                        ((col('lngkey.sourceSystemValue'))==(col('t179t.SPRAS'))),how="inner")\
                                           .join(knw_clientDataSystemReportingLanguage.alias('slg0')\
                                                   ,((col('slg0.clientCode')==(col('t179t.MANDT')))\
                                                    &(col('slg0.languageCode')==(col('lngkey.targetSystemValue')))),how='left')\
                                          .select(col('t179t.MANDT').alias('clientCode'),\
                                                  coalesce(col('slg0.languageCode'),lit('NA')).alias('languageCode'),col('t179t.PRODH').alias('productHierarchy')\
                                                  ,col('t179t.VTEXT').alias('productGroupDescription')).distinct()


    gen_SAP_L0_TMP_ProductHierarchyDescription = productHierarchyDescription1.alias('cte')\
                                  .join(gen_SAP_L0_TMP_LanguageCodeForT179T.alias('lct0')\
                                       ,((col('lct0.productHierarchy')==(col('cte.productHierarchy')))\
                                                    &(col('lct0.clientCode')==(col('cte.clientCode')))),how='leftanti')\
                                  .select(col('cte.clientCode'),col('cte.languageCode'),col('cte.productHierarchy')\
                                                  ,col('cte.productGroupDescription'))\
                                  .union(\
                                        gen_SAP_L0_TMP_LanguageCodeForT179T.select(col('clientCode'),col('languageCode'),col('productHierarchy')\
                                                  ,col('productGroupDescription'))).distinct()


    gen_L0_STG_ProductHierarchy = gen_SAP_L0_TMP_ProductHierarchyDescription.alias('phd0')\
                                  .join(clientLanguageCode_get.alias('rlg0')\
                                       ,(col('phd0.clientCode')==(col('rlg0.clientCode'))),how='inner')\
                                  .join(gen_SAP_L0_TMP_ProductHierarchyDescription.alias('phr1')\
                                       ,((col('phr1.clientCode')==(col('phd0.clientCode')))\
                                         &(col('phr1.productHierarchy')==(substring(col('phd0.productHierarchy'),1,lengthLevel1)))\
                                         &(col('phr1.languageCode')==(col('rlg0.languageCode')))),how='left')\
                                  .join(gen_SAP_L0_TMP_ProductHierarchyDescription.alias('phr2')\
                                       ,((col('phr2.clientCode')==(col('phd0.clientCode')))\
                                         &(col('phr2.productHierarchy')==(substring(col('phd0.productHierarchy'),1,lengthLevel2)))\
                                         &(col('phr2.languageCode')==(col('rlg0.languageCode')))),how='left')\
                                  .join(gen_SAP_L0_TMP_ProductHierarchyDescription.alias('phr3')\
                                       ,((col('phr3.clientCode')==(col('phd0.clientCode')))\
                                         &(col('phr3.productHierarchy')==(substring(col('phd0.productHierarchy'),1,lengthLevel3)))\
                                         &(col('phr3.languageCode')==(col('rlg0.languageCode')))),how='left')\
                                  .join(gen_SAP_L0_TMP_ProductHierarchyDescription.alias('phr4')\
                                       ,((col('phr4.clientCode')==(col('phd0.clientCode')))\
                                         &(col('phr4.productHierarchy')==(substring(col('phd0.productHierarchy'),1,lengthLevel4)))\
                                         &(col('phr4.languageCode')==(col('rlg0.languageCode')))),how='left')\
                                  .join(gen_SAP_L0_TMP_ProductHierarchyDescription.alias('phr5')\
                                       ,((col('phr5.clientCode')==(col('phd0.clientCode')))\
                                         &(col('phr5.productHierarchy')==(substring(col('phd0.productHierarchy'),1,lengthLevel5)))\
                                         &(col('phr5.languageCode')==(col('rlg0.languageCode')))),how='left')\
                                  .join(gen_SAP_L0_TMP_ProductHierarchyDescription.alias('phr6')\
                                       ,((col('phr6.clientCode')==(col('phd0.clientCode')))\
                                         &(col('phr6.productHierarchy')==(substring(col('phd0.productHierarchy'),1,lengthLevel6)))\
                                         &(col('phr6.languageCode')==(col('rlg0.languageCode')))),how='left')\
                                  .join(gen_SAP_L0_TMP_ProductHierarchyDescription.alias('phr7')\
                                       ,((col('phr7.clientCode')==(col('phd0.clientCode')))\
                                         &(col('phr7.productHierarchy')==(substring(col('phd0.productHierarchy'),1,lengthLevel7)))\
                                         &(col('phr7.languageCode')==(col('rlg0.languageCode')))),how='left')\
                                  .join(gen_SAP_L0_TMP_ProductHierarchyDescription.alias('phr8')\
                                       ,((col('phr8.clientCode')==(col('phd0.clientCode')))\
                                         &(col('phr8.productHierarchy')==(substring(col('phd0.productHierarchy'),1,lengthLevel8)))\
                                         &(col('phr8.languageCode')==(col('rlg0.languageCode')))),how='left')\
                                  .join(gen_SAP_L0_TMP_ProductHierarchyDescription.alias('phr9')\
                                       ,((col('phr9.clientCode')==(col('phd0.clientCode')))\
                                         &(col('phr9.productHierarchy')==(substring(col('phd0.productHierarchy'),1,lengthLevel9)))\
                                         &(col('phr9.languageCode')==(col('rlg0.languageCode')))),how='left')\
                                  .select(col('phd0.clientCode').alias('productHierarchyClientCode'),coalesce(col('rlg0.languageCode'),lit('NA')).alias('productHierarchyLanguageCode')\
                                         ,col('phd0.productHierarchy').alias('productHierarchyNumber')\
                                         ,coalesce(substring('phd0.productHierarchy',1,lengthLevel1),lit('')).alias('productHierarchyLevel1')\
                                         ,coalesce('phr1.productGroupDescription',lit('')).alias('productHierarchyDescriptionLevel1')
                                         ,when(lit(lengthLevel2) != lit(lengthLevel1),coalesce(substring('phd0.productHierarchy',1,lengthLevel2),lit('')))\
                                               .otherwise(None).alias('productHierarchyLevel2')\
                                         ,when(lit(lengthLevel2) != lit(lengthLevel1),coalesce('phr2.productGroupDescription',lit('')))\
                                               .otherwise(None).alias('productHierarchyDescriptionLevel2')\
                                         ,when(lit(lengthLevel3) != lit(lengthLevel2),coalesce(substring('phd0.productHierarchy',1,lengthLevel3),lit('')))\
                                               .otherwise(None).alias('productHierarchyLevel3')\
                                         ,when(lit(lengthLevel3) != lit(lengthLevel2),coalesce('phr3.productGroupDescription',lit('')))\
                                               .otherwise(None).alias('productHierarchyDescriptionLevel3')\
                                         ,when(lit(lengthLevel3) != lit(lengthLevel4),coalesce(substring('phd0.productHierarchy',1,lengthLevel4),lit('')))\
                                               .otherwise(None).alias('productHierarchyLevel4')\
                                         ,when(lit(lengthLevel3) != lit(lengthLevel4),coalesce('phr4.productGroupDescription',lit('')))\
                                               .otherwise(None).alias('productHierarchyDescriptionLevel4')\
                                         ,when(lit(lengthLevel5) != lit(lengthLevel4),coalesce(substring('phd0.productHierarchy',1,lengthLevel5),lit('')))\
                                               .otherwise(None).alias('productHierarchyLevel5')\
                                         ,when(lit(lengthLevel5) != lit(lengthLevel4),coalesce('phr5.productGroupDescription',lit('')))\
                                               .otherwise(None).alias('productHierarchyDescriptionLevel5')\
                                         ,when(lit(lengthLevel5) != lit(lengthLevel6),coalesce(substring('phd0.productHierarchy',1,lengthLevel6),lit('')))\
                                               .otherwise(None).alias('productHierarchyLevel6')\
                                         ,when(lit(lengthLevel5) != lit(lengthLevel6),coalesce('phr6.productGroupDescription',lit('')))\
                                               .otherwise(None).alias('productHierarchyDescriptionLevel6')\
                                         ,when(lit(lengthLevel7) != lit(lengthLevel6),coalesce(substring('phd0.productHierarchy',1,lengthLevel7),lit('')))\
                                               .otherwise(None).alias('productHierarchyLevel7')\
                                         ,when(lit(lengthLevel7) != lit(lengthLevel6),coalesce('phr7.productGroupDescription',lit('')))\
                                               .otherwise(None).alias('productHierarchyDescriptionLevel7')\
                                         ,when(lit(lengthLevel7) != lit(lengthLevel8),coalesce(substring('phd0.productHierarchy',1,lengthLevel8),lit('')))\
                                               .otherwise(None).alias('productHierarchyLevel8')\
                                         ,when(lit(lengthLevel7) != lit(lengthLevel8),coalesce('phr8.productGroupDescription',lit('')))\
                                               .otherwise(None).alias('productHierarchyDescriptionLevel8')\
                                         ,when(lit(lengthLevel9) != lit(lengthLevel8),coalesce(substring('phd0.productHierarchy',1,lengthLevel9),lit('')))\
                                               .otherwise(None).alias('productHierarchyLevel9')\
                                         ,when(lit(lengthLevel9) != lit(lengthLevel8),coalesce('phr9.productGroupDescription',lit('')))\
                                               .otherwise(None).alias('productHierarchyDescriptionLevel9'))


    gen_L0_STG_ProductHierarchyLevel  =  gen_L0_STG_ProductHierarchy.select(col('productHierarchyClientCode'),col('productHierarchyNumber'),col('productHierarchyLevel1')\
                                                                           ,col('productHierarchyLevel2'),col('productHierarchyLevel3'),col('productHierarchyLevel4')\
                                                                           ,col('productHierarchyLevel5'),col('productHierarchyLevel6'),col('productHierarchyLevel7')\
                                                                           ,col('productHierarchyLevel8'),col('productHierarchyLevel9')).distinct()
  
    gen_L0_STG_ProductHierarchy =  objDataTransformation.gen_convertToCDMandCache \
          (gen_L0_STG_ProductHierarchy,'gen','L0_STG_ProductHierarchy',targetPath=gl_CDMLayer1Path)
    
    gen_L0_STG_ProductHierarchyLevel =  objDataTransformation.gen_convertToCDMandCache \
          (gen_L0_STG_ProductHierarchyLevel,'gen','L0_STG_ProductHierarchyLevel',targetPath=gl_CDMLayer1Path)
      
    executionStatus = "gen_L0_STG_ProductHierarchy populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]






