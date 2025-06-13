# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import lit,col,when,expr,coalesce,max,min

def gen_SAP_L1_MD_Product_populate(): 
    try:

      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
      global gen_L1_MD_Product


      erpSAPSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL','ERP_SYSTEM_ID')
      reportingLanguage = knw_KPMGDataReportingLanguage.select(col('languageCode')).distinct().collect()[0][0]
      knw_usf_clientLanguageCode = knw_clientDataSystemReportingLanguage.select('clientCode','languageCode')
     
      gen_SAP_L0_TMP_DistinctLanguageCodeFromMAKT = erp_MAKT.alias('MAKT')\
                               .select(col('MAKT.MANDT').alias('client')\
                                       ,col('MAKT.SPRAS').alias('languageCode'))\
                               .distinct()
        
      gen_SAP_L0_TMP_DistinctReportingLanguage = knw_LK_CD_ReportingSetup.alias('rstup')\
                               .select(col('rstup.clientCode').alias('client')\
                                       ,col('rstup.clientDataReportingLanguage').alias('clientDataReportingLanguage'))\
                               .distinct()
      
      gen_SAP_L0_TMP_DistinctLanguageCodeDetail = gen_SAP_L0_TMP_DistinctLanguageCodeFromMAKT.alias('dlcfm')\
                               .join(knw_LK_ISOLanguageKey.alias('lngkey')\
                                     ,(col('lngkey.sourceSystemValue')==(col('dlcfm.languageCode'))),how='inner')\
                               .join(gen_SAP_L0_TMP_DistinctReportingLanguage.alias("drlc")\
                                     ,((col('dlcfm.client')==(col('drlc.client')))\
                                     &(col('lngkey.targetSystemValue')==(col('drlc.clientDataReportingLanguage')))),how='inner')\
                               .select(col('dlcfm.client').alias('client')\
                                     ,col('dlcfm.languageCode').alias('languageCode')\
                                     ,col('drlc.clientDataReportingLanguage').alias('clientDataReportingLanguage'))

      gen_SAP_L0_TMP_LanguageCodeForMAKT = erp_MAKT.alias('mak0')\
                               .join(gen_SAP_L0_TMP_DistinctLanguageCodeDetail.alias('dlcd')\
                                      ,((col('mak0.MANDT')==(col('dlcd.client')))\
                                      &(col('mak0.SPRAS')==(col('dlcd.languageCode')))),how='inner')\
                               .select(col('mak0.MANDT').alias('clientCode')\
                                      ,col('mak0.MATNR').alias('productNumber')\
                                      ,col('mak0.MAKTX').alias('productName')\
                                      ,col('dlcd.clientDataReportingLanguage').alias('languageCode'))\
                              .distinct()
      
      
      gen_SAP_L0_TMP_LanguageCodeForMAKT_NotExists = erp_MAKT.alias('mak0')\
                               .join(gen_SAP_L0_TMP_LanguageCodeForMAKT.alias('lcm0')\
                                     ,((col('lcm0.productNumber') == col('mak0.MATNR'))\
                                     &(col('lcm0.clientCode')==(col('mak0.MANDT')))) ,how='leftanti')
                                            
      df1_productName = gen_SAP_L0_TMP_LanguageCodeForMAKT_NotExists.alias('mak0')\
                              .join (knw_LK_ISOLanguageKey.alias("lngkey"),(col("lngkey.sourceSystemValue").eqNullSafe(col("mak0.SPRAS"))), "left")\
                              .join (knw_clientDataSystemReportingLanguage.alias("slg0")\
                                     ,((col("slg0.languageCode").eqNullSafe(col("lngkey.targetSystemValue")))\
                                        &(col('slg0.clientCode')==(col('mak0.MANDT')))), "left")\
                              .select(col('mak0.MANDT').alias('clientCode')\
                                     ,col('mak0.MATNR').alias('productNumber')\
                                     ,col('mak0.MAKTX').alias('productName')\
                                     ,coalesce(col('slg0.languageCode'),lit('NA')).alias('languageCode'))
      
      df2_productName = gen_SAP_L0_TMP_LanguageCodeForMAKT.alias('lcm0')\
                              .select(col('lcm0.clientCode').alias('clientCode')\
                                     ,col('lcm0.productNumber').alias('productNumber')\
                                     ,col('lcm0.productName').alias('productName')\
                                     ,col('lcm0.languageCode').alias('languageCode'))
      gen_SAP_L0_TMP_ProductName = df1_productName.union(df2_productName)

      isService = when(col('t134.MTREF')=='DIEN',lit(1)).otherwise(lit(0))\
      						        .alias('isService')\
      
      gen_SAP_L0_TMP_LanguageCodeForT134T = erp_T134T.alias('t134t')\
                              .join(knw_LK_ISOLanguageKey.alias('lngkey')\
                                    ,(col('lngkey.sourceSystemValue')==(col('t134t.SPRAS'))),how='inner')\
                              .join(knw_clientDataReportingLanguage.alias('rlg0')\
                                   ,((col('t134t.MANDT')==(col('rlg0.clientCode')))\
                                    &(col('lngkey.sourceSystemValue')==(col('rlg0.languageCode')))),how='inner')\
                              .join(erp_T134.alias('t134')\
                                   ,((col('t134t.MANDT').eqNullSafe(col('t134.MANDT')))\
                                    &(col('t134t.MTART').eqNullSafe(col('t134.MTART')))),how='left')\
                              .select(col('t134t.MANDT').alias('clientCode')\
                                     ,col('rlg0.languageCode').alias('languageCode')\
                                     ,col('t134t.MTART').alias('productType')\
                                     ,col('t134t.MTBEZ').alias('productTypeDescription')\
                                     ,lit(isService).alias('isService'))\
                              .distinct()
  
      gen_SAP_L0_TMP_LanguageCodeForT134T_NotExists = erp_T134T.alias('t134t')\
                              .join(gen_SAP_L0_TMP_LanguageCodeForT134T.alias('lct0')\
                                    ,((col('lct0.productType') == col('t134t.MTART'))\
                                    &(col('lct0.clientCode')==(col('t134t.MANDT')))) ,how='leftanti')                               		
      
      df1_productType = gen_SAP_L0_TMP_LanguageCodeForT134T_NotExists.alias('t134t')\
                              .join(erp_T134.alias('t134')\
                                   ,((col('t134t.MANDT').eqNullSafe(col('t134.MANDT')))\
                                   &(col('t134t.MTART').eqNullSafe(col('t134.MTART')))),how='left')\
                              .join (knw_LK_ISOLanguageKey.alias('lngkey')\
                                   ,(col('lngkey.sourceSystemValue').eqNullSafe(col('t134t.SPRAS'))),how="left")\
                              .join (knw_clientDataSystemReportingLanguage.alias('slg0')\
                                    ,((col('slg0.languageCode').eqNullSafe(col('lngkey.targetSystemValue')))\
                                    &(col('slg0.clientCode').eqNullSafe(col('t134t.MANDT')))),how="left")\
                              .select(col('t134t.MANDT').alias('clientCode')\
                                    ,coalesce(col('slg0.languageCode'),lit('NA')).alias('languageCode')\
                                    ,col('t134t.MTART').alias('productType')\
                                    ,col('t134t.MTBEZ').alias('productTypeDescription')\
                                    ,lit(isService).alias('isService'))\

      df2_productType = gen_SAP_L0_TMP_LanguageCodeForT134T.alias('lct0')\
                              .select(col('lct0.clientCode').alias('clientCode')\
                                     ,col('lct0.languageCode').alias('languageCode')\
                                     ,col('lct0.productType').alias('productType')\
                                     ,col('lct0.productTypeDescription').alias('productTypeDescription')\
                                     ,col('lct0.isService').alias('isService'))
       
      gen_SAP_L0_TMP_ProductType = df1_productType.union(df2_productType)

      gen_SAP_L0_TMP_LanguageCodeForT006A = erp_T006A.alias('t006a')\
                              .join(knw_LK_ISOLanguageKey.alias("lngkey")\
                                    ,(col("lngkey.sourceSystemValue")==(col("t006a.SPRAS"))),how="inner")\
                              .join (knw_clientDataReportingLanguage.alias("rlg0")\
                                    ,((col('rlg0.clientCode')==(col('t006a.MANDT')))\
                                    &(col('rlg0.languageCode')==(col('lngkey.targetSystemValue')))),how='inner')\
                              .select(col('t006a.MANDT').alias('clientCode')\
                                    ,col('rlg0.languageCode').alias('languageCode')\
                                    ,col('t006a.MSEHI').alias('baseUnitOfMeasure')\
                                    ,col('t006a.MSEHL').alias('baseUnitOfMeasureDescription'))\
                              .distinct()
      
      gen_SAP_L0_TMP_LanguageCodeForT006A_NotExists = erp_T006A.alias('t006a')\
                              .join(gen_SAP_L0_TMP_LanguageCodeForT006A.alias('lct0')\
                                    ,((col('lct0.baseUnitOfMeasure')==(col('t006a.MSEHI')))\
                                    &(col('lct0.clientCode')==(col('t006a.MANDT')))),how='leftanti')
      
      df1_baseUnitOfMeasure = gen_SAP_L0_TMP_LanguageCodeForT006A_NotExists.alias('t006a')\
                              .join (knw_LK_ISOLanguageKey.alias("lngkey")\
                                     ,(col("lngkey.sourceSystemValue").eqNullSafe(col('t006a.SPRAS'))),how='left')\
                              .join (knw_clientDataSystemReportingLanguage.alias('slg0')\
                                    ,((col('slg0.clientCode').eqNullSafe(col('t006a.MANDT')))\
                                    &(col('slg0.languageCode').eqNullSafe(col('lngkey.targetSystemValue')))),how='left')\
                              .select(col('t006a.MANDT').alias('clientCode')\
                                    ,coalesce(col('slg0.languageCode'),lit('NA')).alias('languageCode')\
                                    ,col('t006a.MSEHI').alias('baseUnitOfMeasure')\
                                    ,col('t006a.MSEHL').alias('baseUnitOfMeasureDescription'))
        
      df2_baseUnitOfMeasure = gen_SAP_L0_TMP_LanguageCodeForT006A.alias('lct0')\
                              .select(col('lct0.clientCode').alias('clientCode')\
                                    ,col('lct0.languageCode').alias('languageCode')\
                                    ,col('lct0.baseUnitOfMeasure').alias('baseUnitOfMeasure')\
                                    ,col('lct0.baseUnitOfMeasureDescription').alias('baseUnitOfMeasureDescription'))
      
      gen_SAP_L0_TMP_ProductBaseUnitOfMeasure = df1_baseUnitOfMeasure.union(df2_baseUnitOfMeasure)

      gen_SAP_L0_TMP_MaterialPlantData = erp_MARC.alias('agg')\
           				      .groupBy('agg.MATNR')\
                              .agg(max('agg.BESKZ').alias('maxValue')\
                                  ,min('agg.BESKZ').alias('minValue'))\
                              .select(col('agg.MATNR').alias('materialNumber')\
                                      ,when(col('maxValue')!= (col('minValue')), lit('X'))\
                                      .otherwise(col('maxValue')).alias('procurementType'))

      gen_SAP_L0_TMP_LanguageCodeForT023T = erp_T023T.alias('t023t')\
                              .join(knw_LK_ISOLanguageKey.alias('lngkey')\
                                    ,(col('lngkey.sourceSystemValue')==(col('t023t.SPRAS'))),how='inner')\
                              .join(knw_clientDataReportingLanguage.alias('rlg0')\
                                    ,((col('rlg0.clientCode')==(col('t023t.MANDT')))\
                                    &(col('rlg0.languageCode')==(col('lngkey.targetSystemValue')))),how='inner')\
                              .select(col('t023t.MANDT').alias('clientCode')\
                                    ,col('rlg0.languageCode').alias('languageCode')\
                                    ,col('t023t.MATKL').alias('productGroup')\
                                    ,col('t023t.WGBEZ').alias('productGroupDescription'))\
                              .distinct()

      gen_SAP_L0_TMP_LanguageCodeForT023T_NotExists = erp_T023T.alias('t023t')\
                              .join(gen_SAP_L0_TMP_LanguageCodeForT023T.alias('lct0')\
                                    ,((col('lct0.productGroup') == col('t023t.MATKL'))\
                                                    &(col('lct0.clientCode')==(col('t023t.MANDT')))) ,how='leftanti')

      df1_productGroup = gen_SAP_L0_TMP_LanguageCodeForT023T_NotExists.alias('t023t')\
                              .join (knw_LK_ISOLanguageKey.alias('lngkey'),(col('lngkey.sourceSystemValue').eqNullSafe(col('t023t.SPRAS'))),how="left")\
                              .join (knw_clientDataSystemReportingLanguage.alias('slg0')\
                                    ,((col('slg0.clientCode').eqNullSafe(col('t023t.MANDT')))\
                                    &(col('slg0.languageCode').eqNullSafe(col('lngkey.targetSystemValue')))),how="left")\
                              .select(col('t023t.MANDT').alias('clientCode')\
                                    ,coalesce(col('slg0.languageCode'),lit('NA')).alias('languageCode')\
                                    ,col('t023t.MATKL').alias('productGroup')\
                                    ,col('t023t.WGBEZ').alias('productGroupDescription'))
                              
      df2_productGroup = gen_SAP_L0_TMP_LanguageCodeForT023T.alias('lct0')\
                              .select(col('lct0.clientCode').alias('clientCode')\
                                    ,col('lct0.languageCode').alias('languageCode')\
                                    ,col('lct0.productGroup').alias('productGroup')\
                                    ,col('lct0.productGroupDescription').alias('productGroupDescription'))
      gen_SAP_L0_TMP_ProductGroup = df1_productGroup.union(df2_productGroup)

##FINAL TABLE
      productProductionType_isnull = when(col('bdt3.targetSystemValue').isNull(),col('mpd0.procurementType'))\
                                     .otherwise(col('bdt3.targetSystemValue')).alias('productProductionType')
      
      
      productTypeKPMG_isnull = when(col('bdt1.targetSystemValue').isNull(),col('t134.MTREF'))\
                               .otherwise(col('bdt1.targetSystemValue')).alias('productTypeKPMG')
      
      gen_L1_MD_Product = erp_MARA.alias('mar0')\
                              .join(knw_usf_clientLanguageCode.alias('rlg0')\
			                        ,(col('mar0.MANDT')==(col('rlg0.clientCode'))),how='inner')\
                              .join(gen_SAP_L0_TMP_ProductName.alias('pnm0')\
                                    ,((col('pnm0.clientCode').eqNullSafe(col('mar0.MANDT')))\
                                    &(col('pnm0.productNumber').eqNullSafe(col('mar0.MATNR')))\
                                    &(col('pnm0.languageCode').eqNullSafe(col('rlg0.languageCode')))),how='left')\
                              .join(gen_SAP_L0_TMP_ProductType.alias('pdt0')\
                                    ,((col('pdt0.clientCode').eqNullSafe(col('mar0.MANDT')))\
                                    &(col('pdt0.productType').eqNullSafe(col('mar0.MTART')))\
                                    &(col('pdt0.languageCode').eqNullSafe(col('rlg0.languageCode')))),how='left')\
                              .join(gen_SAP_L0_TMP_ProductGroup.alias('pdg0')\
                                    ,((col('pdg0.clientCode').eqNullSafe(col('mar0.MANDT')))\
                                    &(col('pdg0.productGroup').eqNullSafe(col('mar0.MATKL')))\
                                    &(col('pdg0.languageCode').eqNullSafe(col('rlg0.languageCode')))),how='left')\
                              .join(gen_SAP_L0_TMP_MaterialPlantData.alias('mpd0')\
			                        ,(col('mpd0.materialNumber').eqNullSafe(col('mar0.MATNR'))),how='left')\
                              .join(gen_SAP_L0_TMP_ProductBaseUnitOfMeasure.alias('bmu0')\
                                    ,((col('bmu0.clientCode').eqNullSafe(col('mar0.MANDT')))\
                                    &(col('bmu0.baseUnitOfMeasure').eqNullSafe(col('mar0.MEINS')))\
                                    &(col('bmu0.languageCode').eqNullSafe(col('rlg0.languageCode')))),how='left')\
                              .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdt0')\
			                        ,((col('bdt0.sourceSystemValue').eqNullSafe(col('mar0.MEINS')))\
				                    &(col('bdt0.businessDatatype').eqNullSafe(lit('Base Unit Of Measure')))\
				              	    &(col('bdt0.targetLanguageCode').eqNullSafe(lit(reportingLanguage)))\
                                    &(col('bdt0.sourceERPSystemID').eqNullSafe(lit(erpSAPSystemID)))),how='left')\
                              .join(erp_T134.alias('t134')\
			                        ,((col('t134.MANDT').eqNullSafe(col('mar0.MANDT')))\
				                    &(col('mar0.MTART').eqNullSafe(col('t134.MTART')))),how='left')\
                              .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdt1')\
			                        ,((col('bdt1.sourceSystemValue').eqNullSafe(col('t134.MTREF')))\
				                    &(col('bdt1.businessDatatype').eqNullSafe(lit('Material Type')))\
				              	    &(col('bdt1.targetLanguageCode').eqNullSafe(lit(reportingLanguage)))\
                                    &(col('bdt1.sourceERPSystemID').eqNullSafe(lit(erpSAPSystemID)))),how='left')\
                              .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdt2')\
			                        ,((col('bdt2.sourceSystemValue').eqNullSafe(col('mar0.MBRSH')))\
				                    &(col('bdt2.businessDatatype').eqNullSafe(lit('Industry Sector')))\
				              	    &(col('bdt2.targetLanguageCode').eqNullSafe(lit(reportingLanguage)))\
                                    &(col('bdt2.sourceERPSystemID').eqNullSafe(lit(erpSAPSystemID)))),how='left')\
                              .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdt3')\
			                        ,((col('bdt3.sourceSystemValue').eqNullSafe(col('mpd0.procurementType')))\
				                    &(col('bdt3.businessDatatype').eqNullSafe(lit('Procurement Type')))\
				              	    &(col('bdt3.targetLanguageCode').eqNullSafe(lit(reportingLanguage)))\
                                    &(col('bdt3.sourceERPSystemID').eqNullSafe(lit(erpSAPSystemID)))),how='left')\
                              .select(col('mar0.MATNR').alias('productArtificialID')\
                                    ,col('mar0.MATNR').alias('productNumber')\
                                    ,lit(None).alias('productCompanyCode')\
                                    ,coalesce(col('rlg0.languageCode'),lit('')).alias('productLanguageCode')\
                                    ,coalesce(col('pnm0.productName'),lit('')).alias('productName')\
                                    ,col('mar0.ERSDA').alias('productCreationDate')\
                                    ,col('mar0.ERNAM').alias('productCreationUser')\
                                    ,col('mar0.LAEDA').alias('productChangeDate')\
                                    ,col('mar0.AENAM').alias('productChangeUser')\
                                    ,when(col('mar0.LVORM')=='X',lit(1)).otherwise(lit(0)).alias('isDeleted')\
                                    ,coalesce(col('pdt0.isService'),lit(0)).alias('isService')\
                                    ,col('mar0.PRDHA').alias('productHierarchyNumber')\
                                    ,col('mar0.BISMT').alias('productPreviousNumber')\
                                    ,col('mar0.EAN11').alias('productInternationalArticleNumber')\
                                    ,col('mar0.MEINS').alias('productBaseUnitOfMeasureClient')\
                                    ,coalesce(col('bmu0.baseUnitOfMeasureDescription'),lit(''))\
                                                      .alias('productBaseUnitOfMeasureClientDescription')\
                                    ,when(col('bdt0.targetSystemValueDescription').isNull(),'')\
                                                      .otherwise(col('bdt0.targetSystemValueDescription'))\
                                                      .alias('productBaseUnitOfMeasureKPMGDescription')\
                                    ,col('mar0.MTART').alias('productTypeClient')\
                                    ,coalesce(col('pdt0.productTypeDescription'),lit(''))\
                                                      .alias('productTypeClientDescription')\
                                    ,when(lit(productTypeKPMG_isnull).isNull(),'').otherwise(lit(productTypeKPMG_isnull))\
                                                      .alias('productTypeKPMG')\
                                    ,when(col('bdt1.targetSystemValueDescription').isNull(),'').otherwise(col('bdt1.targetSystemValueDescription'))\
                                                      .alias('productTypeKPMGDescription')\
                                    ,col('mar0.MATKL').alias('productGroup')\
                                    ,coalesce(col('pdg0.productGroupDescription'),lit(''))\
                                                      .alias('productGroupDescription')\
                                    ,when(col('bdt2.targetSystemValue').isNull(),col('mar0.MBRSH'))\
                                                      .otherwise(col('bdt2.targetSystemValue'))\
                                                      .alias('productIndustrySector')\
                                    ,when(col('bdt2.targetSystemValueDescription').isNull(),'')\
                                                      .otherwise(col('bdt2.targetSystemValueDescription'))\
                                                      .alias('productIndustrySectorDescription')\
                                    ,when(lit(productProductionType_isnull).isNull(),'')\
                                                      .otherwise(lit(productProductionType_isnull))\
                                                      .alias('productProductionType')\
                                    ,when(col('bdt3.targetSystemValueDescription').isNull(),'')\
                                                      .otherwise(col('bdt3.targetSystemValueDescription'))\
                                                      .alias('productProductionTypeDescription')\
                                    ).distinct()
     
      gen_L1_MD_Product =  objDataTransformation.gen_convertToCDMandCache \
         (gen_L1_MD_Product,'gen','L1_MD_Product',targetPath=gl_CDMLayer1Path)
       
      executionStatus = "L1_MD_Product populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
