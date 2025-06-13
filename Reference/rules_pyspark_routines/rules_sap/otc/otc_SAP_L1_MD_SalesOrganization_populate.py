# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col,when,coalesce,min,max

def otc_SAP_L1_MD_SalesOrganization_populate(): 
  try:
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()

    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    global otc_L1_MD_SalesOrganization
    	
    otc_SAP_L0_TMP_SalesOrganization = erp_TVKO.alias('tvko_erp')\
                                    .select(col('tvko_erp.MANDT').alias('clientCode')\
                                           ,col('tvko_erp.BUKRS').alias('companyCode')\
                                           ,col('tvko_erp.VKORG').alias('salesOrganization'))\
                                    .distinct()
    otc_SAP_L0_TMP_SalesOrganizationReportingLanguage = erp_TVKO.alias('tvko_erp')\
              .join(erp_TVKOT.alias('tvkot_erp'),((col('tvko_erp.MANDT')==(col('tvkot_erp.MANDT')))&
                           (col('tvko_erp.VKORG')==(col('tvkot_erp.VKORG')))),how='inner')\
              .join (knw_LK_ISOLanguageKey.alias('lngkey'),\
                          (col('lngkey.sourceSystemValue')==(col('tvkot_erp.SPRAS'))),how='inner')\
              .join(knw_clientDataReportingLanguage.alias('rlg0')\
                     ,((col('tvkot_erp.MANDT')==(col('rlg0.clientCode')))&\
                      (col('tvko_erp.BUKRS')==(col('rlg0.companyCode')))&\
                      (col('rlg0.languageCode')==(col('lngkey.targetSystemValue')))),how='inner')\
             .select(col('tvko_erp.MANDT').alias('clientCode')\
                    ,col('tvko_erp.VKORG').alias('salesOrganization')\
                    ,col('tvkot_erp.VTEXT').alias('salesOrganizationDescription')\
                    ,col('rlg0.languageCode').alias('languageCode')\
                    ,col('tvko_erp.BUKRS').alias('companyCode'))\
             .groupBy(col("clientCode"),col("salesOrganization"),col("languageCode"),\
                     lower(col('salesOrganizationDescription')),col("companyCode"))\
             .agg(expr("min(salesOrganizationDescription) as salesOrganizationDescription"))
              
    salesOrganization_NotExists = erp_TVKO.alias('tvko_erp')\
            .join(otc_SAP_L0_TMP_SalesOrganizationReportingLanguage.alias('rpt')\
                ,((col('tvko_erp.MANDT')==(col('rpt.clientCode')))&
                 (col('tvko_erp.VKORG')==(col('rpt.salesOrganization')))&
                 (col('tvko_erp.BUKRS')==(col('rpt.companyCode')))),how='leftanti')
    df_salesOrganization = salesOrganization_NotExists.alias('tvko_erp')\
          .join(erp_TVKOT.alias('tvkot_erp'),((col('tvko_erp.MANDT').eqNullSafe(col('tvkot_erp.MANDT')))&
               (col('tvko_erp.VKORG').eqNullSafe(col('tvkot_erp.VKORG')))),how='left')\
          .join (knw_LK_ISOLanguageKey.alias('lngkey'),\
                 (col('lngkey.sourceSystemValue').eqNullSafe(col('tvkot_erp.SPRAS'))),how='left')\
          .join (knw_clientDataSystemReportingLanguage.alias('rlg2')\
                        ,((col('tvkot_erp.MANDT').eqNullSafe(col('rlg2.clientCode')))\
                           &(col('tvko_erp.BUKRS').eqNullSafe(col('rlg2.companyCode')))\
                         &(col('rlg2.languageCode').eqNullSafe(col('lngkey.targetSystemValue')))),how='left')\
          .select(col('tvko_erp.VKORG').alias('salesOrganization')\
                  ,when(col('tvkot_erp.VTEXT').isNull(),'#NA#')\
                     .otherwise(col('tvkot_erp.VTEXT')).alias('salesOrganizationDescription')\
                  ,when(col('rlg2.languageCode').isNull(),'NA').otherwise(col('rlg2.languageCode'))\
                     .alias('languageCode')\
                 ,col('tvko_erp.BUKRS').alias('companyCode'))\
         .groupBy(col("salesOrganization"),col("languageCode"),\
                      lower(col('salesOrganizationDescription')),col("companyCode"))\
         .agg(expr("min(salesOrganizationDescription) as salesOrganizationDescription"))\
         .select('salesOrganization','salesOrganizationDescription','languageCode','companyCode')
              
    df_otc_SAP_L0_TMP_SalesOrganizationReportingLanguage = otc_SAP_L0_TMP_SalesOrganizationReportingLanguage\
          .select('salesOrganization','salesOrganizationDescription','languageCode','companyCode')
     
    otc_SAP_L0_TMP_SalesOrganizationDescription = df_salesOrganization.union(df_otc_SAP_L0_TMP_SalesOrganizationReportingLanguage)
    otc_SAP_L0_TMP_SalesOrganizationDescription = otc_SAP_L0_TMP_SalesOrganizationDescription\
           .groupBy(col("salesOrganization"),col("languageCode"),\
             lower(col('salesOrganizationDescription')),col("companyCode"))\
           .agg(expr("min(salesOrganizationDescription) as salesOrganizationDescription"))
    
#2
    otc_SAP_L0_TMP_SalesOrganizationDistributionChannelReportingLanguage = erp_TVKOV.alias('tvkov_erp')\
          .join(otc_SAP_L0_TMP_SalesOrganization.alias('lars')\
               ,((col('tvkov_erp.MANDT')==(col('lars.clientCode')))&
               (col('tvkov_erp.VKORG')==(col('lars.salesOrganization')))),how='inner')\
         .join(erp_TVTWT.alias('tvtwt_erp'),((col('tvkov_erp.MANDT')==(col('tvtwt_erp.MANDT')))&
                  (col('tvkov_erp.VTWEG')==(col('tvtwt_erp.VTWEG')))),how='inner')\
         .join(knw_LK_ISOLanguageKey.alias("lngkey"),\
                           (col("lngkey.sourceSystemValue")==(col("tvtwt_erp.SPRAS"))),how="inner")\
         .join(knw_clientDataReportingLanguage.alias("rlg0")\
          ,((col("tvtwt_erp.MANDT")==(col("rlg0.clientCode")))&
                (col('lars.companyCode')==(col('rlg0.companyCode')))&
               (col('rlg0.languageCode')==(col('lngkey.targetSystemValue')))),how="inner")\
         .select(col('tvkov_erp.MANDT').alias('clientCode')\
                ,col('tvkov_erp.VKORG').alias('salesOrganization')\
                ,col('tvkov_erp.VTWEG').alias('distributionChannel')\
                ,col('tvtwt_erp.VTEXT').alias('distributionChannelDescription')\
                ,col('rlg0.languageCode').alias('languageCode')\
                ,col('lars.companyCode').alias('companyCode'))\
         .groupBy(col("clientCode"),col("salesOrganization"),col("distributionChannel"),\
              upper(col('distributionChannelDescription')),col("languageCode"),col("companyCode"))\
         .agg(expr("max(distributionChannelDescription) as distributionChannelDescription"))\
         .select('clientCode','salesOrganization','distributionChannel','distributionChannelDescription','languageCode','companyCode')
    
    df_NotExists_1 = erp_TVKOV.alias('tvkov_erp')\
        .join(otc_SAP_L0_TMP_SalesOrganization.alias('lars')\
             ,((col('tvkov_erp.MANDT')==(col('lars.clientCode')))&
             (col('tvkov_erp.VKORG')==(col('lars.salesOrganization')))),how='inner')\
        .select(col('tvkov_erp.MANDT'),col('tvkov_erp.VKORG'),col('tvkov_erp.VTWEG'),col('lars.companyCode'))
    
    salesOrganizationDistribution_NotExists = df_NotExists_1.alias('tvkov_erp')\
          .join(otc_SAP_L0_TMP_SalesOrganizationDistributionChannelReportingLanguage.alias('rpt'),\
               ((col('tvkov_erp.MANDT')==(col('rpt.clientCode')))&
               (col('tvkov_erp.VKORG')==(col('rpt.salesOrganization')))&
               (col('tvkov_erp.VTWEG')==(col('rpt.distributionChannel')))&
               (col('tvkov_erp.companyCode')==(col('rpt.companyCode')))),how='leftanti')
    
    df_salesOrganizationDistribution = salesOrganizationDistribution_NotExists.alias('tvkov_erp')\
          .join(otc_SAP_L0_TMP_SalesOrganization.alias('lars')\
                         ,((col('tvkov_erp.MANDT')==(col('lars.clientCode')))&
                         (col('tvkov_erp.VKORG')==(col('lars.salesOrganization')))),how='inner')\
          .join(erp_TVTWT.alias('tvtwt_erp'),\
                        ((col('tvkov_erp.MANDT').eqNullSafe(col('tvtwt_erp.MANDT')))&
                        (col('tvkov_erp.VTWEG').eqNullSafe(col('tvtwt_erp.VTWEG')))),how='left')\
          .join(knw_LK_ISOLanguageKey.alias("lngkey"),\
                 (col('lngkey.sourceSystemValue').eqNullSafe(col('tvtwt_erp.SPRAS'))),how="left")\
          .join(knw_clientDataSystemReportingLanguage.alias("rlg2")\
                        ,((col('tvtwt_erp.MANDT').eqNullSafe(col('rlg2.clientCode')))\
                           &(col('lars.companyCode').eqNullSafe(col('rlg2.companyCode')))\
                         &(col('rlg2.languageCode').eqNullSafe(col('lngkey.targetSystemValue')))),how="left")\
          .select(col('tvkov_erp.VKORG').alias('salesOrganization')\
                ,col('tvkov_erp.VTWEG').alias('distributionChannel')\
                ,coalesce(col('tvtwt_erp.VTEXT'),lit('#NA#')).alias('distributionChannelDescription')\
                ,coalesce(col('rlg2.languageCode'),lit('NA')).alias('languageCode')\
                ,col('lars.companyCode').alias('companyCode'))\
          .groupBy(col("salesOrganization"),col("distributionChannel"),\
                      upper(col('distributionChannelDescription')),col("languageCode"),col("companyCode"))\
         .agg(expr("max(distributionChannelDescription) as distributionChannelDescription"))\
         .select('salesOrganization','distributionChannel','distributionChannelDescription','languageCode','companyCode')
                              
    df_otc_SAP_L0_TMP_SalesOrganizationDistributionChannelReportingLanguage = \
    otc_SAP_L0_TMP_SalesOrganizationDistributionChannelReportingLanguage.select('salesOrganization','distributionChannel'\
                         ,'distributionChannelDescription','languageCode','companyCode')
    
    otc_SAP_L0_TMP_SalesOrganizationDistributionChannelDescription = df_salesOrganizationDistribution.\
                  union(df_otc_SAP_L0_TMP_SalesOrganizationDistributionChannelReportingLanguage)\
                .groupBy(col("salesOrganization"),col("distributionChannel"),\
                       upper(col('distributionChannelDescription')),col("languageCode"),col("companyCode"))\
          .agg(expr("max(distributionChannelDescription) as distributionChannelDescription"))\
          .select('salesOrganization','distributionChannel','distributionChannelDescription','languageCode','companyCode')
    
    otc_SAP_L0_TMP_SalesOrganizationDivisionReportingLanguage = erp_TVKOS.alias('tvkos_erp')\
            .join(otc_SAP_L0_TMP_SalesOrganization.alias('lars'),\
                 ((col('tvkos_erp.MANDT')==(col('lars.clientCode')))&
                 (col('tvkos_erp.VKORG')==(col('lars.salesOrganization')))),how='inner')\
           .join(erp_TSPAT.alias('tspat_erp'),\
                ((col('tvkos_erp.MANDT')==(col('tspat_erp.MANDT')))&
                (col('tvkos_erp.SPART')==(col('tspat_erp.SPART')))),how='inner')\
           .join (knw_LK_ISOLanguageKey.alias("lngkey"),\
                     (col("lngkey.sourceSystemValue")==(col("tspat_erp.SPRAS"))),how="inner")\
           .join(knw_clientDataSystemReportingLanguage.alias("rlg0")\
                            ,((col("tspat_erp.MANDT")==(col("rlg0.clientCode")))\
                               &(col('lars.companyCode')==(col('rlg0.companyCode')))\
                             &(col('rlg0.languageCode')==(col('lngkey.targetSystemValue')))),how="inner")\
          .select(col('tvkos_erp.MANDT').alias('clientCode')\
                 ,col('tvkos_erp.VKORG').alias('salesOrganization')\
                 ,col('tvkos_erp.SPART').alias('division')\
                 ,col('tspat_erp.VTEXT').alias('divisionDescription')\
                 ,col('rlg0.languageCode').alias('languageCode')\
                 ,col('lars.companyCode').alias('companyCode')).distinct()
          
    df_NotExists_2 = erp_TVKOS.alias('tvkos_erp')\
              .join(otc_SAP_L0_TMP_SalesOrganization.alias('lars')\
                   ,((col('tvkos_erp.MANDT')==(col('lars.clientCode')))&
                   (col('tvkos_erp.VKORG')==(col('lars.salesOrganization')))),how='inner')\
              .select(col('tvkos_erp.MANDT'),col('tvkos_erp.VKORG'),col('tvkos_erp.SPART'),col('lars.companyCode'))
    
    salesOrganizationDistribution_NotExists = df_NotExists_2.alias('tvkos_erp')\
               .join(otc_SAP_L0_TMP_SalesOrganizationDivisionReportingLanguage.alias('rpt'),\
                    ((col('tvkos_erp.MANDT')==(col('rpt.clientCode')))&
                    (col('tvkos_erp.VKORG')==(col('rpt.salesOrganization')))&
                    (col('tvkos_erp.SPART')==(col('rpt.division')))&
                    (col('tvkos_erp.companyCode')==(col('rpt.companyCode')))),how='leftanti')
        
        
    df_salesOrganizationDivision = salesOrganizationDistribution_NotExists.alias('tvkos_erp')\
              .join(otc_SAP_L0_TMP_SalesOrganization.alias('lars'),\
                    ((col('tvkos_erp.MANDT')==(col('lars.clientCode')))&\
                     (col('tvkos_erp.VKORG')==(col('lars.salesOrganization')))),how='inner')\
              .join(erp_TSPAT.alias('tspat_erp'),\
                    ((col('tvkos_erp.MANDT')==(col('tspat_erp.MANDT')))&\
                     (col('tvkos_erp.SPART')==(col('tspat_erp.SPART')))),how='inner')\
               .join (knw_LK_ISOLanguageKey.alias("lngkey"),\
                   (col("lngkey.sourceSystemValue")==(col("tspat_erp.SPRAS"))),how="left")\
              .join(knw_clientDataSystemReportingLanguage.alias("rlg2")\
                      ,((col("tspat_erp.MANDT").eqNullSafe(col("rlg2.clientCode")))\
                         &(col('lars.companyCode').eqNullSafe(col('rlg2.companyCode')))\
                       &(col('rlg2.languageCode').eqNullSafe(col('lngkey.targetSystemValue')))),how="left")\
              .select(col('tvkos_erp.VKORG').alias('salesOrganization')\
                      ,col('tvkos_erp.SPART').alias('division')\
                      ,coalesce(col('tspat_erp.VTEXT'),lit('#NA#')).alias('divisionDescription')\
                      ,coalesce(col('rlg2.languageCode'),lit('NA')).alias('languageCode')\
                      ,col('lars.companyCode').alias('companyCode')).distinct()
        
    df_otc_SAP_L0_TMP_SalesOrganizationDivisionReportingLanguage = otc_SAP_L0_TMP_SalesOrganizationDivisionReportingLanguage\
                   .select('salesOrganization','division','divisionDescription','languageCode','companyCode')
    otc_SAP_L0_TMP_SalesOrganizationDivisionDescription = df_salesOrganizationDivision.\
                              union(df_otc_SAP_L0_TMP_SalesOrganizationDivisionReportingLanguage)
    
    otc_L1_MD_SalesOrganization = otc_SAP_L0_TMP_SalesOrganization.alias('slorg')\
        .join(otc_SAP_L0_TMP_SalesOrganizationDescription.alias('slorgd'),\
        	    ((col('slorg.salesOrganization').eqNullSafe(col('slorgd.salesOrganization')))&\
        	 (col('slorg.companyCode').eqNullSafe(col('slorgd.companyCode')))),how='left')\
        .join(otc_SAP_L0_TMP_SalesOrganizationDistributionChannelDescription.alias('sldcd'),\
        	       ((col('slorg.salesOrganization').eqNullSafe(col('sldcd.salesOrganization')))&
        		(col('slorg.companyCode').eqNullSafe(col('sldcd.companyCode')))),how='left')\
        .join(otc_SAP_L0_TMP_SalesOrganizationDivisionDescription.alias('sldvd'),\
        	      ((col('slorg.salesOrganization').eqNullSafe(col('sldvd.salesOrganization')))&\
        	   (col('slorg.companyCode').eqNullSafe(col('sldvd.companyCode')))),how='left')\
        .select(col('slorg.companyCode').alias('companyCode')\
        	,col('slorg.salesOrganization').alias('salesOrganization')\
        	,when(col('slorgd.salesOrganizationDescription').isNull(),lit('#NA#'))\
        	         .otherwise(col('slorgd.salesOrganizationDescription')).alias('salesOrganizationDescription')\
        	,when(col('sldcd.distributionChannel').isNull(),lit('#NA#'))\
        	        .otherwise(col('sldcd.distributionChannel')).alias('distributionChannel')\
        	,when(col('sldcd.distributionChannelDescription').isNull(),lit('#NA#'))\
        	        .otherwise(col('sldcd.distributionChannelDescription'))\
                    .alias('distributionChannelDescription')\
        	,when(col('sldvd.division').isNull(),lit('#NA#'))\
        	       .otherwise(col('sldvd.division')).alias('division')\
        	,when(col('sldvd.divisionDescription').isNull(),lit('#NA#'))\
        	       .otherwise(col('sldvd.divisionDescription')).alias('divisionDescription'))\
            .distinct()
          
            
    otc_L1_MD_SalesOrganization =  objDataTransformation.gen_convertToCDMandCache \
          (otc_L1_MD_SalesOrganization,'otc','L1_MD_SalesOrganization',targetPath=gl_CDMLayer1Path)
       
    executionStatus = "L1_MD_SalesOrganization populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

