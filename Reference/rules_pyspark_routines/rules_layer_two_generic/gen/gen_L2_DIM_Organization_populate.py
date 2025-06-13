# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import StringType,DateType
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col,concat,coalesce

def gen_L2_DIM_Organization_populate(): 
  """ Populate L2_DIM_Organization """
  try:
    
    global gen_L2_DIM_Organization
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    #organizationCollectSchema = StructType([StructField("companyCode",StringType(),True)])
    
    analysisid = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ANALYSISID'))   
 
    df_CompnayCode = spark.createDataFrame(gl_lstOfOrganizationCollect,organizationSchemaCollect)\
                      .select(col("companyCode")).distinct() 
   
    df_org1 = df_CompnayCode.alias('orgct1').join(knw_LK_CD_ReportingSetup.alias('RptSetup'),\
                 on='companyCode', how='inner')\
                .select( ('orgct1.companyCode'),lit('#NA#').alias('companyName'),\
                        lit(None).cast('String').alias('countryCode'),\
                        lit(None).cast('String').alias('countryName'),\
                        lit(None).cast('String').alias('companyCity'),\
                        col('RptSetup.chartOfAccount').alias('chartOfAccounts'),\
                        col('RptSetup.localCurrencyCode').alias('localCurrency'),\
                        lit(analysisid).alias('analysisid'),\
                        col('RptSetup.clientCode').alias('clientID'),\
                        col('RptSetup.clientName').alias('clientName'),\
                        col('RptSetup.reportingGroup').alias('reportingGroup'),\
                        col('RptSetup.reportingLanguageCode').alias('reportingLanguage'),\
                        col('RptSetup.reportingCurrencyCode').alias('reportingCurrency'),\
                        lit(None).cast('String').alias('reportingLedger')                    
                       ).distinct()

    df_org1,status = gen_L2_DIM_Organization_AddedDetails(df_org1,gl_CDMLayer1Path)

    df_org2=knw_LK_CD_ReportingSetup.select(lit('#NA#').alias('companyCode'),\
                                                lit('#NA#').alias('companyName'),\
                                                lit(None).cast('String').alias('countryCode'),\
                                                lit('#NA#').alias('countryName'),\
                                                lit('#NA#').alias('companyCity'),\
                                                lit('#NA#').alias('chartOfAccounts'),\
                                                lit(None).cast('String').alias('localCurrency'),\
                                                lit(analysisid).alias('analysisid'),\
                                                col('clientCode').alias('clientID'),\
                                                col('clientName').alias('clientName'),\
                                                lit('#NA#').alias('reportingGroup'),\
                                                lit(None).cast('String').alias('reportingLanguage'),\
                                                lit(None).cast('String').alias('reportingCurrency'),\
                                                lit(None).cast('String').alias('reportingLedger')                    
                                              ).distinct()

    df_org = df_org1.union(df_org2)
    dr_wrw= Window.orderBy(lit('A'))
    gen_L2_DIM_Organization = df_org.select(F.row_number().over(dr_wrw).alias("organizationUnitSurrogateKey"),\
                                            "companyCode","companyName","countryCode",\
                                            "countryName","companyCity","analysisid",\
                                            "chartOfAccounts","localCurrency","clientID","clientName",\
                                            "reportingGroup","reportingLanguage",\
                                            "reportingCurrency","reportingLedger",\
                                            lit("-1").alias("segment01SurrogateKey"),\
                                            lit("-1").alias("segment02SurrogateKey"),\
                                            lit("-1").alias("segment03SurrogateKey"),\
                                            lit("-1").alias("segment04SurrogateKey"),\
                                            lit("-1").alias("segment05SurrogateKey"),\
                                            lit("0").alias("phaseID"),\
                                           )
   
    gen_L2_DIM_Organization = objDataTransformation.gen_convertToCDMandCache(gen_L2_DIM_Organization,\
                                          'gen','L2_DIM_Organization',False)
    #dwh view logic
    df_dwh_Organization=gen_L2_DIM_Organization.alias('orgl2')\
                .select(col('orgl2.analysisid').alias('analysisid')\
                        ,col('orgl2.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
                        ,col('orgl2.chartOfAccounts').alias('chartOfAccounts')\
                        ,col('orgl2.companyCity').alias('city')\
                        ,col('orgl2.clientID').alias('clientCode')\
                        ,col('orgl2.clientName').alias('clientName')\
                        ,col('orgl2.companyCode').alias('companyCode')\
                        ,concat(col('orgl2.companyCode'),lit(' ('),col('orgl2.companyName'),lit(')')).\
                                   alias('companyCodeDescription')\
                        ,coalesce(col('orgl2.localCurrency'),lit('NONE')).alias('companyLocalCurrencyKey')
                        ,col('orgl2.companyName').alias('companyName')\
                        ,col('orgl2.countryName').alias('countryName')\
                        ,col('orgl2.reportingGroup').alias('reportingGroup')\
                        ,col('orgl2.reportingCurrency').alias('reportingGroupCurrency')\
                        ,col('orgl2.reportingLanguage').alias('reportingGroupLanguage')\
                        ,coalesce(col('orgl2.localCurrency'),lit('')).alias('localCurrency')    
                      )
    #Writing to parquet file in dwh schema format
    dwh_vw_DIM_Organization = objDataTransformation.gen_convertToCDMStructure_generate(\
                                df_dwh_Organization,'dwh','vw_DIM_Organization',False)[0]
    keysAndValues = {'organizationUnitSurrogateKey':0,'analysisID':analysisid}
    dwh_vw_DIM_Organization = objGenHelper.gen_placeHolderRecords_populate('dwh.vw_DIM_Organization',\
                                            keysAndValues,dwh_vw_DIM_Organization)  
    objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_Organization,gl_CDMLayer2Path + "gen_L2_DIM_Organization.parquet" ) 

    executionStatus = "L2_DIM_Organization populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)

    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

def gen_L2_DIM_Organization_AddedDetails(df_org1,fileLocation):
  try:
    
    objGenHelper = gen_genericHelper()

    fileFullName = fileLocation+"gen_L1_MD_Organization.delta"
    
    if objGenHelper.file_exist(fileFullName) == True:
      gen_L1_MD_Organization = objGenHelper.gen_readFromFile_perform(fileFullName)
          
      isnull_expr = when(col('orga1.companyCode').isNull(),'').otherwise(col('orga1.companyCode'))
      nullif_expr = when(lit(isnull_expr) == lit('#NA#'), lit('') )\
                    .otherwise(lit(isnull_expr))
      df_org1 = df_org1.alias('orgct1')\
                 .join(gen_L1_MD_Organization.alias('orga1')\
                       ,(col('orga1.companyCode').eqNullSafe(col('orgct1.companyCode'))),how='left')\
                .filter(lit(nullif_expr)!= '#NA#')\
                .select(col('orgct1.companyCode').alias('companyCode')\
                       ,when(col('orga1.companyCodeDescription').isNull(),'#NA#').otherwise(col('orga1.companyCodeDescription'))\
                            .alias('companyName')\
                       ,col('orga1.countryCode').alias('countryCode')\
                       ,col('orga1.countryName').alias('countryName')\
                       ,col('orga1.city').alias('companyCity'),col('orgct1.chartOfAccounts').alias('chartOfAccounts')\
                       ,when(col('orga1.localCurrency').isNull(),col('orgct1.localCurrency'))\
                               .otherwise(col('orga1.localCurrency')).alias('localCurrency')\
                       ,col('orgct1.analysisID').alias('analysisID')\
                       ,col('orgct1.clientID').alias('clientID')\
                       ,col('orgct1.clientName').alias('clientName')\
                       ,col('orgct1.reportingGroup').alias('reportingGroup')\
                       ,col('orgct1.reportingLanguage').alias('reportingLanguage')\
                       ,col('orgct1.reportingCurrency').alias('reportingCurrency')\
                       ,col('orgct1.reportingLedger').alias('reportingLedger'))\
               .distinct()
      status="Successfully update L2 Organization additional details"   
      
    else:
      status="The file L1 Organization does not exists in specified location: "+fileLocation
      df_org1 = df_org1
         
    return df_org1,status
  except Exception as err:
    raise
