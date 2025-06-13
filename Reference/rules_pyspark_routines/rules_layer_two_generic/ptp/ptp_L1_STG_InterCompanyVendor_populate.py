# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col

def ptp_L1_STG_InterCompanyVendor_populate(): 
  try:
    
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global ptp_L1_STG_InterCompanyVendor
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()

    df_TMP_interCompanyVendor_L1TDJournal = fin_L1_TD_Journal.alias('jrnl')\
      .join(knw_LK_CD_ReportingSetup.alias('RptSetup'),\
         on='companyCode', how='inner')\
      .select( col('jrnl.companyCode')\
         ,col('RptSetup.clientCode').alias('clientCode')\
         ,lit('3').alias('interCompanyAttribute')\
         ,col('jrnl.vendorNumber').alias('vendorNumber')\
         ,lit('1').alias('businessPartnerTypeAttributeFlag') \
              )\
      .filter( ~(col("jrnl.vendorNumber").isin('#NA#','')) \
                & (col("jrnl.vendorNumber").isNotNull()) \
                & (col("jrnl.isInterCoFlag")==1) \
             )\
      .distinct()    
    
    df_TMP_interCompanyVendor,strmsg=L1_STG_InterCompanyVendor_AddedDetails(df_TMP_interCompanyVendor_L1TDJournal,gl_CDMLayer1Path)
    
    df_TMP_interCompanyVendorDistinct=df_TMP_interCompanyVendor.select('clientCode','companyCode','vendorNumber').distinct()

    col_icFromAcMapping = when(col("icFromAcMapping.interCompanyAttribute")==1 \
                                     , col("icFromAcMapping.businessPartnerTypeAttributeFlag")) \
                                .otherwise(lit(3))
    
    col_icFromMaster = when(col("icFromMaster.interCompanyAttribute")==2 \
                                     , col("icFromMaster.businessPartnerTypeAttributeFlag")) \
                                .otherwise(lit(3))

    col_icFromTrans = when(col("icFromTrans.interCompanyAttribute")==3 \
                                     , col("icFromTrans.businessPartnerTypeAttributeFlag")) \
                                .otherwise(lit(3))

    ptp_L1_STG_InterCompanyVendor =df_TMP_interCompanyVendorDistinct.alias('vnd')\
          .join(df_TMP_interCompanyVendor.alias('icFromAcMapping'),\
                                              ((col('vnd.clientCode').eqNullSafe(col('icFromAcMapping.clientCode')))\
                                                & (col('vnd.companyCode').eqNullSafe(col('icFromAcMapping.companyCode')))\
                                                & (col('vnd.vendorNumber').eqNullSafe(col('icFromAcMapping.vendorNumber')))\
                                                & (col('icFromAcMapping.interCompanyAttribute')==lit(1))
                                              ),how='left' \
                                            )\
          .join(df_TMP_interCompanyVendor.alias('icFromMaster'),\
                                              ((col('vnd.clientCode').eqNullSafe(col('icFromMaster.clientCode')))\
                                                & (col('vnd.companyCode').eqNullSafe(col('icFromMaster.companyCode')))\
                                                & (col('vnd.vendorNumber').eqNullSafe(col('icFromMaster.vendorNumber')))\
                                                & (col('icFromMaster.interCompanyAttribute')==lit(2))
                                              ),how='left' \
                                            )\
          .join(df_TMP_interCompanyVendor.alias('icFromTrans'),\
                                              ((col('vnd.clientCode').eqNullSafe(col('icFromTrans.clientCode')))\
                                                & (col('vnd.companyCode').eqNullSafe(col('icFromTrans.companyCode')))\
                                                & (col('vnd.vendorNumber').eqNullSafe(col('icFromTrans.vendorNumber')))\
                                                & (col('icFromTrans.interCompanyAttribute')==lit(3))
                                              ),how='left' \
                                            )\
          .select(col('vnd.clientCode').alias('clientCode')\
                 ,col('vnd.companyCode').alias('companyCode')\
                 ,col('vnd.vendorNumber').alias('vendorNumber')\
                 ,lit(col_icFromAcMapping).alias('isInterCoFromAccountMapping')\
                 ,lit(col_icFromMaster).alias('isInterCoFromMasterData')\
                 ,lit(col_icFromTrans).alias('isInterCoFromTransactionData')\
                 ).withColumn("isInterCoFlag", when((col('isInterCoFromAccountMapping')==1)\
                                              | (col('isInterCoFromMasterData')==1) \
                                              | (col('isInterCoFromTransactionData')==1) \
                                              ,lit(1))\
                                         .when((col('isInterCoFromAccountMapping')==2)\
                                              | (col('isInterCoFromMasterData')==2) \
                                              | (col('isInterCoFromTransactionData')==2) \
                                              ,lit(2))\
                                         .when((col('isInterCoFromAccountMapping')==3)\
                                              | (col('isInterCoFromMasterData')==3) \
                                              | (col('isInterCoFromTransactionData')==3) \
                                              ,lit(3))\
                                          .otherwise(lit(3))\
                                )        
  
    ptp_L1_STG_InterCompanyVendor = objDataTransformation.gen_convertToCDMandCache \
        (ptp_L1_STG_InterCompanyVendor,'ptp','L1_STG_InterCompanyVendor',False)

    executionStatus = "L1_STG_InterCompanyVendor populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus] 
 
  
def L1_STG_InterCompanyVendor_AddedDetails(df_TMP_interCompanyVendor_L1TDJournal,fileLocation):
  try:
    objGenHelper = gen_genericHelper()
    status="Update L1_STG InterCompanyVendor additional details"
    
    fileFullName=fileLocation+"ptp_L1_MD_Vendor.delta"
    
    if objGenHelper.file_exist(fileFullName) == True:
      ptp_L1_MD_Vendor = objGenHelper.gen_readFromFile_perform(fileFullName)      
      
      ##Part 1(FromAcMapping-interCompanyAttribute)
      df_L1_TMP_GLAccountMapping_Data=fin_L1_STG_GLAccountMapping.alias('glam1')\
        .join(ptp_L1_MD_Vendor.alias('vnd0'),(col('glam1.accountNumber')==col('vnd0.vendorAccountNumber')),how='inner')\
        .select(col('vnd0.vendorCompanyCode').alias('companyCode')\
                ,col('vnd0.vendorClientCode').alias('clientCode')\
                ,col('vnd0.vendorNumber').alias('vendorNumber')\
                ,col('glam1.isIntercompany').alias('isIntercompany')\
                ,col('glam1.is3rdParty').alias('is3rdParty'))\
        .filter((col('glam1.isIntercompany')==1) | (col('glam1.is3rdParty')==1))\
        .distinct()
      
      df_TMP_interCompanyVendor_STGAcountMapping=df_L1_TMP_GLAccountMapping_Data.alias('glam1')\
        .select(lit(1).alias('interCompanyAttribute')\
            ,col('glam1.clientCode').alias('clientCode')\
            ,col('glam1.companyCode').alias('companyCode')\
            ,col('glam1.vendorNumber').alias('vendorNumber')\
            ,(when(col('glam1.isIntercompany')==1 ,lit(1)).otherwise(lit(2))).alias('businessPartnerTypeAttributeFlag'))\
        .distinct()
      
      ##Part 2(FromMaster-interCompanyAttribute)   
      ##A
      df_TMP_interCompanyVendor_L1MDVendor_A=ptp_L1_MD_Vendor.alias('vnd0')\
          .select(lit(2).alias('interCompanyAttribute')\
            ,col('vnd0.vendorClientCode').alias('clientCode')\
            ,col('vnd0.vendorCompanyCode').alias('companyCode')\
            ,col('vnd0.vendorNumber').alias('vendorNumber')\
            ,lit('1').alias('businessPartnerTypeAttributeFlag')\
                )\
          .filter(((col('vnd0.vendorCompanyID').isNotNull()) & (col('vnd0.vendorCompanyID')!='')))\
          .distinct()      
      
      ##B
      df_L1_TMP_GLAVndReconcilationAccountType_Data=ptp_L1_MD_Vendor.alias('vnd0')\
        .join(fin_L1_MD_GLAccount.alias('gla'),((col('gla.companyCode')==col('vnd0.vendorCompanyCode')) \
             & (col('gla.accountNumber')==col('vnd0.vendorAccountNumber')) ),how='inner')\
         .select(col('vnd0.vendorCompanyCode').alias('companyCode')\
                ,col('vnd0.vendorClientCode').alias('clientCode')\
                ,col('vnd0.vendorNumber').alias('vendorNumber')\
                ,col('vnd0.vendorCompanyID').alias('vendorCompanyID'))\
         .filter(col('gla.reconcilationAccountType').isin(2,3))\
         .distinct()
      
      #--Except
      df_TMP_interCompanyVendor_L1MDVendor_B=df_L1_TMP_GLAVndReconcilationAccountType_Data.alias('vndactype')\
        .join(df_TMP_interCompanyVendor_L1MDVendor_A.alias('vnd0'),\
               ((col('vndactype.clientCode')==col('vnd0.clientCode'))\
               & (col('vndactype.companyCode')==col('vnd0.companyCode'))\
               & (col('vndactype.vendorNumber')==col('vnd0.vendorNumber'))\
               ),how='ANTI' \
             )\
         .select(lit(2).alias('interCompanyAttribute')\
                ,col('vndactype.clientCode').alias('clientCode')\
                ,col('vndactype.companyCode').alias('companyCode')\
                ,col('vndactype.vendorNumber').alias('vendorNumber')\
                ,lit('2').alias('businessPartnerTypeAttributeFlag')\
                )\
          .distinct()
     
      df_TMP_interCompanyVendor_L1MDVendor= df_TMP_interCompanyVendor_L1MDVendor_A.unionAll(df_TMP_interCompanyVendor_L1MDVendor_B) 
      
      ##Part 3(icFromTransaction-interCompanyAttribute)  
      ##A
      #df_TMP_interCompanyVendor_JET_A=df_TMP_interCompanyVendor_L1TDJournal
      df_TMP_interCompanyVendor_JET_A=df_TMP_interCompanyVendor_L1TDJournal\
            .select(col('interCompanyAttribute'),col('clientCode'),col('companyCode')\
                   ,col('vendorNumber'),col('businessPartnerTypeAttributeFlag'))
      ##B
      df_L1_TMP_GLAVndReconcilationAccountType_Data1=df_L1_TMP_GLAVndReconcilationAccountType_Data.filter(col('vendorCompanyID').isin('','#NA#'))

      df_TMP_interCompanyVendor_JET_B=df_L1_TMP_GLAVndReconcilationAccountType_Data1.alias('vndactype')\
        .join(df_TMP_interCompanyVendor_JET_A.alias('vndjet'),\
               ((col('vndactype.clientCode')==col('vndjet.clientCode'))\
               & (col('vndactype.companyCode')==col('vndjet.companyCode'))\
               & (col('vndactype.vendorNumber')==col('vndjet.vendorNumber'))\
               ),how='ANTI' \
             )\
         .select(lit(3).alias('interCompanyAttribute')\
                ,col('vndactype.clientCode').alias('clientCode')\
                ,col('vndactype.companyCode').alias('companyCode')\
                ,col('vndactype.vendorNumber').alias('vendorNumber')\
                ,lit('2').alias('businessPartnerTypeAttributeFlag')\
                )\
          .distinct()

      df_TMP_interCompanyVendor_JET=df_TMP_interCompanyVendor_JET_A.unionAll(df_TMP_interCompanyVendor_JET_B) 
           
      df_TMP_interCompanyVendor=df_TMP_interCompanyVendor_STGAcountMapping.unionAll(df_TMP_interCompanyVendor_L1MDVendor)\
            .unionAll(df_TMP_interCompanyVendor_JET)
      
      status="Successfully update L1_STG InterCompanyVendor additional details"
    else:
      #df_l2DimUserAddedInfo=df_L2_user.drop('companyCode')
      status="The file L1_MD_Vendor does not exists in specified location: "+fileLocation
      
      df_TMP_interCompanyVendor=df_TMP_interCompanyVendor_L1TDJournal
    
    return df_TMP_interCompanyVendor ,status
  
  except Exception as err:
    raise 

##/*
  
##ptp_L1_STG_InterCompanyVendor_populate()

#print(ptp_L1_STG_InterCompanyVendor.filter(col('isInterCoFlag')=='3').count())
#print(ptp_L1_STG_InterCompanyVendor.filter(col('isInterCoFlag')=='2').count())
#print(ptp_L1_STG_InterCompanyVendor.filter(col('isInterCoFlag')=='1').count())
#
# print(ptp_L1_STG_InterCompanyVendor.filter(col('isInterCoFromAccountMapping')=='3').count())
# print(ptp_L1_STG_InterCompanyVendor.filter(col('isInterCoFromAccountMapping')=='2').count())
# print(ptp_L1_STG_InterCompanyVendor.filter(col('isInterCoFromAccountMapping')=='1').count())

# print(ptp_L1_STG_InterCompanyVendor.filter(col('isInterCoFromMasterData')=='3').count())
# print(ptp_L1_STG_InterCompanyVendor.filter(col('isInterCoFromMasterData')=='2').count())
# print(ptpL1_STG_InterCompanyVendor.filter(col('isInterCoFromMasterData')=='1').count())

#print(ptp_L1_STG_InterCompanyVendor.filter(col('isInterCoFromTransactionData')=='3').count())
#print(ptp_L1_STG_InterCompanyVendor.filter(col('isInterCoFromTransactionData')=='2').count())
#print(ptp_L1_STG_InterCompanyVendor.filter(col('isInterCoFromTransactionData')=='1').count())

##/
