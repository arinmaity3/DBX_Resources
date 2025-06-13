# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col
def otc_L1_STG_InterCompanyCustomer_populate(): 
  try:
    
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global otc_L1_STG_InterCompanyCustomer
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()

    df_TMP_interCompanyCustomer_L1TDJournal = fin_L1_TD_Journal.alias('jrnl')\
      .join(knw_LK_CD_ReportingSetup.alias('RptSetup'),\
         on='companyCode', how='inner')\
      .select( col('jrnl.companyCode')\
         ,col('RptSetup.clientCode').alias('clientCode')\
         ,lit('3').alias('interCompanyAttribute')\
         ,col('jrnl.customerNumber').alias('customerNumber')\
         ,lit('1').alias('businessPartnerTypeAttributeFlag') \
              )\
      .filter( ~(col("jrnl.customerNumber").isin('#NA#','')) \
                & (col("jrnl.customerNumber").isNotNull()) \
                & (col("jrnl.isInterCoFlag")==1) \
             )\
      .distinct()    
    
    df_TMP_interCompanyCustomer,strmsg=L1_STG_InterCompanyCustomer_AddedDetails(df_TMP_interCompanyCustomer_L1TDJournal,gl_CDMLayer1Path)
    
    df_TMP_interCompanyCustomerDistinct=df_TMP_interCompanyCustomer.select('clientCode','companyCode','customerNumber').distinct()

    col_icFromAcMapping = when(col("icFromAcMapping.interCompanyAttribute")==1 \
        ,col("icFromAcMapping.businessPartnerTypeAttributeFlag")) \
                        .otherwise(lit(3))
    
    col_icFromMaster = when(col("icFromMaster.interCompanyAttribute")==2 \
        ,col("icFromMaster.businessPartnerTypeAttributeFlag")) \
                      .otherwise(lit(3))

    col_icFromTrans = when(col("icFromTrans.interCompanyAttribute")==3 \
        ,col("icFromTrans.businessPartnerTypeAttributeFlag")) \
                      .otherwise(lit(3))

    otc_L1_STG_InterCompanyCustomer =df_TMP_interCompanyCustomerDistinct.alias('cust')\
          .join(df_TMP_interCompanyCustomer.alias('icFromAcMapping'),\
                                              ((col('cust.clientCode').eqNullSafe(col('icFromAcMapping.clientCode')))\
                                                & (col('cust.companyCode').eqNullSafe(col('icFromAcMapping.companyCode')))\
                                                & (col('cust.customerNumber').eqNullSafe(col('icFromAcMapping.customerNumber')))\
                                                & (col('icFromAcMapping.interCompanyAttribute')==lit(1))
                                              ),how='left' \
                                            )\
          .join(df_TMP_interCompanyCustomer.alias('icFromMaster'),\
                                              ((col('cust.clientCode').eqNullSafe(col('icFromMaster.clientCode')))\
                                                & (col('cust.companyCode').eqNullSafe(col('icFromMaster.companyCode')))\
                                                & (col('cust.customerNumber').eqNullSafe(col('icFromMaster.customerNumber')))\
                                                & (col('icFromMaster.interCompanyAttribute')==lit(2))
                                              ),how='left' \
                                            )\
          .join(df_TMP_interCompanyCustomer.alias('icFromTrans'),\
                                              ((col('cust.clientCode').eqNullSafe(col('icFromTrans.clientCode')))\
                                                & (col('cust.companyCode').eqNullSafe(col('icFromTrans.companyCode')))\
                                                & (col('cust.customerNumber').eqNullSafe(col('icFromTrans.customerNumber')))\
                                                & (col('icFromTrans.interCompanyAttribute')==lit(3))
                                              ),how='left' \
                                            )\
          .select(col('cust.clientCode').alias('clientCode')\
                 ,col('cust.companyCode').alias('companyCode')\
                 ,col('cust.customerNumber').alias('customerNumber')\
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
  
    otc_L1_STG_InterCompanyCustomer = objDataTransformation.gen_convertToCDMandCache \
        (otc_L1_STG_InterCompanyCustomer,'otc','L1_STG_InterCompanyCustomer',False)

    executionStatus = "L1_STG_InterCompanyCustomer populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]  

  
def L1_STG_InterCompanyCustomer_AddedDetails(df_TMP_interCompanyCustomer_L1TDJournal,fileLocation):
  try:
    objGenHelper = gen_genericHelper()
    status="Update L1_STG InterCompanyCustomer additional details"
    
    fileFullName=fileLocation+"otc_L1_MD_Customer.delta"
    
    if objGenHelper.file_exist(fileFullName) == True:
      otc_L1_MD_Customer = objGenHelper.gen_readFromFile_perform(fileFullName)      
      
      ##Part 1(FromAcMapping-interCompanyAttribute)
      df_L1_TMP_GLAccountMapping_Data=fin_L1_STG_GLAccountMapping.alias('glam1')\
        .join(otc_L1_MD_Customer.alias('cust'),(col('glam1.accountNumber')==col('cust.customerAccountNumber')),how='inner')\
        .select('cust.companyCode','cust.clientCode','cust.customerNumber','glam1.isIntercompany','glam1.is3rdParty')\
        .filter((col('glam1.isIntercompany')==1) | (col('glam1.is3rdParty')==1))\
        .distinct()
      
      df_TMP_interCompanyCustomer_STGAcountMapping=df_L1_TMP_GLAccountMapping_Data.alias('glam1')\
        .select(lit(1).alias('interCompanyAttribute')\
            ,col('glam1.clientCode').alias('clientCode')\
            ,col('glam1.companyCode').alias('companyCode')\
            ,col('glam1.customerNumber').alias('customerNumber')\
            ,(when(col('glam1.isIntercompany')==1 ,lit(1)).otherwise(lit(2))).alias('businessPartnerTypeAttributeFlag'))\
        .distinct()
      
      ##Part 2(FromMaster-interCompanyAttribute)   
      ##A
      df_TMP_interCompanyCustomer_L1MDCusromer_A=otc_L1_MD_Customer.alias('cust')\
          .select(lit(2).alias('interCompanyAttribute')\
            ,col('cust.clientCode').alias('clientCode')\
            ,col('cust.companyCode').alias('companyCode')\
            ,col('cust.customerNumber').alias('customerNumber')\
            ,lit('1').alias('businessPartnerTypeAttributeFlag')\
                )\
          .filter(((col('cust.customerCompanyID').isNotNull()) & (col('cust.customerCompanyID')!='')))\
          .distinct()      
      
      ##B
      df_L1_TMP_GLACustReconcilationAccountType_Data=otc_L1_MD_Customer.alias('cust')\
        .join(fin_L1_MD_GLAccount.alias('gla'),((col('gla.companyCode')==col('cust.companyCode')) \
             & (col('gla.accountNumber')==col('cust.customerAccountNumber')) ),how='inner')\
         .select('cust.companyCode','cust.clientCode','cust.customerNumber','cust.customerCompanyID')\
         .filter(col('gla.reconcilationAccountType').isin(2,3))\
         .distinct()
      
      #--Except
      df_TMP_interCompanyCustomer_L1MDCusromer_B=df_L1_TMP_GLACustReconcilationAccountType_Data.alias('custactype')\
        .join(df_TMP_interCompanyCustomer_L1MDCusromer_A.alias('custmd'),\
               ((col('custactype.clientCode')==col('custmd.clientCode'))\
               & (col('custactype.companyCode')==col('custmd.companyCode'))\
               & (col('custactype.customerNumber')==col('custmd.customerNumber'))\
               ),how='ANTI' \
             )\
         .select(lit(2).alias('interCompanyAttribute')\
                ,col('custactype.clientCode').alias('clientCode')\
                ,col('custactype.companyCode').alias('companyCode')\
                ,col('custactype.customerNumber').alias('customerNumber')\
                ,lit('2').alias('businessPartnerTypeAttributeFlag')\
                )\
          .distinct()
     
      df_TMP_interCompanyCustomer_L1MDCusromer= df_TMP_interCompanyCustomer_L1MDCusromer_A.unionAll(df_TMP_interCompanyCustomer_L1MDCusromer_B) 
      
      ##Part 3(icFromTransaction-interCompanyAttribute)  
      ##A
      #df_TMP_interCompanyCustomer_JET_A=df_TMP_interCompanyCustomer_L1TDJournal
      df_TMP_interCompanyCustomer_JET_A=df_TMP_interCompanyCustomer_L1TDJournal\
            .select(col('interCompanyAttribute'),col('clientCode'),col('companyCode')\
                   ,col('customerNumber'),col('businessPartnerTypeAttributeFlag'))
      ##B
      df_L1_TMP_GLACustReconcilationAccountType_Data1=df_L1_TMP_GLACustReconcilationAccountType_Data.filter(col('customerCompanyID').isin('','#NA#'))

      df_TMP_interCompanyCustomer_JET_B=df_L1_TMP_GLACustReconcilationAccountType_Data1.alias('custactype')\
        .join(df_TMP_interCompanyCustomer_JET_A.alias('custjet'),\
               ((col('custactype.clientCode')==col('custjet.clientCode'))\
               & (col('custactype.companyCode')==col('custjet.companyCode'))\
               & (col('custactype.customerNumber')==col('custjet.customerNumber'))\
               ),how='ANTI' \
             )\
         .select(lit(3).alias('interCompanyAttribute')\
                ,col('custactype.clientCode').alias('clientCode')\
                ,col('custactype.companyCode').alias('companyCode')\
                ,col('custactype.customerNumber').alias('customerNumber')\
                ,lit('2').alias('businessPartnerTypeAttributeFlag')\
                )\
          .distinct()

      df_TMP_interCompanyCustomer_JET=df_TMP_interCompanyCustomer_JET_A.unionAll(df_TMP_interCompanyCustomer_JET_B) 
           
      df_TMP_interCompanyCustomer=df_TMP_interCompanyCustomer_STGAcountMapping.unionAll(df_TMP_interCompanyCustomer_L1MDCusromer)\
            .unionAll(df_TMP_interCompanyCustomer_JET)
      
      status="Successfully update L1_STG InterCompanyCustomer additional details"
    else:
      
      status="The file L1_MD_Customer does not exists in specified location: "+fileLocation
      
      df_TMP_interCompanyCustomer=df_TMP_interCompanyCustomer_L1TDJournal
    
    return df_TMP_interCompanyCustomer ,status
  
  except Exception as err:
    raise 

##/*
  
##otc_L1_STG_InterCompanyCustomer_populate()

#print(otc_L1_STG_InterCompanyCustomer.filter(col('isInterCoFlag')=='3').count())
#print(otc_L1_STG_InterCompanyCustomer.filter(col('isInterCoFlag')=='2').count())
#print(otc_L1_STG_InterCompanyCustomer.filter(col('isInterCoFlag')=='1').count())
#
# print(otc_L1_STG_InterCompanyCustomer.filter(col('isInterCoFromAccountMapping')=='3').count())
# print(otc_L1_STG_InterCompanyCustomer.filter(col('isInterCoFromAccountMapping')=='2').count())
# print(otc_L1_STG_InterCompanyCustomer.filter(col('isInterCoFromAccountMapping')=='1').count())

# print(otc_L1_STG_InterCompanyCustomer.filter(col('isInterCoFromMasterData')=='3').count())
# print(otc_L1_STG_InterCompanyCustomer.filter(col('isInterCoFromMasterData')=='2').count())
# print(otc_L1_STG_InterCompanyCustomer.filter(col('isInterCoFromMasterData')=='1').count())

#print(otc_L1_STG_InterCompanyCustomer.filter(col('isInterCoFromTransactionData')=='3').count())
#print(otc_L1_STG_InterCompanyCustomer.filter(col('isInterCoFromTransactionData')=='2').count())
#print(otc_L1_STG_InterCompanyCustomer.filter(col('isInterCoFromTransactionData')=='1').count())

##/
