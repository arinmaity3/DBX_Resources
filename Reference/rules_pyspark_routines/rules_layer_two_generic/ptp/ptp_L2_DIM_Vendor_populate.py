# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number
from dateutil.parser import parse
from delta.tables import *

def ptp_L2_DIM_Vendor_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global ptp_L2_DIM_Vendor 

    w = Window().orderBy(lit('vendorSurrogateKey'))
    ptp_L1_TMP_Vendor = spark.createDataFrame(gl_lstOfVendorCollect,vendorSchemaCollect).\
                        select(col("companyCode"),col("vendorNumber")).distinct()

    dfL2_DIM_Vendor = ptp_L1_TMP_Vendor.alias("vendor").join \
                (gen_L2_DIM_Organization.alias("org"), \
                [(col("vendor.companyCode") == col("org.companyCode"))],how = "inner")\
                .select(col("org.organizationUnitSurrogateKey")\
                 ,col("vendor.vendorNumber")\
                 ,col("vendor.companyCode"))\
                .withColumn("vendorName", lit("#NA#"))\
                .withColumn("vendorSurrogateKey", row_number().over(w))\
                .withColumn("vendorCreationUser", lit("#NA#"))\
                .withColumn("vendorCreationDate", lit("1900-01-01"))\
                .withColumn("vendorCompanyGroup", lit("#NA#"))\
                .withColumn("vendorCountry", lit("#NA#"))\
                .withColumn("vendorCity", lit("#NA#"))\
                .withColumn("vendorIndustryType", lit("#NA#"))\
                .withColumn("isOneTimeVendor", lit("0"))\
                .withColumn("isBlockedForPosting", lit("0"))\
                .withColumn("isBlockedForPayment", lit("0"))\
                .withColumn("isBlockedForManualPayment", lit("0"))\
                .withColumn("vendorCompanyGroupDescription", lit("#NA#"))\

    dfL2_DIM_Vendor = dfL2_DIM_Vendor.alias("vendor").join\
                (ptp_L1_STG_InterCompanyVendor.alias("inc"), \
                [((col("vendor.companyCode").eqNullSafe(col("inc.companyCode"))) & \
                (col("vendor.vendorNumber").eqNullSafe(col("inc.vendorNumber"))))],how = "left")\
                .select(col("vendor.*"),\
                 expr("case when isInterCoFromAccountMapping is null then '3' else isInterCoFromAccountMapping end").\
                        alias("vendorIntercompanyTypeFromAccountMapping"),\
                 expr("case when isInterCoFromMasterData is null then '3' else isInterCoFromMasterData end").\
                        alias("vendorIntercompanyTypeFromMasterData"),\
                 expr("case when isInterCoFromTransactionData is null then '3' else isInterCoFromTransactionData end").\
                        alias("vendorIntercompanyTypeFromTransactionData"),\
                 expr("case when isintercoFlag is null then '3' else isintercoFlag end").\
                        alias("vendorIntercompanyType"))

    ptp_L2_DIM_Vendor = objDataTransformation.gen_convertToCDMandCache \
        (dfL2_DIM_Vendor,'ptp','L2_DIM_Vendor',False)

    status=L2_Vendor_AddedDetails(ptp_L2_DIM_Vendor,gl_CDMLayer1Path)

    __dwhVendor_populate(objGenHelper,objDataTransformation,ptp_L2_DIM_Vendor)

    executionStatus = "ptp_L2_DIM_Vendor population"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
  
def L2_Vendor_AddedDetails(ptp_L2_DIM_Vendor,fileLocation):
  try:

    objGenHelper = gen_genericHelper()
    status="Update L2 Vendor additional details"
    fileFullName=fileLocation+"ptp_L1_MD_Vendor.delta"

    if objGenHelper.file_exist(fileFullName) == True:
      ptp_L1_MD_Vendor = objGenHelper.gen_readFromFile_perform(fileFullName)  
      df_L2_DIM_Vendor= ptp_L2_DIM_Vendor.alias("vendor")\
          .join(gen_L2_DIM_Organization.alias("org"), \
                  [(col("vendor.organizationUnitSurrogateKey") == col("org.organizationUnitSurrogateKey"))],how = "inner")\
          .join (ptp_L1_MD_Vendor.alias("l1vendor"), \
                  [((col("org.companyCode") == col("l1vendor.vendorCompanyCode") ) &\
                   (col("vendor.vendorNumber") == col("l1vendor.vendorNumber") ) \
                   )],how = "left")\
           .join(ptp_L2_DIM_Vendor.alias("vendor_2"), \
                  [((col("org.companyCode") == col("l1vendor.vendorCompanyCode")) & \
                   (col("vendor_2.vendorNumber") == col("l1vendor.vendorPreviousNumber")) \
                   )],how = "left")\
           .join(ptp_L2_DIM_Vendor.alias("vendor_3"), \
                  [((col("org.companyCode") == col("l1vendor.vendorCompanyCode")) &\
                   (col("vendor.vendorNumber") == col("l1vendor.vendorAlternativePayeeNumberKPMG")) \
                   )],how = "left")\
          .select(col("vendor.vendorSurrogateKey")\
                  ,col("vendor.organizationUnitSurrogateKey")\
                  ,col("vendor_2.vendorSurrogateKey").alias("vendorPreviousSurrogateKey")\
                  ,col("vendor_3.vendorSurrogateKey").alias("vendorPayeeSurrogateKey")\
                  ,col("vendor.vendorNumber")\
                  ,col("l1vendor.vendorName").alias("vendorName")\
                  ,col("l1vendor.vendorCreationUser").alias("vendorCreationUser")\
                  ,col("l1vendor.vendorCreationDate")\
                  ,col("l1vendor.vendorCompanyGroup")\
                  ,col("l1vendor.vendorCountryCode").alias("vendorCountry")\
                  ,col("l1vendor.vendorCity")\
                  ,col("l1vendor.vendorIndustryType")\
                  ,col("l1vendor.isOneTimeVendor")\
                  ,col("l1vendor.isPostingBlockedKPMG").alias("isBlockedForPosting")\
                  ,col("l1vendor.isBlockedForPaymentKPMG").alias("isBlockedForPayment")\
                  ,col("l1vendor.isBlockedForManualPayment")\
                  ,col("l1vendor.vendorCompanyGroupDescription")\
                 ) 
      if df_L2_DIM_Vendor.count() >0:
        deltaL2venderPath=gl_Layer2Staging+"ptp_L2_DIM_Vendor.delta"
        deltaL2vender = DeltaTable.forPath(spark, deltaL2venderPath)
        #df_deltaL2venderUpdate = deltaL2vender.toDF()
        deltaL2vender.alias("l2vender")\
          .merge( df_L2_DIM_Vendor.alias("l1vender"),"l2vender.vendorSurrogateKey=l1vender.vendorSurrogateKey")\
          .whenMatchedUpdate(set =
            {
              "vendorPreviousSurrogateKey":"l1vender.vendorPreviousSurrogateKey",
              "vendorPayeeSurrogateKey":"l1vender.vendorPayeeSurrogateKey",
              "vendorName":"l1vender.vendorName",
              "vendorCreationUser":"l1vender.vendorCreationUser",
               "vendorCreationDate":"l1vender.vendorCreationDate",  
               "vendorCompanyGroup":"l1vender.vendorCompanyGroup",  
               "vendorCountry":"l1vender.vendorCountry",  
               "vendorCity":"l1vender.vendorCity",  
               "vendorIndustryType":"l1vender.vendorIndustryType",
               "isOneTimeVendor":"l1vender.isOneTimeVendor",
              "isBlockedForPosting":"l1vender.isBlockedForPosting",  
              "isBlockedForPayment":"l1vender.isBlockedForPayment",  
              "isBlockedForManualPayment":"l1vender.isBlockedForManualPayment", 
              "vendorCompanyGroupDescription":"l1vender.vendorCompanyGroupDescription"
            }
                      )\
          .execute()   
      #dfUpdates = ptp_L2_DIM_Vendor.toDF()
      status="Successfully update L2 Vendor additional details"
    else:
      status="The file L1 vendor does not exists in specified location: "+fileLocation
  except Exception as err:
    raise
  finally:
    return status

def __dwhVendor_populate(objGenHelper,objDataTransformation,ptp_L2_DIM_Vendor):
  try:
        
    genricERPSystemID = 10
    busienssDatatypes = ['One Time Vendor',
                         'Is InterCompany From MasterData',
                         'Is Intercompany From Accountmapping',
                         'Is InterCompany From TransactionData']
    
    dfBusinessDatatypes = gl_metadataDictionary['knw_LK_GD_BusinessDatatypeValueMapping'].\
                         select(col("businessDatatype"),col("sourceSystemValue"),\
                               col("sourceSystemValueDescription"),col("targetLanguageCode")).\
                         filter((col("businessDatatype").isin(busienssDatatypes)) & \
                               (col("targetERPSystemID") == genricERPSystemID)) 
    dfBusinessDatatypes.cache()

    isOneTimeVendor = when(col('ven.isOneTimeVendor') == 0, lit('0')) \
                      .when(col('ven.isOneTimeVendor')== 1 ,lit('1'))                  

    isIntercompanyFromAccountmapping = when(col('ven.vendorIntercompanyTypeFromAccountMapping') == 1, lit('1')) \
                        .when(col('ven.vendorIntercompanyTypeFromAccountMapping') == 2 ,lit('2'))  \
                        .when(col('ven.vendorIntercompanyTypeFromAccountMapping') == 3 ,lit('NA'))

    isInterCompanyFromMasterData = when(col('ven.vendorIntercompanyTypeFromMasterData') == 1, lit('1')) \
                        .when(col('ven.vendorIntercompanyTypeFromMasterData') == 2 ,lit('2'))  \
                        .when(col('ven.vendorIntercompanyTypeFromMasterData') == 3 ,lit('NA'))

    isInterCompanyFromTransactionData = when(col('ven.vendorIntercompanyTypeFromTransactionData') == 1, lit('1')) \
                        .when(col('ven.vendorIntercompanyTypeFromTransactionData') == 2 ,lit('2'))  \
                        .when(col('ven.vendorIntercompanyTypeFromTransactionData') == 3 ,lit('NA'))


    dwh_vw_DIM_Vendor = ptp_L2_DIM_Vendor.alias("ven").join \
                    (gen_L2_DIM_Organization.alias("org"), \
                    [(col("ven.organizationUnitSurrogateKey") == col("org.organizationUnitSurrogateKey"))]\
                         ,how = "inner")\
                    .join(knw_LK_CD_ReportingSetup.alias("rpt"), \
                      [col("org.companyCode") == col("rpt.companyCode")],how='inner').join\
                      (dfBusinessDatatypes.alias("bdt1"),\
                      [((col("bdt1.businessDatatype") =='One Time Vendor') & \
                        (col("bdt1.targetLanguageCode").eqNullSafe(col("rpt.KPMGDataReportingLanguage"))) & \
                        (col("bdt1.sourceSystemValue") == lit(isOneTimeVendor)))],how = "left").join\
                      (dfBusinessDatatypes.alias("bdt2"),\
                      [((col("bdt2.businessDatatype") =='Is Intercompany From Accountmapping') & \
                        (col("bdt2.targetLanguageCode").eqNullSafe(col("rpt.KPMGDataReportingLanguage"))) & \
                        (col("bdt2.sourceSystemValue") == lit(isIntercompanyFromAccountmapping)))],how = "left").join\
                      (dfBusinessDatatypes.alias("bdt3"),\
                      [((col("bdt3.businessDatatype") =='Is InterCompany From MasterData') & \
                        (col("bdt3.targetLanguageCode").eqNullSafe(col("rpt.KPMGDataReportingLanguage"))) & \
                        (col("bdt3.sourceSystemValue") == lit(isInterCompanyFromMasterData)))],how = "left").join\
                      (dfBusinessDatatypes.alias("bdt4"),\
                      [((col("bdt4.businessDatatype") =='Is InterCompany From TransactionData') & \
                        (col("bdt4.targetLanguageCode").eqNullSafe(col("rpt.KPMGDataReportingLanguage"))) & \
                        (col("bdt4.sourceSystemValue") == lit(isInterCompanyFromTransactionData)))],how = "left").join\
                      (knw_L2_DIM_BusinessPartnerTypeDescription.alias("p"),\
                      [((col("p.accountMappingFlag").eqNullSafe(col("ven.vendorIntercompanyTypeFromAccountMapping"))) & \
                        (col("p.masterDataFlag").eqNullSafe(col("ven.vendorIntercompanyTypeFromMasterData"))) & \
                        (col("p.transactionDataFlag").eqNullSafe(col("ven.vendorIntercompanyTypeFromTransactionData"))))]\
                         ,how = "left").\
                      select(col("ven.organizationUnitSurrogateKey"),col("vendorSurrogateKey"),\
                            col("vendorNumber"),col("vendorCountry").alias("vendorCountryName"),\
                            when(col("ven.vendorName").isNull(),lit("#NA#")).otherwise(col("ven.vendorName")).alias('vendorName'),\
                            col("vendorCreationUser").alias("createdBy"),\
                            col("bdt1.sourceSystemValueDescription").alias("isOnetimeVendor"),\
                            col("vendorCreationDate").alias("createdOn"),\
                            col("bdt2.sourceSystemValueDescription").alias("isInterCoFromAccountMapping"),\
                            col("bdt3.sourceSystemValueDescription").alias("isInterCoFromMasterData"),\
                            col("bdt4.sourceSystemValueDescription").alias("isInterCoFromTransactionData"),\
                            col("p.ouputText").alias("businessPartnerType"))\
                           .withColumn("vendorCountryCode",lit("#NA#"))

    dwh_vw_DIM_Vendor = objDataTransformation.gen_convertToCDMStructure_generate(dwh_vw_DIM_Vendor\
                            ,'dwh','vw_DIM_Vendor'\
                            ,isIncludeAnalysisID=True,isSqlFormat=True)[0] 
    dDate =parse('1900-01-01').date()
    keysAndValues = {'organizationUnitSurrogateKey':0,'vendorSurrogateKey':0,'createdOn' :dDate }
    dwh_vw_DIM_Vendor = objGenHelper.gen_placeHolderRecords_populate('dwh.vw_DIM_Vendor'\
                               ,keysAndValues,dwh_vw_DIM_Vendor)  
    objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_Vendor,gl_CDMLayer2Path + "ptp_L2_DIM_Vendor.parquet" )     
  except Exception as err:
    raise
  finally:
    if(dfBusinessDatatypes != None):
      dfBusinessDatatypes.unpersist()
