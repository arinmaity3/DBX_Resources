# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number
from dateutil.parser import parse
from delta.tables import *


def L2_DIM_Customer_AddedDetails(otc_L2_DIM_Customer,fileLocation):
  try:
    objGenHelper = gen_genericHelper()

    status="Update Dim L2 Customer additional details"
    fileFullName=fileLocation+"otc_L1_MD_Customer.delta"
    
    if objGenHelper.file_exist(fileFullName) == True:

      otc_L1_MD_Customer = objGenHelper.gen_readFromFile_perform(fileFullName)

      otc_L2_DIM_Customer = otc_L2_DIM_Customer.alias("L2_Customer")\
      .join(gen_L2_DIM_Organization.alias("L2_Org")\
      ,(col("L2_Customer.organizationUnitSurrogateKey")==col("L2_Org.organizationUnitSurrogateKey")),how="inner")\
      .join(otc_L1_MD_Customer.alias("L1_Customer"),(col("L1_Customer.companyCode")==col("L2_Org.companyCode"))\
      &(col("L1_Customer.customerNumber")==col("L2_Customer.customerNumber")),how="left")\
      .select(col("L2_Customer.customerSurrogateKey").alias("customerSurrogateKey")\
      ,col("L2_Customer.organizationUnitSurrogateKey").alias("organizationUnitSurrogateKey")\
      ,col("L2_Customer.customerNumber").alias("customerNumber")\
      ,coalesce(col("L1_Customer.customerName"),col("L2_Customer.customerName")).alias("customerName")\
      ,coalesce(col("L1_Customer.customerCountryCode"),col("L2_Customer.customerCountryCode")).alias("customerCountryCode")\
      ,coalesce(col("L1_Customer.customerCountryName"),col("L2_Customer.customerCountryName")).alias("customerCountryName")\
      ,coalesce(col("L1_Customer.customerCity"),col("L2_Customer.customerCity")).alias("customerCity")\
      ,coalesce(col("L1_Customer.createdBy"),col("L2_Customer.createdBy")).alias("createdBy")\
      ,coalesce(col("L1_Customer.createdOn"),col("L1_Customer.createdOn")).alias("createdOn")\
      ,col("L1_Customer.blockType").alias("blockType")\
      ,col("L1_Customer.creditLimit").alias("creditLimitLC")\
      ,col("L1_Customer.paymentTerm").alias("paymentTerm")\
      ,coalesce(col("L1_Customer.isOneTimeAccount"),lit(False)).alias("isOneTimeAccount"))
      
      if otc_L2_DIM_Customer.count() >0:
        deltaL2CustomerPath=gl_Layer2Staging+"otc_L2_DIM_Customer.delta"
        deltaL2CustomerPath = DeltaTable.forPath(spark, deltaL2CustomerPath)
        
        deltaL2CustomerPath.alias("L2_Customer_O")\
        .merge(otc_L2_DIM_Customer.alias("L2_Customer_N"),"L2_Customer_N.customerSurrogateKey=L2_Customer_O.customerSurrogateKey")\
        .whenMatchedUpdate(set =
        {
          "organizationUnitSurrogateKey":"L2_Customer_N.organizationUnitSurrogateKey",
          "customerNumber":"L2_Customer_N.customerNumber",
          "customerName":"L2_Customer_N.customerName",
          "customerCountryCode":"L2_Customer_N.customerCountryCode",  
          "customerCountryName":"L2_Customer_N.customerCountryName",
          "customerCity":"L2_Customer_N.customerCity", 
          "createdBy":"L2_Customer_N.createdBy",  
          "createdOn":"L2_Customer_N.createdOn",
          "blockType":"L2_Customer_N.blockType",
          "creditLimitLC":"L2_Customer_N.creditLimitLC",
          "paymentTerm":"L2_Customer_N.paymentTerm",
          "isOneTimeAccount":"L2_Customer_N.isOneTimeAccount"
        }).execute()   
        
    else:
      status="The file L1 Customer does not exists in the specified location: "+fileLocation
  except Exception as err:
    raise
  finally:
    return status

def otc_L2_DIM_Customer_populate():    
  try: 
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global otc_L2_DIM_Customer
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    w = Window().orderBy(lit(''))

    otc_L1_TMP_Customer = spark.createDataFrame(gl_lstOfCustomerCollect,customerSchemaCollect).\
                          select(col("companyCode"),col("customerNumber")).distinct()

    dfL2_DIM_Customer = otc_L1_TMP_Customer.alias("cust").join \
                (gen_L2_DIM_Organization.alias("org"), \
                [(col("cust.companyCode")    == col("org.companyCode"))],"inner")\
                .select(col("cust.companyCode").alias("companyCode"),  \
                        col("organizationUnitSurrogateKey"),  \
                        col("customerNumber").alias("customerNumber"))\
                        .withColumn("customerSurrogateKey",row_number().over(w))\
                        .withColumn("customerName",lit("#NA#"))\
                        .withColumn("customerCountryCode",lit("#NA#"))\
                        .withColumn("customerCountryName",lit("#NA#"))\
                        .withColumn("customerCity",lit("#NA#"))\
                        .withColumn("createdBy",lit("#NA#"))\
                        .withColumn("createdOn",lit("1900-01-01"))\
                        .withColumn("isOneTimeAccount",lit("0"))

    dfL2_DIM_Customer = dfL2_DIM_Customer.alias("customer").join\
              (otc_L1_STG_InterCompanyCustomer.alias("inc"), \
              [((col("customer.companyCode").eqNullSafe(col("inc.companyCode"))) & \
              (col("customer.customerNumber").eqNullSafe(col("inc.customerNumber"))))],how = "left")\
              .select(col("customer.*"),\
               expr("case when isInterCoFromAccountMapping is null then '3' else isInterCoFromAccountMapping end").\
                      alias("isInterCoFromAccountMapping"),\
               expr("case when isInterCoFromMasterData is null then '3' else isInterCoFromMasterData end").\
                      alias("isInterCoFromMasterData"),\
               expr("case when isInterCoFromTransactionData is null then '3' else isInterCoFromTransactionData end").\
                      alias("isInterCoFromTransactionData"),\
               expr("case when isintercoFlag is null then '3' else isintercoFlag end").\
                      alias("isInterCoFlag"))

    #dfL2_DIM_Customer.display()
    otc_L2_DIM_Customer = objDataTransformation.gen_convertToCDMandCache \
        (dfL2_DIM_Customer,'otc','L2_DIM_Customer',False)

    status=L2_DIM_Customer_AddedDetails(otc_L2_DIM_Customer,gl_CDMLayer1Path)

    __dwhCustomer_populate(objGenHelper,objDataTransformation,otc_L2_DIM_Customer)

    executionStatus = "otc_L2_DIM_Customer  population sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as err:
     executionStatus = objGenHelper.gen_exceptionDetails_log()       
     executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
     return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
      
     
def __dwhCustomer_populate(objGenHelper,objDataTransformation,otc_L2_DIM_Customer):
  try:
        
    genricERPSystemID = 10
    busienssDatatypes = ['Is InterCompany From MasterData',
                         'Is Intercompany From Accountmapping',
                         'Is InterCompany From TransactionData']
    
    dfBusinessDatatypes = gl_metadataDictionary['knw_LK_GD_BusinessDatatypeValueMapping'].\
                         select(col("businessDatatype"),col("sourceSystemValue"),\
                               col("sourceSystemValueDescription"),col("targetLanguageCode")).\
                         filter((col("businessDatatype").isin(busienssDatatypes)) & \
                               (col("targetERPSystemID") == genricERPSystemID)) 
    dfBusinessDatatypes.cache()
   
    isIntercompanyFromAccountmapping = when(col('cus.isInterCoFromAccountMapping') == 1, lit('1')) \
                                      .when(col('cus.isInterCoFromAccountMapping') == 2 ,lit('2'))  \
                                      .when(col('cus.isInterCoFromAccountMapping') == 3 ,lit('NA'))

    isInterCompanyFromMasterData = when(col('cus.isInterCoFromMasterData') == 1, lit('1')) \
                                      .when(col('cus.isInterCoFromMasterData') == 2 ,lit('2'))  \
                                      .when(col('cus.isInterCoFromMasterData') == 3 ,lit('NA'))

    isInterCompanyFromTransactionData = when(col('cus.isInterCoFromTransactionData') == 1, lit('1')) \
                                      .when(col('cus.isInterCoFromTransactionData') == 2 ,lit('2'))  \
                                      .when(col('cus.isInterCoFromTransactionData') == 3 ,lit('NA'))


    dwh_vw_DIM_Customer = otc_L2_DIM_Customer.alias("cus").join \
                  (gen_L2_DIM_Organization.alias("org"), \
                  [(col("cus.organizationUnitSurrogateKey")    == col("org.organizationUnitSurrogateKey"))]\
                     ,"inner")\
                  .join(knw_LK_CD_ReportingSetup.alias("rpt"), \
                  [col("org.companyCode") == col("rpt.companyCode")],how='inner').join\
                  (dfBusinessDatatypes.alias("bdt2"),\
                  [((col("bdt2.businessDatatype") =='Is Intercompany From Accountmapping') & \
                    (col("bdt2.targetLanguageCode").eqNullSafe(col("rpt.KPMGDataReportingLanguage"))) & \
                    (col("bdt2.sourceSystemValue") == lit(isIntercompanyFromAccountmapping)))]\
                     ,how = "left").join\
                  (dfBusinessDatatypes.alias("bdt3"),\
                  [((col("bdt3.businessDatatype") =='Is InterCompany From MasterData') & \
                    (col("bdt3.targetLanguageCode").eqNullSafe(col("rpt.KPMGDataReportingLanguage"))) & \
                    (col("bdt3.sourceSystemValue") == lit(isInterCompanyFromMasterData)))]\
                     ,how = "left").join\
                  (dfBusinessDatatypes.alias("bdt4"),\
                  [((col("bdt4.businessDatatype") =='Is InterCompany From TransactionData') & \
                    (col("bdt4.targetLanguageCode").eqNullSafe(col("rpt.KPMGDataReportingLanguage"))) & \
                    (col("bdt4.sourceSystemValue") == lit(isInterCompanyFromTransactionData)))]\
                     ,how = "left").join\
                  (knw_L2_DIM_BusinessPartnerTypeDescription.alias("p"),\
                  [((col("p.accountMappingFlag").eqNullSafe(col("cus.isInterCoFromAccountMapping"))) & \
                    (col("p.masterDataFlag").eqNullSafe(col("cus.isInterCoFromMasterData"))) & \
                    (col("p.transactionDataFlag").eqNullSafe(col("cus.isInterCoFromTransactionData"))))]\
                     ,how = "left").\
                     select(col("cus.organizationUnitSurrogateKey"),col("customerSurrogateKey"),\
                        col("customerNumber"),\
                        when(col('cus.customerName')== '', lit('')).otherwise(col('cus.customerName')).alias("customerName"),\
                        col("createdBy").alias("createdBy"),\
                        col("createdOn"),\
                        col("bdt2.sourceSystemValueDescription").alias("isInterCoFromAccountMapping"),\
                        col("bdt3.sourceSystemValueDescription").alias("isInterCoFromMasterData"),\
                        col("bdt4.sourceSystemValueDescription").alias("isInterCoFromTransactionData"),\
                        col("p.ouputText").alias("businessPartnerType"),\
                        when(col("cus.customerCountryName").isNull(),lit('NONE')).otherwise(col("cus.customerCountryName"))\
                               .alias("customerCountryName"),\
                        when(col("cus.blockType").isNull(),lit('NONE')).otherwise(col("cus.blockType")).alias("blockType"))    
    
    dwh_vw_DIM_Customer = objDataTransformation.gen_convertToCDMStructure_generate(dwh_vw_DIM_Customer\
                          ,'dwh','vw_DIM_Customer'\
                          ,isIncludeAnalysisID=True,isSqlFormat=True)[0]
    dDate =parse('1900-01-01').date()
    keysAndValues = {'organizationUnitSurrogateKey':0,'customerSurrogateKey':0,'createdOn' :dDate }
    dwh_vw_DIM_Customer = objGenHelper.gen_placeHolderRecords_populate('dwh.vw_DIM_Customer'\
                                                                    ,keysAndValues,dwh_vw_DIM_Customer)  
    objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_Customer,gl_CDMLayer2Path + "otc_L2_DIM_Customer.parquet" )     
    
  except Exception as err:
    raise
  finally:
    if(dfBusinessDatatypes != None):
      dfBusinessDatatypes.unpersist()      

#otc_L2_DIM_Customer_populate()      


