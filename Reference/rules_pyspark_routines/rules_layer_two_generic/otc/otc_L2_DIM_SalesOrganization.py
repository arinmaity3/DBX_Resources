# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number
from dateutil.parser import parse
from delta.tables import *

def otc_L2_DIM_SalesOrganization():
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        global otc_L2_DIM_SalesOrganization
  
        otc_L2_TMP_SalesOrganizationCollect = spark.sql(
        """

        SELECT DISTINCT
            blig1.billingDocumentCompanyCode as salesOrganizationCompanyCode,
            blig1.billingDocumentSalesOrganization as salesOrganization,
            blig1.billingDocumentDistributionChannel as salesOrganizationDistributionChannel,
            blig1.billingDocumentDivision as salesOrganizationDivision
        FROM otc_L1_TD_Billing blig1

        """)
        otc_L2_TMP_SalesOrganizationCollect.createOrReplaceTempView("otc_L2_TMP_SalesOrganizationCollect")
  
        otc_L2_DIM_SalesOrganization = spark.sql(
        """

        SELECT
            row_number() OVER (ORDER BY (SELECT NULL)) as salesOrganizationSurrogateKey,
            coalesce(tsor2.salesOrganization,'#NA#')  as salesOrganization,
            coalesce(sortz1.salesOrganizationDescription,'#NA#') as salesOrganizationDescription,
            coalesce(tsor2.salesOrganizationDistributionChannel,'#NA#') as salesOrganizationDistributionChannel,
            coalesce(sortz1.distributionChannelDescription,'#NA#') as salesOrganizationDistributionChannelDescription,
            coalesce(tsor2.salesOrganizationDivision,'#NA#') as salesOrganizationDivision,
            coalesce(sortz1.divisionDescription,'#NA#') as salesOrganizationDivisionDescription

        FROM otc_L2_TMP_SalesOrganizationCollect tsor2
        INNER JOIN	gen_L2_DIM_Organization	org2 
        ON 
            (
                org2.companyCode = tsor2.salesOrganizationCompanyCode
            )

        LEFT JOIN otc_L1_MD_SalesOrganization sortz1 
        ON 
            (
                sortz1.companyCode					= tsor2.salesOrganizationCompanyCode
                AND sortz1.salesOrganization		= tsor2.salesOrganization
                AND sortz1.distributionChannel		= tsor2.salesOrganizationDistributionChannel
                AND sortz1.division					= tsor2.salesOrganizationDivision
            )


        """)
        otc_L2_DIM_SalesOrganization.createOrReplaceTempView("otc_L2_DIM_SalesOrganization")
        otc_L2_DIM_SalesOrganization2 = objDataTransformation.gen_convertToCDMandCache(otc_L2_DIM_SalesOrganization,'otc','L2_DIM_SalesOrganization',True)

        default_List =[[0,'NONE','NONE','NONE']]
        default_df = spark.createDataFrame(default_List)

        otc_vw_DIM_SalesOrganization = spark.sql(
        """
        SELECT
            salesOrganizationSurrogateKey,						
            concat(salesOrganization, ' (' , salesOrganizationDescription, ')' ) as salesOrganization,
            concat(salesOrganizationDistributionChannel, ' (', salesOrganizationDistributionChannelDescription, ')') as salesOrganizationDistributionChannel,
            concat(salesOrganizationDivision, ' (', salesOrganizationDivisionDescription, ')') as salesOrganizationDivision

        FROM  otc_L2_DIM_SalesOrganization
            
        """).union(default_df) 

        objGenHelper.gen_writeToFile_perfom(otc_vw_DIM_SalesOrganization,gl_CDMLayer2Path +"otc_vw_DIM_SalesOrganization.parquet")
    
            
        executionStatus = "L2_DIM_SalesOrganization populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as e:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]



