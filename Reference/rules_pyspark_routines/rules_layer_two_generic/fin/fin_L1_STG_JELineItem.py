# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import *



def fin_L1_STG_JELineItem_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global fin_L1_STG_JELineItem

    fin_L1_TMP_JEItem = spark.sql("""

        SELECT
                 lkrs.clientCode as client						
                ,joi1.companyCode as companyCode						
                ,joi1.fiscalYear as financialYear						
                ,joi1.documentNumber as GLJEDocumentNumber				
                ,joi1.lineItem as GLJELineNumber						
                ,joi1.transactionIDbyPrimaryKey as idGLJELine		
                ,joi1.transactionIDbyJEDocumnetNumber as idGLJE	
        FROM fin_L1_TD_Journal joi1
        --#Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181204_1311
        INNER JOIN knw_LK_CD_ReportingSetup lkrs ON
                                                    (
                                                        joi1.companyCode	= lkrs.companyCode
                                                    )


        """)
    fin_L1_TMP_JEItem.createOrReplaceTempView("fin_L1_TMP_JEItem")

    GLJELineMaxfromJrnl = fin_L1_TMP_JEItem.count()
    GLJEMaxfromJrnl = spark.sql("SELECT MAX(idGLJE) FROM fin_L1_TMP_JEItem ").collect()[0][0]

    fin_L1_TMP_AccountGRIRLineItem = spark.sql("""

        SELECT
                grr.accountGRIRClientCode as client
            ,grr.accountGRIRCompanyCode as companyCode
            ,grr.accountGRIRFinancialYear as financialYear
            ,grr.accountGRIRGLJEDocumentNumber as GLJEDocumentNumber
            ,grr.accountGRIRGLJELineNumber as GLJELineNumber
        FROM fin_L1_TD_AccountGRIR grr
        --#Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181204_1347
        INNER JOIN knw_LK_CD_ReportingSetup lkrs ON
                                                (
                                                    grr.accountGRIRCompanyCode	= lkrs.companyCode
                                                AND grr.accountGRIRClientCode	= lkrs.clientCode
                                                )
        --#/Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181204_1347
        --#Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181204_1350
        LEFT OUTER JOIN fin_L1_TMP_JEItem jel
                ON  grr.accountGRIRClientCode				= jel.client
                AND grr.accountGRIRCompanyCode				= jel.companyCode
                AND grr.accountGRIRFinancialYear			= jel.financialYear
                AND grr.accountGRIRGLJEDocumentNumber		= jel.GLJEDocumentNumber
                AND grr.accountGRIRGLJELineNumber			= jel.GLJELineNumber
        --#/Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181204_1350
        WHERE jel.GLJEDocumentNumber IS NULL
        GROUP BY
                grr.accountGRIRClientCode
            ,grr.accountGRIRCompanyCode
            ,grr.accountGRIRFinancialYear
            ,grr.accountGRIRGLJEDocumentNumber
            ,grr.accountGRIRGLJELineNumber


        """)
    fin_L1_TMP_AccountGRIRLineItem.createOrReplaceTempView("fin_L1_TMP_AccountGRIRLineItem")

    fin_L1_TMP_AccountPayableLineItem = spark.sql("""

        SELECT
             lkrs.clientCode as client
            ,ltap.accountPayableCompanyCode as companyCode
            ,ltap.accountPayableFiscalYear as financialYear
            ,ltap.accountPayableDocumentNumber as GLJEDocumentNumber
            ,ltap.accountPayableDocumentLineItem as GLJELineNumber 
        FROM fin_L1_TD_AccountPayable ltap
        --#Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181204_1439
        INNER JOIN knw_LK_CD_ReportingSetup lkrs ON
                                                (
                                                    ltap.accountPayableCompanyCode	= lkrs.companyCode
                                                )
        --#/Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181204_1439
        --#Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181204_1441
        LEFT OUTER JOIN fin_L1_TMP_JEItem jel
                ON  lkrs.clientCode				= jel.client
                AND ltap.accountPayableCompanyCode			= jel.companyCode
                AND ltap.accountPayableFiscalYear			= jel.financialYear
                AND ltap.accountPayableDocumentNumber		= jel.GLJEDocumentNumber
                AND ltap.accountPayableDocumentLineItem		= jel.GLJELineNumber
        --#/Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181204_1441
        WHERE jel.GLJEDocumentNumber IS NULL
        GROUP BY
             lkrs.clientCode
            ,ltap.accountPayableCompanyCode
            ,ltap.accountPayableFiscalYear
            ,ltap.accountPayableDocumentNumber
            ,ltap.accountPayableDocumentLineItem


        """)
    fin_L1_TMP_AccountPayableLineItem.createOrReplaceTempView("fin_L1_TMP_AccountPayableLineItem")
    
    fin_L1_TMP_AccountReceivableLineItem = spark.sql("""

        SELECT
             lkrs.clientCode as client
            ,arr.companyCode as companyCode
            ,arr.financialYear as financialYear
            ,arr.GLJEDocumentNumber as GLJEDocumentNumber
            ,arr.GLJELineNumber as GLJELineNumber
        FROM fin_L1_TD_AccountReceivable arr
        --#Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181205_1022
        INNER JOIN knw_LK_CD_ReportingSetup lkrs ON
                                                (
                                                    arr.companyCode	= lkrs.companyCode
                                                )
        --#/Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181205_1022
        --#Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181205_1024
        LEFT OUTER JOIN fin_L1_TMP_JEItem jel
                ON  lkrs.clientCode			= jel.client
                AND arr.companyCode			= jel.companyCode
                AND arr.financialYear		= jel.financialYear
                AND arr.GLJEDocumentNumber	= jel.GLJEDocumentNumber
                AND arr.GLJELineNumber		= jel.GLJELineNumber
        --#/Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181205_1024
        WHERE jel.GLJEDocumentNumber IS NULL
        GROUP BY
              lkrs.clientCode
             ,arr.companyCode
             ,arr.financialYear
             ,arr.GLJEDocumentNumber
             ,arr.GLJELineNumber
        --#/Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181205_1018


        """)
    fin_L1_TMP_AccountReceivableLineItem.createOrReplaceTempView("fin_L1_TMP_AccountReceivableLineItem")

    fin_L1_TMP_JELineItem = spark.sql("""


        --#Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181205_1058
        SELECT
             client
            ,companyCode
            ,financialYear
            ,GLJEDocumentNumber
            ,GLJELineNumber
        FROM fin_L1_TMP_AccountGRIRLineItem
        --#/Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181205_1058

        UNION

        --#Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181205_1103
        SELECT
             client
            ,companyCode
            ,financialYear
            ,GLJEDocumentNumber
            ,GLJELineNumber
        FROM fin_L1_TMP_AccountPayableLineItem
        --#/Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181205_1103

        UNION 

        --#Technical_fin_usp_L1_STG_JELineItem_populate_vraju_20181205_1105
        SELECT
             client
            ,companyCode
            ,financialYear
            ,GLJEDocumentNumber
            ,GLJELineNumber
        FROM fin_L1_TMP_AccountReceivableLineItem

        """)

    fin_L1_STG_JELineItem_1 = spark.sql("""

        SELECT
       	        joi1.client as client						
              ,joi1.companyCode as companyCode						
              ,joi1.financialYear as financialYear						
              ,joi1.GLJEDocumentNumber as GLJEDocumentNumber				
              ,joi1.GLJELineNumber as GLJELineNumber						
              ,joi1.idGLJELine as idGLJELine		
              ,joi1.idGLJE as idGLJE	
        FROM fin_L1_TMP_JEItem joi1
        """)
    fin_L1_STG_JELineItem_1.createOrReplaceTempView("fin_L1_STG_JELineItem_1")

    fin_L1_TMP_idGLJEItem = spark.sql("""

        SELECT DISTINCT
           client
          ,companyCode
          ,financialYear
          ,GLJEDocumentNumber
          ,idGLJE
        FROM fin_L1_TMP_JEItem

        """)

    fin_L1_TMP_JEItem.createOrReplaceTempView("fin_L1_TMP_JEItem")

    window1 = Window.partitionBy().orderBy("ltjle.client","ltjle.companyCode","ltjle.financialYear","ltjle.GLJEDocumentNumber","ltjle.GLJELineNumber")
    window2 = Window.partitionBy().orderBy("ltjle.client","ltjle.companyCode","ltjle.financialYear","ltjle.GLJEDocumentNumber")

    fin_L1_STG_JELineItem_2 = fin_L1_TMP_JELineItem.alias('ltjle') \
    .join(fin_L1_TMP_idGLJEItem.alias('idgi1'),
         ((col('idgi1.client') == col('ltjle.client')) 
          &(col('idgi1.companyCode') == col('ltjle.companyCode')) 
          &(col('idgi1.financialYear') == col('ltjle.financialYear')) 
          &(col('idgi1.GLJEDocumentNumber') == col('ltjle.GLJEDocumentNumber'))),
         how="left") \
    .select("ltjle.client"
            ,"ltjle.companyCode"
            ,"ltjle.financialYear"
            ,"ltjle.GLJEDocumentNumber"
            ,"ltjle.GLJELineNumber"
            ,(F.row_number().over(window1).alias("idGLJELine") + GLJELineMaxfromJrnl).alias('idGLJELine')
            ,(coalesce(col('idgi1.idGLJE'), F.dense_rank().over(window2) + GLJEMaxfromJrnl)).alias('idGLJE')
           )

    fin_L1_STG_JELineItem = fin_L1_STG_JELineItem_2.union(fin_L1_STG_JELineItem_1)

    fin_L1_STG_JELineItem = objDataTransformation.gen_convertToCDMandCache(fin_L1_STG_JELineItem,'fin','L1_STG_JELineItem',False)


    executionStatus = "L1_STG_JELineItem populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
