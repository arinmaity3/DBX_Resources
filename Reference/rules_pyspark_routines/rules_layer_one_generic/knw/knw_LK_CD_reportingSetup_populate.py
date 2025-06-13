# Databricks notebook source
import pathlib
from pathlib import Path
import uuid
from pyspark.sql.window import Window

def knw_LK_CD_reportingSetup_populate():
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
      
        global knw_LK_CD_ReportingSetup
        knw_LK_CD_ReportingSetup = None
        logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,validationID = VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION) 

        df1 = spark.sql('SELECT DISTINCT companyCode FROM fin_L1_TD_Journal UNION SELECT DISTINCT companyCode FROM fin_L1_TD_GLBalance')
        df1.createOrReplaceTempView("vw_companyCode")
      
        df2 = spark.sql('SELECT DISTINCT "000"	AS	clientCode, CASE WHEN ISNULL(CompanyCode) THEN "N/A" ELSE CompanyCode END	AS	clientName,CASE WHEN ISNULL(CompanyCode) THEN "N/A" ELSE CompanyCode END	AS	companyCode,CASE WHEN ISNULL(CompanyCode) THEN "N/A" ELSE CompanyCode END	AS	reportingGroup,"EN"	AS	reportingLanguageCode,"EN"	AS	clientDataReportingLanguage,"EN"	AS	clientDataSystemReportingLanguage,"EN"	AS	KPMGDataReportingLanguage FROM vw_companyCode')
        df2.createOrReplaceTempView("vw_tmp_reportingsetup")
      
        df3 = spark.sql('SELECT CompanyCode, MAX(chartOfAccountID) AS chartOfAccountID FROM fin_L1_MD_GLAccount GROUP BY  CompanyCode')
        df3.createOrReplaceTempView("vw_tmp_chartOfAccount")
      
        df4 = spark.sql('SELECT CompanyCode, MAX(localCurrency) AS localCurrency FROM fin_L1_TD_Journal GROUP BY  CompanyCode UNION SELECT CompanyCode, MAX(localCurrency) AS localCurrency FROM fin_L1_TD_GLBalance GROUP BY  CompanyCode')
        df4.createOrReplaceTempView("vw_tmp_localCurrency_1")
        df4=spark.sql('SELECT CompanyCode, MAX(localCurrency) AS localCurrency FROM vw_tmp_localCurrency_1 GROUP BY  CompanyCode' )
        df4.createOrReplaceTempView("vw_tmp_localCurrency")
      
        knw_LK_CD_ReportingSetup = spark.sql('SELECT R.clientCode, R.clientName,R.companyCode, R.reportingGroup, CASE WHEN ISNULL(L.localCurrency) THEN "N/A" WHEN L.localCurrency="" THEN "N/A" ELSE L.localCurrency END AS reportingCurrencyCode, CASE WHEN ISNULL(L.localCurrency) THEN "N/A" WHEN L.localCurrency="" THEN "N/A" ELSE L.localCurrency END AS localCurrencyCode,  R.reportingLanguageCode, CASE WHEN ISNULL(C.chartOfAccountID) THEN "" ELSE C.chartOfAccountID END AS chartofAccount,  R.clientDataReportingLanguage, R.clientDataSystemReportingLanguage, R.KPMGDataReportingLanguage FROM vw_tmp_reportingsetup AS R LEFT JOIN vw_tmp_chartOfAccount AS C ON R.CompanyCode = C.CompanyCode LEFT JOIN vw_tmp_localCurrency AS L ON R.CompanyCode = L.CompanyCode ')
        knw_LK_CD_ReportingSetup = knw_LK_CD_ReportingSetup.withColumn("clientTerritoryLanguage",lit("")).withColumn("secondCurrencyCode",lit("")).withColumn("thirdCurrencyCode", lit("")).withColumn("ledgerID", lit("")).withColumn("balancingSegment",lit(""))
        knw_LK_CD_ReportingSetup = objDataTransformation.gen_CDMColumnOrder_get(knw_LK_CD_ReportingSetup,'knw','LK_CD_ReportingSetup')
        knw_LK_CD_ReportingSetup.createOrReplaceTempView("knw_LK_CD_ReportingSetup")  
        sqlContext.cacheTable("knw_LK_CD_ReportingSetup")

        reportingSetupPath =  gl_commonParameterPath + "knw_LK_CD_ReportingSetup.csv" 
        objGenHelper.gen_writeSingleCsvFile_perform(df = knw_LK_CD_ReportingSetup,targetFile = reportingSetupPath)
        reportingSetupPathDelta =  gl_commonParameterPath + "knw_LK_CD_ReportingSetup.delta"
        objGenHelper.gen_writeToFile_perfom(df = knw_LK_CD_ReportingSetup, filenamepath = reportingSetupPathDelta)
        knw_LK_CD_ReportingLanguage_get()

        executionStatus = "Created knw_LK_CD_ReportingSetup and  knw_LK_CD_ReportingLanguage succesfully."
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    finally:
        print(executionStatus)

def knw_LK_CD_ReportingLanguage_get():
    """Creates a table with distinct reportinglanguage details for each company and client.
    This table can be looked up based on the languagetype to get distinct values of that language type."""
    try:
        df = spark.sql("SELECT DISTINCT \
            										 clientCode			\
            										,companyCode		\
            										,clientDataReportingLanguage	AS languageCode		\
            										,'clientDataReportingLanguage'	AS languageCodeColumn	\
            					         FROM knw_LK_CD_ReportingSetup	\
            						     UNION ALL						\
            						     SELECT DISTINCT				\
            						     				 clientCode		\
            						     				,companyCode	\
            						     				,clientDataSystemReportingLanguage	AS languageCode	\
            						     				,'clientDataSystemReportingLanguage' AS languageCodeColumn	\
            						     		FROM knw_LK_CD_ReportingSetup \
            						     UNION ALL							\
            						     SELECT DISTINCT					\
            										 clientCode			\
            										,companyCode		\
            										,KPMGDataReportingLanguage		AS languageCode		\
            										,'KPMGDataReportingLanguage'     AS languageCodeColumn	\
            							 FROM knw_LK_CD_ReportingSetup	")

        df.createOrReplaceTempView("knw_LK_CD_ReportingLanguage")
        sqlContext.cacheTable("knw_LK_CD_ReportingLanguage")

    except Exception as e:
        raise  


