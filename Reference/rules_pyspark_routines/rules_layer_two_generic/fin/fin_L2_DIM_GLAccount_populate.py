# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number

def fin_L2_DIM_GLAccount_populate():
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        
        global fin_L2_DIM_GLAccount
        global fin_L2_DIM_GLAccountText

        dfGLAccount = spark.createDataFrame(data = gl_lstOfGLAccountCollect, schema = accountSchemaCollect).\
                         select(col("companyCode"),col("accountNumber")).distinct()
        
        w = Window().orderBy(lit(''))

        GLAccount_ChartOfAccount = when((col('Account.chartOfAccountID').isNull()| \
                                         col('Account.chartOfAccountID').isin('','#NA#')), lit('#NA#') ) \
                              .otherwise(col('Account.chartOfAccountID'))
        
        org_ChartOfAccount = when((col('org.chartOfAccounts').isNull()| \
                                         col('org.chartOfAccounts').isin('','#NA#')), lit('#NA#') ) \
                              .otherwise(col('org.chartOfAccounts'))

        clientDataReportingLanguage = when((col('L1_gla1.languageCode').isNull()| \
                                         col('L1_gla1.languageCode').isin('','#NA#')), col('rpt.clientDataReportingLanguage') ) \
                              .otherwise(col('L1_gla1.languageCode'))

        dfDistinct_GLAccount = dfGLAccount.alias("gla"). \
              join (gen_L2_DIM_Organization.alias("org"), \
                   (col("gla.companyCode")    == col("org.companyCode")), "inner"). \
              join (fin_L1_MD_GLAccount.alias("Account"), \
                   (col("gla.accountNumber").eqNullSafe(col("Account.accountNumber"))) \
                 & (col("org.companyCode").eqNullSafe(col("Account.companyCode"))) \
                 & (lit(GLAccount_ChartOfAccount) == lit(org_ChartOfAccount)),"left"). \
              join (fin_L1_STG_InterCompanyGLAccount.alias("intrCo"), \
                   (col("gla.companyCode").eqNullSafe(col("intrCo.companyCode"))) \
                 & (col("gla.accountNumber").eqNullSafe(col("intrCo.accountNumber"))), how = "left"). \
              select( col("org.organizationUnitSurrogateKey").alias("organizationUnitSurrogateKey"),  \
                      col("gla.accountNumber").alias("accountNumber"),  \
                      col("Account.chartOfAccountID").alias("chartOfAccountID"),\
                      col("Account.chartOfAccountText").alias("chartOfAccountText"),\
                      when((col("Account.accountType").isNull()|\
                            col("Account.accountType").isin('')),lit('#NA#')) \
                           .otherwise(col("Account.accountType")).alias("accountType"),\
                      lit("#NA#").alias("accountCategory"),\
                      when((col("intrCo.isInterCoFromAccountMapping").isNull()|\
                            col("intrCo.isInterCoFromAccountMapping").isin('','#NA#')),lit(3)) \
                           .otherwise(col("intrCo.isInterCoFromAccountMapping")).alias("isInterCoFromAccountMapping"),\
                      when((col("Account.isInterCoFromMasterData").isNull()|\
                            col("Account.isInterCoFromMasterData").isin('','#NA#')),col("intrCo.isInterCoFromMasterData")) \
                           .otherwise(col("Account.isInterCoFromMasterData")).alias("isInterCoFromMasterData"),\
                      when((col("Account.isInterCoFromTransactionData").isNull()|\
                            col("Account.isInterCoFromTransactionData").isin('','#NA#')),col("intrCo.isInterCoFromTransactionData")) \
                           .otherwise(col("Account.isInterCoFromTransactionData")).alias("isInterCoFromTransactionData"),\
                      when((col("Account.isInterCoFlag").isNull()|\
                            col("Account.isInterCoFlag").isin('','#NA#')),col("intrCo.isInterCoFlag")) \
                           .otherwise(col("Account.isInterCoFlag")).alias("isInterCoFlag"),\
                      when((col("Account.isInterCoFlagText").isNull()|\
                            col("Account.isInterCoFlagText").isin('')),lit('#NA#')) \
                           .otherwise(col("Account.isInterCoFlagText")).alias("isInterCoFlagText"),\
                      when(col("Account.isOnlyAutomatedPostingAllowed").isNull(),lit(False)) \
                           .otherwise(col("Account.isOnlyAutomatedPostingAllowed")).alias("isOnlyAutomatedPostingAllowed"),\
                      when((col("Account.accountGroup").isNull()|\
                            col("Account.accountGroup").isin('','#NA#')),lit('')) \
                           .otherwise(col("Account.accountGroup")).alias("accountGroup")).distinct()
        
        dfL2_DIM_GLAccount = dfDistinct_GLAccount.alias("gla"). \
                select( col("gla.organizationUnitSurrogateKey").alias("organizationUnitSurrogateKey"),  \
                        col("gla.accountNumber").alias("accountNumber"),  \
                        col("gla.chartOfAccountID").alias("chartOfAccountID"),\
                        col("gla.chartOfAccountText").alias("chartOfAccountText"),\
                        col("gla.accountType").alias("accountType"),\
                        col("gla.accountCategory").alias("accountCategory"),\
                        lit("#NA#").alias("accountCategoryAudit"),\
                        col("gla.isInterCoFromAccountMapping").alias("isInterCoFromAccountMapping"),\
                        when((col("gla.isInterCoFromMasterData").isNull()|\
                              col("gla.isInterCoFromMasterData").isin('','#NA#')),lit(3)) \
                             .otherwise(col("gla.isInterCoFromMasterData")).alias("isInterCoFromMasterData"),\
                        when((col("gla.isInterCoFromTransactionData").isNull()|\
                              col("gla.isInterCoFromTransactionData").isin('','#NA#')),lit(3)) \
                             .otherwise(col("gla.isInterCoFromTransactionData")).alias("isInterCoFromTransactionData"),\
                        when((col("gla.isInterCoFlag").isNull()|\
                              col("gla.isInterCoFlag").isin('','#NA#')),lit(3)) \
                             .otherwise(col("gla.isInterCoFlag")).alias("isInterCoFlag"),\
                        col("gla.isInterCoFlagText").alias("isInterCoFlagText"),\
                        col("gla.isOnlyAutomatedPostingAllowed").alias("isOnlyAutomatedPostingAllowed"),\
                        lit("#NA#").alias("glAccountCategoryL1"),\
                        lit("#NA#").alias("glAccountCategoryL2"),\
                        lit("#NA#").alias("glAccountCategoryL3"),\
                        lit("#NA#").alias("glAccountCategoryL4"),\
                        lit("#NA#").alias("glAccountCategoryL5"),\
                        lit("#NA#").alias("glAccountCategoryL6"),\
                        lit("#NA#").alias("glAccountCategoryL7"),\
                        lit("#NA#").alias("glAccountCategoryL8"),\
                        lit("#NA#").alias("glAccountCategoryL9"),\
                        col("gla.accountGroup").alias("accountGroup")).distinct().\
               withColumn("glAccountSurrogateKey", row_number().over(w))
        
        fin_L2_DIM_GLAccount   = objDataTransformation.gen_convertToCDMandCache \
                      (dfL2_DIM_GLAccount,'fin','L2_DIM_GLAccount',True)

        dfL2_DIM_GLAccountText = fin_L2_DIM_GLAccount.alias("gla"). \
                join (gen_L2_DIM_Organization.alias("org"), \
                     (col("gla.organizationUnitSurrogateKey") == col("org.organizationUnitSurrogateKey")), how = "inner"). \
                join (knw_LK_CD_ReportingSetup.alias("rpt"), \
                     (col("org.companyCode") == col("rpt.companyCode")), how = "inner"). \
                join (fin_L1_MD_GLAccount.alias("L1_gla"), \
                     (col("org.companyCode").eqNullSafe(col("L1_gla.companyCode"))) \
                   & (col("gla.accountNumber").eqNullSafe(col("L1_gla.accountNumber"))) \
                   & (col("rpt.clientDataReportingLanguage").eqNullSafe(col("L1_gla.languageCode"))), how = "left"). \
                join (fin_L1_MD_GLAccount.alias("L1_gla1"), \
                     (col("org.companyCode").eqNullSafe(col("L1_gla1.companyCode"))) \
                   & (col("gla.accountNumber").eqNullSafe(col("L1_gla1.accountNumber"))) \
                   & (col("rpt.clientDataReportingLanguage") == lit(clientDataReportingLanguage)), how="left"). \
                select(col("gla.glAccountSurrogateKey").alias("glAccountSurrogateKey"),  \
                      lit(coalesce(col("org.reportingLanguage"),col("L1_gla.languageCode"))).alias("languageCode"),\
                      lit(coalesce(col("L1_gla.accountName"),col("L1_gla1.accountName"),lit("#NA#"))).alias("accountName")).distinct().\
               withColumn("glAccountTextSurrogateKey", row_number().over(w))



        fin_L2_DIM_GLAccountText  = objDataTransformation.gen_convertToCDMandCache \
            (dfL2_DIM_GLAccountText,'fin','L2_DIM_GLAccountText',True)

        executionStatus = "fin_L2_DIM_GLAccount populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus] 
 
    except Exception as err:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
      
      



