# Databricks notebook source
import sys
import traceback

def fin_L1_STG_GLBalancesByKnowledgeAccount_populate():
  try:
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    
    accumulatedBalance = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ACCUMULATED_BALANCE')
    if (accumulatedBalance==None):
      accumulatedBalance =1

    fiscalYear = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR'))
    fiscalYearPY = str(int(fiscalYear) - 1)
    
    dfActiveAccounts = fin_L1_STG_PYCYBalance.select(col("companyCode"),\
                               col("accountNumber")).distinct().persist()
    
    dfKnowledgeMapping = knw_LK_CD_GLAccountToFinancialStructureDetailSnapshot.alias("glfsd").\
            select(col("financialStructureDetailID"),col("glAccountNumber")).distinct().\
            join(knw_LK_CD_FinancialStructureDetailSnapshot.alias("fsd"),\
               [col("fsd.financialStructureDetailID") == col("glfsd.financialStructureDetailID")],how = 'inner').\
            join(knw_LK_CD_KnowledgeAccountSnapshot.alias("acc"),\
               [(col("acc.knowledgeAccountID").eqNullSafe(col("fsd.knowledgeAccountNodeID")))
               & (col("acc.knowledgeAccountType") == 'Account')],how = 'left').\
            select(col("glfsd.glAccountNumber"),col("acc.knowledgeAccountID")).persist() 
    
    dfPeriodwiseAccounts = dfActiveAccounts.alias("A").\
              join(fin_L1_MD_GLAccount.alias("B"),\
                  [(col("A.accountNumber").eqNullSafe(col("B.accountNumber")))&
                  (col("A.companyCode").eqNullSafe(col("B.companyCode")))],how="left").\
              join(knw_LK_CD_ReportingSetup.alias("rpt"),\
                  [col("rpt.companyCode") == col("A.companyCode")],how ='inner').\
              join(dfKnowledgeMapping.alias("knw"),
                  [col("A.accountNumber").eqNullSafe(col("knw.glAccountNumber"))],how = 'left').\
              select(col("A.companyCode"),col("rpt.reportingGroup"),\
                    col("A.accountNumber"),col("B.accountName"),
                    col("B.accountType"),col("B.accountGroup"),
                    col("knw.knowledgeAccountID")).createOrReplaceTempView("ActiveAccounts")

    if(accumulatedBalance ==1 ):
      
        fin_L1_TD_GLBalance_CY_PY = fin_L1_TD_GLBalance.select\
            ("companyCode"\
             ,"fiscalYear"\
             ,"financialPeriod"\
             ,"accountNumber"\
             ,col("endingBalanceLC").alias("endingBalanceLC")\
            ).filter(col("fiscalYear").isin({fiscalYear,fiscalYearPY})).\
            createOrReplaceTempView("fin_L1_TD_GLBalance_CY_PY")
    else:
      
      dfPeriods = fin_L1_TD_GLBalance.filter(col('fiscalYear').isin({fiscalYear,fiscalYearPY})).\
                  select(col("fiscalYear"),
                  col("financialPeriod")).distinct().alias("P").\
                  join(fin_L1_TD_GLBalance.filter(col('fiscalYear').isin({fiscalYear,fiscalYearPY})).\
                  select(col("companyCode")).alias("B"),how='cross').\
                  select(col("companyCode"),
                  col("fiscalYear"),
                  col("financialPeriod")).distinct()
      
      fin_L1_TD_GLBalance_CY_PY = fin_L1_TD_GLBalance.alias('b').\
                                  join(dfPeriods.alias("p"),\
                                       [ (col("b.companyCode") == col("p.companyCode")) &\
                                        (col("b.fiscalYear") == col("p.fiscalYear")) & \
                                        (col("b.financialPeriod") <= (col("p.financialPeriod")))],how='inner').\
                                  select(col("b.companyCode"),
                                        col("b.fiscalYear"),
                                        col("p.financialPeriod"),
                                        col("b.accountNumber"),
                                        col("b.endingBalanceLC")).\
                                  groupBy(col("companyCode"),\
                                          col("fiscalYear"),\
                                          col("financialPeriod"),\
                                          col("accountNumber")).agg({'endingBalanceLC':'sum'}).\
                                 select(col("companyCode"),
                                      col("fiscalYear"),
                                      col("financialPeriod"),
                                      col("accountNumber"),
                                      col("sum(endingBalanceLC)").alias("endingBalanceLC")).\
                                createOrReplaceTempView("fin_L1_TD_GLBalance_CY_PY")
      
    spark.sql('SELECT A.companyCode,\
            A.reportingGroup,A.accountNumber,\
            A.accountName,A.accountType,\
            A.accountGroup,B.financialPeriod,\
            SUM(B.endingBalanceLC) AS PYCB,\
            A.knowledgeAccountID \
           FROM ActiveAccounts A \
           LEFT JOIN fin_L1_TD_GLBalance_CY_PY B ON A.companyCode = B.companyCode \
                                          AND A.accountNumber = B.accountNumber \
                                          AND B.fiscalYear = ' + "'"+ fiscalYearPY + "'" + ' GROUP BY A.companyCode \
					,A.reportingGroup \
					,A.accountNumber \
					,A.accountName \
					,A.accountType \
					,A.accountGroup \
					,B.financialPeriod\
					,A.knowledgeAccountID').createOrReplaceTempView("L1_TMP_PYBalances")
        
    spark.sql('SELECT A.companyCode,\
            A.reportingGroup,A.accountNumber,\
            A.accountName,A.accountType,\
            A.accountGroup,B.financialPeriod,\
           SUM(B.endingBalanceLC) AS CYCB,\
           A.knowledgeAccountID \
          FROM ActiveAccounts A \
          LEFT JOIN fin_L1_TD_GLBalance_CY_PY B ON A.companyCode = B.companyCode \
                                          AND A.accountNumber = B.accountNumber \
                                          AND B.fiscalYear = ' + "'"+ fiscalYear + "'" + ' GROUP BY A.companyCode \
					,A.reportingGroup \
					,A.accountNumber \
					,A.accountName \
					,A.accountType \
					,A.accountGroup \
					,B.financialPeriod\
					,A.knowledgeAccountID').createOrReplaceTempView("L1_TMP_CYBalances")
    
    
    spark.sql("SELECT \
           CASE WHEN PY.companyCode IS NOT NULL THEN PY.companyCode ELSE CY.companyCode END\
                AS companyCode  \
          ,CASE WHEN PY.reportingGroup IS NOT NULL THEN PY.reportingGroup ELSE CY.reportingGroup END \
                AS reportingGroup\
          ,CASE WHEN PY.accountNumber IS NOT NULL THEN PY.accountNumber ELSE CY.accountNumber END \
                AS accountNumber\
          ,CASE WHEN PY.accountName IS NOT NULL THEN PY.accountName ELSE CY.accountName END \
               AS accountName\
          ,CASE WHEN PY.accountType IS NOT NULL THEN PY.accountType ELSE CY.accountType END \
               AS accountType\
          ,CASE WHEN PY.accountGroup IS NOT NULL THEN PY.accountGroup ELSE CY.accountGroup END\
              AS accountGroup\
          ,CASE WHEN PY.financialPeriod IS NOT NULL THEN PY.financialPeriod ELSE CY.financialPeriod END\
              AS financialPeriod\
          ,CASE WHEN PY.knowledgeAccountID IS NOT NULL THEN PY.knowledgeAccountID \
               ELSE CY.knowledgeAccountID END AS knowledgeAccountID\
          ,SUM(PY.PYCB) AS PYCB\
          ,SUM(CY.CYCB) AS CYCB\
          FROM L1_TMP_PYBalances PY\
           FULL OUTER JOIN L1_TMP_CYBalances CY \
           ON (CASE WHEN PY.companyCode IS NULL THEN '' ELSE PY.companyCode END) = \
                    (CASE WHEN CY.companyCode IS NULL THEN '' ELSE CY.companyCode END) \
           AND (CASE WHEN PY.reportingGroup IS NULL THEN '' ELSE PY.reportingGroup END) = \
                    (CASE WHEN CY.reportingGroup IS NULL THEN '' ELSE CY.reportingGroup END) \
           AND (CASE WHEN PY.accountNumber IS NULL THEN '' ELSE PY.accountNumber END) = \
                    (CASE WHEN CY.accountNumber IS NULL THEN '' ELSE CY.accountNumber END) \
           AND (CASE WHEN PY.accountName IS NULL THEN '' ELSE PY.accountName END) = \
                    (CASE WHEN CY.accountName IS NULL THEN '' ELSE CY.accountName END) \
           AND (CASE WHEN PY.accountType IS NULL THEN '' ELSE PY.accountType END) = \
                    (CASE WHEN CY.accountType IS NULL THEN '' ELSE CY.accountType END) \
           AND (CASE WHEN PY.accountGroup IS NULL THEN '' ELSE PY.accountGroup END) = \
                    (CASE WHEN CY.accountGroup IS NULL THEN '' ELSE CY.accountGroup END) \
           AND (CASE WHEN PY.financialPeriod IS NULL THEN '' ELSE PY.financialPeriod END) = \
                    (CASE WHEN CY.financialPeriod IS NULL THEN '' ELSE CY.financialPeriod END) \
           AND (CASE WHEN PY.knowledgeAccountID IS NULL THEN '' ELSE PY.knowledgeAccountID END) = \
                    (CASE WHEN CY.knowledgeAccountID IS NULL THEN '' ELSE CY.knowledgeAccountID END) \
           GROUP BY CASE WHEN PY.companyCode IS NOT NULL THEN PY.companyCode ELSE CY.companyCode END\
          ,CASE WHEN PY.reportingGroup IS NOT NULL THEN PY.reportingGroup ELSE CY.reportingGroup END\
          ,CASE WHEN PY.accountNumber IS NOT NULL THEN PY.accountNumber ELSE CY.accountNumber END\
          ,CASE WHEN PY.accountName IS NOT NULL THEN PY.accountName ELSE CY.accountName END\
          ,CASE WHEN PY.accountType IS NOT NULL THEN PY.accountType ELSE CY.accountType END\
          ,CASE WHEN PY.accountGroup IS NOT NULL THEN PY.accountGroup ELSE CY.accountGroup END\
          ,CASE WHEN PY.financialPeriod IS NOT NULL THEN PY.financialPeriod ELSE CY.financialPeriod END\
          ,CASE WHEN PY.knowledgeAccountID IS NOT NULL THEN PY.knowledgeAccountID ELSE CY.knowledgeAccountID END")\
          .createOrReplaceTempView("PYCYBalances")
    
    fin_L1_STG_PYCYBalanceByGLAccount = spark.sql("SELECT companyCode,\
                                                reportingGroup,\
                                                accountNumber,\
                                                accountName,\
                                                accountType,\
                                                accountGroup,\
                                                financialPeriod,\
                                                knowledgeAccountID,\
                                                SUM(PYCB) PYCB,\
                                                SUM(CYCB) CYCB\
                                              FROM PYCYBalances \
                                              GROUP BY companyCode,\
                                                reportingGroup,\
                                                accountNumber,\
                                                accountName,\
                                                accountType,\
                                                accountGroup,\
                                                financialPeriod,\
                                                knowledgeAccountID")

    fin_L1_STG_PYCYBalanceByKnowledgeAccount = spark.sql("SELECT companyCode,\
                                                reportingGroup,\
                                                knowledgeAccountID,\
                                                financialPeriod,\
                                                SUM(PYCB) PYCB,\
                                                SUM(CYCB) CYCB\
                                              FROM PYCYBalances \
                                              GROUP BY companyCode,\
                                                reportingGroup,\
                                                knowledgeAccountID,\
                                                financialPeriod")
    
    fin_L1_STG_PYCYBalanceByGLAccount = objDataTransformation.gen_convertToCDMStructure_generate\
                                              (fin_L1_STG_PYCYBalanceByGLAccount,'fin','L1_STG_PYCYBalanceByGLAccount',\
                                              isIncludeAnalysisID = False)[0]
    objGenHelper.gen_writeSingleCsvFile_perform(df = fin_L1_STG_PYCYBalanceByGLAccount\
                         ,targetFile = gl_CDMLayer2Path + "fin_L1_STG_PYCYBalanceByGLAccount.csv")

    fin_L1_STG_PYCYBalanceByKnowledgeAccount = objDataTransformation.gen_convertToCDMStructure_generate\
                                              (fin_L1_STG_PYCYBalanceByKnowledgeAccount,'fin','L1_STG_PYCYBalanceByKnowledgeAccount',\
                                              isIncludeAnalysisID = False)[0]  
    
    objGenHelper.gen_writeSingleCsvFile_perform(df = fin_L1_STG_PYCYBalanceByKnowledgeAccount\
                                  ,targetFile = gl_CDMLayer2Path + "fin_L1_STG_PYCYBalanceByKnowledgeAccount.csv")
    executionStatus = "L1_STG_GLBalancesByKnowledgeAccount populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
  
