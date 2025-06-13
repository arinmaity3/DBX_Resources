# Databricks notebook source
from pyspark.sql.functions import expr
import itertools

class gen_dimensionAttributesKnowledge_prepare():
    @staticmethod
    def knowledgeAttributes_collect():
        try:
            objGenHelper = gen_genericHelper()
            logID = executionLog.init(processID = PROCESS_ID.L2_TRANSFORMATION,className = __class__.__name__)

            #region GLAccount
            reportingGroup = list()
            glAccounts = list()
            key = ['Knowledge']
            [reportingGroup.append(rpt.companyCode) \
                for rpt in  knw_LK_CD_ReportingSetup.\
                select(col("companyCode")).distinct().rdd.collect()]

            [glAccounts.append(acc.glAccountNumber) \
                for acc in  knw_LK_CD_GLAccountToFinancialStructureDetailSnapshot.\
                select(col("glAccountNumber")).distinct().\
                filter(~(col("glAccountNumber").isin('#NA#','','Unbalanced')) &\
                (col("glAccountNumber").isNotNull())).rdd.collect()]

            gl_lstOfGLAccountCollect.\
                extend(list(itertools.product(reportingGroup,glAccounts,key)))

            #endregion

            executionStatus = "Knowledge related attributes populated sucessfully"
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log() 
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
