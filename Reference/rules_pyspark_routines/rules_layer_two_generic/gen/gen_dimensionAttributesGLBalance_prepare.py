# Databricks notebook source
class gen_dimensionAttributesGLBalance_prepare():
    @staticmethod
    def glBalanceAttributes_collect():
        try:
            
            objGenHelper = gen_genericHelper()
            logID = executionLog.init(processID = PROCESS_ID.L2_TRANSFORMATION,className = __class__.__name__)

            #region CompanyCode
            gl_lstOfOrganizationCollect.extend(fin_L1_TD_GLBalance.\
                select(col("companyCode"),lit('GLBalance'))\
                .filter((col("companyCode") != "#NA#") & (col("companyCode").isNotNull()))\
                .distinct()\
                .rdd.map(lambda row : [row[0],row[1]]).collect())

            #regionend

            #region Financial Period
            gl_lstOfFinancialPeriodCollect.extend(fin_L1_TD_GLBalance.\
                select(col('companyCode'),col('fiscalYear'),lit('1900-02-02').cast("Date"),\
                when (col('financialPeriod') < lit(1), lit(-1)).\
                otherwise(col('financialPeriod')),lit('GLBalance'))                
                .distinct() \
                .rdd.map(lambda row : [row[0],row[1],row[2],row[3],row[4]]).collect() ) 
            
            #endregion

            #region GLAccount
            gl_lstOfGLAccountCollect.extend(fin_L1_TD_GLBalance.\
                select(col("companyCode"),col("accountNumber"),\
                lit('GLBalance'))\
                .filter((col("accountNumber") != "#NA#") & (col("accountNumber").isNotNull()))
                .distinct().rdd.map(lambda row : [row[0],row[1],row[2]]).collect())
            #endregion

            executionStatus = "GL balances attributes populated sucessfully"
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log() 
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    


