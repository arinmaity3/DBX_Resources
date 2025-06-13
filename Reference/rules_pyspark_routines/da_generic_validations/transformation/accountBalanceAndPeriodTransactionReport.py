# Databricks notebook source
from dateutil.parser import parse
from pyspark.sql.functions import  expr, when,lit,col,concat
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sys
import traceback
import uuid
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import Window
from functools import reduce

def fin_accountBalanceAndPeriodTransactionReport():    
    try:
        objGenHelper = gen_genericHelper()
        logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,None,None,None,VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION) 
        fin_L1_STG_JEPeriodBalance = None
        fin_L1_STG_JEAccountBalance = None
        df_ALLBalanceDetails = None
        finYear = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR')
        startDate = str(parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date())
        endDate = str(parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date())
        periodStart = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'period_START')
        periodEnd = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'period_End')
        erpSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')      
        

        ## Get All account
        #1
        df_GlaOpeningwithaccountName =spark.sql("SELECT \
                        glab.companyCode,glab.accountNumber,glab.accountName\
                        ,IF (bdvm.targetSystemValueDescription IS NULL ,'',bdvm.targetSystemValueDescription)	 AS accountType \
                        ,-1 AS financialPeriod,'openingBalance' AS periodDet \
                        ,sum(glab.CYOB)	 AS openingBalance \
                        ,sum(glab.CYCB)	 AS closingGLBalance \
                        FROM  fin_L1_STG_PYCYBalance glab \
                        LEFT JOIN knw_LK_CD_ReportingLanguage rep on \
                        (\
                          rep.companyCode		=	glab.companyCode\
                          AND rep.languageCodeColumn = 'KPMGDataReportingLanguage'\
                         )\
                         LEFT JOIN knw_LK_GD_BusinessDatatypeValueMapping bdvm  ON\
                         (\
                           bdvm.businessDatatype		=	'Account Type Indicator'\
                           AND bdvm.sourceERPSystemID	=   '" + str(erpSystemID) + "' \
                           AND bdvm.targetLanguageCode  =	rep.languageCode \
                           AND glab.accountType			=	bdvm.sourceSystemValue\
                          )\
                          GROUP BY  glab.companyCode,glab.accountNumber,glab.accountName\
                          ,IF (bdvm.targetSystemValueDescription IS NULL ,'',bdvm.targetSystemValueDescription)	\
                          ")
        #df_GlaOpeningwithaccountName.persist()
        #2
        #Journal-Transcation Details
        df_Jetperiod=spark.sql("SELECT companyCode,accountNumber,financialPeriod\
                                  ,'period'	AS periodDet\
                                  ,SUM(IF (debitCreditIndicator='C',(-1.0*amountLC),amountLC)) AS amount\
                                  ,SUM(IF (debitCreditIndicator='C',(-1.0*amountLC),0)) AS creditBalance\
                                  ,SUM(IF (debitCreditIndicator='C',0,amountLC)) AS debitBalance \
                              FROM  fin_L1_TD_Journal\
                              WHERE	IF (fiscalYear IS NULL,0,fiscalYear) = '" + finYear + \
                            "' AND	postingDate		BETWEEN '" + startDate + "'	 AND  '" + endDate + \
                            "' AND	financialPeriod		BETWEEN " + periodStart + "	 AND  " + periodEnd + \
                            "  AND	IF (documentStatus IS NULL,'',documentStatus) ='N' \
                             GROUP BY companyCode,accountNumber,financialPeriod")               

        #df_Jetperiod.persist()
        #3
        #Get distinct Required AccountNumber(df_GlaOpening contains account from JET,GLA,GLAB aslo)
        df_distinctAccount=df_GlaOpeningwithaccountName.select('companyCode', 'accountNumber').distinct()
        startlimit=int(periodStart)
        endlimit=int(periodEnd)+2
        periodClosing=int(periodEnd)+1
        lst_period=[]
        for i in range(startlimit,endlimit):
            lst_period.append(i)

        dfPeriod=spark.createDataFrame(lst_period, IntegerType())
        df_distinctAccountPeriod =df_distinctAccount.crossJoin(dfPeriod)
     
        #4
        ##Next Step
        new_column_periodDet = (when((col('prd.value') == int(periodClosing)), lit('closingBalance'))\
                              .otherwise(concat(lit('period'),col('prd.value').cast("String"))))

        df_Jetperiod1=df_distinctAccountPeriod.alias('prd')\
                            .join(df_Jetperiod.alias('jrnl'),\
                                   ((col('jrnl.companyCode')== col('prd.companyCode'))\
                                     & (col('jrnl.accountNumber')  == col('prd.accountNumber'))\
                                     & (col('jrnl.financialPeriod')  == col('prd.value'))\
                                     ),how='left')\
                            .select( col('prd.companyCode'), col('prd.value').alias('financialPeriod')\
                                    ,col('prd.accountNumber')\
                                    ,lit(new_column_periodDet).alias('periodDet')
                                    ,(when(col('jrnl.amount').isNull(),lit(0) ).otherwise(col('jrnl.amount'))).alias('amount') )

        #df_Jetperiod1.cache()  

        df_ALLBalance =df_Jetperiod1.select('companyCode',  'financialPeriod', 'accountNumber', 'periodDet','amount')\
         .unionAll(df_GlaOpeningwithaccountName.select('companyCode',  'financialPeriod', 'accountNumber', 'periodDet','openingBalance'))
      
        #Calculate Account Balance
        windowval = (Window.partitionBy("companyCode","accountNumber").orderBy('financialPeriod')
                   .rangeBetween(Window.unboundedPreceding, 0))
        df_ALLBalance = df_ALLBalance.withColumn('accountBalance', F.sum('amount').over(windowval))

        #Calculate Period Balance
        new_column_periodBalance = when(col("financialPeriod")== periodClosing, col('accountBalance'))\
                              .otherwise(col("amount"))

        df_ALLBalance=df_ALLBalance.withColumn('periodBalance',new_column_periodBalance)
   
        #5      
        ##Get the Account Details
        df_ALLBalanceDetails=df_ALLBalance.alias('glab')\
                    .join(df_GlaOpeningwithaccountName.alias('gla'),on=['companyCode','accountNumber'],how='inner')\
                    .select(col('glab.companyCode').alias('companyCode'),col('glab.accountNumber').alias('accountNumber')\
                           ,col('gla.accountName').alias('accountName'),col('gla.accountType').alias('accountType')\
                           ,col('glab.periodDet').alias('periodDet'),col('glab.accountBalance').alias('accountBalance')\
                           ,col('glab.periodBalance').alias('periodBalance')\
                           )     
        df_ALLBalanceDetails.persist()
      
        #To get the volumn order
        lst_period1=[]
        lst_period1.append("openingBalance")
        for i in range(startlimit,endlimit-1):
            lst_period1.append("period" + str(i))
        lst_period1.append("closingBalance")

        #Period Balance
        fin_L1_STG_JEPeriodBalance = df_ALLBalanceDetails.groupby("companyCode","accountNumber","accountName","accountType") \
                                              .pivot('periodDet',lst_period1) \
                                              .max('periodBalance')\
                                              .fillna(0)     
        fin_L1_STG_JEPeriodBalance.persist()
      
        #Account Balance      
        fin_L1_STG_JEAccountBalance = df_ALLBalanceDetails.groupby("companyCode","accountNumber","accountName","accountType") \
                                              .pivot('periodDet',lst_period1) \
                                              .max('accountBalance')\
                                              .fillna(0)
      
        fin_L1_STG_JEAccountBalance.persist()
        #Writing the Periodic Balance reports
        objGenHelper.gen_writeSingleCsvFile_perform(df = fin_L1_STG_JEPeriodBalance,targetFile = gl_reportPath + "fin_L1_STG_JEPeriodBalance.csv")
        print("Account balance report file generated sucessfully.")       
        #Writing the Account Balance reports
        objGenHelper.gen_writeSingleCsvFile_perform(df = fin_L1_STG_JEAccountBalance,targetFile = gl_reportPath +  "fin_L1_STG_JEAccountBalance.csv")
        print("Period balance report generated sucessfully.")
        executionStatus = "Account balance and Period balance reports generated successfully."
        executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    except Exception as e:
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
        print(executionStatus)         
    finally:
        if fin_L1_STG_JEPeriodBalance is not None:
            fin_L1_STG_JEPeriodBalance.unpersist()
        if fin_L1_STG_JEAccountBalance is not None:
            fin_L1_STG_JEAccountBalance.unpersist()
        if df_ALLBalanceDetails is not None:
            df_ALLBalanceDetails.unpersist()
        return [executionStatusID,executionStatus]
