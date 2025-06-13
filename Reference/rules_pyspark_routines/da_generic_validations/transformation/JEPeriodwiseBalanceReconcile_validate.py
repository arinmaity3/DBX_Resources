# Databricks notebook source
import sys
import traceback
import uuid
from pyspark.sql.functions import col, asc, lit, when,regexp_extract,asc,row_number			
import pyspark.sql.functions as func

def fin_JEPeriodwiseBalanceReconcile_validate(fileType):
    try:
        objGenHelper = gen_genericHelper() #JE
        logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                  fileType = fileType,
                                  validationID = VALIDATION_ID.PERIOD_BALANCE_EQUAL_ZERO)

        dfResult_Detail = None
        
        finYear = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR')
        startMonth = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'period_START')
        endMonth = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'period_End')
        
        d0 = spark.sql("SELECT jrnl.companyCode "\
            ",jrnl.fiscalYear "\
            ",jrnl.financialPeriod "\
            ",SUM(CASE WHEN jrnl.debitCreditIndicator ='C'	THEN jrnl.amountLC ELSE 0 END)  as jeCrAmount " \
            ",SUM(CASE WHEN jrnl.debitCreditIndicator <>'C'	THEN  jrnl.amountLC ELSE 0 END) as jeDrAmount " \
            ",SUM(CASE WHEN jrnl.debitCreditIndicator ='C'	THEN jrnl.amountLC ELSE 0 END) -SUM(CASE WHEN jrnl.debitCreditIndicator <>'C' "\
            "THEN  jrnl.amountLC ELSE 0 END) as diffAmount "\
            ", '"+ fileType +"' as fileType " \
            "FROM	fin_L1_TD_Journal				jrnl " \
            "WHERE	jrnl.fiscalYear		= "+finYear+" AND "\
            "jrnl.financialPeriod BETWEEN "+startMonth+" AND "+endMonth+ " "\
            " AND jrnl.documentStatus	= 'N' "\
            " GROUP BY jrnl.companyCode"\
            ",jrnl.fiscalYear "\
            ",jrnl.financialPeriod ").createOrReplaceTempView("fin_L1_TMP_JEperiodwiseBalance")
        
        spark.sql("select companyCode,fiscalYear,financialPeriod,jeCrAmount,jeDrAmount,diffAmount,fileType"\
                   " from fin_L1_TMP_JEperiodwiseBalance where diffAmount!=0 ")\
                   .limit(gl_maximumNumberOfValidationDetails)\
                   .createOrReplaceTempView("fin_L1_TMP_JEperiodwiseBalance_1")
        
        dfCnt= spark.sql("select 1 from fin_L1_TMP_JEperiodwiseBalance_1")
        dfresult = dfCnt.count()
        
        if dfresult >0:
            executionStatus = 'Periodwise balance reconciliation faild.'
            executionStatusID = LOG_EXECUTION_STATUS.WARNING
            
            DF1=spark.sql("select companyCode,cast(financialPeriod as string)financialPeriod,fileType,cast(sum(diffAmount)as string)diffAmount "\
               "from  fin_L1_TMP_JEperiodwiseBalance_1 GROUP BY companyCode,financialPeriod,fileType")
            dfResult_Detail= objGenHelper.gen_dynamicPivot(fileType =fileType,
                                      validationID =VALIDATION_ID.PERIOD_BALANCE_EQUAL_ZERO.value,
                                      orderBycolumnName1='companyCode',
                                      orderBycolumnName2='financialPeriod',
                                      orderBycolumnName3='fileType',
                                      pivotColumn1='companyCode',
                                      pivotColumn2='financialPeriod',
                                      pivotColumn3='diffAmount',
                                      dfToPivot=DF1)
            
        else:
            executionStatus =  'Periodwise balance reconciliation succeeded.'
            executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
            
    except Exception as err:
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED
    finally:
        executionLog.add(executionStatusID,logID,executionStatus,dfDetail = dfResult_Detail)
        return [executionStatusID,executionStatus,dfResult_Detail]
