# Databricks notebook source
import sys
import traceback
import uuid
from pyspark.sql.functions import col, asc, lit, when,regexp_extract,asc,row_number			
import pyspark.sql.functions as func

def fin_GLPeriodwiseBalanceReconcile_validate(fileType):
    try:
        objGenHelper = gen_genericHelper() #GL
        logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                  fileType = fileType,
                                  validationID = VALIDATION_ID.PERIOD_BALANCE_EQUAL_ZERO)
        dfResult_Detail = None
        
        finYear = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR')
        startMonth = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'period_START')
        endMonth = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'period_End')
        accumulatedBalanceFlag = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ACCUMULATED_BALANCE')
        
        if  accumulatedBalanceFlag is None:
            accumulatedBalanceFlag = "1"
        
        fin_L1_TMP_GLperiodwiseBalance=spark.sql("SELECT glb.companyCode "\
                ",glb.fiscalYear "\
                ",glb.financialPeriod "\
                ",SUM(CASE WHEN glb.debitCreditIndicator ='C'  THEN  glb.endingBalanceLC ELSE 0 END) as glCrAmount " \
                ", SUM(CASE WHEN glb.debitCreditIndicator <>'C' THEN glb.endingBalanceLC ELSE 0 END)as glDrAmount " \
                ", '"+ fileType +"' as fileType " \
                "FROM	fin_L1_TD_GLBalance				glb " \
                "WHERE	glb.fiscalYear		= "+finYear+" AND "\
                "glb.financialPeriod BETWEEN "+startMonth+" AND "+endMonth+""\
                " GROUP BY glb.companyCode"\
                ",glb.fiscalYear "\
                ",glb.financialPeriod ")
            
        fin_L1_TMP_GLperiodwiseBalance.select(col("companyCode"),col("fiscalYear"), \
                col("financialPeriod"),col("glDrAmount"),col("glCrAmount"),col("fileType")).createOrReplaceTempView("fin_L1_TMP_GLperiodwiseBalance_1") 

        spark.sql("SELECT companyCode,fiscalYear,financialPeriod,glCrAmount,glDrAmount,"\
                  "round(glCrAmount+glDrAmount) as diffAmount,fileType FROM "\
                  "fin_L1_TMP_GLperiodwiseBalance_1 WHERE "\
                  "round(glDrAmount+glCrAmount)!=0")\
                  .limit(gl_maximumNumberOfValidationDetails)\
                  .createOrReplaceTempView("fin_L1_TMP_GLperiodwiseBalance_2")
        dfCnt= spark.sql("select 1 from fin_L1_TMP_GLperiodwiseBalance_2")
        dfresult = dfCnt.count()
        
        if dfresult >0:
            executionStatus = 'Periodwise balance reconciliation failed.'
            executionStatusID = LOG_EXECUTION_STATUS.WARNING
            
            DF1=spark.sql("select companyCode,cast(financialPeriod as string)financialPeriod,fileType,cast(sum(diffAmount)as string)diffAmount "\
               "from  fin_L1_TMP_GLperiodwiseBalance_2 GROUP BY companyCode,financialPeriod,fileType")
            
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
        gen_usf_workSpace_clean(executionStatus)        
    finally:
        executionLog.add(executionStatusID,logID,executionStatus,dfDetail = dfResult_Detail)
        spark.sql("UNCACHE TABLE  IF EXISTS fin_L1_TMP_GLperiodwiseBalance_1")
        return [executionStatusID,executionStatus,dfResult_Detail]

