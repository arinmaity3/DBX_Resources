# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField
from pyspark.sql.window import Window

def fin_L1_STG_GLBalanceAccumulated_populate():
    try:
        global fin_L1_STG_GLBalanceAccumulated
        fin_L1_STG_GLBalanceAccumulated = None
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()   
        fileName1 = gl_CDMLayer1Path + "fin_L1_STG_GLBalanceAccumulated.csv"
        fileNamedelta =  gl_CDMLayer1Path + "fin_L1_STG_GLBalanceAccumulated.delta"
        logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,\
                                    fileType = 'GLAB',
                                    validationID = VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION)

        accumulatedBalanceFlag = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ACCUMULATED_BALANCE'))
        finYear     = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR')
        prvfinYear = str(int(finYear)-1) 
    
        if  accumulatedBalanceFlag is None:
            accumulatedBalanceFlag = "1"

        if accumulatedBalanceFlag == "0":
            fin_L1_STG_GLBalanceAccumulated = spark.createDataFrame([], StructType([]))
            fin_L1_STG_GLBalanceAccumulated = objDataTransformation.\
                gen_convertToCDMStructure_generate(fin_L1_STG_GLBalanceAccumulated,'fin','L1_STG_GLBalanceAccumulated')[0]  
            fin_L1_STG_GLBalanceAccumulated = objDataTransformation.gen_convertToCDMStructure_generate\
                (fin_L1_STG_GLBalanceAccumulated,'fin','L1_STG_GLBalanceAccumulated')[0]
            fin_L1_STG_GLBalanceAccumulated.createOrReplaceTempView("fin_L1_STG_GLBalanceAccumulated")               
            sqlContext.cacheTable("fin_L1_STG_GLBalanceAccumulated")            
            objGenHelper.gen_writeToFile_perfom(fin_L1_STG_GLBalanceAccumulated,fileName1)            
            objGenHelper.gen_writeToFile_perfom(df = fin_L1_STG_GLBalanceAccumulated, filenamepath = fileNamedelta)        
            executionStatus =  "No accoumulated balanaces, skipping this routine."
            dfValidationResultSummary = executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
            print(executionStatus)            
            return None
         
        spark.sql("SELECT  companyCode"
                                              ",accountNumber"
                                              ",fiscalYear"
                                              ",CASE WHEN financialPeriod = 0 THEN -1 ELSE financialPeriod end as  financialPeriod"
                                              ",SUM(endingBalanceLC ) endingBalanceLC"
                                               ",debitCreditIndicator from fin_L1_TD_GLBalance"
                                               " GROUP BY companyCode,accountNumber,fiscalYear,"
                                               "financialPeriod,debitCreditIndicator").createOrReplaceTempView('dfFin_L1_TMP_GLBalanceDC1')

        spark.sql( "SELECT 'D' debitCreditIndicator  union   select 'C' debitCreditIndicator").createOrReplaceTempView('dfDebitCreditIndicator')

        spark.sql("SELECT "
                                              "wrapper.companyCode "
                                              ",wrapper.accountNumber "
                                              ",wrapper.fiscalYear "
                                              ",wrapper.financialPeriod "
                                              ",case when ISNULL(amount.endingBalanceLC) then 0 else amount.endingBalanceLC end endingBalanceLC "
                                              ",wrapper.debitCreditIndicator "
                                              " FROM ("
                                              " SELECT a.companyCode, a.fiscalYear, a.financialPeriod, a.accountNumber, b.debitCreditIndicator"
                                              " FROM dfFin_L1_TMP_GLBalanceDC1 a"
                                              " CROSS JOIN (SELECT * from dfDebitCreditIndicator) as b "
                                              " ) wrapper"
                                              " LEFT JOIN  dfFin_L1_TMP_GLBalanceDC1 amount ON wrapper.companyCode = amount.companyCode"
                                              " and wrapper.fiscalYear = amount.fiscalYear "
                                              " AND wrapper.financialPeriod = amount.financialPeriod "
                                              " AND wrapper.debitCreditIndicator= amount.debitCreditIndicator "
                                              " AND wrapper.accountNumber = amount.accountNumber "
                                              "where amount.companyCode IS NULL"
                                              " order by fiscalyear,financialPeriod,"
                                              "debitCreditIndicator").createOrReplaceTempView('dfFin_L1_TMP_GLBalanceDC2')

        fin_L1_TMP_GLBalanceCredit = spark.sql("select "
                                               "* from (select * from dfFin_L1_TMP_GLBalanceDC1 union select * from dfFin_L1_TMP_GLBalanceDC2"
                                               ")A where debitCreditIndicator='C'")
        

        my_window = Window.partitionBy("companyCode" , "accountNumber", "fiscalYear", "debitCreditIndicator").orderBy("financialPeriod")
        fin_L1_TMP_GLBalanceCredit = fin_L1_TMP_GLBalanceCredit.withColumn("prev_value", F.lag( fin_L1_TMP_GLBalanceCredit.endingBalanceLC).over(my_window))

        fin_L1_TMP_GLBalanceCredit = fin_L1_TMP_GLBalanceCredit.withColumn("diff_Credit", F.when( F.isnull(fin_L1_TMP_GLBalanceCredit.endingBalanceLC - fin_L1_TMP_GLBalanceCredit.prev_value),
            fin_L1_TMP_GLBalanceCredit.endingBalanceLC) .otherwise( fin_L1_TMP_GLBalanceCredit.endingBalanceLC - fin_L1_TMP_GLBalanceCredit.prev_value))

        fin_L1_TMP_GLBalanceDebit = spark.sql("select "  "* from (select * from dfFin_L1_TMP_GLBalanceDC1 union select * from dfFin_L1_TMP_GLBalanceDC2"
                                              ")A where debitCreditIndicator='D'")

        fin_L1_TMP_GLBalanceDebit = fin_L1_TMP_GLBalanceDebit.withColumn("prev_value",
                                                                         F.lag(fin_L1_TMP_GLBalanceDebit.endingBalanceLC).over( my_window))
        fin_L1_TMP_GLBalanceDebit = fin_L1_TMP_GLBalanceDebit.withColumn("diff_Debit", F.when(
            F.isnull(fin_L1_TMP_GLBalanceDebit.endingBalanceLC - fin_L1_TMP_GLBalanceDebit.prev_value),
            fin_L1_TMP_GLBalanceDebit.endingBalanceLC) .otherwise( fin_L1_TMP_GLBalanceDebit.endingBalanceLC - fin_L1_TMP_GLBalanceDebit.prev_value))

        fin_L1_TMP_GLBalanceDebit.union(fin_L1_TMP_GLBalanceCredit).createOrReplaceTempView("temp")

        fin_L1_STG_GLBalanceAccumulated = spark.sql("select "
        "companyCode"
        ",accountNumber"
        ",fiscalYear"
        ",financialPeriod"
        ",CASE WHEN ISNULL(debitCreditIndicator)   THEN"
        " CASE WHEN endingBalanceLC < CAST(0 AS FLOAT) THEN 'C' ELSE 'D' END"
        " ELSE debitCreditIndicator "
        " END debitCreditIndicator"        
        ",cast(CASE WHEN financialPeriod =-1 then endingBalanceLC else  diff_Debit END  as numeric(32,6)) endingBalanceLC from temp")
        
        if(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'BALANCE_TYPE') == 'GLTB'):
            ls_periodColumns = ['period16', 'period15', 'period14', 'period13', 'period12', 'period11', 'period10', 'period09', 'period08', 'period07', 'period06', 'period05', 'period04', 'period03', 'period02', 'period01']
            for periodCol in sorted(ls_periodColumns,reverse=True):
              strcmd = f'fin_L1_TD_GLBalancePeriodic.select(col("{periodCol}")).filter((col("{periodCol}").isNotNull())&(col("fiscalYear")==lit("{prvfinYear}"))).first()'
              retval = eval(strcmd)
              if retval is not None:
                break
            prevEndPeriod = periodCol.replace('period','')
            
            fin_L1_STG_GLBalanceAccumulated = fin_L1_STG_GLBalanceAccumulated.\
            filter((col("fiscalYear") != lit(prvfinYear)) \
                  |((col("fiscalYear") == lit(prvfinYear)) & (col("financialPeriod") <= lit(int(prevEndPeriod)))))

        w = Window().orderBy(lit('groupSlNo'))

        fin_L1_STG_GLBalanceAccumulated = fin_L1_STG_GLBalanceAccumulated.withColumn("localCurrency",lit(""))\
        .withColumn("documentCurrency",lit("")).withColumn("amountLC", lit(""))\
        .withColumn("amountDC", lit("")).withColumn("endingBalanceDC",lit(""))\
        .withColumn("ID", row_number().over(w))
        
        fin_L1_STG_GLBalanceAccumulated = objDataTransformation.gen_convertToCDMStructure_generate(fin_L1_STG_GLBalanceAccumulated,'fin','L1_STG_GLBalanceAccumulated')[0]
        fin_L1_STG_GLBalanceAccumulated.createOrReplaceTempView("fin_L1_STG_GLBalanceAccumulated")               
        sqlContext.cacheTable("fin_L1_STG_GLBalanceAccumulated")

        objGenHelper.gen_writeToFile_perfom(fin_L1_STG_GLBalanceAccumulated,fileName1)        
        objGenHelper.gen_writeToFile_perfom(df = fin_L1_STG_GLBalanceAccumulated, filenamepath = fileNamedelta)
        
        executionStatus = "Created fin_L1_STG_GLBalanceAccumulated succesfully."
        executionStatusID = LOG_EXECUTION_STATUS.SUCCESS 
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
        
    except Exception as e:
        fin_L1_STG_GLBalanceAccumulated = None
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED
        executionLog.add(executionStatusID,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    finally:
        print(executionStatus)
        

 


