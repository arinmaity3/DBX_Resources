# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, asc, lit, when,regexp_extract,asc,row_number
from pyspark.sql import Row
from functools import reduce
from pyspark.sql import DataFrame
from dateutil.parser import parse
import pyspark.sql.functions as func
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


def fin_L1_STG_PYCYBalance_populate(processID = PROCESS_ID.TRANSFORMATION_VALIDATION):
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(processID = processID,\
                                    fileType = 'GLAB',
                                    validationID = VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION)

        global fin_L1_STG_PYCYBalance
        fin_L1_STG_PYCYBalance = None
        FIN_YEAR = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR'))
        
        if FIN_YEAR == 'None':
            FIN_YEAR = 'NULL'
            PRV_FIN_YEAR = 'NULL'
        else:
            PRV_FIN_YEAR =  str(int(FIN_YEAR)-1)
            
        PERIOD_START = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_START'))
      
        GLAB_MIN_FIN_PERIOD = str(spark.sql('select MIN(financialPeriod) FROM fin_L1_TD_GLBalance where fiscalYear = '+ FIN_YEAR).collect()[0][0])

        startDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date()
        endDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date()
        
        if PERIOD_START == 'None':
            PERIOD_START = 'NULL'
            PRV_PERIOD = 'NULL'
        else:
            if GLAB_MIN_FIN_PERIOD == '-1':
                PRV_PERIOD = '-1'
            else:
                PRV_PERIOD = str(int(PERIOD_START)-1)
                
        PERIOD_END = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_END'))
        if PERIOD_END == 'None':
            PERIOD_END = 'NULL'
        accumulatedBalanceFlag = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ACCUMULATED_BALANCE')

        if(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'BALANCE_TYPE') == 'GLTB'):
            PYPeriodEnd = str(spark.sql('select MAX(financialPeriod) FROM fin_L1_TD_GLBalance \
                where endingBalanceLC is not null and fiscalYear = '+ PRV_FIN_YEAR).collect()[0][0])
        else:
            PYPeriodEnd = str(spark.sql('select MAX(financialPeriod) FROM fin_L1_TD_GLBalance \
               where fiscalYear = '+ PRV_FIN_YEAR).collect()[0][0])

        if PYPeriodEnd == 'None':
            PYPeriodEnd = 'NULL'
        #Identify all active accounts#
        
        if accumulatedBalanceFlag is None:
            accumulatedBalanceFlag = "1"
        
        df12 = spark.sql('  SELECT DISTINCT companyCode'
				 ', accountNumber'
                 ' FROM fin_L1_TD_GLBalance '
                 ' WHERE fiscalYear in ('+FIN_YEAR+','+PRV_FIN_YEAR+')')
        df13 = spark.sql('  SELECT DISTINCT companyCode'
				 ', accountNumber'
                 ' FROM fin_L1_TD_Journal '
                 'WHERE   fiscalYear == '+FIN_YEAR+' ')
        dfs = [df12,df13]
        dfResult_Detail = reduce(DataFrame.union, dfs)
        dfResult_Detail.createOrReplaceTempView('dfResult_Detail')
        dfJetOnly = df13.select(col('accountNumber')).distinct()
        
        L1_TMP_ActiveAccounts = spark.sql('SELECT	DISTINCT  A.companyCode '
				',A.accountNumber '
				',B.accountType '	
                'FROM	dfResult_Detail A '				
				'LEFT JOIN fin_L1_MD_GLAccount B ON (A.accountNumber = B.accountNumber)')
        L1_TMP_ActiveAccounts.createOrReplaceTempView('L1_TMP_ActiveAccounts')
        
        if str(accumulatedBalanceFlag) == "1":
            spark.sql('SELECT  A.companyCode '
                       ',A.accountNumber '
                       ',A.accountType '
                       ',SUM(B.endingBalanceLC) AS PYCBamount'
                       ',"PYCB" AS PYCBbalanceType '
                       'from L1_TMP_ActiveAccounts A '
                       'LEFT JOIN fin_L1_TD_GLBalance B ON (A.accountNumber = B.accountNumber'
                                      ' AND A.companyCode = B.companyCode '
                                      ' AND B.fiscalYear  = '''+PRV_FIN_YEAR+''
                                      ' AND B.financialPeriod	= '+PYPeriodEnd+') GROUP BY A.companyCode,A.accountNumber,A.accountType ').createOrReplaceTempView("L1_TMP_PYCYBalancePYCB")
            
            spark.sql('SELECT  A.companyCode '
                       ',A.accountNumber '
                       ',A.accountType '
                       ',SUM(B.endingBalanceLC) AS CYOBamount'
                       ',"CYOB" AS CYOBbalanceType '
                       'from L1_TMP_ActiveAccounts A '
                       'LEFT JOIN fin_L1_TD_GLBalance B ON (A.accountNumber = B.accountNumber'
                                      ' AND A.companyCode = B.companyCode '
                                      ' AND B.fiscalYear  = '''+FIN_YEAR+''
                                      ' AND B.financialPeriod	= '+PRV_PERIOD+') GROUP BY A.companyCode,A.accountNumber,A.accountType ').createOrReplaceTempView("L1_TMP_PYCYBalanceCYOB")
            
            spark.sql('SELECT  A.companyCode '
                       ',A.accountNumber '
                       ',A.accountType '
                       ',SUM(B.endingBalanceLC) AS CYCBamount'
                       ',"CYCB" AS CYCBbalanceType '
                       'from L1_TMP_ActiveAccounts A '
                       'LEFT JOIN fin_L1_TD_GLBalance B ON (A.accountNumber = B.accountNumber '
                                      ' AND A.companyCode = B.companyCode '
                                      ' AND B.fiscalYear  = '+FIN_YEAR+''
                                      ' AND B.financialPeriod	= '+PERIOD_END+') GROUP BY A.companyCode,A.accountNumber,A.accountType ').createOrReplaceTempView("L1_TMP_PYCYBalanceCYCB")
            
            spark.sql("select case when pycb.companyCode is null then cyob.companyCode else pycb.companyCode end as companyCode \
                          ,case when pycb.accountNumber is null then cyob.accountNumber else pycb.accountNumber end as accountNumber \
                          ,case when pycb.accountType is null then cyob.accountType else pycb.accountType end as accountType \
                          ,cyob.CYOBamount as CYOB \
                          ,pycb.PYCBamount as PYCB \
                          from L1_TMP_PYCYBalancePYCB pycb \
                          full outer join L1_TMP_PYCYBalanceCYOB cyob \
                          on  pycb.companyCode = cyob.companyCode \
                          and pycb.accountNumber = cyob.accountNumber \
                          and (case when pycb.accountType is null then '' else pycb.accountType end) = (case when cyob.accountType is null then '' else cyob.accountType end) \
                          where pycb.companyCode is not null or cyob.companyCode is not null").createOrReplaceTempView("L1_TMP_PYCYBalancePYCB_and_CYOB")
            
            spark.sql("select case when pycb_cyob.companyCode is null then cycb.companyCode else pycb_cyob.companyCode end as companyCode \
                          ,case when pycb_cyob.accountNumber is null then cycb.accountNumber else pycb_cyob.accountNumber end as accountNumber \
                          ,case when pycb_cyob.accountType is null then cycb.accountType else pycb_cyob.accountType end as accountType \
                          ,cycb.CYCBamount as CYCB \
                          ,pycb_cyob.PYCB as PYCB \
                          ,pycb_cyob.CYOB as CYOB \
                          from L1_TMP_PYCYBalancePYCB_and_CYOB pycb_cyob \
                          full outer join L1_TMP_PYCYBalanceCYCB cycb \
                          on  pycb_cyob.companyCode = cycb.companyCode \
                          and pycb_cyob.accountNumber = cycb.accountNumber \
                          and (case when pycb_cyob.accountType is null then '' else pycb_cyob.accountType end) = (case when cycb.accountType is null then '' else cycb.accountType end) \
                          where pycb_cyob.companyCode is not null or cycb.companyCode is not null").createOrReplaceTempView("L1_TMP_PYCYBalance_PYCB_CYOB_CYCB")
            
            spark.sql("select companyCode,accountNumber,accountType,sum(CYCB) as CYCB,sum(PYCB) as PYCB,sum(CYOB) as CYOB \
                    from L1_TMP_PYCYBalance_PYCB_CYOB_CYCB \
                   group by companyCode,accountNumber,accountType").createOrReplaceTempView("L1_STG_PYCYBalance_summarised")
            
            fin_L1_STG_PYCYBalance = spark.sql("select \
                                        case when ISNULL(P.companyCode) then A.companyCode else p.companyCode end as companyCode, \
                                        case when ISNULL(P.accountNumber) then A.accountNumber else p.accountNumber end as accountNumber, \
                                        case when ISNULL(P.accountType) then A.accountType else p.accountType end as accountType, \
                                        cast(P.CYCB as numeric(32,6)) as CYCB, \
                                        cast(P.PYCB as numeric(32,6)) as PYCB, \
                                        cast(P.CYOB as numeric(32,6)) as CYOB, \
                                        A.accountName, \
                                        A.accountDescription, \
                                        A.accountGroup, \
                                        case when ISNULL(P.accountNumber) then 0 else 1 end as isTransactionAccount \
                                       from L1_STG_PYCYBalance_summarised P \
                                       FULL OUTER JOIN fin_L1_MD_GLAccount A \
                                        ON P.companyCode = A.companyCode \
                                        AND P.accountNumber = A.accountNumber")
        else:
            L1_TMP_PYCYBalanceCYOB3 =  spark.sql('SELECT A.companyCode,A.accountNumber,A.accountType, '
                                    ' sum(B.endingBalanceLC)  as amount '
                                    ' FROM L1_TMP_ActiveAccounts A '
                                   'LEFT JOIN fin_L1_TD_GLBalance B ON ('
                                   'A.companyCode		= B.companyCode '
                                   'AND A.accountNumber = B.accountNumber '
                                   'AND B.fiscalYear	= '+PRV_FIN_YEAR+') '
                                   'GROUP BY A.companyCode '
					 ',A.accountNumber' 			 
					 ',A.accountType')
            L1_TMP_PYCYBalanceCYOB3.createOrReplaceTempView('L1_TMP_PYCYBalanceCYOB3')
            
            CYBalances =  spark.sql('SELECT B.companyCode,B.accountNumber,A.accountType, '
                                    ' sum( CASE WHEN B.financialPeriod < '+ PERIOD_START +' THEN '
                                     ' B.endingBalanceLC '
                                     ' ELSE 0 END)  as CYOB, '
                                     ' sum( CASE WHEN B.financialPeriod <= '+PERIOD_END+' THEN '
                                     ' B.endingBalanceLC '
                                     ' ELSE 0 END)  as CYCB '
                                    ' FROM L1_TMP_ActiveAccounts A '
                                   'LEFT JOIN fin_L1_TD_GLBalance B ON ('
                                   'A.companyCode		= B.companyCode '
                                   'AND A.accountNumber = B.accountNumber '
                                   'AND B.fiscalYear	= '+ FIN_YEAR +'  ) '
                                   'GROUP BY B.companyCode '
					 ',B.accountNumber' 			 
					 ',A.accountType')
            CYBalances.createOrReplaceTempView('CYBalances')
            
            spark.sql("SELECT case when ISNULL(PY.companyCode) then CY.companyCode else PY.companyCode end as companyCode , "
                                 "case when ISNULL(PY.accountNumber) then CY.accountNumber else PY.accountNumber end as accountNumber , "
                                 "case when ISNULL(PY.accountType) then CY.accountType else PY.accountType end as accountType,  "
                                 "PY.amount as PYCB, "
                                 "CY.CYOB, "
                                 "CY.CYCB "
                                 " FROM CYBalances CY FULL OUTER JOIN L1_TMP_PYCYBalanceCYOB3 PY ON ( CY.companyCode		= PY.companyCode "
                                                                                                " AND CY.accountNumber	= PY.accountNumber "
                                                                                                " AND CY.accountType	= PY.accountType ) "
                                " WHERE PY.companyCode IS NOT NULL "
                                " or CY.companyCode IS NOT NULL").createOrReplaceTempView("L1_STG_PYCYBalance_summarised")
            
            fin_L1_STG_PYCYBalance = spark.sql("select \
                                        case when ISNULL(P.companyCode) then A.companyCode else p.companyCode end as companyCode, \
                                        case when ISNULL(P.accountNumber) then A.accountNumber else p.accountNumber end as accountNumber, \
                                        case when ISNULL(P.accountType) then A.accountType else p.accountType end as accountType, \
                                        cast(P.CYCB as numeric(32,6)) as CYCB, \
                                        cast(P.PYCB as numeric(32,6)) as PYCB, \
                                        cast(P.CYOB as numeric(32,6)) as CYOB, \
                                        A.accountName, \
                                        A.accountDescription, \
                                        A.accountGroup, \
                                        case when ISNULL(P.accountNumber) then 0 else 1 end as isTransactionAccount \
                                       from L1_STG_PYCYBalance_summarised P \
                                       FULL OUTER JOIN fin_L1_MD_GLAccount A \
                                        ON P.companyCode = A.companyCode \
                                        AND P.accountNumber = A.accountNumber")
        
        # User Story 942155: [PaaS][Data] Account without having journal entry transaction showing as an active account in account mapping screen
        if fin_L1_TD_Journal.first() is None:
            ActiveAccounts = fin_L1_STG_PYCYBalance.select(col('accountNumber')).distinct()
        else:
            ActiveAccounts = fin_L1_TD_Journal.filter((col('fiscalYear') == FIN_YEAR) & (col('postingDate') >= startDate) & (col('postingDate') <= endDate)). \
                select(col('accountNumber')).distinct()

        fin_L1_STG_PYCYBalance = fin_L1_STG_PYCYBalance.alias('p') \
            .join (ActiveAccounts.alias('a'), \
            col('p.accountNumber') == col('a.accountNumber'),how ='left') \
            .join (dfJetOnly.alias('jet'), \
            col('p.accountNumber') == col('jet.accountNumber'),how = 'left') \
            .select(col('p.companyCode') \
            ,col('p.accountNumber') \
            ,col('p.accountType') \
            ,col('p.CYCB') \
            ,col('p.PYCB') \
            ,col('p.CYOB') \
            ,col('p.accountName') \
            ,col('p.accountDescription') \
            ,col('p.accountGroup') \
            ,col('p.isTransactionAccount')\
            ,when(col('a.accountNumber').isNull(),lit(0)). \
            otherwise(lit(1)).alias('isActiveAccount') \
            ,when(col('jet.accountNumber').isNull(),lit(0)). \
            otherwise(lit(1)).alias('isJETTransactionOnly'))

        fin_L1_STG_PYCYBalance = objDataTransformation.gen_convertToCDMStructure_generate(fin_L1_STG_PYCYBalance,'fin','L1_STG_PYCYBalance')[0]
        fin_L1_STG_PYCYBalance.createOrReplaceTempView("fin_L1_STG_PYCYBalance")
        sqlContext.cacheTable("fin_L1_STG_PYCYBalance")
        PYCYBalancePath =  gl_CDMLayer1Path + "fin_L1_STG_PYCYBalance.csv"
        objGenHelper.gen_writeSingleCsvFile_perform(df = fin_L1_STG_PYCYBalance,targetFile = PYCYBalancePath)
        PYCYBalancePathdelta =  gl_CDMLayer1Path + "fin_L1_STG_PYCYBalance.delta"
        objGenHelper.gen_writeToFile_perfom(df = fin_L1_STG_PYCYBalance, filenamepath = PYCYBalancePathdelta)
        
        executionStatus = "Created fin_L1_STG_PYCYBalance succesfully."
        executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
        fin_L1_STG_PYCYBalance = None
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED
        executionLog.add(executionStatusID,logID,executionStatus)                 
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]




