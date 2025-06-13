# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import lit,col,when,date_format,max,min
from datetime import date,timedelta 
import calendar
from pyspark.sql.types import DateType

def GenerateDateRange(RowStartDateEndDate):
  try:
    startDate=RowStartDateEndDate.startDate
    endDate=RowStartDateEndDate.endDate
    pandasDF=pd.date_range(startDate,endDate).to_frame()
    dfDates=spark.createDataFrame(pandasDF)
    dfDates=dfDates.withColumnRenamed("0","calendarDate")\
                   .withColumn("calendarDate",to_date(col("calendarDate")).cast(StringType()))\
                   .withColumn("startDate",lit(startDate).cast(StringType()))\
                   .withColumn("endDate",lit(endDate).cast(StringType()))
    dfDates.write.format("delta").mode('append').save(gl_layer0Temp+'date_sequence.delta')
  except Exception as e:
    return False

def gen_ORA_L1_MD_Period_populate():
    try:

      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()
      logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)

      global gen_L1_MD_Period
      
      dbutils.fs.rm(gl_layer0Temp+'date_sequence.delta',True)

      L0_TMP_PeriodDetails = erp_GL_PERIODS.alias('GLP')\
                              .join(erp_GL_LEDGERS.alias('GLL'),((col("GLP.PERIOD_SET_NAME") == col("GLL.PERIOD_SET_NAME")) \
                                                              & (col("GLP.PERIOD_TYPE")   == col("GLL.ACCOUNTED_PERIOD_TYPE"))), "inner")\
                              .join(knw_LK_CD_ReportingSetup.alias('lcr'),((col("GLL.LEDGER_ID") == col('lcr.ledgerID'))), "inner")\
                              .select(col('GLP.START_DATE').alias('startDate'),col('GLP.END_DATE').alias('endDate'),col('GLP.PERIOD_YEAR').alias('periodYear'),\
                                     col('GLP.PERIOD_NUM').alias('periodNum'),col('GLP.PERIOD_SET_NAME').alias('periodsetname'),col('lcr.companyCode').alias('companycode'))\
                              .sort(col('PERIOD_YEAR'),col('PERIOD_NUM'))

      lsStartDatesAndEndDates = L0_TMP_PeriodDetails.select(col('startDate'),col('endDate')).distinct().collect()
      # print(lsStartDatesAndEndDates)


      date_sequence_schema = StructType([
        StructField('calendarDate', StringType(), True),
        StructField('startDate', StringType(), True),
        StructField('endDate', StringType(), True)
        ])
      df_date_sequence = spark.createDataFrame([], date_sequence_schema)
      df_date_sequence.write.format("delta").save(gl_layer0Temp+'date_sequence.delta')

      lstOfDateSequence=map(GenerateDateRange,lsStartDatesAndEndDates)
      for x in lstOfDateSequence:
        pass

      date_sequence = objGenHelper.gen_readFromFile_perform(\
                                gl_layer0Temp + "date_sequence.delta")
      
      
      df_gen_L1_MD_Period = date_sequence.alias('s1')\
                                .join(L0_TMP_PeriodDetails.alias('gcc'),((col("s1.startDate") == col("gcc.startDate")) \
                                                                              & (col("s1.endDate")   == col("gcc.endDate"))), "inner")\
                                .select(col('companyCode'),col('calendarDate'),col('periodYear').alias('fiscalYear'),col('periodNum').alias('financialPeriod')).distinct()
      
      gen_L1_MD_Period = objDataTransformation.gen_convertToCDMandCache \
        (df_gen_L1_MD_Period,'gen','L1_MD_Period',targetPath=gl_CDMLayer1Path)
      
      executionStatus = "L1_MD_Period populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
