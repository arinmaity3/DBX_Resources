# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  col,lit

def gen_ORA_L1_MD_UserTimeZone_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    
    global gen_L1_MD_UserTimeZone

    levelValue = erp_ORA_USER_TIMEZONES.alias('ust')\
                 .filter(col('LEVEL_VALUE') == lit('SITE'))\
                 .select(col('LEVEL_VALUE')).head()


    if(levelValue is None):
      levelValue = erp_ORA_USER_TIMEZONES.alias('ust')\
                 .filter(col('LEVEL_VALUE') == lit('SERVER'))\
                 .select(col('LEVEL_VALUE')).head()

    TIMEZONE_INFO = erp_ORA_USER_TIMEZONES.alias('ust1')\
                    .filter(col('ust1.LEVEL_VALUE') == lit(levelValue))\
                    .select(col('TIMEZONE_INFO')).head()


    GMT_OFFSET = erp_ORA_USER_TIMEZONES.alias('ust1')\
                    .filter(col('ust1.LEVEL_VALUE') == lit(levelValue))\
                    .select(col('GMT_OFFSET')).head()


    gen_L1_MD_UserTimeZone = erp_FND_USER.alias('t1')\
                              .join(erp_ORA_USER_TIMEZONES.alias('t2'),(col("t1.USER_NAME") == col("t2.USER_NAME"))\
                                                                        & (col("t2.LEVEL_VALUE") == lit("USER")),how="left")\
                              .select(col('t1.USER_NAME').alias('userName'),when(col('t2.TIMEZONE_INFO').isNull(),lit(TIMEZONE_INFO))\
                                                                 .otherwise(col('t2.TIMEZONE_INFO')).alias('userTimeZone')\
                                                         ,when(col('t2.GMT_OFFSET').isNull(),lit(GMT_OFFSET))\
                                                                 .otherwise(col('t2.GMT_OFFSET')).alias('timeZoneShiftInHours')\
                                     ).distinct()
    

    gen_L1_MD_UserTimeZone =  objDataTransformation.gen_convertToCDMandCache \
          (gen_L1_MD_UserTimeZone,'gen','L1_MD_UserTimeZone',targetPath=gl_CDMLayer1Path)

    executionStatus = "gen_L1_MD_UserTimeZone populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
