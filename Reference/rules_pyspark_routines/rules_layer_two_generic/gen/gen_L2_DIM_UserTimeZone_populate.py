# Databricks notebook source
from pyspark.sql.functions import row_number,expr,trim
from pyspark.sql.window import Window
import sys
import traceback

def L2_DIM_UserTimeZone_AddedDetails(gen_L2_DIM_UserTimeZone,fileLocation):
    try:
        objGenHelper = gen_genericHelper()
        fileFullName = fileLocation + "gen_L1_MD_UserTimeZone.delta"
        
        if objGenHelper.file_exist(fileFullName) == True:
            w = Window().orderBy(lit('userTimeZoneSurrogateKey'))
            gen_L1_MD_UserTimeZone = objGenHelper.gen_readFromFile_perform(fileFullName)
            gen_L2_DIM_UserTimeZone = gen_L1_MD_UserTimeZone.alias('tz') \
                .join(gen_L2_DIM_User.alias('us'), \
                (col('tz.userName') == col('us.userName')) \
                & (col('tz.userTimeZone') == col('us.userTimeZone')), how = 'inner') \
                .select(col('us.userSurrogateKey').alias('userSurrogateKey') \
                ,col('tz.userTimeZone').alias('userTimeZone') \
                ,col('tz.timeZoneShiftInHours').alias('timeZoneShiftInHours') \
                ,col('tz.dateFrom').alias('dateFrom') \
                ,col('tz.dateTo').alias('dateTo')) \
                .withColumn("userTimeZoneSurrogateKey", row_number().over(w))
        else:
            status="The file L1_MD_UserTimeZone does not exists in the specified location: "+fileLocation
            #print(status)
        
    except Exception as err:
        raise
    finally:
        return gen_L2_DIM_UserTimeZone

def gen_L2_DIM_UserTimeZone_populate():
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        global gen_L2_DIM_UserTimeZone

        L2_DIM_UserTimeZone_Schema = StructType([                                    
                                        StructField("userTimeZoneSurrogateKey",StringType(),False),
                                        StructField("userSurrogateKey", StringType(),False),
                                        StructField("userTimeZone",StringType(),False),
                                        StructField("timeZoneShiftInHours", StringType(),False), 
                                        StructField("dateFrom",StringType(),False),
                                        StructField("dateTo", StringType(),False), 
                                        StructField("phaseID",StringType(),False),
                                        ])
        gen_L2_DIM_UserTimeZone = spark.createDataFrame(spark.sparkContext.emptyRDD(), L2_DIM_UserTimeZone_Schema)
        gen_L2_DIM_UserTimeZone = L2_DIM_UserTimeZone_AddedDetails(gen_L2_DIM_UserTimeZone,gl_CDMLayer1Path)
        gen_L2_DIM_UserTimeZone = objDataTransformation.gen_convertToCDMandCache \
            (gen_L2_DIM_UserTimeZone,'gen','L2_DIM_UserTimeZone',False)

        executionStatus = "L2_DIM_UserTimeZone populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as e:
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

