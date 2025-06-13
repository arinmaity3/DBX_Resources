# Databricks notebook source
#fin.usp_L1_JEDocumentClassification_04_prepare
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col,sum,avg,max
import sys
import traceback
import pyspark.sql.functions as F

def fin_L1_JEDocumentClassification_04_prepare(): 
  try:

    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global fin_L1_STG_JEDocumentClassification_04_prepare   
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    
    new_col_createdBy = when(col('jrnl.createdBy').isNull(), lit('#NA#') ) \
                            .when(col('jrnl.createdBy')=='', lit('#NA#') ) \
                            .otherwise(col('jrnl.createdBy'))
    shiftedCreationDate=to_date(to_timestamp(\
                                   unix_timestamp(concat(col("jrnl.creationDate"),lit(" "),date_format(col("creationTime"),'HH:mm:ss')))\
                                   +col("utzone.timeZoneShiftInHours")*3600))
    df_fin_L1_TMP_JournalwithShiftedCreationDate = fin_L1_TD_Journal.alias('jrnl')\
               .join(gen_L2_DIM_Organization.alias('org'),(\
               (col('org.companyCode')==col('jrnl.companyCode'))\
               & (col('org.companyCode') !=lit('1#NA#'))),how='inner')\
               .join(gen_L2_DIM_User.alias('usr2'),(\
                (col('usr2.organizationUnitSurrogateKey')==col('org.organizationUnitSurrogateKey'))\
                & (col('usr2.userName')==lit(new_col_createdBy) )),how='inner')\
                .join(gen_L2_DIM_UserTimeZone.alias("utzone"),\
                (col("utzone.userSurrogateKey").eqNullSafe(col("usr2.userSurrogateKey"))\
                & (col("jrnl.creationDate").cast('date')>=col("utzone.dateFrom").cast('date'))\
                & (col("jrnl.creationDate").cast('date')<=col("utzone.dateTo").cast('date'))),"left")\
              .select(col('transactionIDbyJEDocumnetNumber').alias('transactionIDbyJEDocumnetNumber')\
                      ,col('usr2.userSurrogateKey').alias('userSurrogateKey')\
                      ,col('jrnl.companyCode').alias('companyCode')\
                      ,col('jrnl.amountLC').alias('amountLC')\
                      ,col('jrnl.createdBy').alias('createdBy')\
                      ,col('jrnl.debitcreditindicator').alias('debitcreditindicator')\
                      ,when(col("utzone.userTimeZone").isNotNull(),lit(shiftedCreationDate)).\
                             otherwise(col("jrnl.creationDate")).alias('shiftedCreationDate')\
                     )
    
    dr_wuserCount= Window.partitionBy("jrnl.transactionIDbyJEDocumnetNumber","jrnl.createdBy")\
                  .orderBy(lit("A"))
    dr_wholidayCount=when(col('jrnl.createdBy').isNull(), lit(1) ) \
                            .when(col('jrnl.createdBy')=='', lit(1) ) \
                            .when(col('jrnl.createdBy')=="#NA#", lit(1) )\
                            .otherwise(lit(0))

    dr_wisOutofBalance	=when(col('jrnl.debitCreditIndicator')=='D', col('amountLC') )\
                    .otherwise(-col('amountLC'))

    df_L1_TMP_JournalwithPeriodEndDateGrouped=df_fin_L1_TMP_JournalwithShiftedCreationDate.alias('jrnl')\
                   .join(gen_L2_DIM_Organization.alias('org'),(\
                   (col('org.companyCode')==col('jrnl.companyCode'))\
                   & (col('org.companyCode') !=lit('1#NA#'))),how='inner')\
                   .join(gen_L2_DIM_CalendarDate.alias('ddt'),(\
                    (col('ddt.organizationUnitSurrogateKey')==col('org.organizationUnitSurrogateKey'))\
                    & (col('ddt.calendarDate')==col('jrnl.shiftedCreationDate') )),how='inner')\
                  .select(col('transactionIDbyJEDocumnetNumber').alias('transactionIDbyJEDocumnetNumber')\
                          ,F.row_number().over(dr_wuserCount).alias("userCount")\
                          ,lit(when(col('ddt.isHoliday')==1 ,lit(1)).otherwise(lit(0))).alias("holidayCount")\
                          ,lit(dr_wholidayCount).alias("missingUserCount")\
                          ,lit(dr_wisOutofBalance).alias("isOutofBalance")\
                          ,col('debitCreditIndicator')\
                          ,col('amountLC')\
                         )
    
    df_L1_TMP_JournalwithPeriodEndDateGrouped=df_L1_TMP_JournalwithPeriodEndDateGrouped\
              .withColumn("userCount1",(when(col('userCount')==1 ,lit(1)).otherwise(lit(0))))

    df_L1_TMP_JournalwithPeriodEndDateGrouped=df_L1_TMP_JournalwithPeriodEndDateGrouped\
              .withColumn("holidayCount1",(when(col('holidayCount')==1 ,lit(1)).otherwise(lit(0))))


    df_L1_TMP_JEWithAmbiguousAttribute=df_L1_TMP_JournalwithPeriodEndDateGrouped.alias('jrnl')\
                              .groupBy('jrnl.transactionIDbyJEDocumnetNumber')\
        .agg(
              sum('jrnl.IsOutofBalance').alias("isOutofBalance")\
              ,sum('jrnl.userCount1').alias("userCount")\
              ,sum('jrnl.holidayCount1').alias("holidayCount")\
              ,sum('jrnl.missingUserCount').alias("missingUserCount")\
            )

    fin_L1_STG_JEDocumentClassification_04_prepare=df_L1_TMP_JEWithAmbiguousAttribute.alias('jrnl')\
           .select(col('jrnl.transactionIDbyJEDocumnetNumber')\
           ,when(col('jrnl.isOutofBalance') !=0 ,lit(1)).otherwise(lit(0)).alias("IsOutofBalance")\
           ,when(col('jrnl.userCount') > 1 ,lit(1)).otherwise(lit(0)).alias("IsJEWithMultipleUser")\
           ,when(col('jrnl.holidayCount') > 0 ,lit(1)).otherwise(lit(0)).alias("JEHolidayPosting")\
           ,when(col('jrnl.missingUserCount') > 0 ,lit(1)).otherwise(lit(0)).alias("IsJEWithMissingUser")\
                  )
    
    fin_L1_STG_JEDocumentClassification_04_prepare  = objDataTransformation.gen_convertToCDMandCache \
    (fin_L1_STG_JEDocumentClassification_04_prepare,'fin','L1_STG_JEDocumentClassification_04_prepare',False)
   
    executionStatus = "fin_L1_STG_JEDocumentClassification_04_prepare populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    

