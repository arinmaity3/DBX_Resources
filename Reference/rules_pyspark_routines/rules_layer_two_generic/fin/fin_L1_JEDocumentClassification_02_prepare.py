# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,when,date_format

def fin_L1_JEDocumentClassification_02_prepare():
  try:
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global fin_L1_STG_JEDocumentClassification_02_prepare
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()

    largeamount =  objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'LARGE AMOUNT')
    startDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date()
    endDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date()
    startPeriod = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_START'))
    endPeriod =  int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_END'))

    fin_L1_TMP_CompanyCodeExistingPeriod=gen_L1_MD_Period\
        .select(col("companyCode"),lit(1).alias("isMaxDate")).distinct()
    
    fin_L1_TMP_JournalWithLargeAmount =  fin_L1_TD_Journal.select(                       \
                                  col("companyCode").alias("companyCode"),       \
                                  col("documentNumber").alias("documentNumber"),  \
                                  col("postingDate").alias("postingDate")  \
                                                    )                       \
    .filter((col("amountLC") >= largeamount)).distinct()

    fin_L1_TMP_Distinct_JEWithCriticalComment=fin_L1_STG_JEWithCriticalComment\
                  .select(col('transactionIDbyJEDocumnetNumber'))\
                  .distinct()

    w = Window().orderBy(lit(''))

    new_col_documentSource = when(col('Journal.referenceSubledger')\
                 .isin('Purchase S/L','Inventory S/L','Purchase S/L (Consignment/Pipeline)'),lit('MM'))\
                 .when(col("Journal.referenceSubledger")=='Sales S/L', lit('SD')) \
                 .otherwise(lit('Other'))
    shiftedCreationDate=date_format(to_timestamp(\
                                   unix_timestamp(concat(col("creationDate"),lit(" "),date_format(col("creationTime"),'HH:mm:ss')))\
                                   +col("timeZoneShiftInHours")*3600),'yyyy-MM-dd HH:00:00.000')
    diffDaysPeriodEndCreationDate=datediff(col("creationDate"),col("periodEndDate"))
    diffDaysPeriodEndCreationDateShifted=datediff(shiftedCreationDate,col("periodEndDate"))
    diffDaysPostingDateCreationDate=datediff(col("postingDate"),col("creationDate"))
    diffDaysPostingDateCreationDateShifted=datediff(col("postingDate"),shiftedCreationDate)
    
    isNull0diffDaysPeriodEndCreationDateShifted=when(lit(diffDaysPeriodEndCreationDateShifted).isNull(),0).\
                                otherwise(lit(diffDaysPeriodEndCreationDateShifted))
    isNull0diffDaysPeriodEndCreationDate=when(lit(diffDaysPeriodEndCreationDate).isNull(),0).\
                                otherwise(lit(diffDaysPeriodEndCreationDate))
    dfL1_STG_JEDocumentClassification_02_prepare = \
                fin_L1_TD_Journal.alias("Journal").join \
                (gen_L2_DIM_Organization.alias("org"), \
                (col("Journal.companyCode")    == col("org.companyCode"))\
                 & (col("org.companyCode")    != lit("#NA#")),      \
                "inner").join \
                (fin_L1_TMP_CompanyCodeExistingPeriod.alias("cmex1"),\
                (col("cmex1.companyCode").eqNullSafe(col("org.companyCode"))),\
                "left").join \
                (gen_L2_DIM_PostingKey.alias("pKey"), \
                (col("pKey.organizationUnitSurrogateKey").eqNullSafe(col("org.organizationUnitSurrogateKey")))
                 & (col("pKey.postingKey").eqNullSafe(col("Journal.postingKey"))),      \
                "left").join \
                (fin_L2_DIM_FinancialPeriod.alias("fPeriod"), \
                (col("fPeriod.organizationUnitSurrogateKey").eqNullSafe(col("org.organizationUnitSurrogateKey")))
                 & (col("fPeriod.postingDate").eqNullSafe(col("Journal.postingDate")))\
                 & (col("fPeriod.financialPeriod").eqNullSafe(col("Journal.financialPeriod"))),      \
                "left").join \
                (gen_L2_DIM_User.alias("usr"), \
                (col("usr.organizationUnitSurrogateKey").eqNullSafe(col("org.organizationUnitSurrogateKey")))
                 & (col("usr.userName").eqNullSafe(col("Journal.createdBy"))),\
                "left").join \
                (gen_L2_DIM_UserTimeZone.alias("utzone"), \
                (col("utzone.userSurrogateKey").eqNullSafe(col("usr.userSurrogateKey")))\
                & (col("Journal.creationDate").cast('date')    >= col("utzone.dateFrom").cast('date'))\
                & (col("Journal.creationDate").cast('date')    <= col("utzone.dateTo").cast('date')),\
                "left").join \
                (fin_L1_TMP_JournalWithLargeAmount.alias("lAmt"), \
                (col("lAmt.companyCode").eqNullSafe(col("Journal.companyCode")))\
                & (col("lAmt.documentNumber").eqNullSafe(col("Journal.documentNumber")))\
                & (col("lAmt.postingDate").eqNullSafe(col("Journal.postingDate"))),\
                "left").join\
                (fin_L1_TMP_Distinct_JEWithCriticalComment.alias("jecmt"),\
                (col("jecmt.transactionIDbyJEDocumnetNumber")    == \
                   col("Journal.transactionIDbyJEDocumnetNumber")),"left")\
                .select(
                           lit(gl_analysisID).alias("analysisID"),\
                           col("Journal.transactionIDbyJEDocumnetNumber"),  \
                           col("Journal.journalSurrogateKey"),  \
                           col("Journal.companyCode").alias("companyCode"),  \
                           col("Journal.postingDate").alias("postingDate"),  \
                           col("Journal.creationDate").alias("creationDate"),\
                           col("utzone.timeZoneShiftInHours").alias("timeZoneShiftInHours") ,\
                           col("utzone.userTimeZone").alias("userTimeZone") ,\
                           col("cmex1.isMaxDate").alias("isMaxDate"), \
                           col("Journal.creationTime").alias("creationTime"),\
                           col("Journal.documentStatus").alias("documentStatus"),\
                           col("Journal.documentStatusDetail").alias("documentStatusDetail"),\
                          (when(col("pKey.postingKey").isNotNull(), col("pKey.postingKey")) \
                              .otherwise(lit("N/A"))).alias("postingKey"),\
                          (when(col("Journal.transactionCode").isNotNull(), col("Journal.transactionCode")) \
                              .otherwise(lit("N/A"))).alias("transactionCode"),\
                          lit(new_col_documentSource).alias("documentSource"),\
                          (when(col("Journal.referenceSubledger").isNotNull(), col("Journal.referenceSubledger")) \
                              .otherwise(lit("#NA#"))).alias("documentSubSource"),\
                          (when(col("Journal.referenceSystem").isNotNull(), col("Journal.referenceSystem")) \
                              .otherwise(lit("#NA#"))).alias("documentPreSystem"),\
                          when(col("Journal.isBatchInput") == 1,"2").otherwise(lit("1")).alias("isBatchInput"),  \
                          when(((col("Journal.referenceSubledger") == "Other") \
                               | (col("Journal.referenceSubledger").isNull())
                               | (col("Journal.referenceSubledger")=="")),"0")\
                               .otherwise(lit("1")).alias("isSubledgerPosting"),  \
                          when(col("Journal.reversalType").isNull(),lit("0")).\
                             otherwise(col("Journal.reversalType")).alias("reversalStatus"),\
                         col("Journal.isManualPosting").alias("isManualPosting"),  \
                         col("Journal.manualPostingAttribute1").alias("manualPostingAttribute1"),  \
                         col("Journal.manualPostingAttribute2").alias("manualPostingAttribute2"),  \
                         col("Journal.manualPostingAttribute3").alias("manualPostingAttribute3"),  \
                         col("Journal.manualPostingAttribute4").alias("manualPostingAttribute4"),  \
                         when((col("Journal.postingDate").cast('date')
                               .between(lit(startDate),lit(endDate)))
                              & (col("Journal.financialPeriod").cast('int')
                               .between(lit(startPeriod),lit(endPeriod)))
                              ,"1")\
                               .otherwise(lit("0")).alias("isJEInAnalysisPeriod")\
                         ,when(col("Journal.documentDate").isNull(),lit(-99999999))\
                               .otherwise(datediff(col("Journal.postingDate"),\
                                     col("Journal.documentDate"))).alias("postingDateToDocumentDate")\
                        ,when(col("jecmt.transactionIDbyJEDocumnetNumber").isNull(),lit(0))\
                              .otherwise(lit(1)).alias("isJEWithCritalComment")\
                        ,col("Journal.isClearing").alias("isClearing"),  \
                         col("Journal.isInterCoFlag").alias("isInterCoFlag"),  \
                         col("Journal.isNegativePostingIndicator").alias("isNegativePostingIndicator"),  \
                         when(col("lAmt.documentNumber").isNull(),lit(0)).\
                             otherwise(lit(1)).alias("isJEWithLargeAmount")\
                        ,col("fPeriod.periodEndDate"))\
             .withColumn("shiftedDateOfEntry",when(col("userTimeZone").isNotNull(),lit(shiftedCreationDate)).\
                             otherwise(date_format(to_timestamp(col("creationDate")),'yyyy-MM-dd HH:mm:ss.SSS')))\
             .withColumn("isJEPostingInTimeFrame",\
                  when(col("isMaxDate").isNull(),lit('N/A')).\
                  otherwise(when(col("userTimeZone").isNotNull(),\
                                         when(lit(diffDaysPeriodEndCreationDateShifted)<=lit(0),\
                                                    lit("Yes")).otherwise(lit("No"))).\
                            otherwise(\
                                         when(lit(diffDaysPeriodEndCreationDate)<=lit(0),\
                                                    lit("Yes")).otherwise(lit("No")))))\
             .withColumn("jePostPeriodDays",\
                 when(col("isMaxDate").isNull(),lit(0)).\
                 otherwise(when(col("userTimeZone").isNotNull(),\
                                lit(isNull0diffDaysPeriodEndCreationDateShifted)).\
                           otherwise(lit(isNull0diffDaysPeriodEndCreationDate))))\
             .withColumn("postingDateToDocuemntEntryDate",\
                 when(col("userTimeZone").isNotNull(),\
                                lit(diffDaysPostingDateCreationDateShifted)).\
                           otherwise(lit(diffDaysPostingDateCreationDate)))\
             .withColumn("isJEPostedInPeriodEndDate",when(col("isMaxDate").isNull(),lit('N/A')).\
                  otherwise(when(col("userTimeZone").isNotNull(),\
                                         when(lit(diffDaysPeriodEndCreationDateShifted)==lit(0),\
                                                    lit("Yes")).otherwise(lit("No"))).\
                            otherwise(\
                                         when(lit(diffDaysPeriodEndCreationDate)==lit(0),\
                                                    lit("Yes")).otherwise(lit("No")))))\
             .drop(*['periodEndDate','timeZoneShiftInHours','isMaxDate','creationTime','userTimeZone'])\
             .distinct().\
             withColumn("glAccountSurrogateKey", row_number().over(w))


    fin_L1_STG_JEDocumentClassification_02_prepare = objDataTransformation.gen_convertToCDMandCache \
    (dfL1_STG_JEDocumentClassification_02_prepare,'fin','L1_STG_JEDocumentClassification_02_prepare',True)

    executionStatus = "fin_L1_STG_JEDocumentClassification_02_prepare populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
      
#fin_usp_L1_JEDocumentClassification_02_prepare_populate()

