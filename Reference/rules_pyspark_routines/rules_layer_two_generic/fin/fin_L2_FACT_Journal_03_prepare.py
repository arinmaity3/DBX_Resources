# Databricks notebook source
from pyspark.sql.functions import row_number,expr,date_format
from pyspark.sql.window import Window
from datetime import datetime,time
import inspect
import sys
import traceback

def fin_L2_FACT_Journal_03_prepare():
    try:
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        global fin_L2_STG_Journal_03_prepare
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()

        approvalTime=when(col("approvalTime")=='','00:00:00').otherwise(col("approvalTime"))
        cpudtShifted=to_date(to_timestamp(\
                                    unix_timestamp(concat(col("creationDate"),lit(" "),col("creationTime")))\
                                    +col("timeZoneShiftInHours")*3600))
        approvalDateShifted=to_date(to_timestamp(\
                                    unix_timestamp(concat(col("approvalDate"),lit(" "),lit(approvalTime)))\
                                    +col("timeZoneShiftInHours_approval")*3600))
        cputmShifted=date_format(to_timestamp(\
                                    unix_timestamp(concat(col("creationDate"),lit(" "),col("creationTime")))\
                                    +col("timeZoneShiftInHours")*3600),'HH:mm:ss')
        
        fin_L1_TMP_JournalWithTimeShifted = spark.sql("select \
                jou.journalSurrogateKey AS journalSurrogateKey, \
        		usr2.userSurrogateKey AS userSurrogateKey, \
        		jou.creationDate, \
        	    jou.creationTime, \
        		jou.approvalDate,\
        		jou.approvalTime,\
        		utzone.userTimeZone,\
                utzone2.userTimeZone as userTimeZone_approval,\
        		utzone.timeZoneShiftInHours, \
                utzone2.timeZoneShiftInHours as timeZoneShiftInHours_approval \
        	from fin_L1_TD_Journal	jou \
            join gen_L2_DIM_Organization	dou2 \
        		on dou2.companyCode	= jou.companyCode \
        		and dou2.companyCode <> '#NA#' \
        	join gen_L2_DIM_User usr2 \
        		on usr2.organizationUnitSurrogateKey = dou2.organizationUnitSurrogateKey \
        		and usr2.userName = case when isnull(nullif(jou.createdBy,'')) then '#NA#' else jou.createdBy end \
            join gen_L2_DIM_User usr3 \
        	    on usr3.organizationUnitSurrogateKey= dou2.organizationUnitSurrogateKey\
        		and usr3.userName= case when isnull(nullif(jou.approvedBy,'')) then '#NA#' else jou.approvedBy end \
        	left join gen_L2_DIM_UserTimeZone utzone \
        		on utzone.userSurrogateKey  = usr2.userSurrogateKey	\
        		and cast(jou.creationDate as date) between cast(utzone.dateFrom as date) and cast(utzone.dateTo as date) \
        	left join gen_L2_DIM_UserTimeZone utzone2 \
        		on utzone2.userSurrogateKey = usr3.userSurrogateKey	\
        		and cast(jou.approvalDate as date) between cast(utzone2.dateFrom as date) and cast(utzone2.dateTo as date)")

        fin_L1_TMP_JournalWithTimeShifted=fin_L1_TMP_JournalWithTimeShifted\
        	.withColumn("cpudtShifted",when(col("userTimeZone").isNotNull(),lit(cpudtShifted)).otherwise(col("creationDate")))\
        	.withColumn("cputmShifted",when(col("userTimeZone").isNotNull(),lit(cputmShifted)).otherwise(col("creationTime")))\
        	.withColumn("approvalDateShifted",when(col("userTimeZone_approval").isNotNull(),lit(approvalDateShifted)).otherwise(col("approvalDate")))\
        	.drop(*['creationDate','creationTime','approvalDate','approvalTime','userTimeZone','userTimeZone_approval',
                 'timeZoneShiftInHours','timeZoneShiftInHours_approval'])

        fin_L1_TMP_JournalWithTimeShifted.createOrReplaceTempView("fin_L1_TMP_JournalWithTimeShifted")
        sqlContext.cacheTable("fin_L1_TMP_JournalWithTimeShifted")

        gen_L1_TMP_ProductWithUniqueMeasure = spark.sql("select \
		        max(productSurrogateKey) AS productSurrogateKey \
			    ,organizationUnitSurrogateKey AS organizationUnitSurrogateKey \
			    ,productNumber AS productNumber \
		    from gen_L2_DIM_Product \
		    group by organizationUnitSurrogateKey,productNumber")

        gen_L1_TMP_ProductWithUniqueMeasure.createOrReplaceTempView("gen_L1_TMP_ProductWithUniqueMeasure")
        sqlContext.cacheTable("gen_L1_TMP_ProductWithUniqueMeasure")
              
        fin_L2_STG_Journal_03_prepare = spark.sql("select \
		        dou2.organizationUnitSurrogateKey AS organizationUnitSurrogateKey \
				,jou1.journalSurrogateKey         AS journalSurrogateKey \
				,usr2.userName					  AS userName \
				,pro2.productNumber				  AS productNumber \
				,cur2.currencyISOCode			  AS currencyCode \
				,usr2.userSurrogateKey			  AS userSurrogateKey \
				,pro2.productSurrogateKey		  AS productSurrogateKey \
				,cur2.currencySurrogateKey		  AS documentCurrencySurrogateKey \
				,tim2.timeSurrogateKey			  AS shiftedAccountingTimeSurrogateKey \
				,dat2.dateSurrogateKey			  AS shiftedDateOfEntrySurrogateKey \
				,usr3.userSurrogateKey			  AS approvedByuserSurrogateKey \
				,dat3.dateSurrogateKey			  AS shiftedApprovalDateSurrogateKey \
		    from fin_L1_TD_Journal jou1 	\
            join gen_L2_DIM_Organization	dou2 \
                 on dou2.companyCode = jou1.companyCode \
				and dou2.companyCode <> '#NA#' \
            join gen_L2_DIM_User usr2 \
				on usr2.organizationUnitSurrogateKey = dou2.organizationUnitSurrogateKey \
				and usr2.userName = case when isnull(nullif(jou1.createdBy,'')) then '#NA#' else jou1.createdBy end \
		    join gen_L1_TMP_ProductWithUniqueMeasure	pro2 \
				on pro2.organizationUnitSurrogateKey = dou2.organizationUnitSurrogateKey \
				and pro2.productNumber = case when isnull(nullif(jou1.productNumber,'')) \
				then '#NA#' else jou1.productNumber end \
			join gen_L2_DIM_Currency cur2 \
				on cur2.currencyISOCode = case when isnull(nullif(jou1.documentCurrency,'')) \
				then '#NA' else jou1.documentCurrency end \
			join fin_L1_TMP_JournalWithTimeShifted tshift \
				on tshift.journalSurrogateKey = jou1.journalSurrogateKey \
			join gen_L2_DIM_Time tim2 \
				on case when isnull(nullif(tshift.cputmShifted,'')) then '00:00:00' else \
                tshift.cputmShifted end  = tim2.time24String  \
			join gen_L2_DIM_CalendarDate 	    dat2 \
				on dat2.organizationUnitSurrogateKey = dou2.organizationUnitSurrogateKey \
				and cast(dat2.calendarDate as date) = cast(case when isnull(nullif(tshift.cpudtShifted,'')) \
                then '1900-01-01' else tshift.cpudtShifted end as date) \
			join gen_L2_DIM_User usr3 \
				on usr3.organizationUnitSurrogateKey = dou2.organizationUnitSurrogateKey \
				and usr3.userName = case when isnull(nullif(jou1.approvedBy,'')) \
				then '#NA#' else jou1.approvedBy end \
			join gen_L2_DIM_CalendarDate dat3 \
				on dat3.organizationUnitSurrogateKey = dou2.organizationUnitSurrogateKey \
				and cast(dat3.calendarDate as date) = cast(case when isnull(nullif(tshift.approvalDateShifted,'')) \
                then '1900-01-01' else tshift.approvalDateShifted end as date)")
        
        fin_L2_STG_Journal_03_prepare = objDataTransformation.gen_convertToCDMandCache \
			(fin_L2_STG_Journal_03_prepare,'fin','L2_STG_Journal_03_prepare',False)

        recordCount = fin_L2_STG_Journal_03_prepare.count()
        
        if gl_countJET != recordCount:
          executionStatus="Number of records in fin_L2_FACT_Journal_03_prepare are \
		  not reconciled with number of records in [fin].[L1_TD_Journal]"
          warnings.warn(executionStatus)
          executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
          return [LOG_EXECUTION_STATUS.FAILED,executionStatus]       

        executionStatus = "L2_STG_Journal_03_prepare populated successfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]


    except Exception as e:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    finally:
            spark.sql("UNCACHE TABLE  IF EXISTS fin_L1_TMP_JournalWithTimeShifted")
            spark.sql("UNCACHE TABLE  IF EXISTS gen_L1_TMP_ProductWithUniqueMeasure")
            
