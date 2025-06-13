# Databricks notebook source
from pyspark.sql.functions import substring,concat_ws,collect_set,dayofmonth,month,quarter,date_format,weekofyear,col,lit,dayofweek,year,concat,udf
from dateutil.parser import parse
import sys
import traceback
from pyspark.sql.types import StringType


def _weekID(d):
  firstDay = datetime(int(d.strftime('%Y')),1,1)
  if (firstDay.strftime("%a") == 'Sun'):
    return int(d.strftime('%U'))
  else:
    return int(d.strftime('%U')) + 1  

udf_weekID = udf(lambda d: _weekID(d),StringType())

def L2_DIM_CalendarDate_AddedDetails(gen_L1_TMP_CalendarDateCollect,fileLocation):
  try:
    objGenHelper = gen_genericHelper()
    status="Appended ShiftedDate-values to CalendarDateCollect"
    fileFullName=fileLocation+"gen_L1_MD_UserTimeZone.delta"
    if objGenHelper.file_exist(fileFullName) == True:
        ShiftedCreationDate=to_date(to_timestamp(\
                               unix_timestamp(concat(col("creationDate"),lit(" "),col("creationTime")))\
                               +col("timeZoneShiftInHours")*3600))
        ShiftedApprovalDate=to_date(to_timestamp(\
                               unix_timestamp(concat(col("creationDate"),lit(" "),col("creationTime")))\
                               +col("timeZoneShiftInHours")*3600))
        gen_L1_MD_UserTimeZone=objGenHelper.gen_readFromFile_perform(fileFullName)
        df_ShiftedCreationDates=fin_L1_TD_Journal.alias("jou").join(gen_L1_MD_UserTimeZone.alias("utzone"),\
        	 col("jou.createdBy")==col("utzone.userName"),"left")\
        	.select(col("companyCode"),lit(ShiftedCreationDate)).distinct()
        df_ShiftedApprovalDates=fin_L1_TD_Journal.alias("jou").join(gen_L1_MD_UserTimeZone.alias("utzone"),\
        	 col("jou.approvedBy")==col("utzone.userName"),"left")\
        	.select(col("companyCode"),lit(ShiftedApprovalDate)).distinct()
        gen_L1_TMP_CalendarDateCollect=gen_L1_TMP_CalendarDateCollect.union(df_ShiftedCreationDates)\
        	                                                         .union(df_ShiftedApprovalDates).distinct()
    else:
        status="The file gen_L1_MD_UserTimeZone does not exist in the specified location: "+fileLocation
    return gen_L1_TMP_CalendarDateCollect,status
  except Exception as err:
    raise

def gen_L2_DIM_CalendarDate_populate():
	try:
		logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
		
		objGenHelper = gen_genericHelper()
		objDataTransformation = gen_dataTransformation()

		global gen_L2_DIM_CalendarDate
		# global knw_LK_CD_HolidayDetail
		# Set parameters and variables
		analysisid = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ANALYSISID'))
		erpGENSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')
		if erpGENSystemID is None:
			erpGENSystemID = 'NULL'
		#client_weekend = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'WEEK_END')
		#if client_weekend is None:
		#	client_weekend = ''
		
		
		gen_L1_TMP_CalendarDateHolidayDetail = knw_LK_CD_HolidayDetail.groupby("companyCode","holidayDate")\
                    .agg(substring(concat_ws('/',collect_set("holidayDescription")),1,249).alias("holidayText"))
		gen_L1_TMP_CalendarDateHolidayDetail.createOrReplaceTempView("gen_L1_TMP_CalendarDateHolidayDetail")

		gen_L1_TMP_CalendarDateCollect=spark.createDataFrame(gl_lstOfCalendarDateCollect,calendarDateSchemaCollect).\
                         select(col("companyCode"),col("calendarDate")).distinct()
			
		gen_L1_TMP_CalendarDateCollect,status=L2_DIM_CalendarDate_AddedDetails(gen_L1_TMP_CalendarDateCollect,gl_CDMLayer1Path)
		gen_L1_TMP_CalendarDateCollect.createOrReplaceTempView("gen_L1_TMP_CalendarDateCollect") 

		spark.sql("	SELECT	DISTINCT  \
						 tmp1.companyCode	as companyCode	 \
						,tmp1.calendarDate  as calendarDate	 \
					FROM	gen_L1_TMP_CalendarDateCollect tmp1 		 \
                    INNER JOIN gen_L2_DIM_Organization  org1	ON \
								( \
									org1.companyCode			= tmp1.companyCode \
								) \
                    WHERE  tmp1.calendarDate IS NOT NULL \
		").createOrReplaceTempView("gen_L1_TMP_CalendarDate1") 
		

		gen_L1_TMP_CalendarDate = spark.sql("SELECT	 companyCode,calendarDate		 \
							FROM gen_L1_TMP_CalendarDate1 \
							UNION ALL \
							SELECT	 rptSetup.companyCode	AS companyCode		 \
						,'1900-01-01'                   AS calendarDate		 \
						FROM	knw_LK_CD_ReportingSetup rptSetup \
						WHERE NOT EXISTS (SELECT 1 FROM gen_L1_TMP_CalendarDate1 tdat  \
											WHERE rptSetup.companyCode	= tdat.companyCode \
											AND  tdat.calendarDate	= '1900-01-01' \
										)")
		
		gen_L1_TMP_CalendarDate = gen_L1_TMP_CalendarDate.select("*")\
			.withColumn("dateAsInterger",dayofmonth("calendarDate"))\
			.withColumn("calendarMonth",month("calendarDate"))\
			.withColumn("calendarQuarter",quarter("calendarDate"))\
			.withColumn("calendarYear",year("calendarDate"))\
			.withColumn("yearMonthId",substring(date_format("calendarDate",'yyyyMMdd'),1,6))\
			.withColumn("dayAsText",substring(date_format("calendarDate",'ddMMyyyy'),1,2))\
			.withColumn("dayOfWeekId",dayofweek("calendarDate"))\
			.withColumn("yearQuarterId",concat(col("calendarYear"),lit(" - Q0"),col("calendarQuarter")))\
			.withColumn("dayMonthId",concat(col("dateAsInterger"),col("calendarMonth")))\
			.withColumn("dayname",date_format('calendarDate', 'EEEE'))\
			.withColumn("monthname",date_format('calendarDate', 'MMMM'))\
			.withColumn("WeekId",udf_weekID(col("calendarDate").cast("Date")))

		gen_L1_TMP_CalendarDate.select("*")\
			.withColumn("yearWeekId",concat(col("calendarYear"),substring(concat(lit("0"),col("weekId")),-2,2)))\
			.createOrReplaceTempView("gen_L1_TMP_CalendarDate")\
				  
		gen_L2_DIM_CalendarDate = spark.sql("SELECT '"+analysisid+"' as analysisID\
		 ,row_number() OVER (ORDER BY (SELECT NULL)) AS dateSurrogateKey	\
		 ,org2.organizationUnitSurrogateKey          AS organizationUnitSurrogateKey	 \
		 ,tdate2.calendarDate                        AS calendarDate		 \
		 ,tdate2.dateAsInterger                      AS dayAsInteger		 \
		 ,tdate2.calendarMonth                       AS calendarMonth		 \
		 ,tdate2.calendarQuarter                     AS calendarQuarter	 \
		 ,tdate2.calendarYear 				        AS calendarYear		 \
		 ,tdate2.yearMonthId                         AS yearmonthId		 \
		 ,tdate2.dayAsText  					     	AS dayAsText			 \
		 ,tdate2.dayOfWeekId                         AS dayOfWeekId		 \
		 ,tdate2.weekId                              AS weekId				 \
		 ,tdate2.yearQuarterId                       AS yearQuarterId		 \
		 ,tdate2.yearWeekId                          AS yearWeekId			 \
		 ,tdate2.dayMonthId                          AS dayMonthId			 \
		 ,CASE  WHEN hday.holidayDate IS NOT NULL THEN 1 ELSE 0 END					 AS isHoliday \
		 ,CASE  WHEN hday.holidayDate IS NOT NULL THEN hday.holidayText ELSE NULL END AS holidayText \
		 ,CASE WHEN par.parameterValue IS NOT NULL THEN 1 ELSE 0 	END		 AS isWeekend \
		 ,org2.phaseID																 AS phaseID			 \
		 ,concat(bdtvm.targetSystemValue,' - ', tdate2.calendarYear)					 AS yearMonthAsText	 \
		 ,bdtvm.targetSystemValue													 AS monthAsText		 \
		 ,concat(concat('KW',RIGHT(concat(0,weekId),2)) ,' - ', calendarYear)		 AS weekYearAsText		 \
		 ,concat('KW',RIGHT(concat(0,weekId),2))										 AS weekAsText			 \
		 ,concat(dayAsText,'.',bdtvm.targetSystemValue,' - ',calendarYear)			 AS dayMonthYearAsText	 \
		 ,concat(dayAsText,'.',bdtvm.targetSystemValue)								 AS dayMonthAsText		 \
		 ,bdtvw.targetSystemValue													 AS dayOfWeekAsText	 \
		FROM	gen_L1_TMP_CalendarDate tdate2 \
		INNER JOIN gen_L2_DIM_Organization org2  ON \
						( \
							tdate2.companyCode = org2.companyCode  \
						) \
		LEFT JOIN gen_L1_TMP_CalendarDateHolidayDetail hday  ON \
						( \
						(CASE WHEN hday.companyCode = 'CC' THEN '1' ELSE hday.companyCode	END) \
						=(CASE WHEN hday.companyCode = 'CC' THEN '1' ELSE tdate2.companyCode END)  \
						AND hday.holidayDate	= tdate2.calendarDate \
						) \
		LEFT JOIN knw_LK_CD_ReportingSetup rpt ON \
						( \
							rpt.clientCode		=	org2.clientID \
							AND rpt.companyCode	=	org2.companyCode \
						) \
		LEFT JOIN knw_LK_GD_BusinessDatatypeValueMapping bdtvm  ON \
						(  \
							bdtvm.targetLanguageCode	= CASE WHEN isnull(rpt.KPMGDataReportingLanguage) \
							THEN 'EN' ELSE rpt.KPMGDataReportingLanguage END \
							AND bdtvm.sourceSystemValue =tdate2.monthname \
							AND bdtvm.businessDatatype	= 'Month Name' \
							AND bdtvm.sourceERPSystemID = "+erpGENSystemID+" \
						) \
		LEFT JOIN knw_LK_GD_BusinessDatatypeValueMapping bdtvw  ON \
						(  \
							bdtvw.targetLanguageCode	= CASE WHEN isnull(rpt.KPMGDataReportingLanguage) \
							THEN 'EN' ELSE rpt.KPMGDataReportingLanguage END \
							AND bdtvw.sourceSystemValue = tdate2.dayname \
							AND bdtvw.businessDatatype	= 'Weekdays' \
							AND bdtvw.sourceERPSystemID = "+erpGENSystemID+" \
						) \
        LEFT JOIN knw_LK_CD_Parameter par ON \
						( \
							par.parameterValue =  tdate2.dayname\
							AND par.name = 'WEEK_END' \
						)")
		
		gen_L2_DIM_CalendarDate = objDataTransformation.gen_convertToCDMandCache \
			(gen_L2_DIM_CalendarDate,'gen','L2_DIM_CalendarDate',True)

		vw_DIM_CalendarDate = spark.sql("SELECT analysisID \
		   ,dateSurrogateKey \
	       ,organizationUnitSurrogateKey \
		   ,calendarDate \
		   ,calendarMonth \
		   ,calendarQuarter \
		   ,calendarYear \
		   ,yearMonthId \
		   ,weekId \
		   ,yearQuarterId \
		   ,yearWeekId \
		   ,CASE \
		        WHEN isHoliday = 1 \
				THEN 'Is Holiday - Yes' \
				ELSE 'Is Holiday - No' \
			END AS	isHoliday\
		   ,CASE \
               WHEN holidayText IS NOT NULL \
               THEN holidayText \
               ELSE 'NONE'\
            END AS 	holidayText \
		   ,isWeekend \
		   ,yearMonthAsText \
		   ,monthAsText \
		   ,weekYearAsText \
		   ,weekAsText \
		   ,dayMonthYearAsText \
		   ,dayMonthAsText \
		   ,dayOfWeekAsText \
		FROM gen_L2_DIM_CalendarDate \
		WHERE calendarDate <= '9990-12-31' \
		UNION ALL \
		SELECT analysisID \
		   ,dateSurrogateKey \
	       ,organizationUnitSurrogateKey \
		   ,'9990-12-31' AS calendarDate \
		   ,12 AS calendarMonth \
		   ,4 AS calendarQuarter \
		   ,9990 AS calendarYear \
		   ,999012 AS yearMonthId \
		   ,53 AS weekId \
		   ,'9990 - Q04' AS yearQuarterId \
		   ,999053 AS yearWeekId \
		   ,CASE \
		        WHEN isHoliday = 1 \
				THEN 'Is Holiday - Yes' \
				ELSE 'Is Holiday - No' \
			END AS	isHoliday\
		   ,CASE \
               WHEN holidayText IS NOT NULL \
               THEN holidayText \
               ELSE 'NONE'\
            END AS 	holidayText \
		   ,isWeekend \
		   ,'December - 9990' AS yearMonthAsText \
		   ,'December' AS monthAsText \
		   ,'KW53 - 9990' AS weekYearAsText \
		   ,'KW53' AS weekAsText \
		   ,'31.December - 9990' AS dayMonthYearAsText \
		   ,'31.December' AS dayMonthAsText \
		   ,dayOfWeekAsText \
		FROM gen_L2_DIM_CalendarDate \
		WHERE calendarDate > '9990-12-31' ")
		
		defaultDate='1900-02-02'
		defaultDate = parse(defaultDate).date()

		dwh_vw_DIM_CalendarDate = objDataTransformation.gen_convertToCDMStructure_generate(vw_DIM_CalendarDate,'dwh',\
                               'vw_DIM_CalendarDate',isIncludeAnalysisID = False, isSqlFormat = True)[0]
		keysAndValues = {'calendarDate':defaultDate,'isHoliday':'Is Holiday - No','analysisID': analysisid ,\
			'dateSurrogateKey': 0, 'organizationUnitSurrogateKey': 0, 'calendarMonth' :0, 'calendarQuarter':0,\
			'calendarYear': 0, 'yearMonthId':0, 'weekId':0, 'yearWeekId': 0}
		dwh_vw_DIM_CalendarDate = objGenHelper.gen_placeHolderRecords_populate\
			('dwh.vw_DIM_CalendarDate',keysAndValues,dwh_vw_DIM_CalendarDate)
		objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_CalendarDate,gl_CDMLayer2Path + "gen_L2_DIM_CalendarDate.parquet" ) 
		
		executionStatus = "gen_L2_DIM_CalendarDate populated sucessfully"
		executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
		return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

	except Exception as e:
		executionStatus = objGenHelper.gen_exceptionDetails_log()       
		executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
		return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
	
