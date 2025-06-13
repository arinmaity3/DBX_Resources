# Databricks notebook source
#[gen].[usp_SAP_L1_MD_UserTimezone_populate] 
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col,when,date_format,datediff,sum,max,min,explode
from datetime import date,timedelta 
import calendar
from pyspark.sql.types import DateType

def _returnDateTime(year, month, weekd, weekcd):
    if ((year is None) | (month is None) |(weekd is None) |(weekcd is None)):
      dateReturned = None
    else:
      weekcounter = 1
      year = int(year)
      month = int(month)
      weekd = int(weekd)
      weekcd = int(weekcd)

      if weekd == 1:
        daynameDe = 'Sonntag'
        daynameEn = 'Sunday'
      if weekd == 2:
        daynameDe = 'Montag'
        daynameEn = 'Monday'
      if weekd == 3:
        daynameDe = 'Dienstag'
        daynameEn = 'Tuesday'
      if weekd == 4:
        daynameDe = 'Mittwoch'
        daynameEn = 'Wednesday'
      if weekd == 5:
        daynameDe = 'Donnerstag'
        daynameEn = 'Thursday'
      if weekd == 6:
        daynameDe = 'Freitag'
        daynameEn = 'Friday'
      if weekd == 7:
        daynameDe = 'Samstag'
        daynameEn = 'Saturday'

      if weekcd in (1,2,3,4):
        dateReturned = datetime.strptime((str(year) + '-' + str(month) + '-1'), '%Y-%m-%d').date()
        weekcounter = 0

        while weekcounter < weekcd :
          if (calendar.day_name[dateReturned.weekday()] in (daynameDe,daynameEn)):
            weekcounter = weekcounter + 1

          if weekcounter < weekcd :
            dateReturned = dateReturned + timedelta(days = weekd)
            weekcounter = weekcounter + 1

      else:
        if month in (1,3,5,7,8,10,12) :
          dateReturned = datetime.strptime((str(year) + '-' + str(month) + '-31'), '%Y-%m-%d').date()
        else:
          if month in (4,6,9,11) :
            dateReturned = datetime.strptime((str(year) + '-' + str(month) + '-30'), '%Y-%m-%d').date()
          else:
            if (((year % 4) == 0) & ((year % 100) != 0) | ((year % 400) == 0)):
              dateReturned = datetime.strptime((str(year) + '-' + str(month) + '-29'), '%Y-%m-%d').date()
            else:
              dateReturned = datetime.strptime((str(year) + '-' + str(month) + '-28'), '%Y-%m-%d').date()

        while (calendar.day_name[dateReturned.weekday()] not in (daynameDe,daynameEn)) :
          dateReturned = dateReturned + timedelta(days = -weekd)
        
    return dateReturned

returnDateTime = udf(lambda w,x,y,z: _returnDateTime(w,x,y,z),DateType())

def gen_SAP_L1_MD_UserTimezone_populate():
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
        global gen_L1_MD_UserTimeZone
        erpGENSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')
        startDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date()
        endDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date()

        lst = erp_TTZZ.select(col('TZONE')).distinct().rdd.flatMap(lambda x: x).collect()

        gen_SAP_L0_TMP_UserTimeShift = erp_USR02.alias('u') \
            .join (erp_USER_ADDR.alias('a'),(col('u.BNAME') == col('a.BNAME')) \
            & (col('u.MANDT') == col('a.MANDT')),'left') \
            .join (erp_TTZCU.alias('t'),(col('t.CLIENT') == col('a.MANDT')),'left') \
            .select(col('u.MANDT').alias('client') \
                    ,col('u.BNAME').alias('userName') \
                    ,when(col('u.TZONE') == lit(''),col('t.TZONEDEF')) \
                    .otherwise(col('u.TZONE')).alias('timeZone') \
                    ,col('t.TZONESYS').alias('sysTimeZone') \
                    ,col('t.TZONEDEF').alias('standardTimeZone') \
                    ,col('t.FLAGACTIVE').alias('isFlagActive') \
                    ,when(col('t.FLAGACTIVE') != lit('X'),lit('0')) \
                    .when(col('t.TZONESYS') == col('u.TZONE'),lit('0')) \
                    .when((col('u.TZONE') != lit('')) & (col('u.TZONE').isin(lst) == 0),col('t.TZONEDEF')) \
                    .when((col('u.TZONE') != lit('')) & (col('t.TZONESYS') != col('u.TZONE')),col('u.TZONE')) \
                    .when(col('u.tzone') == lit(''),\
                         (when(col('t.TZONESYS') == col('t.TZONEDEF'),lit('0')).otherwise(col('t.TZONEDEF')))) \
                    .alias('timeZoneShift'))

        gen_SAP_L0_TMP_UserTimeShift = objGenHelper.gen_writeandCache \
			(gen_SAP_L0_TMP_UserTimeShift,'gen','SAP_L0_TMP_UserTimeShift')
        
        data = [["1"]]
        datedf = spark.createDataFrame(data,["id"])
        datedf.withColumn("startDate",F.add_months(lit(startDate), -120)) \
            .withColumn("endDate",F.add_months(lit(endDate), 120)).createOrReplaceTempView('dateRange')
        spark.sql("SELECT sequence(startDate, endDate, interval 1 day) as processTime from dateRange") \
            .withColumn("processTime", explode(col("processTime"))).createOrReplaceTempView('gti')
        gen_SAP_L0_TMP_TimeZoneCalendarDate = spark.sql("SELECT  \
                                    date_format(processTime,'yyyy') AS finYear  \
                                    ,processTime AS dateValue \
                                   FROM gti")

        gen_SAP_L0_TMP_TimeZoneCalendarDate = objGenHelper.gen_writeandCache \
			(gen_SAP_L0_TMP_TimeZoneCalendarDate,'gen','SAP_L0_TMP_TimeZoneCalendarDate')

        Tzone = gen_SAP_L0_TMP_UserTimeShift.filter(col('timeZoneShift')!=lit('0')) \
            .select(col('timeZoneShift').alias('timeZone')).distinct()
        Tzone1 = gen_SAP_L0_TMP_UserTimeShift.select(col('standardTimeZone').alias('timeZone')).distinct()
        Tzone2 = gen_SAP_L0_TMP_UserTimeShift.select(col('sysTimeZone').alias('timeZone')).distinct()
        gen_SAP_L0_TMP_RelevantTzone = Tzone.union(Tzone1.union(Tzone2)).distinct()

        gen_SAP_L0_TMP_RelevantTzone = objGenHelper.gen_writeandCache \
			(gen_SAP_L0_TMP_RelevantTzone,'gen','SAP_L0_TMP_RelevantTzone')

        gen_SAP_L0_TMP_ReplicateCalendarDate = gen_SAP_L0_TMP_TimeZoneCalendarDate.alias('cdate') \
            .crossJoin(gen_SAP_L0_TMP_RelevantTzone.alias('rzone')) \
            .select(col('cdate.finYear').alias('finYear') \
                ,col('cdate.dateValue').alias('dateValue') \
                ,col('rzone.timeZone').alias('timeZone') )

        gen_SAP_L0_TMP_ReplicateCalendarDate = objGenHelper.gen_writeandCache \
			(gen_SAP_L0_TMP_ReplicateCalendarDate,'gen','SAP_L0_TMP_ReplicateCalendarDate')

        defaultDate='1900-01-01'
        defaultDate = parse(defaultDate).date()
        TTZDF = erp_TTZDF \
            .select(col('CLIENT').alias('client') \
            ,col('DSTRULE').alias('dstRule') \
            ,lit('FIX').alias('dstType') \
            ,col('YEARACT').alias('yearAct') \
            ,lit(0).alias('yearFrom') \
            ,col('DATEFROM').alias('dateFrom') \
            ,col('DATETO').alias('dateTo') \
            ,lit(0).alias('monthFrom') \
            ,lit(0).alias('weekdFrom') \
            ,lit(0).alias('weekdcFrom') \
            ,lit(0).alias('monthTo') \
            ,lit(0).alias('weekdTo') \
            ,lit(0).alias('weekdcTo'))
        TTZDV = erp_TTZDV \
            .select(col('CLIENT').alias('client') \
            ,col('DSTRULE').alias('dstRule') \
            ,lit('VAR').alias('dstType') \
            ,lit(0).alias('yearAct') \
            ,col('YEARFROM').alias('yearFrom') \
            ,lit(defaultDate).alias('dateFrom') \
            ,lit(defaultDate).alias('dateTo') \
            ,col('MONTHFROM').alias('monthFrom') \
            ,col('WEEKDFROM').alias('weekdFrom') \
            ,col('WEEKDCFROM').alias('weekdcFrom') \
            ,col('MONTHTO').alias('monthTo') \
            ,col('WEEKDTO').alias('weekdTo') \
            ,col('WEEKDCTO').alias('weekdcTo'))
        gen_SAP_L0_TMP_StandardDayLightTime = TTZDF.union(TTZDV)

        gen_SAP_L0_TMP_StandardDayLightTime = gen_SAP_L0_TMP_StandardDayLightTime.alias('sddlt0')\
            .join (gen_SAP_L0_TMP_StandardDayLightTime.alias('sddlt1'), \
            (col('sddlt0.client') == col('sddlt1.client')) \
            & (col('sddlt0.dstRule') == col('sddlt1.dstRule')) \
            & (col('sddlt0.yearfrom') == col('sddlt1.yearfrom')),how = 'inner') \
            .filter(col('sddlt0.dstType') !=  lit('FIX')) \
            .select(col('sddlt0.client').alias('client') \
            ,col('sddlt0.dstRule').alias('dstRule')\
            ,col('sddlt0.dstType').alias('dstType')\
            ,col('sddlt0.yearAct').alias('yearAct')\
            ,col('sddlt0.yearFrom').alias('yearFrom')\
            ,col('sddlt0.dateFrom').alias('dateFrom')\
            ,col('sddlt0.dateTo').alias('dateTo')\
            ,col('sddlt0.monthFrom').alias('monthFrom')\
            ,col('sddlt0.weekdFrom').alias('weekdFrom')\
            ,col('sddlt0.weekdcFrom').alias('weekdcFrom')\
            ,col('sddlt0.monthTo').alias('monthTo')\
            ,col('sddlt0.weekdTo').alias('weekdTo') \
            ,col('sddlt0.weekdcTo').alias('weekdcTo'))

        gen_SAP_L0_TMP_StandardDayLightTime = objGenHelper.gen_writeandCache \
			(gen_SAP_L0_TMP_StandardDayLightTime,'gen','SAP_L0_TMP_StandardDayLightTime')

        timeZone = gen_SAP_L0_TMP_RelevantTzone.select(col('timeZone').alias('timeZone')).distinct()
        yearFrom = erp_TTZDV.alias('tv') \
            .join (gen_SAP_L0_TMP_StandardDayLightTime.alias('Time1'), \
            (col('tv.DSTRULE') == col('Time1.dstrule')),how = 'inner') \
            .groupBy('tv.DSTRULE').agg(max('tv.YEARFROM').alias('YEARFROM'))

        gen_SAP_L0_TMP_TimeShiftPerUser = erp_TTZZ.alias('tz') \
            .join (erp_TTZR.alias('tr'), \
            (col('tz.CLIENT') == col('tr.CLIENT')) \
            & (col('tz.ZONERULE') == col('tr.ZONERULE')),how = 'inner') \
            .join (erp_TTZD.alias('td'), \
            (col('tz.DSTRULE') == col('td.DSTRULE')) \
            & (col('tz.CLIENT') == col('td.CLIENT')),how = 'inner') \
            .join (timeZone.alias('t'), \
            (col('t.timeZone') == col('tz.TZONE')),how = 'inner') \
            .join (gen_SAP_L0_TMP_StandardDayLightTime.alias('Time1'), \
            (col('td.DSTRULE') == col('Time1.dstrule')) \
            & (col('td.CLIENT') == col('Time1.client')),how = 'left') \
            .join (gen_SAP_L0_TMP_ReplicateCalendarDate.alias('Time2'), \
            (col('tz.TZONE') == col('Time2.timeZone')),how = 'left') \
            .join (yearFrom.alias('yf'), \
            (col('yf.DSTRULE') == col('Time1.dstRule')) \
            & (col('Time1.yearFrom') <= col('Time2.finYear')), how = 'left') \
            .filter((col('tz.FLAGACTIVE') == lit('X')) \
                    & (col('td.FLAGACTIVE') == lit('X')) \
                    & (( \
                           ((col('Time1.yearAct') != lit(0)) & (col('Time2.finYear') == col('Time1.yearAct')))\
                        | ((col('Time1.yearFrom') != lit(0)) & (col('Time1.yearFrom') <= col('Time2.finYear'))) \
                        & (col('Time1.yearfrom') == col('yf.YEARFROM')) \
                        | (col('Time1.dstType').isNull() == 1))\
                    )) \
            .select(col('Time2.finYear').alias('finYear') \
            ,col('Time2.dateValue').alias('dateValue') \
            ,col('tz.TZONE').alias('timeZone') \
            ,col('tr.UTCDIFF').cast('int').alias('utcDifference') \
            ,col('tr.UTCSIGN').alias('utcSign') \
            ,col('td.DSTRULE').alias('dstRule') \
            ,col('td.DSTDIFF').cast('int').alias('dstDifference') \
            ,col('Time1.dstType').alias('dstType') \
            ,col('Time1.yearFrom').alias('yearFrom') \
            ,col('Time1.yearAct').alias('yearAct') \
            ,col('Time1.dateFrom').alias('dateFrom') \
            ,col('Time1.dateTo').alias('dateTo') \
            ,col('Time1.monthFrom').alias('monthFrom') \
            ,col('Time1.weekdFrom').alias('weekdFrom') \
            ,col('Time1.weekdcFrom').alias('weekdcFrom') \
            ,col('Time1.monthTo').alias('monthTo') \
            ,col('Time1.weekdTo').alias('weekdTo') \
            ,col('Time1.weekdcTo').alias('weekdcTo'))
        
        gen_SAP_L0_TMP_TimeShiftPerUser = objGenHelper.gen_writeandCache \
			(gen_SAP_L0_TMP_TimeShiftPerUser,'gen','SAP_L0_TMP_TimeShiftPerUser')

        currentYear = spark.sql("select date_format(current_date(),'y')").rdd.collect()[0][0]
        maxYear = erp_TTZDV.alias('tv') \
            .filter(col('tv.YEARFROM') <= lit(currentYear)) \
            .groupBy(col('tv.CLIENT'),col('tv.DSTRULE')).agg(max(col('tv.YEARFROM')).alias('maximumYear'))

        gen_SAP_L0_TMP_TimeShiftPerSystem = erp_TTZCU.alias('tu') \
            .join (erp_TTZZ.alias('tz') , \
            (col('tu.CLIENT') == col('tz.CLIENT')) \
            & (col('tu.TZONESYS') == col('tz.TZONE')), how='inner') \
            .join (erp_TTZR.alias('tr') , \
            (col('tz.CLIENT') == col('tr.CLIENT')) \
            & (col('tz.ZONERULE') == col('tr.ZONERULE')),how='inner') \
            .join (erp_TTZD.alias('td') , \
            (col('td.CLIENT') == col('tz.CLIENT')) \
            & (col('td.DSTRULE') == col('tz.DSTRULE')),how='left') \
            .join (maxYear.alias('tmp') , \
            (col('tmp.CLIENT') == col('tz.CLIENT')) \
            & (col('tmp.DSTRULE') == col('tz.DSTRULE')),how='left') \
            .join (erp_TTZDV.alias('tv') , \
            (col('tv.CLIENT') == col('tz.CLIENT')) \
            & (col('tv.DSTRULE') == col('tz.DSTRULE')) \
            &  (col('tv.YEARFROM') == col('tmp.maximumYear')),how='left') \
            .select(col('tu.CLIENT').alias('client') \
            ,col('tu.TZONESYS').alias('sysTimeZone') \
            ,col('tu.TZONEDEF').alias('standardTimeZone') \
            ,col('tz.ZONERULE').alias('zoneRule') \
            ,col('tz.DSTRULE').alias('dstRule') \
            ,col('tr.UTCDIFF').alias('utcDifference') \
            ,col('tr.UTCSIGN').alias('utcSign') \
            ,(when(col('tr.UTCSIGN') == '-' , -1 * col('tr.UTCDIFF')) \
            .otherwise(col('tr.UTCDIFF'))).cast(IntegerType()).alias('utcSignDifference') \
            ,when(col('td.DSTRULE').isNull(),lit('')).otherwise(lit('X')).alias('summerTime') \
            ,col('tv.MONTHFROM').alias('monthFrom') \
            ,col('tv.WEEKDCFROM').alias('weekdcFrom') \
            ,col('tv.MONTHTO').alias('monthTo') \
            ,col('tv.WEEKDCTO').alias('weekdcTo') \
            ,col('tv.TIMEFROM').alias('timeFrom') \
            ,col('tv.TIMETO').alias('timeTo'))

        gen_SAP_L0_TMP_TimeShiftPerSystem = objGenHelper.gen_writeandCache \
			(gen_SAP_L0_TMP_TimeShiftPerSystem,'gen','SAP_L0_TMP_TimeShiftPerSystem')

        gen_SAP_L0_TMP_DayLight = gen_SAP_L0_TMP_TimeShiftPerUser.alias('tsu') \
            .select(col('tsu.finYear').alias('finYear') \
            ,col('tsu.timeZone').alias('timeZone') \
            ,returnDateTime(col('tsu.yearFrom'), col('tsu.monthFrom'), col('weekdFrom'), col('weekdcFrom')).alias('dateFrom') \
            ,returnDateTime(col('tsu.yearFrom'), col('tsu.monthTo'), col('weekdTo'), col('weekdcTo')).alias('dateTo')).distinct()

        gen_SAP_L0_TMP_DayLight = objGenHelper.gen_writeandCache \
			(gen_SAP_L0_TMP_DayLight,'gen','SAP_L0_TMP_DayLight')

        case1 = ((when(col('p.utcSign')==lit('+'),col('p.utcDifference')) \
            .otherwise(lit(-1) * col('p.utcDifference'))) + col('p.dstDifference'))
        
        case2 = (when(col('p.utcSign')==lit('+'),col('p.utcDifference')) \
            .otherwise(lit(-1) * col('p.utcDifference')))
        
        caseMain1 = (when(((datediff(col('p.dateValue'),col('u.dateFrom')) > 0) & (datediff(col('u.dateTo'),col('p.dateValue')) > 0)),\
            lit(case1)).otherwise(lit(case2)))
        
        caseMain2 = (when(((datediff(col('p.dateValue'),col('p.dateFrom')) >0) & (datediff(col('p.dateTo'),col('p.dateValue')) > 0)) ,\
            lit(case1)).otherwise(lit(case2)))
        
        gen_SAP_L0_TMP_EachDayShift = gen_SAP_L0_TMP_TimeShiftPerUser.alias('p') \
            .join (gen_SAP_L0_TMP_DayLight.alias('u') , \
            (col('p.finYear') == col('u.finYear')) \
            & (col('p.timeZone') == col('u.timeZone')), how = 'inner') \
            .select(col('p.finYear').alias('finYear') \
            ,col('p.dateValue').alias('dateValue') \
            ,col('p.timeZone').alias('timeZone') \
            ,(when(col('p.yearFrom') != lit(0) ,lit(caseMain1)) \
            .otherwise(lit(caseMain2))).cast(DecimalType(32,6)).alias('timeShift'))

        gen_SAP_L0_TMP_EachDayShift = objGenHelper.gen_writeandCache \
			(gen_SAP_L0_TMP_EachDayShift,'gen','SAP_L0_TMP_EachDayShift')

        gen_SAP_L0_TMP_CreateTimeZone = gen_SAP_L0_TMP_UserTimeShift.alias('u') \
            .join (gen_SAP_L0_TMP_EachDayShift.alias('e') ,\
            (col('u.timeZone') == col('e.timeZone')),how = 'left') \
            .join (gen_SAP_L0_TMP_TimeShiftPerSystem.alias('s') , \
            (col('u.client') == col('s.client')),how = 'left') \
            .select(col('u.client').alias('client') \
            ,col('u.userName').alias('userName') \
            ,col('u.timeZone').alias('timeZone') \
            ,col('u.standardTimeZone').alias('standardTimeZone') \
            ,col('u.sysTimeZone').alias('sysTimeZone') \
            ,col('e.finYear').alias('finYear') \
            ,col('e.dateValue').alias('dateValue') \
            ,when(col('u.timeZoneShift') == lit('0'), lit(0)) \
            .otherwise(col('e.timeShift')).alias('utcTimeZoneShift') \
            ,(when(col('u.timeZoneShift') == lit('0'),lit(0)) \
            .otherwise(col('e.timeShift') - col('s.utcSignDifference'))).cast(DecimalType(32,6)).alias('sysTimeZoneShift'))

        gen_SAP_L0_TMP_CreateTimeZone = objGenHelper.gen_writeandCache \
			(gen_SAP_L0_TMP_CreateTimeZone,'gen','SAP_L0_TMP_CreateTimeZone')

        gen_SAP_L0_TMP_CreateTimeZoneWithDayLight = gen_SAP_L0_TMP_CreateTimeZone.alias('z') \
            .join (gen_SAP_L0_TMP_DayLight.alias('l') , \
            (col('z.finYear') == col('l.finYear')) \
            & (col('z.timeZone') == col('l.timeZone')), how = 'left') \
            .select(col('z.client').alias('client') \
            ,col('z.userName').alias('userName') \
            ,col('z.timeZone').alias('timeZone') \
            ,col('z.standardTimeZone').alias('standardTimeZone') \
            ,col('z.sysTimeZone').alias('sysTimeZone') \
            ,col('z.finYear').alias('finYear') \
            ,col('z.datevalue').alias('datevalue') \
            ,col('z.utcTimeZoneShift').alias('utcTimeZoneShift') \
            ,col('z.sysTimeZoneShift').alias('sysTimeZoneShift') \
            ,col('l.dateFrom').alias('dateFrom') \
            ,col('l.dateTo').alias('dateTo') \
            ,when(col('z.datevalue') <= col('l.dateFrom') , lit(1)) \
            .when((col('z.datevalue') > col('l.dateFrom')) & (col('z.datevalue') < col('l.dateTo')) , lit(2)) \
            .when(col('z.datevalue') >= col('l.dateTo') , lit(3)).alias('dayLightID')).distinct()

        gen_SAP_L0_TMP_CreateTimeZoneWithDayLight = objGenHelper.gen_writeandCache \
			(gen_SAP_L0_TMP_CreateTimeZoneWithDayLight,'gen','SAP_L0_TMP_CreateTimeZoneWithDayLight')

        gen_L1_MD_UserTimeZone =  gen_SAP_L0_TMP_CreateTimeZoneWithDayLight.alias('t') \
            .groupBy(col('t.userName').alias('userName') \
            ,col('t.timeZone').alias('userTimeZone') \
            ,(col('t.sysTimeZoneShift').cast(DecimalType(17,2)) / 10000).cast(DecimalType(32,6)).alias('timeZoneShiftInHours') \
            ,col('t.dayLightID')) \
            .agg(min(col('t.dateValue')).alias('dateFrom') \
                ,max(col('t.dateValue')).alias('dateTo'))

        gen_L1_MD_UserTimeZone = objDataTransformation.gen_convertToCDMandCache\
            (gen_L1_MD_UserTimeZone,'gen','L1_MD_UserTimeZone',targetPath = gl_CDMLayer1Path)
        
        executionStatus = "L1_MD_UserTimeZone populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    
    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


