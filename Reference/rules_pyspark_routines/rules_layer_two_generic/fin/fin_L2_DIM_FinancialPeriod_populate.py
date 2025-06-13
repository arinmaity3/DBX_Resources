# Databricks notebook source
from pyspark.sql.functions import substring,concat_ws,collect_set,dayofmonth,month,quarter,date_format,weekofyear,col,lit,dayofweek,year,concat
from dateutil.parser import parse
import sys
import traceback

def fin_L2_DIM_FinancialPeriod_populate():
    
  try:
    
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()

    global fin_L2_DIM_FinancialPeriod
    
    # Set parameters and variables
    
    analysisid = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ANALYSISID'))
    erpGENSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')
    
    if erpGENSystemID is None:
      erpGENSystemID = 'NULL'
      
    finYear = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR'))
    
    if finYear == 'None':
      finYear = ''
      prvfinYear1 = -1
      prvfinYear2 = -2
      syfinYear = 1
    else:
      prvfinYear1 =  str(int(finYear)-1)
      prvfinYear2 =  str(int(finYear)-2)
      syfinYear  =   str(int(finYear)+1)
      
    periodStart = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_START'))
    
    if periodStart == 'None':
      periodStart = 0
      
    periodEnd = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'PERIOD_END'))
    
    if periodEnd == 'None':
      periodEnd = 0
      
    gen_L1_MD_Period.createOrReplaceTempView("gen_L1_MD_Period")
    sqlContext.cacheTable('gen_L1_MD_Period')
    
    TMP_DateCollect = spark.createDataFrame(gl_lstOfFinancialPeriodCollect,financailPerodSchemaCollect).\
                             select(col("companyCode"),col("fiscalYear"),col("calendarDate")\
                                    ,col("financialPeriod"),col("key").alias("originOfEntry")).distinct() 
    
    TMP_DateCollect.createOrReplaceTempView("TMP_DateCollect")
    sqlContext.cacheTable("TMP_DateCollect")
    
    agg_gen_L1_MD_Period = gen_L1_MD_Period.alias('agg')\
    				  .groupBy('agg.companyCode','agg.fiscalYear','agg.financialPeriod')\
                      .agg(max('calendarDate').alias('maxDate'))
    
    agg_gen_L1_MD_Period.createOrReplaceTempView("agg_gen_L1_MD_Period")
    sqlContext.cacheTable("agg_gen_L1_MD_Period")
    
    originOfEntry_case = when(col('tmp1.originOfEntry')== 'Jouranal',\
                              when(col('prd.maxDate').isNull(),col('tmp1.calendarDate')).otherwise(col('prd.maxDate')))\
                         .when(col('tmp1.originOfEntry')== 'GLBalance',\
                              when(col('prd.maxDate').isNull(),lit('1900-02-02')).otherwise(col('prd.maxDate')))
    
    calendarDate_case = when(col('tmp1.originOfEntry')== 'Jouranal',\
                    col('tmp1.calendarDate'))\
              .when(col('tmp1.originOfEntry')== 'GLBalance',\
                              when(col('prd.maxDate').isNull(),lit('1900-02-02')).otherwise(col('prd.maxDate')))
    
    TMP_DateCollect = TMP_DateCollect.alias('tmp1')\
                    .join(agg_gen_L1_MD_Period.alias('prd'),\
					    ((col('prd.companyCode')==col('tmp1.companyCode'))\
					    &(col('prd.fiscalYear')==col('tmp1.fiscalYear'))\
					    &(expr("prd.financialPeriod 	= CASE WHEN tmp1.financialPeriod < 1 \
															   THEN -1 \
															   ELSE tmp1.financialPeriod \
														  END"))),how='left')\
                    .select(col('tmp1.companyCode'),col('tmp1.fiscalYear'),\
                         lit(calendarDate_case).alias('calendarDate'),col('tmp1.financialPeriod')\
                         ,col('tmp1.originOfEntry'),lit(originOfEntry_case).alias('maxDate'))

    updt1 = fin_L1_TD_Journal.groupBy("companyCode","fiscalYear","financialPeriod")\
                            .agg(max("postingDate").alias("postingDate"))
    
    updt1.createOrReplaceTempView("updt1")
    sqlContext.cacheTable('updt1')                                
    fin_L1_TMP_DateCollect1 = TMP_DateCollect.alias('dtc11')\
                                             .join(updt1.alias('jrnl'),\
                                             (col('dtc11.companyCode').eqNullSafe(col('jrnl.companyCode')))\
                                           & (col('dtc11.fiscalYear').eqNullSafe(col('jrnl.fiscalYear')))\
                                           & (col('dtc11.financialPeriod').eqNullSafe(col('jrnl.financialPeriod'))),how='left')\
                                          .select(col('dtc11.companyCode'),col('dtc11.fiscalYear')\
                                           ,when (((col('dtc11.originOfEntry')== 'GLBalance') \
                                            & (col('dtc11.financialPeriod').cast("Int") > lit(0)))\
                                            & (col('jrnl.companyCode').isNotNull())\
                                            & (col('jrnl.fiscalYear').isNotNull())\
                                            ,col('jrnl.postingDate'))\
                                          .otherwise(col('dtc11.calendarDate')).alias('calendarDate'),\
                                          col('dtc11.financialPeriod'),\
                                          col('dtc11.originOfEntry')\
                                          ,when (((col('dtc11.originOfEntry')== 'GLBalance') \
                                            & (col('dtc11.financialPeriod').cast("Int") > lit(0)))
                                            & (col('jrnl.companyCode').isNotNull())\
                                            & (col('jrnl.fiscalYear').isNotNull())\
                                            ,col('jrnl.postingDate'))\
                                          .otherwise(col('dtc11.calendarDate')).alias('maxDate'))
                                           
    fin_L1_TMP_DateCollect1.createOrReplaceTempView("fin_L1_TMP_DateCollect1")
    sqlContext.cacheTable('fin_L1_TMP_DateCollect1')
    fin_L1_TMP_DateCollect = fin_L1_TMP_DateCollect1.alias('dtc11')\
                                         .join(gen_L1_MD_Period.alias('prod1'),\
                                               ((col('dtc11.companyCode').eqNullSafe(col('prod1.companyCode')))\
                                               &(col('dtc11.calendarDate').eqNullSafe(col('prod1.calendarDate')))),how='left')\
                                          .select(col('dtc11.companyCode')\
                                           ,when(((col('dtc11.financialPeriod')== '') \
                                                 &(~(col('dtc11.originOfEntry').isin('Posting Date'))))\
                                                 ,col('prod1.fiscalYear'))\
                                                 .otherwise(col('dtc11.fiscalYear')).alias('fiscalYear')\
                                           ,col('dtc11.calendarDate')\
                                          ,when((col('dtc11.financialPeriod')== '') \
                                               &(~(col('dtc11.originOfEntry').isin('Posting Date')))\
                                               ,col('prod1.financialPeriod'))\
                                              .otherwise(col('dtc11.financialPeriod')).alias('financialPeriod')\
                                          ,col('dtc11.originOfEntry')\
                                          ,col('dtc11.maxDate'))
                                           
    fin_L1_TMP_DateCollect.createOrReplaceTempView("fin_L1_TMP_DateCollect")
    sqlContext.cacheTable('fin_L1_TMP_DateCollect')
    
    fin_L1_TMP_Date1 = spark.sql(" SELECT	DISTINCT  \
                                   tmp1.companyCode	as companyCode \
                                   ,tmp1.calendarDate  as calendarDate	 \
                                   ,CASE WHEN tmp1.financialPeriod IS NULL \
                                         THEN peri1.financialPeriod \
                                         ELSE tmp1.financialPeriod \
                                    END as financialPeriod \
                                   ,CASE WHEN tmp1.fiscalYear IS NULL THEN peri1.fiscalYear ELSE tmp1.fiscalYear END as  fiscalYear \
                                   ,CASE WHEN prd.maxDate IS NULL THEN tmp1.maxDate ELSE prd.maxDate END as  maxDate \
                                  FROM	fin_L1_TMP_DateCollect tmp1 \
                                  INNER JOIN gen_L2_DIM_Organization  org1	ON \
    														( \
    															org1.companyCode			= tmp1.companyCode \
    														) \
                                  LEFT JOIN  gen_L1_MD_Period peri1			ON \
    														(\
    															tmp1.companyCode			= peri1.companyCode\
    															AND tmp1.calendarDate		= peri1.calendarDate\
    														    AND tmp1.financialPeriod	= peri1.financialPeriod\
														)\
                                 LEFT JOIN	agg_gen_L1_MD_Period prd  ON \
															(\
																prd.companyCode				= tmp1.companyCode\
																AND prd.fiscalYear			= tmp1.fiscalYear\
																AND prd.financialPeriod 	= tmp1.financialPeriod \
															)	\
                                 WHERE  tmp1.calendarDate IS NOT NULL \
                                  ")
    fin_L1_TMP_Date1.createOrReplaceTempView("fin_L1_TMP_Date1")
    sqlContext.cacheTable('fin_L1_TMP_Date1')
    
    spark.sql("UNCACHE TABLE  IF EXISTS fin_L1_TMP_Date")
    
    fin_L1_TMP_Date = spark.sql("SELECT	 companyCode,calendarDate,financialPeriod,fiscalYear,maxDate \
											  FROM fin_L1_TMP_Date1 \
											 UNION  ALL\
											 SELECT	 rptSetup.companyCode	AS companyCode		 \
											,'1900-01-01'                   AS calendarDate		 \
                                            ,'1'                            AS financialPeriod   \
                                            ,'1900'                         AS fiscalYear     \
                                            ,'1900-01-01'                   AS maxDate \
											FROM	knw_LK_CD_ReportingSetup rptSetup \
											WHERE NOT EXISTS (SELECT 1 FROM fin_L1_TMP_Date1 tdat  \
																WHERE rptSetup.companyCode	= tdat.companyCode \
																AND  tdat.calendarDate	= '1900-01-01' \
															)")

    fiscalQuarter =  when (col('tmp.financialPeriod').isin(1,2,3), lit(1)) \
                        .when (col('tmp.financialPeriod').isin(4,5,6), lit(2)) \
                        .when (col('tmp.financialPeriod').isin(7,8,9), lit(3)) \
                        .when (col('tmp.financialPeriod') > 9, lit(4)) \
                        .when (col('tmp.financialPeriod') < 1, lit(-1)) \
                             
    fin_L1_TMP_Date = fin_L1_TMP_Date.alias('tmp')\
                        .join(gen_L1_MD_Period.alias('pri'),(((col('tmp.companyCode'))==(col('pri.companyCode')))\
                                                                &(col('tmp.calendarDate')==(col('pri.calendarDate')))\
                                                                &(col('tmp.financialPeriod')==(col('pri.financialPeriod'))))\
                                                                 ,how='left')\
                        .select(col('tmp.companyCode').alias('companyCode')\
                                ,col('tmp.calendarDate').alias('calendarDate')\
                                ,when(col("tmp.financialPeriod").isNull(),lit(99)).otherwise(col("tmp.financialPeriod")).alias('financialPeriod')
                                ,when(col("tmp.fiscalYear").isNull(),lit(1900)).otherwise(col("tmp.fiscalYear")).alias('fiscalYear')\
                                ,lit(fiscalQuarter).alias('fiscalQuarter')\
                                ,col('tmp.maxDate').alias('periodEndDate'))
    
    fin_L1_TMP_Date.withColumn("dateAsInterger",dayofmonth("calendarDate"))\
					.withColumn("calendarMonth",month("calendarDate"))\
					.withColumn("calendarQuarter",quarter("calendarDate"))\
					.withColumn("calendarYear",year("calendarDate"))\
                    .withColumn("financialPeriod",col('financialPeriod'))\
                    .withColumn("fiscalYear",col("fiscalYear"))\
                    .withColumn("fiscalQuarter",col('fiscalQuarter'))\
                    .withColumn("yearMonthId",substring(date_format("calendarDate",'yyyyMMdd'),1,6))\
					.withColumn("dayAsText",substring(date_format("calendarDate",'ddMMyyyy'),1,2))\
                    .withColumn("dayId",dayofmonth("calendarDate"))\
					.withColumn("dayOfWeekId",dayofweek("calendarDate"))\
					.withColumn("weekId",weekofyear("calendarDate"))\
                    .withColumn("yearQuarterId",concat(col("calendarYear"),lit(" - Q0"),col("calendarQuarter")))\
					.withColumn("yearWeekId",concat(col("calendarYear"),substring(concat(lit("0"),col("weekId")),-2,2)))\
					.withColumn("dayMonthId",concat(col("dateAsInterger"),col("calendarMonth")))\
                    .withColumn("monthname",date_format('calendarDate', 'MMMM'))\
                    .withColumn("datename",date_format('calendarDate', 'EEEE'))\
                    .createOrReplaceTempView("fin_L1_TMP_Date")
    sqlContext.cacheTable('fin_L1_TMP_Date')
    
    
    fin_L1_TMP_Date = spark.sql("select * from fin_L1_TMP_Date")
    
    fiscalYearIndicator = when(col('tdate2.fiscalYear') == finYear,lit('CY'))\
                      .when(col('tdate2.fiscalYear') == prvfinYear1,lit('PY'))\
                      .when(col('tdate2.fiscalYear') == prvfinYear2,lit('PPY'))\
                      .when(col('tdate2.fiscalYear') == syfinYear,lit('SY'))\
                      .otherwise(lit('#NA#'))

    openingPeriodIndicator = when ((col('tdate2.fiscalYear')== finYear) & (col('tdate2.financialPeriod').cast("Int") < periodStart),lit(1))\
                          .when(col('tdate2.financialPeriod').isin(0,-1),lit(1))\
                           .otherwise(lit(0))

    periodText = when ((col('tdate2.fiscalYear')== finYear) & (col('tdate2.financialPeriod').cast("Int") < periodStart),lit('Opening Period'))\
              .when(col('tdate2.financialPeriod').isin(0,-1),lit('Opening Period'))\
              .otherwise(col('tdate2.financialPeriod'))

    periodTextSortOrder = when ((col('tdate2.fiscalYear')== finYear) & (col('tdate2.financialPeriod').cast("Int") < periodStart),lit(-1))\
              .when(col('tdate2.financialPeriod').isin(0,-1),lit(-1))\
              .otherwise(col('tdate2.financialPeriod'))



    bdtvmjoin1 = when(col('rep.KPMGDataReportingLanguage').isNull(), lit('EN') ) \
                              .otherwise(col('rep.KPMGDataReportingLanguage')) 

    bdtvwjoin1 = when(col('rep.KPMGDataReportingLanguage').isNull(), lit('EN') ) \
                              .otherwise(col('rep.KPMGDataReportingLanguage')) 



    fin_L2_DIM_FinancialPeriod = fin_L1_TMP_Date.alias('tdate2')\
                            .join(gen_L2_DIM_Organization.alias('org2'),(col('tdate2.companyCode') == (col('org2.companyCode'))),how='inner')\
                            .join(knw_LK_CD_ReportingSetup.alias('rep'),(col('rep.companyCode')	==	col('org2.companyCode')) \
                                             & (col('rep.clientCode') == (col('org2.clientID'))),how =('inner')) \
                            .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdtvm'),(col('bdtvm.targetLanguageCode') == lit(bdtvmjoin1)) \
                                  & (col('bdtvm.sourceSystemValue') == (col('tdate2.monthname')))  & (col('bdtvm.businessDatatype') == lit('Month Name')) \
                                  & (col('bdtvm.sourceERPSystemID') == lit(erpGENSystemID)),how =('inner')) \
                            .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdtvw'),(col('bdtvw.targetLanguageCode') == lit(bdtvwjoin1)) \
                                  & (col('bdtvw.sourceSystemValue') == (col('tdate2.monthname')))  & (col('bdtvw.businessDatatype') == lit('Month Name')) \
                                   & (col('bdtvw.sourceERPSystemID') == lit(erpGENSystemID)),how =('inner')) \
                            .select(col('org2.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey'),col('tdate2.calendarDate').alias('postingDate'),col('tdate2.dateAsInterger').alias('dayAsInteger'),\
                                    col('tdate2.calendarMonth').alias('calendarMonth'),col('tdate2.calendarQuarter').alias('calendarQuarter'),col('tdate2.calendarYear').alias('calendarYear'),\
                                    col('tdate2.financialPeriod').alias('financialPeriod'),col('tdate2.fiscalYear').alias('fiscalYear'),col('tdate2.fiscalQuarter').alias('fiscalQuarter'),\
                                    lit(0).alias('fiscalYearVariantId'), col('tdate2.yearMonthId').alias('yearMonthId'),col('tdate2.dayAsText').alias('dayAsText'),\
                                    col('tdate2.dayOfWeekId').alias('dayOfWeekId'),col('tdate2.weekId').alias('weekId'),col('tdate2.yearQuarterId').alias('yearQuarterId'),\
                                    col('tdate2.yearWeekId').alias('yearWeekId'),col('tdate2.dayMonthId').alias('dayMonthId'),col('org2.phaseID').alias('phaseID'),\
                                    lit(fiscalYearIndicator).alias('fiscalYearIndicator'),lit(openingPeriodIndicator).alias('openingPeriodIndicator'),lit(periodText).alias('periodText'),\
                                    col('tdate2.periodEndDate').alias('periodEndDate'),lit(periodTextSortOrder).alias('periodTextSortOrder'),  \
                                    concat(col('tdate2.fiscalYear'),lit(' - '),col('tdate2.financialPeriod')).alias('reportingYearPeriod'), \
                                    concat(col('tdate2.fiscalYear'),lit(' - '),col('tdate2.fiscalQuarter')).alias('reportingYearQuarter'))
    
    w = Window().orderBy(lit('financialPeriodSurrogateKey'))
    fin_L2_DIM_FinancialPeriod = fin_L2_DIM_FinancialPeriod.withColumn("financialPeriodSurrogateKey", row_number().over(w))\
                                                           .withColumn("analysisID",lit(analysisid))
    
    fin_L2_DIM_FinancialPeriod  = objDataTransformation.gen_convertToCDMandCache \
        (fin_L2_DIM_FinancialPeriod,'fin','L2_DIM_FinancialPeriod',True)

    defaultDate='1900-02-02'
    defaultDate = parse(defaultDate).date()

    #Writing to parquet file in dwh schema format
    dwh_vw_DIM_FinancialPeriod = objDataTransformation.gen_convertToCDMStructure_generate(fin_L2_DIM_FinancialPeriod,\
        'dwh','vw_DIM_FinancialPeriod',isIncludeAnalysisID = False, isSqlFormat = True)[0]
    keysAndValues = {'organizationUnitSurrogateKey':0,'financialPeriodSurrogateKey':0,'postingDate':defaultDate,\
        'financialPeriod':2,'fiscalYear':1900,'fiscalQuarter':1,'reportingYearPeriod':'1900 - 2','reportingYearQuarter':'1900 - 1',\
        'periodEndDate':defaultDate,'fiscalYearIndicator':'#NA#','periodText':'#NA#','periodTextSortOrder':-2,'analysisID':analysisid}
    dwh_vw_DIM_FinancialPeriod = objGenHelper.gen_placeHolderRecords_populate('dwh.vw_DIM_FinancialPeriod',keysAndValues,dwh_vw_DIM_FinancialPeriod)  
    objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_FinancialPeriod,gl_CDMLayer2Path + "fin_L2_DIM_FinancialPeriod.parquet" )

    executionStatus = "fin_L2_DIM_FinancialPeriod populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

  finally:
    spark.sql("UNCACHE TABLE  IF EXISTS TMP_DateCollect")
    spark.sql("UNCACHE TABLE  IF EXISTS fin_L1_TMP_DateCollect")
    spark.sql("UNCACHE TABLE  IF EXISTS updt1")
    spark.sql("UNCACHE TABLE  IF EXISTS fin_L1_TMP_Date1")
    spark.sql("UNCACHE TABLE  IF EXISTS fin_L1_TMP_Date")
    
    
