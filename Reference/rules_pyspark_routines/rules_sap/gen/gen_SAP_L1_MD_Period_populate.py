# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import traceback
from pyspark.sql.functions import row_number,lit,col,when,min,max,concat_ws,datediff,year,month,to_date
import pandas as pd
from datetime import date

def gen_SAP_L1_MD_Period_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    endDate = parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date()
    finYear = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR')
    prevYear = int(finYear)-1
    
    global gen_L1_MD_Period

    df_erp_GLT0_min_year = erp_GLT0.alias("GLT0").filter((when(col("GLT0.RYEAR").isNull(),'').\
      otherwise(col("GLT0.RYEAR")))!="1900").select(min("GLT0.RYEAR").alias("ryear"))

    df_erp_FAGLFLEXT_min_year = erp_FAGLFLEXT.alias("FAGLFLEXT").filter((when(col("FAGLFLEXT.RYEAR").isNull(),'').\
      otherwise(col("FAGLFLEXT.RYEAR")))!="1900").select(min("FAGLFLEXT.RYEAR").alias("ryear"))
    
    df_tmp_MinimumYear = df_erp_GLT0_min_year.union(df_erp_FAGLFLEXT_min_year)
    
    minYear = df_tmp_MinimumYear.select(min("ryear")).collect()[0][0]
    if (minYear> finYear) :
      minYear = finYear
    
    mindate = datetime(year=(int(minYear)-3), month=int("01"),day=int("01"))
    endDate_new = endDate.replace(year=endDate.year + 7)
    
    date_sequence = pd.date_range(mindate,endDate_new)
    df_date_sequence = spark.createDataFrame(pd.DataFrame(date_sequence,columns =['calendarDate']))
    
    df_knw_LK_CD_ReportingSetup = knw_LK_CD_ReportingSetup.select("clientCode","companyCode")
    
    df_date_period_detail = df_knw_LK_CD_ReportingSetup.alias("rpt")\
      .crossJoin(df_date_sequence.alias("dts").select("dts.calendarDate").alias("calendarDate"))\
      .select(col("calendarDate").alias("calendarDate").cast("date")\
             ,col("clientCode").alias("clientCode")\
             ,col("companyCode").alias("companyCode")\
             ,lit("0").alias("isExtendedDate"))
    
    minDate = df_date_period_detail.select(min("calendarDate")).collect()[0][0]
    maxDate = df_date_period_detail.select(max("calendarDate")).collect()[0][0]
    
    df_erp_BKPF = erp_BKPF.alias("BKPF")\
      .select(col("BKPF.BLDAT").alias("calendarDate").cast("date")\
      ,col("BKPF.MANDT").alias("clientCode")\
      ,col("BKPF.BUKRS").alias("companyCode")\
      ,lit("1").alias("isExtendedDate")).distinct()
    
    df_erp_BKPF_CPUDT = erp_BKPF.alias("BKPF")\
      .select(col("BKPF.CPUDT").alias("calendarDate").cast("date")\
      ,col("BKPF.MANDT").alias("clientCode")\
      ,col("BKPF.BUKRS").alias("companyCode")\
      ,lit("1").alias("isExtendedDate")).distinct()
    
    df_erp_BSEG = erp_BSEG.alias("BSEG")\
      .select(col("BSEG.AUGDT").alias("calendarDate").cast("date")\
      ,col("BSEG.MANDT").alias("clientCode")\
      ,col("BSEG.BUKRS").alias("companyCode")\
      ,lit("1").alias("isExtendedDate")).distinct()
      
    ExtendedDate_all = df_erp_BKPF.union(df_erp_BKPF_CPUDT).union(df_erp_BSEG)
    ExtendedDate_all = ExtendedDate_all.filter(col("calendarDate")!="1900-01-01")
    
    df_date_period_detail_extended = df_date_period_detail.union(ExtendedDate_all)
    
    df_date_year_distinct = df_date_period_detail_extended.select(year(col("calendarDate")).alias("fiscalYear")).distinct()

    df_erp_T009B_BDATJ_notNull = erp_T009B.alias("T009B")\
      .select(col("T009B.MANDT").alias("MANDT")\
      ,col("T009B.RELJR").alias("RELJR")\
      ,col("T009B.PERIV").alias("PERIV")\
      ,col("T009B.BDATJ").alias("BDATJ").cast("int")\
      ,col("T009B.BUMON").alias("BUMON")\
      ,col("T009B.BUTAG").alias("BUTAG")\
      ,col("T009B.POPER").alias("POPER"))\
      .filter(col("T009B.BDATJ").cast("int").isNotNull())

    df_erp_T009B_BDATJ_Null = erp_T009B.alias("T009B").join(df_date_year_distinct.alias("yr"),how="cross")\
      .select(col("T009B.MANDT").alias("MANDT")\
      ,col("T009B.RELJR").alias("RELJR")\
      ,col("T009B.PERIV").alias("PERIV")\
      ,col("yr.fiscalYear").alias("BDATJ").cast("int")\
      ,col("T009B.BUMON").alias("BUMON")\
      ,col("T009B.BUTAG").alias("BUTAG")\
      ,col("T009B.POPER").alias("POPER"))\
      .filter(col("T009B.BDATJ").cast("int").isNull()).distinct()

    df_erp_T009B_BDATJ_all = df_erp_T009B_BDATJ_notNull.union(df_erp_T009B_BDATJ_Null)


    df_erp_T009B = df_erp_T009B_BDATJ_all.alias("T009B")\
      .select(col("T009B.MANDT").alias("MANDT")\
      ,col("T009B.RELJR").alias("RELJR")\
      ,col("T009B.PERIV").alias("fiscalYearVariant")\
      ,col("T009B.BDATJ").alias("BDATJ").cast("int")\
      ,col("T009B.BUMON").alias("BUMON")\
      ,col("T009B.BUTAG").alias("BUTAG")\
      ,col("T009B.POPER").alias("POPER")\
      ,when((col("T009B.BDATJ") % 400 == 0) |((col("T009B.BDATJ") % 4 == 0) & (col("T009B.BDATJ")%100 !=0) ),1)\
        .otherwise(0).alias("is_BDATJ_leap"))
    
    df_erp_T009B_new = df_erp_T009B.alias("T009B")\
      .select(col("T009B.MANDT").alias("MANDT")\
      ,col("T009B.RELJR").alias("RELJR").cast("int")\
      ,col("T009B.fiscalYearVariant").alias("fiscalYearVariant")\
      ,col("T009B.BDATJ").alias("BDATJ").cast("int")\
      ,col("T009B.BUMON").alias("BUMON")\
      ,col("T009B.BUTAG").alias("BUTAG")\
      ,col("T009B.POPER").alias("POPER")\
      ,col("T009B.is_BDATJ_leap").alias("is_BDATJ_leap")\
      ,when(((col("T009B.BUMON")=="02") & (col("T009B.BUTAG") == "29") & (col("T009B.is_BDATJ_leap") =="0")),lit("28"))\
        .otherwise(col("T009B.BUTAG")).alias("BUTAG_new"))
    
    df_period_T001_T009 = df_date_period_detail_extended.alias("dp")\
      .join(erp_T001.alias("T001"),(col("dp.companyCode")==col("T001.BUKRS"))& (col("dp.clientCode")==col("T001.MANDT")),"inner")\
      .join(erp_T009.alias("T009"),(col("T009.PERIV")==col("T001.PERIV"))&(col("dp.clientCode")==col("T001.MANDT")),"inner")\
      .select(col("dp.companyCode").alias("companyCode")\
      ,col("dp.clientCode").alias("clientCode")\
      ,to_date(col("dp.calendarDate")).alias("calendarDate")\
      ,col("T001.PERIV").alias("fiscalYearVariant")\
      ,col("T009.XKALE").alias("indicatorCalendarYear")\
      ,col("T009.XJABH").alias("fiscalYearVariantIndicator")\
      ,col("dp.isExtendedDate").alias("isExtendedDate")).distinct()
    
    RowNoScript  = Window.partitionBy(col("dp.companyCode"),col("dp.clientCode"),col("dp.calendarDate")).orderBy(col("T009B.BDATJ")
    ,col("T009B.BUMON"),col("T009B.BUTAG"))
   
    #Finanacial Year ---------------------------------------
    
    fiscalYearVariantIndicator_x = df_period_T001_T009.alias("dp")\
      .join(df_erp_T009B_new.alias("T009B"),(col("dp.clientCode") == col("T009B.MANDT"))\
      & (col("dp.fiscalYearVariant") == col("T009B.fiscalYearVariant"))\
      & (col("T009B.BDATJ").cast("string") != "")\
      & (datediff(col("dp.calendarDate")\
      ,(concat_ws("-",col("T009B.BDATJ"),col("T009B.BUMON"),col("T009B.BUTAG_new"))))<=0),"left")\
      .select(col("dp.companyCode").alias("companyCode")\
      ,col("dp.clientCode").alias("clientCode")\
      ,col("dp.calendarDate").alias("calendarDate")\
      ,col("T009B.BDATJ").alias("BDATJ")\
      ,col("T009B.BUMON").alias("BUMON")\
      ,col("T009B.BUTAG").alias("BUTAG")\
      ,col("T009B.RELJR").alias("RELJR")\
      ,row_number().over(RowNoScript).alias("row_no"))
       
    fiscalYearVariantIndicator_x_f = fiscalYearVariantIndicator_x.filter(col("row_no")==1)  
    
    fiscalYearVariantIndicator_not_x = df_period_T001_T009.alias("dp")\
      .join(df_erp_T009B_new.alias("T009B"),(col("dp.clientCode") == col("T009B.MANDT"))\
      & (col("dp.fiscalYearVariant") == col("T009B.fiscalYearVariant"))\
      & (datediff(col("dp.calendarDate")\
      ,(concat_ws("-",col("T009B.BDATJ"),col("T009B.BUMON"),col("T009B.BUTAG_new"))))<=0),"left")\
      .select(col("dp.companyCode").alias("companyCode")\
      ,col("dp.clientCode").alias("clientCode")\
      ,col("dp.calendarDate").alias("calendarDate")\
      ,col("T009B.BDATJ").alias("BDATJ")\
      ,col("T009B.BUMON").alias("BUMON")\
      ,col("T009B.BUTAG").alias("BUTAG")\
      ,col("T009B.RELJR").alias("RELJR")\
      ,row_number().over(RowNoScript).alias("row_no"))

    fiscalYearVariantIndicator_not_x_f = fiscalYearVariantIndicator_not_x.filter(col("row_no")==1)

    
    #Finanacial Period ---------------------------------------
    
    FinancialPeriodVariantIndicator_x = df_period_T001_T009.alias("dp")\
      .join(df_erp_T009B_new.alias("T009B"),(col("dp.clientCode") == col("T009B.MANDT"))\
      & (col("dp.fiscalYearVariant") == col("T009B.fiscalYearVariant"))\
      & (col("T009B.BDATJ").between(1970,2030))\
      & (datediff(col("dp.calendarDate")\
      ,(concat_ws("-",col("T009B.BDATJ"),col("T009B.BUMON"),col("T009B.BUTAG"))))<=0),"left")\
      .select(col("dp.companyCode").alias("companyCode")\
      ,col("dp.clientCode").alias("clientCode")\
      ,col("dp.calendarDate").alias("calendarDate")\
      ,col("T009B.BDATJ").alias("BDATJ")\
      ,col("T009B.BUMON").alias("BUMON")\
      ,col("T009B.BUTAG").alias("BUTAG")\
      ,col("T009B.RELJR").alias("RELJR")\
      ,col("T009B.POPER").alias("POPER")\
      ,row_number().over(RowNoScript).alias("row_no"))
    
    FinancialPeriodVariantIndicator_x_f = FinancialPeriodVariantIndicator_x.filter(col("row_no")==1)  
    
    FinancialPeriodVariantIndicator_not_x = df_period_T001_T009.alias("dp")\
      .join(df_erp_T009B_new.alias("T009B"),(col("dp.clientCode") == col("T009B.MANDT"))\
      & (col("dp.fiscalYearVariant") == col("T009B.fiscalYearVariant"))\
      & (datediff(col("dp.calendarDate")\
      ,(concat_ws("-",col("T009B.BDATJ"),col("T009B.BUMON"),col("T009B.BUTAG_new"))))<=0),"left")\
      .select(col("dp.companyCode").alias("companyCode")\
      ,col("dp.clientCode").alias("clientCode")\
      ,col("dp.calendarDate").alias("calendarDate")\
      ,col("T009B.BDATJ").alias("BDATJ")\
      ,col("T009B.BUMON").alias("BUMON")\
      ,col("T009B.BUTAG").alias("BUTAG")\
      ,col("T009B.RELJR").alias("RELJR")\
      ,col("T009B.POPER").alias("POPER")\
      ,row_number().over(RowNoScript).alias("row_no"))
    
    FinancialPeriodVariantIndicator_not_x_f = FinancialPeriodVariantIndicator_not_x.filter(col("row_no")==1)
    
    df_T001_T009B = erp_T009B.alias("T009B").join(erp_T001.alias("T001"),col("T009B.PERIV")==col("T001.PERIV"),how="inner")\
      .filter(col("T009B.BDATJ").cast("int").isNotNull())\
      .agg(max("T009B.BDATJ").alias("max_BDATJ"),min("T009B.BDATJ").alias("min_BDATJ"))\
      .select("max_BDATJ","min_BDATJ",lit(1).alias("valueExist_BDATJ"))

    df_DatePeriodDetail = df_period_T001_T009.alias("dp")\
      .join(fiscalYearVariantIndicator_x_f.alias("fyvi_x"),(col("dp.companyCode")==col("fyvi_x.companyCode"))\
      &(col("dp.clientCode")==col("fyvi_x.clientCode"))\
      &(col("dp.calendarDate")==col("fyvi_x.calendarDate")),"left")\
      .join(fiscalYearVariantIndicator_not_x_f.alias("fyvi_not_x"),(col("dp.companyCode")==col("fyvi_not_x.companyCode"))\
      &(col("dp.clientCode")==col("fyvi_not_x.clientCode"))\
      &(col("dp.calendarDate")==col("fyvi_not_x.calendarDate")),"left")\
      .join(FinancialPeriodVariantIndicator_x_f.alias("fpvi_x"),(col("dp.companyCode")==col("fpvi_x.companyCode"))\
      &(col("dp.clientCode")==col("fpvi_x.clientCode"))\
      &(col("dp.calendarDate")==col("fpvi_x.calendarDate")),"left")\
      .join(FinancialPeriodVariantIndicator_not_x_f.alias("fpvi_not_x"),(col("dp.companyCode")==col("fpvi_not_x.companyCode"))\
      &(col("dp.clientCode")==col("fpvi_not_x.clientCode"))\
      &(col("dp.calendarDate")==col("fpvi_not_x.calendarDate")),"left")\
      .select(col("dp.calendarDate").alias("calendarDate")\
      ,col("dp.clientCode").alias("MANDT")\
      ,col("dp.companyCode").alias("BUKRS")\
      ,when(col("dp.indicatorCalendarYear")=="X",year(col("dp.calendarDate")))\
      .when(col("dp.fiscalYearVariantIndicator")=="X",year(col("dp.calendarDate"))+ col("fyvi_x.RELJR"))\
      .otherwise(year(col("dp.calendarDate"))+ col("fyvi_not_x.RELJR")).alias("finYear")\
      ,when(col("dp.indicatorCalendarYear")=="X",month(col("dp.calendarDate")))\
      .when(col("dp.fiscalYearVariantIndicator")=="X",(col("fpvi_x.POPER")))\
      .otherwise((col("fpvi_not_x.POPER"))).alias("finPeriod").cast("int")\
      ,col("dp.isExtendedDate").alias("isExtendedDate"))
    
    df_DatePeriodDetail = df_DatePeriodDetail.alias("res")\
      .join(df_T001_T009B.alias("fy"),(col("res.finYear")>col("fy.max_BDATJ"))|\
      (col("res.finYear")<col("fy.min_BDATJ")),how="left")\
      .select("res.calendarDate"\
      ,"res.MANDT"\
      ,"res.BUKRS"\
      ,"res.finYear"\
      ,when(col("fy.valueExist_BDATJ")==1,month(col("res.calendarDate")))\
      .otherwise(col("res.finPeriod")).alias("finPeriod")\
      ,"res.isExtendedDate")

    df_DatePeriodDetail_final_isExtended_0 = df_DatePeriodDetail.alias("res")\
      .filter((col("res.finPeriod").isNotNull()) & (col("res.isExtendedDate")==0 ))\
      .select(col("res.BUKRS").alias("companyCode")\
      ,col("res.calendarDate").alias("calendarDate")\
      ,(when(col("res.finYear").isNull(),year(col("res.calendarDate")))\
      .otherwise(col("res.finYear"))).cast("int").alias("finYear")\
      ,when(col("res.finPeriod").isNull(),month(col("res.calendarDate")))\
      .otherwise(col("res.finPeriod")).alias("finPeriod"))\
      .distinct()
    
    df_DatePeriodDetail_final_isExtended_1 = df_DatePeriodDetail.alias("res")\
      .filter((col("res.isExtendedDate")==1))\
      .join(df_DatePeriodDetail_final_isExtended_0.alias("ext0"),(col("res.BUKRS")==col("ext0.companyCode"))\
      & (col("res.calendarDate")== col("ext0.calendarDate")),'leftanti')\
      .select(col("res.BUKRS").alias("companyCode")\
      ,col("res.calendarDate").alias("calendarDate")\
      ,(when(col("res.finYear").isNull(),year(col("res.calendarDate")))\
      .otherwise(col("res.finYear"))).cast("int").alias("finYear")\
      ,when(col("res.finPeriod").isNull(),month(col("res.calendarDate")))\
      .otherwise(col("res.finPeriod")).alias("finPeriod"))\
      .distinct()
    
    df_DatePeriodDetail_final = df_DatePeriodDetail_final_isExtended_0.union(df_DatePeriodDetail_final_isExtended_1)
    
    gen_L1_MD_Period = df_DatePeriodDetail_final.alias("res")\
      .select(col("res.companyCode").alias("companyCode")\
      ,col("res.calendarDate").alias("calendarDate")\
      ,col("res.finYear").alias("fiscalYear")\
      ,col("res.finPeriod").alias("financialPeriod"))
    
    gen_L1_MD_Period = objDataTransformation.gen_convertToCDMandCache \
      (gen_L1_MD_Period,'gen','L1_MD_Period',targetPath=gl_CDMLayer1Path)
    
    executionStatus = "L1_MD_Period populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
