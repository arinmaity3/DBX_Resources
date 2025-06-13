# Databricks notebook source
import sys
import traceback
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number,expr,trim,col,lit,when,sum,avg,max,concat,coalesce,abs,trim,floor
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType    
from datetime import datetime
from pyspark.sql.functions import *
###1
##Common Functions
def _currencyConversion_validations(dateOrder="",overrideWithDate=None,sourcetableName=""\
                                    ,df_GenericCurrencyCode=None,mode=1):
  defaultDate='1900-01-01'
  objGenHelper = gen_genericHelper()
  executionStatus="Currency Conversion Validation is passed"
  executionStatusID= LOG_EXECUTION_STATUS.SUCCESS
  df_L1_TD_CurrencyExchangeRate=None  
  
  startDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date()
  endDate =  parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date()
  
  if mode==1:
    if ((overrideWithDate==None) & (len(dateOrder)==0)):
      executionStatus = 'Metadata (the dates which needs to be considered for currency exchange rate) is missing for \
          the input source table '+ sourcetableName +'. Please update the \
          metadata to [knw].[LK_GD_GenericCurrencyConversionDateOrder]'
      executionStatusID= LOG_EXECUTION_STATUS.FAILED
    #return[executionStatusID,executionStatus,df_L1_TD_CurrencyExchangeRate]    
  else:
    df_L1_TD_CurrencyExchangeRate_tmp=df_GenericCurrencyCode.alias("tccv")\
      .join (gen_L1_TD_CurrencyExchangeRate.alias('crsource')\
             ,((coalesce(col('tccv.sourcedate'),lit(defaultDate)).eqNullSafe(col("crsource.exchangeDate")))\
               & (col("tccv.sourceCurrency").eqNullSafe(col("crsource.currencyCode")))\
              ),"left")\
     .select(coalesce(col('tccv.sourcedate'),lit(defaultDate)).alias('exchangeDate')\
        ,col('tccv.sourceCurrency').alias('currencyCode')\
        ,when(col('crsource.exchangeDate').isNotNull(),col('crsource.exchangeRate'))\
             .otherwise(lit(1)).alias('exchangeRate')\
        ,lit(None).alias('phaseID')\
        ,col("crsource.exchangeDate").alias("crsource_exchangeDate")
           )   
    #1) Identifying Missing Currency Rates'
    df_missingCurrency=df_L1_TD_CurrencyExchangeRate_tmp.alias('ExchangeRat')\
      .select(col('exchangeDate'),col('currencyCode'))\
      .filter((~col('exchangeDate').isin('01-01-1900','01-02-1900'))\
             & (col('crsource_exchangeDate').isNull()))    
    if df_missingCurrency.first()!= None:
      ls_missingCurrency=()
      ls_missingCurrency=[row for row in df_missingCurrency.select('exchangeDate','currencyCode').collect()]
      strmsg=""
      for i in ls_dateOrder:
        s=i[0]+":"+i[1]
        strmsg=strmsg+s
      if len(strmsg)>1:
        executionStatusID= LOG_EXECUTION_STATUS.FAILED
        executionStatus= 'Found Missing Currencies : ' + strmsg       
    else:
      #2) Identifying Missing Currency Rates Between the Analysis Period'
      #Identifying Missing Currency Rates Between the Analysis Period
      df_rec_count=df_L1_TD_CurrencyExchangeRate_tmp.alias("tccv")\
         .filter(col('tccv.exchangeDate').between(lit(startDate),lit(endDate)))
      if df_rec_count.first() is None:
        executionStatusID= LOG_EXECUTION_STATUS.FAILED
        executionStatus = 'Currency Rate Not Found Between the Analysis Period'
      else:
        #3) Duplicate Currency Rate Found in the Currency Table gen.L1_TD_CurrencyExchangeRate'
        df_dup_currencyCode=df_L1_TD_CurrencyExchangeRate_tmp.groupBy("exchangeDate","currencyCode").count()\
              .filter(col('count')>1)
        if df_dup_currencyCode.first()!= None:
          executionStatusID= LOG_EXECUTION_STATUS.FAILED
          executionStatus ='Duplicate Currency Rate Found in the Currency \
                Table gen.L1_TD_CurrencyExchangeRate , Please check'    
    
    
    if  executionStatusID==LOG_EXECUTION_STATUS.SUCCESS :
      df_L1_TD_CurrencyExchangeRate=df_L1_TD_CurrencyExchangeRate_tmp.alias("cr")\
        .select(col('cr.exchangeDate'),col('cr.currencyCode'),col('cr.exchangeRate'),col('cr.phaseID'))\
        .filter(col('crsource_exchangeDate').isNotNull())
      
      #df_L1_TD_CurrencyExchangeRate.display() 
      
  return[executionStatusID,executionStatus,df_L1_TD_CurrencyExchangeRate]
  
def currencyConversionSourceDate_get(sourceschemaName,sourcetableName,overrideWithDate=None):
  #STEP 2
  #Declaration and Initialization of variables    
  executionStatus=""
  dateOrder	=''
  defaultDate='1900-01-01'
  exeQuery=''    
  debug=0
  objGenHelper = gen_genericHelper()
  
  if overrideWithDate==None:
    lstValidationStatus=list()
    df_dateOrder = knw_LK_GD_CurrencyConversionMetadata\
        .select(col('fieldName')\
                ,when(col('fieldName').isNull(),defaultDate).otherwise(col('fieldName')).alias('_Field'))\
        .filter((col("scenarioID") == 1) \
                  & (col("schemaName") == sourceschemaName) & (col("tableName") == sourcetableName)\
                  & (col("scenarioName") == 'ExchangeDateOrder') &  (col("fieldName") != '') \
                )
    #df_dateOrder.display()
    ls_dateOrder=()
    ls_dateOrder=[row[0]  for row in df_dateOrder.select('_Field').collect()]
    #print(ls_dateOrder)
    dateOrder="coalesce("
    for colDate in ls_dateOrder:
        dateOrder=dateOrder +" case when {colName}=='{defaultDate}' then NULL else {colName} end,"\
        .format(colName    = colDate,
               defaultDate=defaultDate)
    sourceDate=dateOrder+"NULL)"
  else:
    sourceDate= ("{}".format(overrideWithDate)) 
      
  lstValidationStatus =_currencyConversion_validations(dateOrder=dateOrder,overrideWithDate=overrideWithDate\
                                           ,sourcetableName=sourcetableName,mode=1)
  
  executionStatusID=lstValidationStatus[0]
  executionStatus=lstValidationStatus[1]
   
  return[executionStatusID,executionStatus,sourceDate]

def currencyConversionSecondThirdCurrencyValue_get(sourceschemaName,sourcetableName,targetAmountField):
  #STEP 2
  #Declaration and Initialization of variables   
  defaultDate='1900-01-01'
  ls_secondCurrencyValueField =()      
  df_secondCurrencyValueField= knw_LK_GD_CurrencyConversionMetadata\
      .select(col('fieldName')\
              ,when(col('fieldName').isNull(),defaultDate).otherwise(col('fieldName')).alias('_Field')\
             )\
      .filter((col("scenarioID") == 3) \
              & (col("schemaName") == sourceschemaName) & (col("tableName") == sourcetableName)\
              & (col("scenarioName") == 'secondCurrencyValue') &  (col("fieldName") != '')\
              & (col("reportingValueField") == targetAmountField)\
             )
  ls_secondCurrencyValueField=[row[0] for row in df_secondCurrencyValueField.select('_Field').collect()]  
  if len(ls_secondCurrencyValueField)==0:
    secondCurrencyValueField=None
  ls_thirdCurrencyValueField =()
  df_thirdCurrencyValueField= knw_LK_GD_CurrencyConversionMetadata\
      .select(col('fieldName')\
              ,when(col('fieldName').isNull(),defaultDate).otherwise(col('fieldName')).alias('_Field'))\
      .filter((col("scenarioID") == 4) \
              & (col("schemaName") == sourceschemaName) & (col("tableName") == sourcetableName)\
              & (col("scenarioName") == 'thirdCurrencyValue') &  (col("fieldName") != '')\
              & (col("reportingValueField") == targetAmountField))                  

  ls_thirdCurrencyValueField=[row[0] for row in df_thirdCurrencyValueField.select('_Field').collect()]  
  if len(ls_thirdCurrencyValueField)==0:
    thirdCurrencyValueField=None
  return[secondCurrencyValueField,thirdCurrencyValueField]

def gen_currencyConversion_perform(df_tmp_GenericCurrencyConversion,sourcetable ="" \
                                  ,isToBeConvertedToRC=0,overrideWithDate=None):
  try:
    #Declaration and Initialization of variables 
    executionStatus=""
    objGenHelper = gen_genericHelper()
    defaultDate='1900-01-01'

    #Collecting All Currency Informations and populating into a dataframe #tmp_GenericCurrencyCode'    
    df_GenericCurrencyCode1=df_tmp_GenericCurrencyConversion.alias("tccv")\
          .select(col('tccv.sourcedate'),col('tccv.sourceCurrency'))\
          .filter(coalesce(col('sourceCurrency'),lit('')) != lit(''))\
          .distinct()


    #Collecting All Currency Informations and populating into a dataframe #tmp_GenericCurrencyCode'    
    v_targetCurrency=when(isToBeConvertedToRC==lit(0),col('localCurrencyCode'))\
                  .otherwise(col('reportingCurrencyCode'))
    df_currency1 =knw_LK_CD_ReportingSetup.select(lit(v_targetCurrency).alias('targetCurrency'))\
            .filter(coalesce(lit(v_targetCurrency),lit('')) != lit(''))
    df_currency2 =knw_LK_CD_ReportingSetup.select(col('secondCurrencyCode').alias('targetCurrency'))\
            .filter(coalesce(col('secondCurrencyCode'),lit('')) != lit(''))
    df_currency3 =knw_LK_CD_ReportingSetup.select(col('thirdCurrencyCode').alias('targetCurrency'))\
            .filter(coalesce(col('thirdCurrencyCode'),lit('')) != lit(''))

    if df_currency2.rdd.isEmpty()== True:
       if df_currency3.rdd.isEmpty()== True:
         df_currencyCode=df_currency1
       else:
         df_currencyCode=df_currency1.union(df_currency3).distinct()
    else:
       if df_currency3.rdd.isEmpty()== True:
         df_currencyCode=df_currency1.union(df_currency2).distinct()
       else:
         df_currencyCode=df_currency1.union(df_currency2).union(df_currency3).distinct()

    df_tmp_sourcedate=df_tmp_GenericCurrencyConversion.alias("t1")\
        .select(col('sourcedate')).distinct()
   
    df_GenericCurrencyCode2=df_tmp_sourcedate.alias("t1")\
        .join(df_currencyCode.alias("t2"),how="cross")\
        .select(col('t1.sourcedate'),col('t2.targetCurrency'))

    df_tmp_GenericCurrencyCode=df_GenericCurrencyCode1.union(df_GenericCurrencyCode2).distinct()
   
    ##CurrencyValidation
    lstCurrencyValidationStatus=list()
    lstCurrencyValidationStatus =_currencyConversion_validations\
        (sourcetableName=sourcetable,df_GenericCurrencyCode=df_tmp_GenericCurrencyCode,mode=2)

    if(lstCurrencyValidationStatus[0] == LOG_EXECUTION_STATUS.FAILED): 
      #raise ValueError('Invalid path or file extension: '+ filenamepath )
      raise Exception(lstCurrencyValidationStatus[1])

    df_CurrencyExchangeRate_tmp=lstCurrencyValidationStatus[2]
    targetPath = gl_layer0Temp_temp + "GCC_ExchangeRate_" + sourcetable +".delta"
    objGenHelper.gen_writeToFile_perfom(df_CurrencyExchangeRate_tmp,targetPath,mode='overwrite')

    df_L1_TD_CurrencyExchangeRate=objGenHelper.gen_readFromFile_perform(targetPath)

    #Populating the table #tmp_CurrencyExchangeRate For narrowing the table [gen].[L1_TD_CurrencyExchangeRate]
    df_tmp_CurrencyExchangeRate=  df_L1_TD_CurrencyExchangeRate.alias("crsource")\
        .select(col('crsource.exchangeDate')\
                ,col('crsource.currencyCode')\
                ,when(col('currencyCode')=='DEM',lit(1.46933))\
                  .otherwise(col('crsource.exchangeRate')).alias('exchangeRate')\
              ).distinct()

    #df_tmp_CurrencyExchangeRate.display()

    #Rule-0-Handling whether the document currency is null or Blank  
    v_rule0_str=coalesce(col('sourceCurrency'),lit(''))

    df_tmp_GenericCurrencyConversion_rule0=df_tmp_GenericCurrencyConversion.alias("gcc")\
           .select(\
                   col('gcc.sourceCompanyCode')\
                   ,col('gcc.sourceDate')\
                   ,col('gcc.sourceAmount')\
                   ,col('gcc.secondCurrencyValue')\
                   ,col('gcc.thirdCurrencyValue')\
                   ,col('gcc.sourceCurrency')\
                   ,col('sourceColName')
                   ,(when(lit(v_rule0_str)==lit(''),lit(0)).otherwise(lit(None))).alias('targetAmount')\
                   ,(when(lit(v_rule0_str)==lit(''),col('gcc.sourceCurrency'))\
                       .otherwise(lit(None))).alias('targetCurrency')\
                   ,(when(lit(v_rule0_str)==lit(''),lit(1)).otherwise(lit(0))).alias('isCurrencyUpdated')\
                   ,(when(lit(v_rule0_str)==lit(''),lit(0)).otherwise(lit(None))).alias('appliedRuleId')\
                  )
    #Rule 0 Dataframe
    #df_tmp_GenericCurrencyConversion_rule0.display()

    #Rule-123-Handling whether the document currency is null or Blank
    #'Applying the 1st Rule Source Currency Equals Target Currency ReportingCurrency1'
    #'Applying the 2nd Rule Source Currency Equals Target Currency [secondCurrencyCode]'
    #'Applying the 3rd Rule Source Currency Equals Target Currency [thirdCurrencyCode]'

    col_RowID= F.row_number().over(Window.orderBy(lit('')))

    df_tmp_GenericCurrencyConversion_ref=df_tmp_GenericCurrencyConversion_rule0.alias("tccv")\
        .join(knw_LK_CD_ReportingSetup.alias("lkrs")\
            ,(col("lkrs.companyCode") == col("tccv.sourcecompanyCode")),"inner")\
        .select(col('tccv.sourceCompanyCode')\
            ,col('tccv.sourceDate')\
            ,col('tccv.sourceAmount')\
            ,coalesce(col('tccv.secondCurrencyValue'),lit(0)).alias('secondCurrencyValue')\
            ,coalesce(col('tccv.thirdCurrencyValue'),lit(0)).alias('thirdCurrencyValue')\
            ,col('tccv.sourceCurrency')\
            ,col('tccv.targetAmount')\
            ,col('tccv.targetCurrency')\
            ,when(lit(isToBeConvertedToRC)==lit(0),col('lkrs.localCurrencyCode'))\
                   .otherwise(col('lkrs.reportingCurrencyCode')).alias('localORreportingCurrency')\
            ,coalesce(col('lkrs.secondCurrencyCode'),lit('')).alias('lkrs_secondCurrencyCode')\
            ,coalesce(col('lkrs.thirdCurrencyCode'),lit('')).alias('lkrs_thirdCurrencyCode')\
            ,col('tccv.isCurrencyUpdated')\
            ,col('tccv.appliedRuleId')\
              ).distinct()\
          .withColumn("rowID", lit(col_RowID))

    #df_tmp_GenericCurrencyConversion_ref.display()
    targetPath = gl_layer0Temp_temp + "GCC_ref_" + sourcetable +".delta"
    objGenHelper.gen_writeToFile_perfom(df_tmp_GenericCurrencyConversion_ref,targetPath,mode='overwrite',isMergeSchema = True) 
    df_tmp_GenericCurrencyConversion_ref=objGenHelper.gen_readFromFile_perform(targetPath) 
   
    #STEP 6
    df_tmp_GenericCurrencyConversion_rule123=df_tmp_GenericCurrencyConversion_ref.alias("tccv")\
        .select(col('tccv.rowID'),col('tccv.sourceCompanyCode')\
           ,col('tccv.sourceDate')\
           ,col('tccv.sourceAmount')\
           ,col('tccv.secondCurrencyValue')\
           ,col('tccv.thirdCurrencyValue')\
           ,col('tccv.sourceCurrency')\
           ,when(col('tccv.localORreportingCurrency')==col('tccv.sourceCurrency'),col('tccv.sourceAmount'))\
            .when((col('tccv.localORreportingCurrency')==col('tccv.lkrs_secondCurrencyCode'))\
                & (coalesce(col('tccv.sourceAmount'),lit(0))!=0)\
                & (coalesce(col('tccv.secondCurrencyValue'),lit(0))!=0)\
                ,col('tccv.secondCurrencyValue'))\
            .when((col('tccv.localORreportingCurrency')==col('tccv.lkrs_thirdCurrencyCode'))\
                & (coalesce(col('tccv.sourceAmount'),lit(0))!=0)\
                & (coalesce(col('tccv.thirdCurrencyValue'),lit(0))!=0)\
                ,col('tccv.thirdCurrencyValue'))\
            .otherwise(col('tccv.targetAmount')).alias('targetAmount')\
           ,when(col('tccv.localORreportingCurrency')==col('tccv.sourceCurrency'),col('tccv.sourceCurrency'))\
            .when((col('tccv.localORreportingCurrency')==col('tccv.lkrs_secondCurrencyCode'))\
                & (coalesce(col('tccv.sourceAmount'),lit(0))!=0)\
                & (coalesce(col('tccv.secondCurrencyValue'),lit(0))!=0)\
                ,col('tccv.lkrs_secondCurrencyCode'))\
            .when((col('tccv.localORreportingCurrency')==col('tccv.lkrs_thirdCurrencyCode'))\
                & (coalesce(col('tccv.sourceAmount'),lit(0))!=0)\
                & (coalesce(col('tccv.thirdCurrencyValue'),lit(0))!=0)\
                ,col('tccv.lkrs_thirdCurrencyCode'))\
            .otherwise(col('tccv.targetCurrency')).alias('targetCurrency')\
           ,when(col('tccv.localORreportingCurrency')==col('tccv.sourceCurrency'),lit(1))\
            .when((col('tccv.localORreportingCurrency')==col('tccv.lkrs_secondCurrencyCode'))\
                & (coalesce(col('tccv.sourceAmount'),lit(0))!=0)\
                & (coalesce(col('tccv.secondCurrencyValue'),lit(0))!=0)\
                ,lit(1))\
            .when((col('tccv.localORreportingCurrency')==col('tccv.lkrs_thirdCurrencyCode'))\
                & (coalesce(col('tccv.sourceAmount'),lit(0))!=0)\
                & (coalesce(col('tccv.thirdCurrencyValue'),lit(0))!=0)\
                ,lit(1))\
            .otherwise(col('tccv.isCurrencyUpdated')).alias('isCurrencyUpdated')\
           ,when(col('tccv.localORreportingCurrency')==col('tccv.sourceCurrency'),lit(1))\
            .when((col('tccv.localORreportingCurrency')==col('tccv.lkrs_secondCurrencyCode'))\
                & (coalesce(col('tccv.sourceAmount'),lit(0))!=0)\
                & (coalesce(col('tccv.secondCurrencyValue'),lit(0))!=0)\
                ,lit(2))\
            .when((col('tccv.localORreportingCurrency')==col('tccv.lkrs_thirdCurrencyCode'))\
                & (coalesce(col('tccv.sourceAmount'),lit(0))!=0)\
                & (coalesce(col('tccv.thirdCurrencyValue'),lit(0))!=0)\
                ,lit(3))\
            .otherwise(col('tccv.appliedRuleId')).alias('appliedRuleId')\
         ,col('tccv.localORreportingCurrency')\
           )
    #Rule-4-Applying the 4th Rule, Source Currency is USD Target Currency is Not USD
    df_tmp_GenericCurrencyConversion_rule123For4=df_tmp_GenericCurrencyConversion_rule123\
        .filter((col('isCurrencyUpdated')==lit(0))\
              & (col('tccv.sourceCurrency')==lit('USD'))\
              & (col('localORreportingCurrency')!=lit('USD'))\
             )
    df_tmp_GenericCurrencyConversion_rule4=df_tmp_GenericCurrencyConversion_rule123For4.alias("tccv")\
       .join (df_tmp_CurrencyExchangeRate.alias('crsource')\
                 ,((col("crsource.exchangeDate").eqNullSafe(col("tccv.sourcedate")))\
                   & (col("crsource.currencyCode").eqNullSafe(col("tccv.localORreportingCurrency")))\
                  ),"left")\
        .select(col('tccv.rowID'),col('tccv.sourceCompanyCode')\
             ,col('tccv.sourceDate')\
             ,col('tccv.sourceAmount')\
             ,col('tccv.sourceCurrency')\
             ,col('tccv.localORreportingCurrency')\
             ,(col('tccv.sourceAmount')*col('crsource.exchangeRate')).alias('targetAmount')\
             ,col('tccv.localORreportingCurrency').alias('targetCurrency')\
             ,col('tccv.localORreportingCurrency')\
             ,lit(1).alias('isCurrencyUpdated')\
             ,lit(4).alias('appliedRuleId')\
              )

    #Rule-5-Applying the 5th Rule, Source Currency is Not USD Target Currency is USD
    df_tmp_GenericCurrencyConversion_rule123For5=df_tmp_GenericCurrencyConversion_rule123\
        .filter((col('isCurrencyUpdated')==lit(0))\
              & (col('tccv.sourceCurrency')!=lit('USD'))\
              & (col('localORreportingCurrency')==lit('USD'))\
             )   
    df_tmp_GenericCurrencyConversion_rule5=df_tmp_GenericCurrencyConversion_rule123For5.alias("tccv")\
        .join (df_tmp_CurrencyExchangeRate.alias('crsource')\
                 ,((col("crsource.exchangeDate").eqNullSafe(col("tccv.sourcedate")))\
                   & (col("crsource.currencyCode").eqNullSafe(col("tccv.sourceCurrency")))\
                  ),"left")\
        .select(col('tccv.rowID'),col('tccv.sourceCompanyCode')\
             ,col('tccv.sourceDate')\
             ,col('tccv.sourceAmount')\
             ,col('tccv.sourceCurrency')\
             ,col('tccv.localORreportingCurrency')\
             ,when((lit(coalesce(col('crsource.exchangeRate'),lit(0.00)))==lit(0.00)),lit(None))\
               .otherwise((col('tccv.sourceAmount')/col('crsource.exchangeRate'))).alias('targetAmount')\
             ,col('tccv.localORreportingCurrency').alias('targetCurrency')\
             ,col('tccv.localORreportingCurrency')\
             ,lit(1).alias('isCurrencyUpdated')\
             ,lit(5).alias('appliedRuleId')\
              )

    #Rule-5-Applying the 6th Rule, Source Currency is Not USD Target Currency is Not USD
    df_tmp_GenericCurrencyConversion_rule123For6=df_tmp_GenericCurrencyConversion_rule123\
        .filter((col('isCurrencyUpdated')==lit(0))\
              & (col('tccv.sourceCurrency')!=lit('USD'))\
              & (col('localORreportingCurrency')!=lit('USD'))\
             )
    df_tmp_GenericCurrencyConversion_rule6=df_tmp_GenericCurrencyConversion_rule123For6.alias("tccv")\
        .join (df_tmp_CurrencyExchangeRate.alias('crsource')\
                 ,((col("crsource.exchangeDate").eqNullSafe(col("tccv.sourcedate")))\
                   & (col("crsource.currencyCode").eqNullSafe(col("tccv.sourceCurrency")))\
                  ),"left")\
        .join (df_tmp_CurrencyExchangeRate.alias('crtarget')\
                 ,((col("crtarget.exchangeDate").eqNullSafe(col("tccv.sourcedate")))\
                   & (col("crtarget.currencyCode").eqNullSafe(col("tccv.localORreportingCurrency")))\
                  ),"left")\
        .select(col('tccv.rowID'),col('tccv.sourceCompanyCode')\
             ,col('tccv.sourceDate')\
             ,col('tccv.sourceAmount')\
             ,col('tccv.sourceCurrency')\
             ,col('tccv.localORreportingCurrency')\
             ,when((lit(coalesce(col('crsource.exchangeRate'),lit(0.00)))==lit(0.00)),lit(None))\
               .otherwise((col('tccv.sourceAmount')/col('crsource.exchangeRate'))\
                          * col('crtarget.exchangeRate')).alias('targetAmount')\
             ,col('tccv.localORreportingCurrency').alias('targetCurrency')\
             ,col('tccv.localORreportingCurrency')\
             ,lit(1).alias('isCurrencyUpdated')\
             ,lit(6).alias('appliedRuleId')\
              )

    df_tmp_GenericCurrencyConversion_rule123456=df_tmp_GenericCurrencyConversion_rule123.alias("rule123")\
          .join (df_tmp_GenericCurrencyConversion_rule4.alias('rule4')\
                 ,(col("rule123.rowID").eqNullSafe(col("rule4.rowID"))),"left")\
          .join (df_tmp_GenericCurrencyConversion_rule5.alias('rule5')\
                 ,(col("rule123.rowID").eqNullSafe(col("rule5.rowID"))),"left")\
          .join (df_tmp_GenericCurrencyConversion_rule6.alias('rule6')\
                 ,(col("rule123.rowID").eqNullSafe(col("rule6.rowID"))),"left")\
          .select(col('rule123.sourceCompanyCode')\
                  ,col('rule123.sourceDate')\
                  ,col('rule123.sourceAmount')\
                  ,col('rule123.sourceCurrency')\
                  ,when(col('rule123.isCurrencyUpdated')==1,col('rule123.targetAmount'))\
                  .when(col('rule4.isCurrencyUpdated')==1,col('rule4.targetAmount'))\
                  .when(col('rule5.isCurrencyUpdated')==1,col('rule5.targetAmount'))\
                  .when(col('rule6.isCurrencyUpdated')==1,col('rule6.targetAmount'))\
                  .otherwise(lit(None)).alias('targetAmount')\
                 )
   
    df_result=df_tmp_GenericCurrencyConversion.alias("a")\
      .join (df_tmp_GenericCurrencyConversion_rule123456.alias('b')\
                   ,( (col('a.sourceCompanyCode')==col('b.sourceCompanyCode'))\
                     & (col('a.sourceDate')==col('b.sourceDate'))\
                     & (col('a.sourceCurrency')==col('b.sourceCurrency'))\
                     & (col('a.sourceAmount')==col('b.sourceAmount'))\
                    ),"inner")\
      .select(col('a.sourceCompanyCode')\
           ,col('a.sourceCurrency')\
            ,col('a.sourceDate')\
            ,col('a.sourceAmount')\
            ,col('b.targetAmount')\
            ,col('a.sourceColName')\
           )
    
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus,df_result]    

  except Exception as err:        
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus,df_result]  
