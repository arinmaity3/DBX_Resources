# Databricks notebook source
from pyspark.sql.functions import collect_list
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql import functions as F

def app_missingCurrency_validate(executionID = ""):
  try:
    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = VALIDATION_ID.MISSING_CURRENCY,
                              executionID = executionID)
    erpSAPSystemID = str(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID'))
    ExecuteString  = ""
    
    filePath = gl_commonParameterPath+'gen_L1_TD_CurrencyExchangeRateHistory.csv'
    filePath_Currency = gl_knowledgePath+'gen_L1_TD_CurrencyExchangeRate.delta'
    objDataTransformation = gen_dataTransformation()
    global gen_L1_TD_CurrencyExchangeRate
    
    isFileExists = True
    try:
      dbutils.fs.ls(filePath)
    except Exception as e:  
      if 'java.io.FileNotFoundException' in str(e): 
        pass
        isFileExists = False
        
    if isFileExists == True:    
      df_History = objGenHelper.gen_readFromFile_perform(filePath)
      df_CurrencyExchangeRate = objGenHelper.gen_readFromFile_perform(filePath_Currency)
      df_CurrencyExchangeRate = df_CurrencyExchangeRate.select("exchangeDate","currencyCode","exchangeRate")

      df_History = df_History.alias("dh").join(df_CurrencyExchangeRate.alias("cr")\
      ,(col("dh.exchangeDate")==col("cr.exchangeDate"))\
      &(col("dh.currencyCode")==col("cr.currencyCode")),how="leftanti")\
      .select("dh.exchangeDate","dh.currencyCode","dh.exchangeRate")
      
      df_History_CurrencyExchangeRate = df_History.union(df_CurrencyExchangeRate)

      gen_L1_TD_CurrencyExchangeRate = objDataTransformation.gen_convertToCDMandCache\
      (dfSource=df_History_CurrencyExchangeRate, schemaName='gen',tableName='L1_TD_CurrencyExchangeRate',\
      targetPath=gl_commonParameterPath,mode="overwrite")
      
    else:
      df_CurrencyExchangeRate = objGenHelper.gen_readFromFile_perform(filePath_Currency)
      df_CurrencyExchangeRate = df_CurrencyExchangeRate.select("exchangeDate","currencyCode","exchangeRate")
      gen_L1_TD_CurrencyExchangeRate = objDataTransformation.gen_convertToCDMandCache\
      (dfSource=df_CurrencyExchangeRate, schemaName='gen',tableName='L1_TD_CurrencyExchangeRate',\
      targetPath=gl_commonParameterPath,mode="overwrite")
      
    schema = StructType([
                        StructField('schemaName', StringType(), True),
                        StructField('tableName', StringType(), True),
                        StructField('DateField', StringType(), True),
                        StructField('currencyCode', StringType(), True)
                        ])

    df_currency_validation_full = spark.createDataFrame([], schema)

    df_TMP_CurrencyValidation = gl_metadataDictionary['knw_LK_GD_CurrencyConversionMetadata'].alias("cur_m")\
    .filter((col("scenarioName")=="CurrencyValidation")&(col("erpSystemID")==lit(erpSAPSystemID)))\
    .sort(col("cur_m.conversionDateOrder"))\
    .groupby("cur_m.schemaName","cur_m.tableName","cur_m.currencyFieldName").agg(collect_list("fieldName").alias("fieldName"))\
    .select(col("cur_m.schemaName").alias("schemaName")\
          ,col("cur_m.tableName").alias("tableName")\
          ,col("fieldName").alias("fieldName")\
          ,col("cur_m.currencyFieldName").alias("currencyCode")).collect()

    for row in df_TMP_CurrencyValidation:
      ExecuteString = row["schemaName"]+"_"+row["tableName"]+\
        ".join(gen_L1_TD_CurrencyExchangeRate.alias('crsource'),(col('crsource.exchangeDate')==coalesce("+str(row["fieldName"])+"))"\
        "&(col('crsource.currencyCode')==col('"+row["currencyCode"]+"')),how='left')"\
        ".filter((col('crsource.exchangeDate').isNull())&(coalesce('"+row['currencyCode']+"',lit('')).isNotNull()))"\
        ".select(lit('"+row["schemaName"]+"').alias('schemaName'),lit('"+row["tableName"]+"').alias('tableName'),\
        coalesce("+str(row["fieldName"])+").alias('DateField'),col('"+row['currencyCode']+"').alias('currencyCode')).distinct()"
    
      query = ExecuteString.replace('[','').replace(']','').replace('"','')
    
      query = "{dfResult} = ({query})".format(dfResult = 'df_currency_validation',query = query)       
      exec(query,globals())
      df_currency_validation_full = df_currency_validation_full.union(df_currency_validation).filter(col("currencyCode")!="")
      

      w = Window.orderBy(col("DateField"))
      
      df_currency_validation_distinct = df_currency_validation_full\
      .select(lit(VALIDATION_ID.MISSING_CURRENCY.value).alias("validationID")\
      ,"DateField"\
      ,"currencyCode")\
      .filter(~col("DateField").isin('1900-01-01','1900-01-02')).distinct()
      
      df_currency_validation_distinct = df_currency_validation_distinct.withColumn("groupSlNo",F.row_number().over(w))\
      .select("groupSlNo"\
              ,"validationID"\
              ,"DateField"\
              ,"currencyCode").sort("groupSlNo")
      
      df_res_col_1 = df_currency_validation_distinct.select("groupSlNo","validationID"\
                    ,lit(None).alias("validationObject"),lit(None).alias("fileType")\
                    ,lit("DateField").alias("resultKey"),col("DateField").alias("resultValue"))
      
      df_res_col_2 = df_currency_validation_distinct.select("groupSlNo","validationID"\
                    ,lit(None).alias("validationObject"),lit(None).alias("fileType")\
                    ,lit("currencyCode").alias("resultKey"),col("currencyCode").alias("resultValue"))
      
      df_res_final = df_res_col_1.union(df_res_col_2).sort("groupSlNo").distinct()
      
    if(df_currency_validation_distinct.count() == 0):                            
      executionStatus = "Missing currency validation succeeded"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    else:                
      executionStatus = "Missing currency validation failed"
  
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,
                       executionStatus,
                       writeSummaryFile = True,
                       dfDetail = df_res_final)  
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

  except Exception as err:        
      executionStatus = objGenHelper.gen_exceptionDetails_log()       
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
