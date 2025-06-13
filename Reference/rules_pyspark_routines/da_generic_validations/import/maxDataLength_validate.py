# Databricks notebook source
import sys
import traceback
from pyspark.sql.window import Window
import uuid

def _maxDataLength_validate(dfSource,fileName,fileID,fileType,ERPSystemID,
                            schemaName,tableName,
                            isCDM= False,
                            lstOfDataTypes = ['tinyint','smallint','int','bigint','numeric']):
  #validation Max data length 
  try:
    
    objGenHelper = gen_genericHelper()
      
    query = "{dfName} = spark.createDataFrame([], StructType([]))".format(dfName = 'dfMaxLengthResult')            
    exec(query,globals())
           
    lstOfColumns = list()
    [lstOfColumns.append(c.upper()) for c in dfSource.columns]
    
    if(isCDM == False):
      lstOfDataTypes = ['date','datetime','datetime2','time','uniqueidentifier']
      filterCondition = False
    else:      
      filterCondition = True
          
    dfFieldLength = gl_metadataDictionary['dic_ddic_column'].alias("C").\
                    select(concat(col("C.schemaName"),lit("_"),col("C.tableName")).alias("fileName"),\
                       col("C.fileType"),\
                       concat(lit("`"),col("C.columnName"),lit("`")).alias("sourceColumn"),\
                       col("C.columnName").alias("targetColumn"),\
                       col("C.columnName"),\
                       col("C.fieldLength"),\
                       col("C.isKey"),\
                       col("C.position"),\
                       col("C.dataType"))\
                    .filter((upper(col("C.schemaName")) == schemaName.upper()) &\
                     (upper(col("C.tableName")) == tableName.upper()) & \
                     (upper(col("C.columnName")).isin(lstOfColumns)) & \
                     (lower(col("C.dataType")).isin(lstOfDataTypes) == filterCondition) & \
                     (col("C.isImported")=="False") & \
                     (col("C.ERPSystemID")== ERPSystemID))\
                    .orderBy(col("C.position"))\
                    .distinct()
    
    sourceView = uuid.uuid4().hex    
    dfSource2 = "f" + uuid.uuid4().hex  
    dfSource.createOrReplaceTempView(sourceView)
    
    query = "{df} = spark.sql('SELECT * FROM {source}')".format(df = dfSource2,source = sourceView)    
    exec(query,globals())
   
    w = Window().orderBy(lit('rowNumber'))
    query = "{df} = {df2}.withColumn('rowNumber', row_number().over(Window().orderBy(lit('rowNumber'))))".\
            format(df = dfSource2,df2 = dfSource2)
    exec(query,globals())
    
    keyColumns = "col('rowNumber').cast('string')"
    query = ''

    #the columns which can be excluded from max data lenght check because seperate other
    #validation are availabe for the same, which will take care of this specific scenario

    for cols in dfFieldLength.\
              filter((col("targetColumn").isNotNull())).\
              orderBy("fileType").rdd.collect():

      if(cols['dataType'].lower() == 'float'):
        continue        
      if (((cols['dataType']=='nvarchar') & (cols["fieldLength"]  =='-1' ))):
        continue
      if(cols['dataType'].lower() == 'tinyint'):
          minValue = 0.00
          maxValue = 255.00    
      elif(cols['dataType'].lower() == 'smallint'):
          minValue = -32768.00
          maxValue = 32767.00
      elif(cols['dataType'].lower() == 'int'):
          minValue = -2147483648.00
          maxValue = 2147483647.00
      elif(cols['dataType'].lower() == 'bigint'):
          minValue = -9223372036854775808.00
          maxValue = 9223372036854775807.00
      elif(cols['dataType'].lower() == 'numeric'):
          maxValue = '9' * eval(cols['fieldLength'].replace(',','-')) +".00"    
          minValue =  '-' + '9' * eval(cols['fieldLength'].replace(',','-'))  + "0.00"                   
      else:
          fieldLength = cols["fieldLength"]

      if(query != ''):
          query =  query + ').unionAll('

      if(cols['dataType'].lower() in ['tinyint','smallint','int','bigint','numeric']):
          query = query + "{fileName}.filter(~col('{sourceColumn}').between({minValue},{maxValue})).\
                  select(lit('{columnName}').alias('columnname'),col('{columnValue}').\
                  cast('string').alias('value'),{keyColumn}).limit(100)".\
                  format(fileName = dfSource2,
                        sourceColumn = cols["sourceColumn"],                  
                        keyColumn = keyColumns,
                        columnName = cols["sourceColumn"],
                        columnValue = cols["sourceColumn"],
                        minValue = minValue,
                        maxValue = maxValue)
      elif(cols['dataType'].lower() == 'bit'):
          query = query + "{fileName}.filter(~col('{sourceColumn}').isin([False,True])).\
                  select(lit('{columnName}').alias('columnname'),col('{columnValue}').\
                  cast('string').alias('value'),{keyColumn}).limit(100)".\
                  format(fileName = dfSource2,
                        sourceColumn = cols["sourceColumn"],                  
                        keyColumn = keyColumns,
                        columnName = cols["sourceColumn"],
                        columnValue = cols["sourceColumn"])                                  
      else:
          query = query + "{fileName}.filter(length(col('{sourceColumn}'))>{fieldLength}).\
                  select(lit('{columnName}').alias('columnname'),col('{columnValue}').\
                  cast('string').alias('value'),{keyColumn}).limit(100)".\
                  format(fileName = dfSource2,
                        sourceColumn = cols["sourceColumn"],
                        fieldLength = fieldLength,                  
                        keyColumn = keyColumns,
                        columnName = cols["sourceColumn"],
                        columnValue = cols["sourceColumn"])
          
    if(query != ''):
      dfMaxLengthResult = eval(f"({query})")
        
    if(dfMaxLengthResult.count() == 0):                            
      executionStatus = "Max data length validation succeeded for the file '" + fileName + "'"
      executionStatusID= LOG_EXECUTION_STATUS.SUCCESS           
    else:                
      executionStatus = "Max data length validation failed for the file '" + fileName + "'"
      executionStatusID = LOG_EXECUTION_STATUS.FAILED  
      
    return [executionStatusID,executionStatus,dfMaxLengthResult]  
  except Exception as err:    
    print(err)    
    raise
  finally:
    spark.sql("DROP VIEW IF EXISTS " + sourceView)
  
        
def app_maxDataLength_validate(dfSource,fileName,fileID,fileType,ERPSystemID = 10,schemaName = "",tableName = "",executionID = ""):
  try:
   
    objGenHelper = gen_genericHelper()

    if(ERPSystemID == 10):
        logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                  fileType = fileType,
                                  validationID = VALIDATION_ID.MAX_DATA_LENGTH)
    else:
        logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,fileID,\
                                    fileName,fileType,VALIDATION_ID.MAX_DATA_LENGTH,\
                                    executionID = executionID)

    dfMaxDataLength = None

    if((objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'BALANCE_TYPE')=='GLTB') and (fileType == 'GLTB')):
        lstOfStatus = _maxDataLength_validate(dfSource,fileName,fileID,fileType,ERPSystemID,schemaName,tableName)
        lstOfStatusGla = _maxDataLength_validate(dfSource = fin_L1_MD_GLAccount,fileName = fileName,\
            fileID = fileID,fileType = fileType,ERPSystemID = ERPSystemID,schemaName = 'fin',tableName ='L1_MD_GLAccount')
        dfMaxDataLength = lstOfStatus[2].union(lstOfStatusGla[2])
    else:
        lstOfStatus = _maxDataLength_validate(dfSource,fileName,fileID,fileType,ERPSystemID,schemaName,tableName)
        dfMaxDataLength = lstOfStatus[2]
        
    if((lstOfStatus[0] == LOG_EXECUTION_STATUS.SUCCESS) and (fileType != 'GLTB')):
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,lstOfStatus[1])
        return [LOG_EXECUTION_STATUS.SUCCESS,lstOfStatus[1],dfMaxDataLength]
    elif((fileType == 'GLTB') and (lstOfStatus[0] == LOG_EXECUTION_STATUS.SUCCESS) and (lstOfStatusGla[0] == LOG_EXECUTION_STATUS.SUCCESS)):
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,lstOfStatus[1])
        return [LOG_EXECUTION_STATUS.SUCCESS,lstOfStatus[1],dfMaxDataLength]
    else:      
      w = Window().orderBy(lit('groupSlno'))
      dfMaxDataLengthdtl = dfMaxDataLength.withColumn("groupSlno", row_number().over(w))

      if(ERPSystemID != 10):
          dfMaxDataLengthdtls= objGenHelper.gen_dynamicPivot(fileType =fileType,
                                      validationID =VALIDATION_ID.MAX_DATA_LENGTH.value,
                                      pivotColumn1='columnname',
                                      pivotColumn2='value',
                                      pivotColumn3='rowNumber',
                                      dfToPivot=dfMaxDataLengthdtl)
          remarks = "Max data length validation failed"
          dfMaxDataLengthdtls = dfMaxDataLengthdtls.withColumn("remarks", lit(remarks))
      else:
          dfMaxDataLengthdtls= objGenHelper.gen_dynamicPivot(fileType =fileType,
                                      validationID =VALIDATION_ID.MAX_DATA_LENGTH.value,
                                      pivotColumn1='columnname',
                                      pivotColumn2='value',
                                      pivotColumn3='rowNumber',
                                      dfToPivot=dfMaxDataLengthdtl)
          remarks = "Max data length validation failed"

      if fileType != 'GLTB':
          executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,lstOfStatus[1],dfDetail = dfMaxDataLengthdtls)  
      else:
          executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,"Max data length validation failed for the file '" + fileName + "'",dfDetail = dfMaxDataLengthdtls)

      return [LOG_EXECUTION_STATUS.FAILED,lstOfStatus[1],dfMaxDataLengthdtls]      

  except Exception as err:        
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus,dfMaxDataLengthdtls] 


  
def gen_CDMDataTypeValidation_perform(dfSource,fileName,fileID,fileType,ERPSystemID,
                                        schemaName,tableName,
                                        isCDM = True,
                                        lstOfDataTypes = ['tinyint','smallint','int','bigint','numeric']):
  try:
    lstOfStatus = _maxDataLength_validate(dfSource,fileName,fileID,fileType,
                                           ERPSystemID,schemaName,tableName,isCDM,lstOfDataTypes)
    
    if(lstOfStatus[0] == LOG_EXECUTION_STATUS.FAILED):
      dfMaxDataLength=lstOfStatus[2]
      executionStatus = "Data type conversion failure.Values in source field are larger than the expected range,file name. '" + fileName + "'."
      errorDetails = list()
      [errorDetails.append(['Field:' + r.columnname,'value:' + r.value,'row number:' + r.rowNumber])
       for r in dfMaxDataLength.limit(1).rdd.collect()]      
      executionStatus = executionStatus + ",".join(errorDetails[0])
      raise Exception(executionStatus)          
  except Exception as err:
    raise
