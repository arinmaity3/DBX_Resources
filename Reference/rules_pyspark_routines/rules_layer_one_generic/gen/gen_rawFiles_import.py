# Databricks notebook source
import sys
import traceback

class gen_rawFiles_import:  
  def importRawFiles(self,fileName,fileID,fileType,delimiter,executionID,textQualifier):
    try:        
        global gl_lstOfImportValidationSummary  
        dfRawFile = None
        executionStatus=''
        filePath = gl_rawFilesPath + fileName
        objGenHelper = gen_genericHelper()
        logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,\
                                fileID,fileName,fileType,\
                                VALIDATION_ID.PACKAGE_FAILURE_IMPORT,\
                                executionID = executionID)
        fileExtension = pathlib.Path(filePath).suffix.lower()  
        
        if (fileExtension == '.xlsx'):
          dfRawFile = objGenHelper.gen_readFromFile_perform(filePath,delimiter,None,True)
          #dataTypeList = dfRawFile.dtypes
          #dfDtaType = spark.createDataFrame(dataTypeList)
          #dfDtaType.createOrReplaceTempView("rawFile_dateTypes")
          #gl_parameterDictionary["SourceTargetColumnMapping"].filter((col("fileID")) == fileID).createOrReplaceTempView("sourceColumnMapping_table")
          #gl_metadataDictionary["dic_ddic_column"].filter(((col("fileType")) == fileType) & ((col("sparkDataType")) == "String")).createOrReplaceTempView("ddic_column_table")
          #dfStringColumnsList = spark.sql("select \
          #                                   s.sourceColumn \
          #                                 from sourceColumnMapping_table s \
          #                                 join ddic_column_table d \
          #                                   on d.columnName = s.targetColumn \
          #                                 join rawFile_dateTypes t \
          #                                   on t._1 = s.sourceColumn\
          #                                 where t._2 = 'double'")
          #rowCount = dfStringColumnsList.count()
          #while rowCount > 0:
          #    stringColumnName = dfStringColumnsList.select('sourceColumn').collect()[rowCount - 1][0]
          #    colDerived = stringColumnName+'_derived'
          #    dfRawFile = dfRawFile.withColumn(colDerived,dfRawFile[stringColumnName].cast("int").cast("string"))
          #    dfRawFile = dfRawFile.drop(stringColumnName)
          #    dfRawFile = dfRawFile.withColumnRenamed(colDerived,stringColumnName)
          #    rowCount = rowCount - 1
        else:
          dfRawFile = objGenHelper.gen_readFromFile_perform(filePath,delimiter,'UTF8',True,textQualifier)
        executionStatus="Raw files imported sucessfully"
        executionStatusID = LOG_EXECUTION_STATUS.SUCCESS                          
    except Exception as e: 
        if(dfRawFile is None):
          emptyDFSchema = StructType([
                                      StructField("blankData", StringType(),True)
                                    ])
          dfRawFile = spark.createDataFrame(spark.sparkContext.emptyRDD(),emptyDFSchema)
        executionStatus =  objGenHelper.gen_exceptionDetails_log()
        executionStatusID = LOG_EXECUTION_STATUS.FAILED
    finally:
        executionLog.add(executionStatusID,logID,executionStatus)
        return [executionStatusID,executionStatus,dfRawFile]   
