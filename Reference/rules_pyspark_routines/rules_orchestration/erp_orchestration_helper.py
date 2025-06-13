# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row
from pyspark.sql.types import StructType,StructField, StringType, ShortType
from pyspark.sql.functions import col, lower,split,explode,desc
from operator import itemgetter
import uuid
import chardet
import itertools

class ERPOrchestration():
    @staticmethod
    def allPreRequisiteMetadata_load(processID = ""):
        try:
            print('loading all prerequisite metadata files...')
            objDDIC = ddic()
            objDDIC.ddicDARules_load()            
            inputParams.inputParams_load(processID)            
        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()            
            print(executionStatus)
            raise

    @staticmethod
    def enableAccountMapping(validationID):
        try:
            logID = executionLog.init(PROCESS_ID.IMPORT,
                                     validationID = validationID,
                                     tableName = "Enable account mapping")

            objGenHelper = gen_genericHelper()
            isEnableAccountMapping = objGenHelper.enableAccountMapping_check()
            isEnableAccountMappingData = [{"isEnableAccountMapping":isEnableAccountMapping}]
            dict_isEnableAccountMapping = spark.createDataFrame(isEnableAccountMappingData)
            objGenHelper.gen_writeSingleCsvFile_perform(dict_isEnableAccountMapping,\
                         targetFile=gl_commonParameterPath + FILES.ACCOUNT_MAPPING_ENABLE_STATUS_FILE.value)

            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,"Account mapping flag updated")
        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            raise
    
    @staticmethod
    def loadLogFile():
      try:
          objGenHelper = gen_genericHelper()
          logID = executionLog.init(PROCESS_ID.IMPORT,
                                    validationID = VALIDATION_ID.PACKAGE_FAILURE_IMPORT,
                                    tableName = "Load log file")

          global gl_parameterDictionary
          listUTF8 = ['UTF-8', 'UTF8', 'UTF-8-SIG','UTF-8-BOM']

          schemaLog = StructType([StructField("sourceFile",StringType(),False),
                                      StructField("tableName",StringType(),False),
                                      StructField("rowsExtracted",IntegerType(),True),
                                      StructField("sourceFileName",StringType(),False)])

          dfLogFile = spark.createDataFrame(data = [], schema = schemaLog)

          for logFile in dbutils.fs.ls(gl_rawFilesPathERP):
              if ((str(logFile.name) != None ) and ('KPMG_LOG_RECORDCOUNT_' in str(logFile.name))):
                   filenamepath = "/dbfs" + gl_rawFilesPathERP + logFile.name
                   rawdata = open(filenamepath, "rb").read()
                   result = chardet.detect(rawdata)
                   detectedEncoding = result['encoding']

                   if str(detectedEncoding).upper() in listUTF8 :
                       fileEncoding = "UTF8"
                   else:
                       fileEncoding = detectedEncoding

                   dfLog = objGenHelper.\
                                gen_readFromFile_perform(filenamepath = logFile.path,delimiter = '#|#',encoding = fileEncoding)

                   dfLog = dfLog.select(dfLog.columns[0],\
                               dfLog.columns[1],\
                               dfLog.columns[2]).\
                               withColumnRenamed(dfLog.columns[0],"sourceFile").\
                               withColumnRenamed(dfLog.columns[1],"tableName").\
                               withColumnRenamed(dfLog.columns[2],"rowsExtracted")
                   
                   dfLog = dfLog.withColumn('sourceFileName',
                                 when(col("sourceFile").contains('\\'),F.substring_index(col("sourceFile"),'\\',-1))
                                   .when(col("sourceFile").contains('/'),F.substring_index(col("sourceFile"),'/',-1))
                                   .otherwise(col("sourceFile")))
                   dfLogFile = dfLogFile.unionAll(dfLog)                   
                   
          gl_parameterDictionary['logFile'] = dfLogFile.persist()

          executionStatus = "Log files loaded sucessfully."  
          executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
          return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
      except Exception as err:
          executionStatus = objGenHelper.gen_exceptionDetails_log()       
          executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
          return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    @staticmethod
    def prepareFilesForImport_prepare():
        try:
      
            objGenHelper = gen_genericHelper()        
            logID = executionLog.init(PROCESS_ID.IMPORT,
                                      validationID = VALIDATION_ID.PACKAGE_FAILURE_IMPORT,
                                      tableName = "Prepare files for import")                
        
            global gl_parameterDictionary

            inputParamPath = gl_inputParameterPath + FILES.ERP_IMPORT_INPUT_PARAM_FILE.value

            dfInputParams = objGenHelper.gen_readFromFile_perform(inputParamPath).\
                select(col("FileID").alias("fileID"),
                       col("FileName").alias("fileName"),
                       col("executionID"))                   
        
            gl_parameterDictionary['ERPImportInputParams'] = dfInputParams
            dfSourceFiles = dfInputParams.alias("i").join(gl_parameterDictionary['logFile'].alias("l"),
                            [lower(col("i.fileName")) == lower(col("l.sourceFileName"))],how ='left').\
                            select(col("i.fileID").alias("fileID"),\
                            col("i.fileName"),\
                            concat(lit(gl_rawFilesPathERP),col("i.fileName")).alias("filePath"),\
                            col("executionID"),\
                            upper(col("tableName")).alias("tableName"),\
                            col("l.rowsExtracted")).persist()
        
            gl_parameterDictionary['sourceFiles'] = dfSourceFiles

            lstOfMissingFiles = list()
            [lstOfMissingFiles.append([f.fileName,f.filePath])
            for f in gl_parameterDictionary['sourceFiles'].\
                filter(col("tableName").isNull()).collect()]

            missingFiles = [",".join(f) for f in lstOfMissingFiles]

            if(len(missingFiles)!= 0):
                executionStatus = "Mismatch in log file\\input parameters\\rawfiles." + ",".join(missingFiles)
                executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
                return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


            executionStatus = "Prepare the list of input files to be loaded"
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()       
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    
    @staticmethod
    def generateERPCDMStructure(lstOfFiles):
      try:
          
          objDataTransformation = gen_dataTransformation()
          objGenHelper = gen_genericHelper()          
          
          dbutils.fs.rm(lstOfFiles[1],True)                     
          dfCDM = objDataTransformation.gen_CDMStructure_get('erp',lstOfFiles[0]).\
                  withColumn("kpmgRowID",lit('')).\
                  withColumn("isShifted",array(lit(0)))
          objGenHelper.gen_writeToFile_perfom(dfCDM,lstOfFiles[1],mode='append')
          
      except Exception as err:
          raise
          
    @staticmethod
    def generateCDMStructureForERPFiles():
      try:

          logID = executionLog.init(PROCESS_ID.IMPORT,
                                    validationID = VALIDATION_ID.PACKAGE_FAILURE_IMPORT,
                                    tableName = "Initialize result files") 
        
          lstOfFiles = list()
          [lstOfFiles.append([r.tableName,r.filePath])
               for r in gl_parameterDictionary['sourceFiles'].select(col("tableName"),\
                    concat(lit(gl_layer0Staging),lit('erp_'),col("tableName"),lit(".delta")).alias("filePath")).\
                    distinct().collect()]

          with ThreadPoolExecutor(max_workers = MAXPARALLELISM.DEFAULT.value) as executor:
              lstOfStatus = list(executor.map(ERPOrchestration.generateERPCDMStructure,lstOfFiles))

          executionStatus = "Result file initialization completed."
          executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
          return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

      except Exception as err:
          executionStatus = objGenHelper.gen_exceptionDetails_log()       
          executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)          
          raise

  
    @staticmethod
    def dataImportAndValidation_perform(lstOfFiles):
      try:    
    
        fileID = lstOfFiles[0]
        fileName = lstOfFiles[1]
        filePath = lstOfFiles[2]
        executionID = lstOfFiles[3]
        tableName = lstOfFiles[4]
        fileType = lstOfFiles[4]
        recordsReceived = lstOfFiles[5]
       
        delimiter = '#|#'
        fileEncoding = ''
        isPersists = False
        textQualifier = '"'      
        lstOfStatus = list()
        importedRows = ""

        objDataTransformation = gen_dataTransformation()

        logIDImport = executionLog.init(PROCESS_ID.IMPORT,\
                                  tableName = tableName,\
                                  fileName = fileName,\
                                  fileID = fileID,\
                                  validationID = VALIDATION_ID.PACKAGE_FAILURE_IMPORT,\
                                  executionID = executionID,
                                  recordsReceived = recordsReceived) 
   
        transformationStatusLog.createLogFile(PROCESS_ID.IMPORT_VALIDATION,executionID)
   
        encodingStatus = app_fileEncoding_validate(fileName    = fileName,
                                                   fileID      = fileID,
                                                   fileType    = fileType,
                                                   executionID = executionID,
                                                   filePath    = filePath)
    
        fileEncoding = encodingStatus[2]
        lstOfStatus.append([VALIDATION_ID.FILE_ENCODING,encodingStatus[0]])
    
        zeroKBStatus = app_zeroKBFile_validate(fileName   = fileName,
                                                 fileID      = fileID,
                                                 fileType    = fileType,
                                                 executionID = executionID,
                                                 filePath    = filePath)

        lstOfStatus.append([VALIDATION_ID.ZERO_KB,zeroKBStatus[0]])
    
        delimiterStatus = app_columnDelimiter_validate (fileName = fileName,
                                                        fileID   = fileID,
                                                        fileType = fileType,
                                                        columnDelimiter = delimiter,
                                                        executionID = executionID,
                                                        filePath = filePath,
                                                        fileEncoding = fileEncoding)
    
        lstOfStatus.append([VALIDATION_ID.COLUMN_DELIMITER,delimiterStatus[0]])

        if (fileEncoding is not None and fileEncoding != 'UTF8' 
                and fileEncoding.upper() != 'ASCII'):
            try:
                logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,\
                                    fileID,fileName,fileType,\
                                    validationID = VALIDATION_ID.PACKAGE_FAILURE_IMPORT,
                                    phaseName = 'Rawfile convert to UTF-8',\
                                    executionID = executionID)

                executionStatus = objDataTransformation.gen_UTF8_convert(fileName = fileName,
                                                                                fileEncoding = fileEncoding,
                                                                                filePath = filePath,
                                                                                convertedRawFilesPath = gl_convertedRawFilesPathERP,
                                                                                inputFileType=None)
                if(executionStatus[0] == LOG_EXECUTION_STATUS.SUCCESS):
                    fileName = executionStatus[2]
                    filePath = executionStatus[3]
                    fileEncoding = executionStatus[4]

                lstOfStatus.append([VALIDATION_ID.PACKAGE_FAILURE_IMPORT,LOG_EXECUTION_STATUS.SUCCESS])
                executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,'Rawfile converted to UTF-8 successfully')
            except:
                lstOfStatus.append([VALIDATION_ID.PACKAGE_FAILURE_IMPORT,LOG_EXECUTION_STATUS.FAILED])
                executionStatus = objGenHelper.gen_exceptionDetails_log()
                executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    
        try:
          logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,\
                                    fileID,fileName,fileType,\
                                    validationID = VALIDATION_ID.PACKAGE_FAILURE_IMPORT,
                                    phaseName = 'Import data',\
                                    executionID = executionID)
    
          dfRawFile = objGenHelper.gen_readFromFile_perform(filenamepath   = filePath,
                                                         delimiter  = delimiter,
                                                         encoding   = fileEncoding,
                                                         isPersists = True,
                                                         textQualifier = textQualifier)
      
          lstOfStatus.append([VALIDATION_ID.PACKAGE_FAILURE_IMPORT,LOG_EXECUTION_STATUS.SUCCESS])
          executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,'Data imported successfully')
        except:
          lstOfStatus.append([VALIDATION_ID.PACKAGE_FAILURE_IMPORT,LOG_EXECUTION_STATUS.FAILED])
          executionStatus = objGenHelper.gen_exceptionDetails_log()
          executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    
        recordCount = app_recordCount_validate(dfSource = dfRawFile,
                                                    fileName = fileName,
                                                    fileID   = fileID,
                                                    fileType = fileType,
                                                    executionID = executionID,
                                                    filePath  = filePath,
                                                    fileEncoding = fileEncoding,
                                                    ERPSystemID = gl_ERPSystemID)    
    
        lstOfStatus.append([VALIDATION_ID.UPLOAD_VS_IMPORT,recordCount[0]])    
        importedRows = recordCount[2]
    
        mandatoryField  = app_mandatoryField_validate(dfSource = dfRawFile,
                                                      fileName = fileName,
                                                      fileID   = fileID,
                                                      fileType = fileType,
                                                      gl_metadataDictionary = gl_metadataDictionary,
                                                      schemaName = 'erp',
                                                      tableName  = tableName,
                                                      executionID = executionID)
    
        lstOfStatus.append([VALIDATION_ID.MANDATORY_FIELD,mandatoryField[0]])
    
        dataPersistence =  app_dataPersistence_validate(fileName = fileName,
                                                        fileID   = fileID,
                                                        fileType = fileType,
                                                        executionID = executionID,
                                                        columnDelimiter = delimiter,
                                                        filePath = filePath,
                                                        fileEncoding = fileEncoding,
                                                        textQualifier = textQualifier)
        
        lstOfStatus.append([VALIDATION_ID.DATA_PERSISTENCE,dataPersistence[0]])
    
            
        maxDataLength = app_maxDataLength_validate(dfSource = dfRawFile,
                                                   fileName = fileName,
                                                   fileID   = fileID,
                                                   fileType = fileType,
                                                   ERPSystemID = gl_ERPSystemID,
                                                   schemaName = 'erp',
                                                   tableName  = tableName,
                                                   executionID = executionID)

        lstOfStatus.append([VALIDATION_ID.MAX_DATA_LENGTH,maxDataLength[0]])
   
        lstOfImportStatus = list(itertools.filterfalse(lambda status : status[1] != LOG_EXECUTION_STATUS.FAILED                                                                             
                                                       or status[0] not in [VALIDATION_ID.ZERO_KB,
                                                                            VALIDATION_ID.FILE_ENCODING,
                                                                            VALIDATION_ID.COLUMN_DELIMITER,
                                                                            VALIDATION_ID.PACKAGE_FAILURE_IMPORT,
                                                                            VALIDATION_ID.DATA_PERSISTENCE,
                                                                            VALIDATION_ID.MAX_DATA_LENGTH], lstOfStatus))

        if(len(lstOfImportStatus)== 0):
            try:
                logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION
                                          ,fileID = fileID,
                                          fileName = fileName,
                                          fileType = fileType,
                                          validationID = VALIDATION_ID.PACKAGE_FAILURE_IMPORT,
                                          phaseName = 'Prepare ERP staging data',
                                          executionID = executionID)
        
                dfERPFile = objDataTransformation.gen_convertToCDMandCache \
                                (dfSource            = dfRawFile,
                                 schemaName          = 'erp',
                                 tableName           = tableName,
                                 isIncludeAnalysisID = False,
                                 targetPath          = gl_layer0Staging,
                                 isERP               = True,
                                 mode                = 'append',
                                 isIncludeRowID      = True,
                                 isDPSFlag           = True)    

                stagStatus = [LOG_EXECUTION_STATUS.SUCCESS,"ERP-CDM conversion succeeded."]
                executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,"ERP-CDM conversion succeeded")
            except Exception as err:
                executionStatus = objGenHelper.gen_exceptionDetails_log()
                stagStatus = [LOG_EXECUTION_STATUS.FAILED,executionStatus]
                executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)

            if(stagStatus[0] == LOG_EXECUTION_STATUS.SUCCESS):
                if(len(list(itertools.filterfalse(lambda status : status[1] != LOG_EXECUTION_STATUS.WARNING,lstOfStatus)))!= 0):
                    statusIDImport = LOG_EXECUTION_STATUS.WARNING
                elif(len(list(itertools.filterfalse(lambda status : status[1] != LOG_EXECUTION_STATUS.FAILED,lstOfStatus)))!= 0):
                    statusIDImport = LOG_EXECUTION_STATUS.FAILED
                else:
                    statusIDImport = LOG_EXECUTION_STATUS.SUCCESS

                executionStatus = "Data import succeeded for the file '" + fileName + "'"
                executionLog.add(statusIDImport,logIDImport,executionStatus,
                                 importedRows = importedRows,
                                 mandatoryFieldStatus = mandatoryField[0].name)      
                return [statusIDImport,executionStatus,lstOfFiles[1]]    
            else:
                executionStatus = "ERP-CDM conversion failed for the file  '" + fileName + "'"
                executionLog.add(LOG_EXECUTION_STATUS.FAILED,logIDImport,executionStatus,
                                 importedRows = importedRows,
                                 mandatoryFieldStatus = mandatoryField[0].name)      
                return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus,lstOfFiles[1]]    
        else:
            try:
                executionStatus = "Data import failed for the file '" + fileName + "'"
                executionLog.add(LOG_EXECUTION_STATUS.FAILED,logIDImport,executionStatus,
                                    importedRows = importedRows,
                                    mandatoryFieldStatus = mandatoryField[0].name)
                                
                logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION
                                          ,fileID = fileID,
                                          fileName = fileName,
                                          fileType = fileType,
                                          validationID = VALIDATION_ID.PACKAGE_FAILURE_IMPORT,
                                          phaseName = 'Prepare ERP staging data',
                                          executionID = executionID)

                dfRawFile = spark.createDataFrame([], StructType([]))

                dfERPFile = objDataTransformation.gen_convertToCDMandCache \
                                (dfSource            = dfRawFile,
                                 schemaName          = 'erp',
                                 tableName           = tableName,
                                 isIncludeAnalysisID = False,
                                 targetPath          = gl_layer0Staging,
                                 isERP               = True,
                                 mode                = 'append',
                                 isIncludeRowID      = True,
                                 isDPSFlag           = True)    
                
                executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
                return [LOG_EXECUTION_STATUS.FAILED,executionStatus,fileName]
            
            except Exception as err:
                executionStatus = objGenHelper.gen_exceptionDetails_log()                
                executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
                return [LOG_EXECUTION_STATUS.FAILED,executionStatus,fileName]

      except Exception as err:    
        executionStatus = objGenHelper.gen_exceptionDetails_log() 
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logIDImport,executionStatus,importedRows = importedRows)
        transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.IMPORT)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus,lstOfFiles[1]] 
      finally:
          if dfRawFile is not None:
              dfRawFile.unpersist()




    @staticmethod
    def submitDataImportAndValidation():
        try:
          lstOfFiles = list()
          lstOfDataImportStatus = list()
       
          [lstOfFiles.append([row.fileID,row.fileName,\
                              row.filePath,row.executionID,\
                              row.tableName,row.rowsExtracted])\
             for row in gl_parameterDictionary['sourceFiles'].collect()]

          with ThreadPoolExecutor(max_workers = MAXPARALLELISM.DEFAULT.value) as executor:
              lstOfLoadStatus = list(executor.map(ERPOrchestration.dataImportAndValidation_perform,lstOfFiles))
          return lstOfLoadStatus          
        except Exception as err:
          executionStatus = objGenHelper.gen_exceptionDetails_log()
          transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.IMPORT)
          return [lstOfLoadStatus,executionStatus]  

    @staticmethod
    def _recordCount_get(lstOfTables):
        try:
            tableName = lstOfTables[0]            
            recordCount = spark.sql('select count(1) from ' + tableName).collect()[0][0]
            return [tableName,recordCount,None,recordCount]
        except Exception as err:
            raise

    @staticmethod
    def dataImportPostProcessing_perform():
        try:          
                    
            objGenHelper = gen_genericHelper()        
            logID = executionLog.init(PROCESS_ID.IMPORT,
                                      validationID = VALIDATION_ID.PACKAGE_FAILURE_IMPORT,
                                      tableName = "Post data import validations") 

            ERPStagingCountSchema = StructType([                                
                                StructField("tableName",StringType(),True),
                                StructField("recordCount",IntegerType(),True),
                                StructField("noOfDuplicateRecords",IntegerType(),True),
                                StructField("netRecordCount",IntegerType(),True)
                                ])

            filePath =  gl_layer0Staging + FILES.KPMG_ERP_STAGING_RECORD_COUNT.value 
            lstOfTables = list()
    
            [lstOfTables.append([c.tableName]) for c in gl_parameterDictionary['sourceFiles'].\
                select(concat(lit('erp_'),col("tableName")).alias('tableName')).distinct().collect()]
        
            with ThreadPoolExecutor(max_workers = MAXPARALLELISM.DEFAULT.value) as executor:
              lstOfRecordCount = list(executor.map(ERPOrchestration._recordCount_get ,lstOfTables))                          
          
            dfRecordCount = spark.createDataFrame(data = lstOfRecordCount, schema = ERPStagingCountSchema)
                
            try:
          
              dbutils.fs.ls(filePath)
              dfStagingRecordCount = DeltaTable.forPath(spark, filePath)          
              dfStagingRecordCount.alias('T').merge(source = dfRecordCount.alias('S'),\
              condition = "upper(T.tableName) == upper(S.tableName)") \
              .whenMatchedUpdate(set = {"recordCount": "S.recordCount",
                                        "noOfDuplicateRecords": lit(""),\
                                        "netRecordCount":"S.recordCount"})\
              .whenNotMatchedInsert(values = 
                                    {"tableName" : "S.tableName",
                                     "recordCount" : "S.recordCount",
                                     "netRecordCount" : "S.recordCount"})\
              .execute()            
              executionStatus = "Layer zero post processing (data import & import validations) completed."
          
              executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
              return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
            except Exception as e:                
              if 'java.io.FileNotFoundException' in str(e):        
                objGenHelper.gen_writeToFile_perfom(dfRecordCount,filePath)
                executionStatus = "Layer zero post processing (data import & import validations) completed."
                executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
                return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
                pass    
        except Exception as err:        
            executionStatus = objGenHelper.gen_exceptionDetails_log()       
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    @staticmethod
    def setStagingRecordCount():
        try:     
            if(FILES.KPMG_ERP_STAGING_RECORD_COUNT in gl_parameterDictionary.keys()):
              gl_parameterDictionary.pop(FILES.KPMG_ERP_STAGING_RECORD_COUNT)

            gl_parameterDictionary[FILES.KPMG_ERP_STAGING_RECORD_COUNT] = objGenHelper.\
              gen_readFromFile_perform(gl_layer0Staging + FILES.KPMG_ERP_STAGING_RECORD_COUNT.value)

        except Exception as err:
          raise

    @staticmethod
    def knowledgeFilesWithLatestPublishID(fileName):
        try:
            if fileName in ['knw_LK_CD_FinancialStructureDetailSnapshot',
                            'knw_LK_CD_ProcessToKnowledgeAccountsRelationSnapshot',
                            'knw_LK_CD_KnowledgeAccountSnapshot',
                            'knw_LK_CD_knowledgeAccountScopingDetail',
                            'knw_LK_CD_GLAccountToFinancialStructureDetailSnapshot']:
                return True
            else:
                return False

        except Exception as err:
            raise

    @staticmethod
    def updatePlaceHolderValuesForKnowledgeAccounts(dfSource,fileName):
      try:
        maxID = 1
        fSID = 1
    
        if(fileName == "knw_LK_CD_FinancialStructureDetailSnapshot"):
          maxID = int(dfSource.select(col("financialStructureDetailID").\
                   cast("int")).agg({"financialStructureDetailID": "max"}).collect()[0][0])      
          fSID = knw_LK_CD_FinancialStructure.select(col("financialStructureID").cast("int")).\
                 agg({"financialStructureID": "max"}).collect()[0][0]
      
        lsOfValues = {"knw_LK_CD_KnowledgeAccountSnapshot": 
                              [
                                ['999999999','UnBifurcated','L1'],
                                ['999999998','UnBalanced','L1']
                              ],
                       "knw_LK_CD_FinancialStructureDetailSnapshot":
                            [
                              ['999999999','999',maxID + 1,fSID],
                              ['999999998','998',maxID + 2,fSID]
                            ]              
                       }
        lstOfColumns = {"knw_LK_CD_KnowledgeAccountSnapshot":
                            {'knowledgeAccountID':"_1",'knowledgeAccountName': "_2",'accountLevel':"_3"},
                          "knw_LK_CD_FinancialStructureDetailSnapshot":
                            {'knowledgeAccountNodeID': "_1","sortOrder":"_2","financialStructureDetailID":"_3","financialStructureID":"_4"},
                          "knw_LK_CD_GLAccountToFinancialStructureDetailSnapshot":
                            {"financialStructureDetailID":"_1","knowledgeAccountNodeID":"_2","chartOfAccount":"_3","glAccountNumber":"_4",
                            "glAccountToFinancialStructureDetailSnapshotID":"_5"}
                         }

        if(fileName =="knw_LK_CD_GLAccountToFinancialStructureDetailSnapshot"):   
          w2 = Window().orderBy(lit("A"))
          maxID = dfSource.select(col("glAccountToFinancialStructureDetailSnapshotID").cast("int")).\
                      agg({"glAccountToFinancialStructureDetailSnapshotID": "max"}).collect()[0][0]
      
          knw_LK_CD_ReportingSetup_chartOfAccount = knw_LK_CD_ReportingSetup.select(col("chartOfAccount")).distinct()
          dfTarget = knw_LK_CD_FinancialStructureDetailSnapshot.select(col("financialStructureDetailID"),\
                     col("knowledgeAccountNodeID")).\
                     join(knw_LK_CD_ReportingSetup_chartOfAccount.select(col("chartOfAccount"))).\
                     withColumn("glAccountNumber",expr("CASE WHEN knowledgeAccountNodeID == '999999999' THEN '#NA#' ELSE 'Unbalanced' END")).\
                     filter(col("knowledgeAccountNodeID").isin(['999999999','999999998']))
          dfTarget = dfTarget.withColumn("glAccountToFinancialStructureDetailSnapshotID",row_number().over(w2) + maxID)      
        else:
          dfTarget = spark.createDataFrame(lsOfValues[fileName]) 
      
        for c in dfSource.columns:
          if (c in lstOfColumns[fileName].keys()):        
            dfTarget = dfTarget.withColumnRenamed(lstOfColumns[fileName].get(c),c)
          else:
            if (c == "publishID"):
              maxID = dfSource.agg({"publishID": "max"}).collect()[0][0]
              dfTarget = dfTarget.withColumn(c,lit(maxID))
            elif(c == "publishDate"):
              dfTarget = dfTarget.withColumn(c,lit(datetime.now()))     
            else:
              dfTarget = dfTarget.withColumn(c,lit(None))

        dfTarget = dfTarget.select([col(c) for c in dfSource.columns])  
        dfTarget = dfSource.union(dfTarget)

        return dfTarget
      except Exception as err:
        raise err


    @staticmethod
    def updateKnowledgeAccountWithLatestPublishID(filePath,fileName):
        try:
            dfSource = objGenHelper.gen_readFromFile_perform(filePath)

            if (fileName in ['knw_LK_CD_KnowledgeAccountSnapshot',
                        'knw_LK_CD_FinancialStructureDetailSnapshot',
                        'knw_LK_CD_GLAccountToFinancialStructureDetailSnapshot']):
                
                 dfSource = ERPOrchestration.updatePlaceHolderValuesForKnowledgeAccounts(dfSource,fileName)
                                  
            if ('publishID' in dfSource.columns)==True:
                dfSource = dfSource.filter(col("publishID") ==(dfSource.agg({"publishID": "max"}).collect()[0][0]))
            dfSource.createOrReplaceTempView(fileName)
            exec("global " + fileName)                      
            dfCreate = "{tabName} = spark.sql('select * from {tabName}')".format(tabName= fileName)
            exec(dfCreate,globals()) 
        except Exception as err:
           raise

    @staticmethod
    def loadAllTables(lstOfTables):
      try:
        isFileExists = True
        schemaName = lstOfTables[0]
        tableName = lstOfTables[1]
        fileName = lstOfTables[2]
        filePath = lstOfTables[3]
            
        if(ERPOrchestration.knowledgeFilesWithLatestPublishID(fileName) == True):
            try:
                dbutils.fs.ls(filePath)
                ERPOrchestration.updateKnowledgeAccountWithLatestPublishID(filePath,fileName)
            except Exception as e:  
                if 'java.io.FileNotFoundException' in str(e): 
                    pass
                    isFileExists = False

        elif(fileName not in globals() and isFileExists == True):
          try:            
            dbutils.fs.ls(filePath)               
            exec("global " + fileName)
            query = "{dfName} = objGenHelper.gen_readFromFile_perform('{filePath}')".\
                                format(dfName = fileName,filePath = filePath)        
            exec(query, globals())  
            query = "{dfName}.createOrReplaceTempView('{dfName}')".format(dfName = fileName)
            exec(query)

            #if knowledge file add the same to gl_metadataDictionary dictionary
            if(gl_knowledgePath == filePath[0:filePath.rfind('/')+1]):
                if fileName in gl_metadataDictionary:
                    gl_metadataDictionary.pop(fileName)
                query = "gl_metadataDictionary[fileName] = {dfName}".format(dfName = fileName)
                exec(query)
                query = "gl_metadataDictionary['{fileName}'].rdd.collect()".format(fileName = fileName)
                exec(query)

          except Exception as e:        
            if 'java.io.FileNotFoundException' in str(e): 
              pass
              isFileExists = False
    
        if(isFileExists == False):
          try:            
            sourceView = uuid.uuid4().hex    
            dfOptional = "f" + uuid.uuid4().hex  
            query = "{dfName} = spark.createDataFrame([], StructType([]))".format(dfName = dfOptional)            
            exec(query,globals())
            exec("global " + fileName)
            query = "{dfName} = objDataTransformation.gen_convertToCDMStructure_generate"\
                                    "(dfSource = {dfSource},schemaName = '{schemaName}',tableName = '{tableName}')[0]"\
                                   .format(dfName = fileName,dfSource = dfOptional,\
                                   schemaName = schemaName, tableName = tableName)                  
            exec(query, globals())
            query = "{dfName}.createOrReplaceTempView('{dfName}')".format(dfName = fileName)
            exec(query)
          except Exception as e:
            raise e
      except Exception as err:
        raise err

    @staticmethod
    def loadAllSourceFiles_load(processID,processStage,
                                isReprocessing = False,
                                executionID = None):
      try:
        
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        if ('objDataTransformation' not in globals()):
                  query = "objDataTransformation = gen_dataTransformation()"
                  exec(query, globals())

        logID = executionLog.init(processID,
                                    validationID = VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION,
                                    tableName = "Load all source files",
                                    executionID = executionID) 

        lstOfTables = list()
        lstOfRoutines = list()

        #get the list of dependent mandatory routines as well
        lstOfRoutines = gl_lstOfScopedAnalytics + objGenHelper.mandatoryRoutines_get(gl_lstOfScopedAnalytics)
        
        lstOfReprocessingStatusToConsider = [False]
        if(isReprocessing == True):
          lstOfReprocessingStatusToConsider.append(True)
    
        fSchema = StructType([StructField("schemaName",StringType(),True),
                StructField("tableName",StringType(),True),
                StructField('fileName',StringType(),True),
                StructField('filePath',StringType(),True),
                StructField("fileType",StringType(),True),
                StructField("executionorder",IntegerType(),True)])        

        if(processStage == PROCESS_STAGE.CLEANSING):
              [lstOfTables.append([t.schemaName,t.tableName,t.tableName,t.filePath,'delta',99999])
                for t in gl_parameterDictionary[FILES.KPMG_ERP_STAGING_RECORD_COUNT].alias("A").\
                join(gl_parameterDictionary['ERPCleansingInputParams'].alias("B"),\
                   [upper(col("A.tableName")) == upper(col("B.fileName"))],how ='inner').\
                select(lit('erp').alias("schemaName"),col("A.tableName"),\
                concat(lit(gl_layer0Staging),col("A.tableName"),lit(".delta")).alias("filePath")).\
                collect()]

        #load from scoping metadata
        if(processID in [PROCESS_ID.DPS,
                        PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                        PROCESS_ID.L1_TRANSFORMATION]):
          
          [lstOfTables.append([t.schemaName,t.tableName,t.fileName,t.filePath,'delta',99999])
           for t in gl_metadataDictionary['dic_ddic_tablesRequiredForERPPackages']\
                    .filter((col("auditProcedureName").isin(lstOfRoutines)) & \
                            (col("ERPSystemID") == gl_ERPSystemID))\
                            .select(col("schemaName"),\
                                    col("tableName"),\
                            concat(col("schemaName"),lit("_"),col("tableName")).alias("fileName"),
                            concat(lit(gl_layer0Staging),col("sparkTableName"),lit(".delta")).alias("filePath"))\
                           .distinct().collect() if t.fileName not in globals()]
      
        #load from stagewise metadata
        lstOfRoutines.append('*')
        for s in gl_metadataDictionary['dic_ddic_processStagewiseSourceFileMapping'].\
                    select(('*'),\
                    split(col('auditProcedure'),",").alias("auditProcedure2")).\
                    select(("*"),explode(col("auditProcedure2")).alias("auditProcedure3")).\
                    filter(((col("processStageID") == PROCESS_STAGE(processStage).value) | (col("processStageID") == -1))&
                    (col("auditProcedure3").isin(lstOfRoutines))).\
                    select(col("schemaName"),
                          col("tableName"),
                          concat(when((col("sourceLocation")=='/cdm-l1/'),lit(gl_CDMLayer1Path))\
                                  .when((col("sourceLocation")=='/staging/l0-stg/'),lit(gl_layer0Staging))\
                                  .when((col("sourceLocation")=='/staging/l0-tmp/knw/'),lit(gl_layer0Temp_knw))\
                                  .when((col("sourceLocation")=='/staging/l0-tmp/fin/'),lit(gl_layer0Temp_fin))\
                                  .when((col("sourceLocation")=='/staging/l0-tmp/gen/'),lit(gl_layer0Temp_gen))\
                                  .when((col("sourceLocation") == '/common-parameters/'),lit(gl_commonParameterPath))\
                                  .when((col("sourceLocation") == '/staging/l2-stg/'),lit(gl_Layer2Staging))\
                                  .when((col("sourceLocation") == '/knowledge/'),lit(gl_knowledgePath))).alias("filePath"),\
                          col("fileName"),
                          col("isFolder"),
                          col("onlyForReprocessing"),
                          col("executionOrder")).\
                          filter(col("onlyForReprocessing").\
                                 isin(lstOfReprocessingStatusToConsider)).distinct().collect():
          if(s.isFolder == True):   
            try:
              dbutils.fs.ls(s.filePath)
            except Exception as e:
              if 'java.io.FileNotFoundException' in str(e): 
                pass
                continue
            for fd in dbutils.fs.ls(s.filePath):        
              if('.' in fd.name):
                fileName = fd.name.split(".")[0]
                if(fileName not in globals()):
                  schemaName = fd.name.split('_')[0] 
                  try:
                    tableName =  fd.name[(fd.name.index('_')+ 1):fd.name.index('.')]            
                    lstOfTables.append([schemaName,
                                   tableName,
                                   fileName,
                                   s.filePath + fd.name.replace("/",""),
                                   fd.name.split(".")[1].replace("/",""),
                                   99999]) 
                  except ValueError:
                    continue
          else:         
            if(s.fileName.split('.')[0] not in globals()):
              lstOfTables.append([s.schemaName,
                             s.tableName,
                             s.fileName.split('.')[0],
                             s.filePath + s.fileName,
                             s.fileName.split('.')[1],
                             s.executionOrder])
            
        dfTables = spark.createDataFrame(data = lstOfTables, schema = fSchema)
        w = Window.partitionBy("fileName").orderBy(desc("fileType"))
        dfTables = dfTables.dropDuplicates(["fileName","filePath"]).\
                   withColumn("id",row_number().over(w)).\
                  filter((col("id") == 1) & (col("tableName") != 'ERP_STAGING_RECORD_COUNT'))

        lstOfTables = list()
        [lstOfTables.append([t.schemaName,t.tableName,t.fileName,t.filePath,t.executionOrder])     
          for t in dfTables.select(col("schemaName"),
                             col("tableName"),
                             col("fileName"),
                             col("filePath"),
                             col("executionOrder")).collect()]
        
        lstOfHierarchies = set()
        [lstOfHierarchies.add(i[4]) for i in lstOfTables]

        for hierarchy in sorted(list(lstOfHierarchies)):
          lstOfInputFiles = list()
          [lstOfInputFiles.append(tab)
           for tab in list(itertools.filterfalse(lambda routine : routine[4] != hierarchy, lstOfTables))]

          with ThreadPoolExecutor(max_workers = MAXPARALLELISM.DEFAULT.value) as executor:
              lstOfLoadStatus = list(executor.map(ERPOrchestration.loadAllTables,lstOfInputFiles))
    
        executionStatus = "Load all source files step completed."            
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
        return[LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

      except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return[LOG_EXECUTION_STATUS.FAILED,executionStatus]
    
    def gen_RemoveDuplicateColumns_get():      
        """Create datafame from metadata dic_ddic_column."""
        try:
            dfKeyColumns = gl_metadataDictionary['dic_ddic_column'].alias("C").\
                  join((gl_metadataDictionary['dic_ddic_RemoveDuplicate']).alias("E"),\
                  [(upper(col("C.schemaName")) == upper(col("E.schemaName"))) & \
                  (upper(col("C.tableName")) == upper(col("E.tableName"))) & \
                  (upper(col("C.ERPSystemID")) == upper(col("E.ERPSystemID")))],how = 'left').\
                  join(gl_parameterDictionary['ERPCleansingInputParams'].alias("I"),\
                  (upper(col("C.schemaName")) == upper(col("I.schemaName"))) & \
                  (upper(col("C.tableName")) == upper(col("I.tableName"))),how="inner").\
                  filter((col("C.ERPSystemID") == gl_ERPSystemID) & \
                  (col("C.isImported") == False) & \
                  (col("C.isKey") == 'X')) \
                  .select(col("C.schemaName")\
                  ,col("C.tableName").alias("tableName")\
                  ,col("C.columnName").alias("columnName")\
                  ,col("C.ERPSystemID").alias("ERPSystemID")\
                  ,col("C.isKey").alias("isKey")\
                  ,col("C.tableCollation").alias("tableCollation")\
                  ,col("E.partitionColumn")\
                  ,col("E.orderByColumn")\
                  ,when(col("E.tableName").isNull(),0).otherwise(1).alias("isExtended"))\
                  .orderBy(col("C.position"))\
                  .groupBy("schemaName","tableName","tableCollation","partitionColumn","orderByColumn","isExtended").\
                  agg(collect_list("columnName").alias("columnList")).cache()
            
            return dfKeyColumns

        except Exception as err:
            raise 

    @staticmethod
    def prepareFilesForCleansing_prepare():
        try:            
            objGenHelper = gen_genericHelper()
           
            logID = executionLog.init(PROCESS_ID.CLEANSING,
                                    validationID = VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION,
                                    tableName = "Prepare files for cleansing") 

            if 'ERPCleansingInputParams' in gl_parameterDictionary:
                gl_parameterDictionary.pop('ERPCleansingInputParams')

            gl_parameterDictionary['ERPCleansingInputParams'] = objGenHelper.\
                        gen_readFromFile_perform(gl_inputParameterPath + "ERPCleansingInputParams.csv").\
                        select(col("executionID"),
                        col("SourceTableSchema").alias("schemaName"),
                        col("SourceTable").alias("tableName"),
                        concat(col("SourceTableSchema"),lit("_"),col("SourceTable")).alias("fileName"))

            ERPOrchestration.setStagingRecordCount()
            df_RD_KeyColumns = ERPOrchestration.gen_RemoveDuplicateColumns_get()

            status = ERPOrchestration.loadAllSourceFiles_load(PROCESS_ID.CLEANSING,
                                               processStage = PROCESS_STAGE.CLEANSING,
                                               isReprocessing = False)

            if(status[0] == LOG_EXECUTION_STATUS.FAILED):
               executionStatus = status[1]               
               return [LOG_EXECUTION_STATUS.FAILED,executionStatus,None,None]

            executionStatus = "Data cleansing prepare completed"
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
            return[LOG_EXECUTION_STATUS.SUCCESS,executionStatus,df_RD_KeyColumns]

        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
            return[LOG_EXECUTION_STATUS.FAILED,executionStatus]
                                
    @staticmethod
    def removeDuplicate_perform(tabledetails):
        try:
            schemaName = tabledetails[0]
            tableName = tabledetails[1]    
            fileName = tabledetails[2]
            filePath = tabledetails[3]
            executionID = tabledetails[4]            
        
            removeDuplicateStatus = app_removeDuplicateRecords_perform(schemaName,tableName,filePath,executionID)
            return [removeDuplicateStatus[0],removeDuplicateStatus[1],fileName,removeDuplicateStatus[2]]
        except Exception as err: 
            executionStatus =   objGenHelper.gen_exceptionDetails_log()                
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus,fileName,None] 

    @staticmethod
    def submitDataCleansing():
        try:
            ls_tabledetails = list()    
            [ls_tabledetails.append([f.schemaName,
                               f.tableName,                               
                               f.tableName,
                               f.filePath,
                               f.executionID]) 
            for f in gl_parameterDictionary['ERPCleansingInputParams'].\
                            select(lower(col("schemaName")).alias("schemaName"),\
                            upper(col("tableName")).alias("tableName"),\
                            concat(lit(gl_layer0Staging),col("schemaName"),lit("_"),\
                            col("tableName"),lit('.delta'))\
                            .alias("filePath"),col("executionID")).distinct().collect()]

            if(len(ls_tabledetails) != 0):
                with ThreadPoolExecutor(max_workers = MAXPARALLELISM.WORKER_COUNT.value) as executor:
                    lstOfDataCleansingStatus = list(executor.map(ERPOrchestration.removeDuplicate_perform,ls_tabledetails))                
                return lstOfDataCleansingStatus
        except Exception as err:
            executionStatus =   objGenHelper.gen_exceptionDetails_log()                
            return [[LOG_EXECUTION_STATUS.FAILED,executionStatus,None]]


    @staticmethod
    def updateStagingRecordStatus(lstOfDuplicateRemovalStatus):
        try:
            lstOfDuplicateRecords = list()
            stagingSchema = StructType([
                                  StructField("tableName", StringType(),False),
                                  StructField("duplicateRows",IntegerType(),False)])

            [lstOfDuplicateRecords.append(l[3]) for l in lstOfDuplicateRemovalStatus if l[3] != None]
            dfDuplicateRecords = spark.createDataFrame(data = lstOfDuplicateRecords, schema = stagingSchema)

            dfDuplicateRecords = dfDuplicateRecords.alias("d").\
                    join(gl_parameterDictionary[FILES.KPMG_ERP_STAGING_RECORD_COUNT].alias("s"),\
                    [col("d.tableName") == col("s.tableName")],how='inner').\
                    select(col("d.tableName"),
                           col("d.duplicateRows"),
                          (col("s.recordCount") - col("d.duplicateRows")).alias("netRecords"))


            dfERPStagingFile = DeltaTable.forPath(spark, gl_layer0Staging + FILES.KPMG_ERP_STAGING_RECORD_COUNT.value)
            dfERPStagingFile.alias("source")\
                      .merge(source = dfDuplicateRecords.alias("dup"),condition = "source.tableName = dup.tableName") \
                      .whenMatchedUpdate(set = {"noOfDuplicateRecords":  col("dup.duplicateRows"),\
                                         "netRecordCount": col("dup.netRecords").cast("string")})\
                      .execute()           
        except Exception as err:
            raise err

    @staticmethod
    def prepareFilesForDPS():
        try:            
            objGenHelper = gen_genericHelper()

            logID = executionLog.init(PROCESS_ID.DPS,
                                    validationID = VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION,
                                    tableName = "Prepare files for DPS") 

            gl_parameterDictionary['ERPDPSInputParams'] = objGenHelper.\
                gen_readFromFile_perform(gl_inputParameterPath + "ERPDPSInputParams.csv").\
                select(col("executionID"),
                       col("SourceTableSchema").alias("schemaName"),
                       col("SourceTable").alias("tableName"))

            ERPOrchestration.setStagingRecordCount()

            status = ERPOrchestration.loadAllSourceFiles_load(PROCESS_ID.DPS,
                                               processStage = PROCESS_STAGE.DPS,
                                               isReprocessing = False)

            if(status[0] == LOG_EXECUTION_STATUS.FAILED):
               executionStatus = status[1]               
               return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

            executionStatus = "DPS prepare completed"
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
            return[LOG_EXECUTION_STATUS.SUCCESS,"DPS prepare completed"]

        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
            return[LOG_EXECUTION_STATUS.FAILED,executionStatus]

    @staticmethod
    def dps_perform(tabledetails):
        try:
            schemaName = tabledetails[0]
            tableName = tabledetails[1]    
            fileName = tabledetails[2]
            filePath = tabledetails[3]
            executionID = tabledetails[4]
            isForShifting = tabledetails[5]
    
            dpsStatus = app_decimalPointShifting_perform(schemaName,tableName,filePath,executionID,isForShifting)
            return [dpsStatus[0],dpsStatus[1],fileName]

        except Exception as err: 
            executionStatus =   objGenHelper.gen_exceptionDetails_log()              
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus,fileName]

    @staticmethod
    def submitDPS():
        try:
            ls_tabledetails = list()                
            lstOfDPSMetadataPrepareStatus = app_decimalPointShiftingMetadata_prepare()      
            if lstOfDPSMetadataPrepareStatus[0] == LOG_EXECUTION_STATUS.SUCCESS:
                if lstOfDPSMetadataPrepareStatus[2]:                     
                    dfDPSQueryList = lstOfDPSMetadataPrepareStatus[3]                                               
                    [ls_tabledetails.append([t.schemaName,
                                               t.tableName,                                     
                                               t.schemaName+ '_' + t.tableName,
                                               gl_layer0Staging + t.schemaName + "_" + t.tableName+'.delta',
                                               t.executionID,
                                               t.isForShifting])                   
                     for t in gl_parameterDictionary['ERPDPSInputParams'].alias("A").\
                          join(dfDPSQueryList.alias("B"),col("A.tableName") == col("B.tableName"),how ="left").\
                          select(lower(col("A.schemaName")).alias("schemaName"),col("A.executionID"),\
                                 upper(col("A.tableName")).alias("tableName"),\
                                 when(col("B.tableName").isNull(),0).otherwise(1).alias("isForShifting")).\
                                 distinct().collect()]  
                else:
                    return [[LOG_EXECUTION_STATUS.SUCCESS,"Decimal Point Shifting Metadata is empty."]]
            else:
                transformationStatusLog.phasewiseExecutionStatusUpdate(LOG_EXECUTION_STATUS.FAILED,PROCESS_STAGE.DPS)
                return [lstOfDPSMetadataPrepareStatus]
            if(len(ls_tabledetails) != 0):
                with ThreadPoolExecutor(max_workers = MAXPARALLELISM.CPU_COUNT_PER_WORKER.value) as executor:
                    lstOfDataCleansingStatus = list(executor.map(ERPOrchestration.dps_perform,ls_tabledetails))
                return lstOfDataCleansingStatus
        
        except Exception as err:    
            executionStatus =   objGenHelper.gen_exceptionDetails_log()
            return [[LOG_EXECUTION_STATUS.FAILED,executionStatus]]
    
    @staticmethod
    def prepareFilesForPreTransformationValidation():
        try:
            isReprocessing = False
            objGenHelper = gen_genericHelper()
            
            gl_parameterDictionary['ERPPreTransformationInputParams'] = objGenHelper.\
                gen_readFromFile_perform(gl_inputParameterPath + "ERPPreTransformationInputParams.csv")\
                .persist()

            params = gl_parameterDictionary['ERPPreTransformationInputParams'].\
                      select(col("executionID"),col("routineName")).limit(1).collect()

            executionID = params[0][0]
            routineName = params[0][1]
            
            transformationStatusLog.createLogFile(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                                                  executionID = executionID)        

            logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                                    validationID = VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION,
                                    tableName = "Prepare files for Pre-transformation validation",
                                    executionID = executionID) 


            dfPreTransRoutines = gl_metadataDictionary['dic_ddic_genericValidationProcedures']\
            .filter((col("processID")== PROCESS_ID.PRE_TRANSFORMATION_VALIDATION.value) & \
            (col("ERPSystemID").cast("int") == gl_ERPSystemID))\
            .select(col("validationID"),
            col("routineName"),\
            lit(executionID).alias("executionID"),
            col("executionOrder").cast('int'),
            col("resultFile"),
            col("parameterValue"),\
            explode(split(col("auditProcedure"),",")).alias("auditProcedure"))
            
            
            dfPreTransRoutines = dfPreTransRoutines.filter(col("auditProcedure").isin(gl_lstOfScopedAnalytics))\
            .select(col("validationID"),
            col("routineName"),\
            col("executionID").alias("executionID"),
            col("executionOrder").cast('int'),
            col("resultFile"),
            col("parameterValue")).distinct().\
            orderBy(col("executionOrder").cast('int')).persist()

            
            #check reprocessing with selected routine
            if(routineName is not None):
                isReprocessing = True
                dfPreTransRoutines = dfPreTransRoutines.alias("A").\
                    join(gl_parameterDictionary['ERPPreTransformationInputParams'].alias("B"),\
                    [lower(col("A.routineName")) == lower(col("B.routineName"))],how = "inner").\
                    select(col("A.validationID"),
                                      col("A.routineName"),\
                                      col("A.executionID"),
                                      col("A.executionOrder"),
                                      col("A.resultFile"),
                                      col("A.parameterValue")).persist()
                    
            dfPreTransValidation = dfPreTransRoutines.select(col("validationID"),
                                                             col("routineName"),
                                                             col("executionID"),
                                                             col("executionOrder"))

            objGenHelper.gen_writeSingleCsvFile_perform(dfPreTransValidation,\
                                                     FILES_PATH.PRE_TRANS_VALIDATION_STATUS_PATH.value \
                                                     + "preTransformationRoutines.csv")

      
            ERPOrchestration.setStagingRecordCount()
            status = ERPOrchestration.loadAllSourceFiles_load(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                                               processStage = PROCESS_STAGE.PRE_TRANSFORMATION_VALIDATION,
                                               isReprocessing = isReprocessing,
                                               executionID = executionID)

            if(status[0] == LOG_EXECUTION_STATUS.FAILED):
               executionStatus = status[1]               
               return [LOG_EXECUTION_STATUS.FAILED,executionStatus,None]

            executionStatus = "Pre-transformation preparation completed"
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 

            return[LOG_EXECUTION_STATUS.SUCCESS,executionStatus,dfPreTransRoutines]

        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus,None]

    @staticmethod
    def submitPreTransformationValidation(dfPreTransRoutines):
      try:    
        objGenHelper = gen_genericHelper()
        lstOfPreTransValidations = list()
                
        executionID = gl_parameterDictionary['ERPPreTransformationInputParams'].\
                      select(col("executionID")).limit(1).collect()[0][0]

        [lstOfPreTransValidations.append([int(v.validationID),
                                        v.routineName,
                                        v.resultFile,
                                        v.parameterValue,
                                        int(v.executionOrder)])
        for v in dfPreTransRoutines.collect()]
        
        executionStatus = ""
        lstOfParamNames = list()
        lstOfPreTransStatus = list()
    
        for proc in (sorted(lstOfPreTransValidations, key=lambda x:x[4])):
            logIDExecutionStatus = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                                      executionID = executionID,
                                      validationID = proc[0],
                                      phaseName = proc[1],
                                      resultFileType = 'E')

            print("Executing '" + proc[1] + "'")
            
            if(proc[3] is None):
              executionStatus = globals()[proc[1]]()
            else:
              parameters =  eval(proc[3])              
              executionStatus = globals()[proc[1]](**parameters)

            lstOfPreTransStatus.append(executionStatus)
            
            executionLog.add(executionStatus[0],logIDExecutionStatus,executionStatus[1],resultFileType = 'E')

        return lstOfPreTransStatus
    
      except Exception as err:
          executionStatus = objGenHelper.gen_exceptionDetails_log()
          return [[LOG_EXECUTION_STATUS.FAILED,executionStatus]]
      
    @staticmethod
    def prepareFilesForTransformation():
      try:
     
          isReprocessing = False
          objGenHelper = gen_genericHelper()
          gl_parameterDictionary['ERPTransformationInputParams'] = objGenHelper.\
              gen_readFromFile_perform(gl_inputParameterPath + "ERPTransformationInputParams.csv")         

          params = gl_parameterDictionary['ERPTransformationInputParams'].\
                   select(col("executionID"),col("routineName")).limit(1).collect()

          gl_parameterDictionary['executionID_TRANSFORMATION'] = params[0][0]
          routineName = params[0][1]
 
          logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,phaseName = "Prepare L1 orchestration")

          dfExecutionOrder = gen_transformationExecutionOrder.\
                              transformationExecutionOrder_get(gl_lstOfScopedAnalytics,gl_ERPSystemID).\
                              persist()
                      
          if(routineName is not None):
              isReprocessing = True
              dfExecutionOrder = dfExecutionOrder.alias("A").\
                                join(gl_parameterDictionary['ERPTransformationInputParams'].alias("B"),
                                   [lower(col("A.objectName")) == lower(col("B.routineName"))],how='inner').\
                                select(col("A.objectName"),
                                      col("A.parameters"),
                                      col("A.hierarchy")).persist()

          objGenHelper.phaseWiseExecutionRoutinesList_prepare(PROCESS_ID.L1_TRANSFORMATION,
                                                              dfExecutionOrder)

          maxParallelism = dfExecutionOrder.\
                             filter(col("hierarchy") >0).\
                             groupBy(col("hierarchy")).\
                             agg(count("hierarchy").alias("maxHierarchy")).\
                             orderBy(col("maxHierarchy").desc()).collect()[0][1]

          if(maxParallelism > MAXPARALLELISM.DEFAULT.value):
                maxParallelism = MAXPARALLELISM.DEFAULT.value
     
          ERPOrchestration.setStagingRecordCount()

          status = ERPOrchestration.loadAllSourceFiles_load(PROCESS_ID.L1_TRANSFORMATION,
                                               processStage = PROCESS_STAGE.L1_TRANSFORMATION,
                                               isReprocessing = isReprocessing)

          if(status[0] == LOG_EXECUTION_STATUS.FAILED):
               executionStatus = status[1]               
               return [[LOG_EXECUTION_STATUS.FAILED,executionStatus,None,None]]
          
          executionStatus = "L1 orchestration scucceded."  
          executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
          return [[LOG_EXECUTION_STATUS.SUCCESS,executionStatus,dfExecutionOrder,maxParallelism]]
      except Exception as err:          
          executionStatus = objGenHelper.gen_exceptionDetails_log()
          executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
          return [[LOG_EXECUTION_STATUS.FAILED,executionStatus,None,None]]

    @staticmethod
    def layerOneTransformation_perform(lstOfClassesAndMethods):      
      try:            
        objectName  = lstOfClassesAndMethods[0]   
        parameters  = lstOfClassesAndMethods[1]
    
        if(parameters !=  ""):
          parameters =  eval(parameters)      
    
        #if it is class
        if('.' in objectName):          
          objClass = globals()[objectName.split('.')[0]]()
          objMethod = objectName.split('.')[1]
          if (parameters != ""):
            result = getattr(objClass,objMethod)(**parameters)
          else:
            result = getattr(objClass,objMethod)()
        else:      
          if (parameters!= ""):
            result = globals()[objectName](**parameters)    
          else:        
            result = globals()[objectName]()
        return result
      except Exception as e:
          executionStatus = objGenHelper.gen_exceptionDetails_log() 
          return [[LOG_EXECUTION_STATUS.FAILED,executionStatus]]        
        
    
    @staticmethod
    def submitTransformation(lstOfClassesAndMethods,maxParallelism):
        try:
            with ThreadPoolExecutor(max_workers = maxParallelism) as executor:
                lstOfTransformationStatus = list(executor.map(ERPOrchestration.layerOneTransformation_perform,lstOfClassesAndMethods))
            return lstOfTransformationStatus
        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()  
            return [[LOG_EXECUTION_STATUS.FAILED,executionStatus]]


    @staticmethod
    def prepareFilesForPostTransformation():
      try:
          isReprocessing = False
          objGenHelper = gen_genericHelper()
          lstOfTransformationValidation = list()
          
          dbutils.fs.mkdirs(gl_reportPath)
          executionID = uuid.uuid4().hex
          
          #get executionID
          if(gl_ERPSystemID != 10):
              gl_parameterDictionary['ERPPostTransformationInputParams']= objGenHelper.\
                      gen_readFromFile_perform(gl_inputParameterPath + "ERPPostTransformationInputParams.csv").\
                      select(col("executionID"),col("routineName"))    

              params = gl_parameterDictionary['ERPPostTransformationInputParams'].\
                          select(col("executionID"),col("routineName")).limit(1).collect()
      
              executionID = params[0][0]
              routineName = params[0][1]
              if(routineName is not None):
                isReprocessing = True

              ERPOrchestration.setStagingRecordCount()

              status = ERPOrchestration.loadAllSourceFiles_load(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                               processStage = PROCESS_STAGE.POST_TRANSFORMATION_VALIDATION,
                                               isReprocessing = isReprocessing)

              if(status[0] == LOG_EXECUTION_STATUS.FAILED):
                   executionStatus = status[1]                   
                   return [LOG_EXECUTION_STATUS.FAILED,executionStatus,None]

              df = spark.sql("SELECT DISTINCT \
            										     clientCode			\
            										    ,companyCode		\
            										    ,clientDataReportingLanguage	AS languageCode		\
            										    ,'clientDataReportingLanguage'	AS languageCodeColumn	\
            					             FROM knw_LK_CD_ReportingSetup	\
            						         UNION ALL						\
            						         SELECT DISTINCT				\
            						     				     clientCode		\
            						     				    ,companyCode	\
            						     				    ,clientDataSystemReportingLanguage	AS languageCode	\
            						     				    ,'clientDataSystemReportingLanguage' AS languageCodeColumn	\
            						     		    FROM knw_LK_CD_ReportingSetup \
            						         UNION ALL							\
            						         SELECT DISTINCT					\
            										     clientCode			\
            										    ,companyCode		\
            										    ,KPMGDataReportingLanguage		AS languageCode		\
            										    ,'KPMGDataReportingLanguage'     AS languageCodeColumn	\
            							     FROM knw_LK_CD_ReportingSetup	")

              df.createOrReplaceTempView("knw_LK_CD_ReportingLanguage")

              dfTransformationValidations = objGenHelper.transformationValdation_get\
                                                 (gl_ERPSystemID,\
                                                 gl_lstOfScopedAnalytics).persist()

              if(routineName is not None):                 
                  dfTransformationValidations = dfTransformationValidations.alias("A").\
                                                join(gl_parameterDictionary['ERPPostTransformationInputParams'].alias("B"),\
                                                [lower(col("A.routineName")) == lower(col("B.routineName"))],how = 'inner').\
                                                select(col("A.validationID"),
                                                       col("A.routineName"),
                                                       col("A.methodName"),
                                                       col("A.parameterValue"),
                                                       col("A.resultFile"),
                                                       col("A.fileType")).persist()

          else:

              for fType in gl_parameterDictionary["InputParams"].\
                           filter(col("IsCustomTransform")== True).\
                           select(col("fileType")).distinct().rdd.flatMap(lambda x: x).collect():
                  gl_fileTypes['Succeeded'].add(fType.upper())

              status = ERPOrchestration.loadAllSourceFiles_load(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                               processStage = PROCESS_STAGE.POST_TRANSFORMATION_VALIDATION)

              if(status[0] == LOG_EXECUTION_STATUS.FAILED):
                   executionStatus = status[1]                   
                   return [LOG_EXECUTION_STATUS.FAILED,executionStatus,None]

              #get all transformaiton validations
              dfTransformationValidations = objGenHelper.transformationValdation_get\
                                                 (gl_ERPSystemID,\
                                                 gl_lstOfScopedAnalytics,gl_fileTypes["Succeeded"]).persist()


          objGenHelper.phaseWiseExecutionRoutinesList_prepare(PROCESS_ID.TRANSFORMATION_VALIDATION,\
                                                     dfTransformationValidations)

          #generate the summay and detail files for all the required validations
          lstOfResultFiles = list ()
          [lstOfResultFiles.append([int(f.validationID),
                              'ValidationSummary-' + f.resultFile + ".csv",
                              'ValidationDetails-' + f.resultFile + ".csv"])
          for f in dfTransformationValidations.select(col("validationID"),\
                      col("resultFile")).\
                      filter(col("validationID")!= VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION.value)\
                      .distinct().collect()]

          for eachFile in lstOfResultFiles:
              transformationStatusLog.createLogFile\
                                       (PROCESS_ID.TRANSFORMATION_VALIDATION,
                                       validationID = eachFile[0],
                                       summaryFileName = eachFile[1],
                                       detailFileName = eachFile[2])

         #prepare the list of validation routine for execution
          
          [lstOfTransformationValidation.append([proc.validationID,
                                         proc.routineName,
                                         proc.parameterValue,
                                         proc.resultFile,
                                         proc.fileType,
                                         executionID])
          for proc in dfTransformationValidations.collect()]
                          
          executionStatus = "Preparation of transformation validation completed"
          return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus,lstOfTransformationValidation] 

      except Exception as err:
          executionStatus = objGenHelper.gen_exceptionDetails_log()          
          return [LOG_EXECUTION_STATUS.FAILED,executionStatus,lstOfTransformationValidation] 


    @staticmethod
    def submitTransformationValidation(lstOfMethods):
        try:
            validationID = lstOfMethods[0]
            routineName = lstOfMethods[1]
            parameters = lstOfMethods[2]
            resultFile = lstOfMethods[3]
            fileType = lstOfMethods[4]
            executionID = lstOfMethods[5]

            logIDExecutionStatus = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                                      executionID = executionID,
                                                      validationID = validationID,
                                                      phaseName = routineName,
                                                      fileType = fileType,
                                                      resultFileType = 'E')
               
            print('Executing ' + routineName)

            if(parameters is None or parameters == ""):      
              executionStatus = globals()[routineName]()
            else:
              param = eval(parameters)
              executionStatus = globals()[routineName](**param)

            executionLog.add(executionStatus[0],logIDExecutionStatus,executionStatus[1],resultFileType = 'E')
      
            return executionStatus

        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            return [[LOG_EXECUTION_STATUS.FAILED,executionStatus]]

    @staticmethod
    def submitPostTransformationValidation(lstOfValidations):
        try:
            with ThreadPoolExecutor(max_workers = gl_maxParallelism) as executor:
                lstOfValidationStatus = list(executor.map(ERPOrchestration.submitTransformationValidation,lstOfValidations))
            return lstOfValidationStatus
        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            return [[LOG_EXECUTION_STATUS.FAILED,executionStatus]]


    @staticmethod
    def prepareFilesForImportValidation(processID,
                                        fileID,
                                        fileName,
                                        fileType,
                                        validationID,
                                        executionID
                                        ):
        try:
            objGenHelper = gen_genericHelper()

            logID = executionLog.init(processID = processID,
                                      fileID = fileID,
                                      fileName = fileName,
                                      fileType = fileType,
                                      validationID = validationID,
                                      executionID = executionID)

            dfImportValidation = objGenHelper.importValdation_populate(gl_ERPSystemID).persist()            
            executionStatus = "Import validation preparation completed."
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)   
            return dfImportValidation
        except Exception as err:
            executionStatus =  "Prepare import validation failed for the file '" + fileID + "'." + objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
            raise err
    
    @staticmethod
    def importValidationPerform(lstOfValidations):
        try:
            validationID = lstOfValidations[0]
            routineName = lstOfValidations[1]
            parameters = lstOfValidations[2]

            print('Executing ' + routineName)

            if(parameters is None or parameters == ""):      
                executionStatus = globals()[routineName]()
            else:
                param = eval(parameters)
                executionStatus = globals()[routineName](**param)

            return executionStatus

        except Exception as err:
            raise

    @staticmethod
    def submitImportValidation(dfValidations, lstOfValidations):
        try:

           lstOfClassesAndMethods = dfImportValidation.filter(col("validationID").isin(lstOfValidations)) \
                                    .select(col("validationID"),col("routineName"),col("parameterValue")).collect()
           
           with ThreadPoolExecutor(max_workers = MAXPARALLELISM.DEFAULT.value) as executor:
               lstOfValidationStatus = list(executor.map(ERPOrchestration.importValidationPerform,lstOfClassesAndMethods)) 
           return lstOfValidationStatus
        except Exception as err:
            raise err
