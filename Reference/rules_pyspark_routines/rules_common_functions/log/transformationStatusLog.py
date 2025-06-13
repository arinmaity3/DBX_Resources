# Databricks notebook source
from datetime import datetime,timedelta
import inspect
import uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import os
from delta.tables import *
import builtins as builtins

def blobFileDetails_get(processID,executionID = "",validationID = "",resultFileType = ""):
      try:
        
        global blob_service_client
        global dictOfValidationResultFiles
        global blob_client
        
        if(processID == PROCESS_ID.IMPORT_VALIDATION):
            importResultPath = FILES_PATH.IMPORT_VALIDATION_RESULTS_PATH.value

            if(resultFileType == 'S'):
                fileNameSummary = "ValidationSummary-{executionID}.csv".format(executionID = executionID)
                container_client = blob_service_client.get_container_client(importResultPath)
                blob_client = container_client.get_blob_client(fileNameSummary)

            elif(resultFileType == 'D'):
                fileNameDetail = "ValidationDetails-{executionID}.csv".format(executionID = executionID)
                container_client = blob_service_client.get_container_client(importResultPath)
                blob_client = container_client.get_blob_client(fileNameDetail)
                        
            return blob_client

        elif(processID == PROCESS_ID.IMPORT):         
            container_client = blob_service_client.get_container_client(FILES_PATH.IMPORT_STATUS_PATH.value)
            blob_client = container_client.get_blob_client(FILES.EXECUTION_STATUS_SUMMARY_FILE.value)
            return blob_client

        elif(processID == PROCESS_ID.CLEANSING):
            container_client = blob_service_client.get_container_client(FILES_PATH.CLEANSING_STATUS_PATH.value)
            blob_client = container_client.get_blob_client(FILES.EXECUTION_STATUS_SUMMARY_FILE.value)
            return blob_client

        elif(processID == PROCESS_ID.DPS):
            container_client = blob_service_client.get_container_client(FILES_PATH.DPS_STATUS_PATH.value)
            blob_client = container_client.get_blob_client(FILES.EXECUTION_STATUS_SUMMARY_FILE.value)
            return blob_client

        elif(processID == PROCESS_ID.PRE_TRANSFORMATION):
            container_client = blob_service_client.get_container_client(FILES_PATH.PRE_TRANS_STATUS_PATH.value)
            blob_client = container_client.get_blob_client(FILES.EXECUTION_STATUS_SUMMARY_FILE.value)
            return blob_client
        
        elif(processID == PROCESS_ID.PRE_TRANSFORMATION_VALIDATION):
            preTransResultPath = FILES_PATH.PRE_TRANS_VALIDATION_STATUS_BLOB_PATH.value

            if(resultFileType == 'S'):                   
                fileNameSummary = "ValidationSummary-{executionID}.csv".format(executionID = executionID)
                container_client = blob_service_client.get_container_client(preTransResultPath)            
                blob_client = container_client.get_blob_client(fileNameSummary)

            elif(resultFileType == 'D'):
                fileNameDetail = "ValidationDetails-{executionID}.csv".format(executionID = executionID)
                container_client = blob_service_client.get_container_client(preTransResultPath)
                blob_client = container_client.get_blob_client(fileNameDetail)
            
            elif(resultFileType == 'E'):
                container_client = blob_service_client.get_container_client(preTransResultPath)
                blob_client = container_client.get_blob_client(FILES.EXECUTION_STATUS_SUMMARY_FILE.value)

            return blob_client

        elif(processID == PROCESS_ID.L1_TRANSFORMATION):
            container_client = blob_service_client.get_container_client(FILES_PATH.L1_TRANSFORMATION_STATUS_PATH.value)
            blob_client = container_client.get_blob_client(FILES.EXECUTION_STATUS_SUMMARY_FILE.value)
            return blob_client

        elif(processID == PROCESS_ID.TRANSFORMATION_VALIDATION):
          
            if(resultFileType == 'S'):
                summaryFileName = ",".join(gl_validationResultFiles[validationID]).split(",")[0]
                container_client = blob_service_client.get_container_client(FILES_PATH.TRANSFORMATION_VALIDATION_STATUS_PATH.value)
                blob_client = container_client.get_blob_client(summaryFileName)

            elif(resultFileType == 'D'):
                detailFileName = ",".join(gl_validationResultFiles[validationID]).split(",")[1]
                container_client = blob_service_client.get_container_client(FILES_PATH.TRANSFORMATION_VALIDATION_STATUS_PATH.value)
                blob_client = container_client.get_blob_client(detailFileName)

            elif(resultFileType == 'E'):
                container_client = blob_service_client.get_container_client(FILES_PATH.TRANSFORMATION_VALIDATION_STATUS_PATH.value)
                blob_client = container_client.get_blob_client(FILES.EXECUTION_STATUS_SUMMARY_FILE.value)

            return blob_client
        elif(processID == PROCESS_ID.L2_TRANSFORMATION):
            container_client = blob_service_client.get_container_client(FILES_PATH.L2_TRANSFORMATION_STATUS_BLOB_PATH.value)
            blob_client = container_client.get_blob_client(FILES.EXECUTION_STATUS_SUMMARY_FILE.value)            
            return blob_client
      except Exception as err:
        raise

class transformationStatusLog():

    @staticmethod
    def initializeLogs(storageAccount):
        try:
           
            #transformationStatus.csv
            global applicationId
            global authenticationKey
            global tenantId

            global container_client
            global blob_client
            global blob_service_client

            applicationId = dbutils.secrets.get(scope="rules-scope",key="ClientId")
            authenticationKey = dbutils.secrets.get(scope="rules-scope",key="ClientSecret")    
            tenantId = dbutils.secrets.get(scope="rules-scope",key="TenantId")
            
            os.environ["AZURE_TENANT_ID"] = tenantId
            os.environ["AZURE_CLIENT_SECRET"] = authenticationKey
            os.environ["AZURE_CLIENT_ID"] = applicationId

            applicationId = dbutils.secrets.get(scope="rules-scope",key="ClientId")  
            token_credential = DefaultAzureCredential(managed_identity_client_id = applicationId ,exclude_interactive_browser_credential=False)
                       
            storageAccountURL = "https://" + storageAccount + ".blob.core.windows.net"
            blob_service_client = BlobServiceClient(
                account_url = storageAccountURL,
                credential = token_credential
            )
            
        except Exception as err:
            raise
   
    @staticmethod
    def transformationStatusUpdate(lstOfStatusInfo,
                                   processID = None,
                                   executionID = None,
                                   validationID = None,
                                   resultFileType = None):
        try:
          
           
            statusInfo = ""
            if(resultFileType is None or resultFileType == "S" or resultFileType == "E"):
              lstOfStatusInfo = [lstOfStatusInfo]
              if(resultFileType is None):
                  resultFileType = "S"              
                  
                        
            cols = len(lstOfStatusInfo[0])
            for eachLine in lstOfStatusInfo:
              eachRow = ""
              for index in range(0,cols):
                eachRow = eachRow + "{quotes}{col}{quotes}{delimiter}".\
                          format(quotes = '"',col = eachLine[index],delimiter =",")    
              statusInfo = statusInfo + eachRow[:-1] + "\n"
                         
            global blob_service_client
            global blob_client 
            
            if(processID is None):                                       
              blob_client.append_block(statusInfo)
              
            elif(processID == PROCESS_ID.IMPORT_VALIDATION):                            
              blob_client = blobFileDetails_get(processID,
                                                executionID = executionID,
                                                resultFileType = resultFileType)
              blob_client.append_block(statusInfo) 
              
            elif(processID == PROCESS_ID.IMPORT):
                blob_client = blobFileDetails_get(processID)
                blob_client.append_block(statusInfo) 

            elif(processID == PROCESS_ID.CLEANSING):
                blob_client = blobFileDetails_get(processID)
                blob_client.append_block(statusInfo) 

            elif(processID == PROCESS_ID.DPS):
                blob_client = blobFileDetails_get(processID)
                blob_client.append_block(statusInfo) 

            elif(processID == PROCESS_ID.PRE_TRANSFORMATION):
                blob_client = blobFileDetails_get(processID)
                blob_client.append_block(statusInfo) 

            elif(processID == PROCESS_ID.PRE_TRANSFORMATION_VALIDATION):                  
                blob_client = blobFileDetails_get(processID,
                                                  executionID = executionID,
                                                  resultFileType = resultFileType)
                blob_client.append_block(statusInfo)               
                
            elif(processID == PROCESS_ID.L1_TRANSFORMATION):
                blob_client = blobFileDetails_get(processID)
                blob_client.append_block(statusInfo) 

            elif(processID == PROCESS_ID.TRANSFORMATION_VALIDATION):
                blob_client = blobFileDetails_get(processID,
                                                  validationID = validationID,
                                                  resultFileType = resultFileType)
                blob_client.append_block(statusInfo) 

            elif(processID == PROCESS_ID.L2_TRANSFORMATION):
                blob_client = blobFileDetails_get(processID)
                blob_client.append_block(statusInfo) 

        except Exception as err:
            raise

    @staticmethod
    def createLogFile(processID,executionID = "",option = True, **kwargs):
        try:
          
            global blob_service_client
            global gl_validationResultFiles

            if ('gl_validationResultFiles' not in globals()):
              gl_validationResultFiles = {}

            if(processID == PROCESS_ID.IMPORT_VALIDATION):                       
              blob_client = blobFileDetails_get(processID,executionID = executionID,resultFileType = 'S')

              if (blob_client.exists()):
                  blob_client.delete_blob()                              
              blob_client.upload_blob(",".join(gl_ImportValidationSummarySchema.fieldNames()) + "\n",blob_type = "AppendBlob") 
              
              blob_client = blobFileDetails_get(processID,executionID = executionID,resultFileType = 'D')
              if (blob_client.exists()):
                  blob_client.delete_blob()
              blob_client.upload_blob(",".join(gl_ValidationResultDetailSchema.fieldNames()) + "\n",blob_type = "AppendBlob") 

            elif(processID == PROCESS_ID.IMPORT):                
                blob_client = blobFileDetails_get(processID)
                if (blob_client.exists()):
                  blob_client.delete_blob()  
                blob_client.upload_blob(",".join(gl_ImportStatusSchema.fieldNames()) + "\n",blob_type = "AppendBlob")

            elif(processID == PROCESS_ID.CLEANSING):
                blob_client = blobFileDetails_get(processID)
                if (blob_client.exists()):
                  blob_client.delete_blob()  
                blob_client.upload_blob(",".join(gl_CleansingStatusSchema.fieldNames()) + "\n",blob_type = "AppendBlob")

            elif(processID == PROCESS_ID.DPS):
                blob_client = blobFileDetails_get(processID)
                if (blob_client.exists()):
                  blob_client.delete_blob()  
                blob_client.upload_blob(",".join(gl_DPSStatusSchema.fieldNames()) + "\n",blob_type = "AppendBlob")

            elif(processID == PROCESS_ID.PRE_TRANSFORMATION):
                blob_client = blobFileDetails_get(processID)
                if (blob_client.exists()):
                  blob_client.delete_blob()  
                blob_client.upload_blob(",".join(gl_PreTransformationSchema.fieldNames()) + "\n",blob_type = "AppendBlob")
            
            elif(processID == PROCESS_ID.PRE_TRANSFORMATION_VALIDATION):  
                
                blob_client = blobFileDetails_get(processID,executionID = executionID,resultFileType = 'S')                
                if (blob_client.exists()):
                  blob_client.delete_blob()                
                blob_client.upload_blob(",".join(gl_PreTransValidationSummarySchema.fieldNames()) + "\n",blob_type = "AppendBlob")

                blob_client = blobFileDetails_get(processID,executionID = executionID,resultFileType = 'D')
                if (blob_client.exists()):
                  blob_client.delete_blob()                
                blob_client.upload_blob(",".join(gl_ValidationResultDetailSchema.fieldNames()) + "\n",blob_type = "AppendBlob")

                blob_client = blobFileDetails_get(processID,executionID = executionID,resultFileType = 'E')
                if (blob_client.exists()):
                  blob_client.delete_blob()                
                blob_client.upload_blob(",".join(gl_PreTransformationSchema.fieldNames()) + "\n",blob_type = "AppendBlob")

            elif(processID == PROCESS_ID.L1_TRANSFORMATION):
                blob_client = blobFileDetails_get(processID)
                if (blob_client.exists()):
                  blob_client.delete_blob()  
                blob_client.upload_blob(",".join(gl_TransformationStatusSchema.fieldNames()) + "\n",blob_type = "AppendBlob")

            elif(processID == PROCESS_ID.TRANSFORMATION_VALIDATION):
                validationID = kwargs['validationID']                
                summaryFileName = kwargs['summaryFileName']
                detailFileName = kwargs['detailFileName']
                if(validationID in gl_validationResultFiles.keys()):
                  gl_validationResultFiles.pop(validationID)
                
                gl_validationResultFiles[validationID] = [summaryFileName,detailFileName]                    
                
                blob_client = blobFileDetails_get(processID,validationID = validationID,resultFileType = 'S')                
                if (blob_client.exists()):
                  blob_client.delete_blob()                
                blob_client.upload_blob(",".join(gl_TransformationValidationSummarySchema.fieldNames()) + "\n",blob_type = "AppendBlob")
                
                blob_client = blobFileDetails_get(processID,validationID = validationID,resultFileType = 'D')
                if (blob_client.exists()):
                  blob_client.delete_blob()                
                blob_client.upload_blob(",".join(gl_ValidationResultDetailSchema.fieldNames()) + "\n",blob_type = "AppendBlob")
                
                blob_client = blobFileDetails_get(processID,validationID = validationID,resultFileType = 'E')                
                if (blob_client.exists()):
                  blob_client.delete_blob()                
                blob_client.upload_blob(",".join(gl_ExecutionSummarySchema.fieldNames()) + "\n",blob_type = "AppendBlob")

            elif(processID == PROCESS_ID.L2_TRANSFORMATION):

                blob_client = blobFileDetails_get(processID)
                if (blob_client.exists()):
                  blob_client.delete_blob()  
                blob_client.upload_blob(",".join(gl_TransformationStatusSchema.fieldNames()) + "\n",blob_type = "AppendBlob")
                  
        except Exception as err:
            print(err)
            raise 
    
    def __processStagesDefined(processStage):
        try:
            
            if(processStage == ORCHESTRATION_PHASE.DM_LAYER_ZERO_LAYER_ONE):
                definedStages = [PROCESS_STAGE.L1_TRANSFORMATION]
            else:
                definedStages = [PROCESS_STAGE.UPLOAD,
                                               PROCESS_STAGE.IMPORT,
                                               PROCESS_STAGE.CLEANSING,
                                               PROCESS_STAGE.DPS,
                                               PROCESS_STAGE.PRE_TRANSFORMATION_VALIDATION,
                                               PROCESS_STAGE.L1_TRANSFORMATION,
                                               PROCESS_STAGE.POST_TRANSFORMATION_VALIDATION,
                                               PROCESS_STAGE.L2_TRANSFORMATION]
            return definedStages
        except Exception as err:
            raise err

    @staticmethod
    def phasewiseExecutionStatusInit(orchestrationPhase = None):
        try:
            objGenHelper = gen_genericHelper()
            global lstOfProcessStage
            lstOfProcessStage = list()

            lstOfSelectedStages = list()
            lstOfCompletedStages = list()
            lstOfUpcomingStages = list()

            processStagesDefined = transformationStatusLog.__processStagesDefined(orchestrationPhase)

            statusFilePath = gl_inputParameterPath + FILES.KPMG_PROCESS_STAGE_STATUS_FILE.value
            
            ERPSystemID = int(objGenHelper.gen_readFromFile_perform(gl_commonParameterPath + "knw_LK_CD_Parameter.csv").\
                          select(col("parameterValue")).\
                          filter(col("name") == 'ERP_SYSTEM_ID').collect()[0][0])

            statusSchema = StructType([StructField("WBTaskTypeID",ShortType(),False),
                                           StructField("statusID",ShortType(),False),                                                
                                           StructField("Status",StringType(),False),                                               
                                           StructField("TaskTypeName", StringType(),True),
                                           StructField("eventTime", StringType(),True)                               
                                          ])

            [lstOfSelectedStages.append(k)
             for k,v in gl_processStagewiseOrder.items() if v in gl_processStage]

            [lstOfUpcomingStages.append(v)
             for k,v in gl_processStagewiseOrder.items() if k > builtins.max(lstOfSelectedStages)]

            [lstOfCompletedStages.append(v)
             for k,v in gl_processStagewiseOrder.items() if k < builtins.min(lstOfSelectedStages)]

            for p in PROCESS_STAGE:
              if(p not in (processStagesDefined)):
                  status = LOG_EXECUTION_STATUS.NOT_APPLICABLE
              elif((p in gl_processStage) or (p in lstOfUpcomingStages)):
                  status = LOG_EXECUTION_STATUS.NOT_STARTED
              elif(p in lstOfCompletedStages):
                  status = LOG_EXECUTION_STATUS.SUCCESS                 
                  
              if(p == PROCESS_STAGE.DPS):
                  if(ERPSystemID != 12):
                      lstOfProcessStage.append([int(p.value),status.value,\
                              status.name,'decimalpointshifting',None])
              elif(p == PROCESS_STAGE.PRE_TRANSFORMATION_VALIDATION):
                lstOfProcessStage.append([int(p.value),status.value,\
                              status.name,'pretransformationvalidation',None])
              elif(p == PROCESS_STAGE.POST_TRANSFORMATION_VALIDATION):
                lstOfProcessStage.append([int(p.value),status.value,\
                              status.name,'posttransformationvalidation',None])
              elif(p == PROCESS_STAGE.L1_TRANSFORMATION):
                lstOfProcessStage.append([int(p.value),status.value,\
                              status.name,'transformation',None])
              else:                
                lstOfProcessStage.append([int(p.value),status.value,status.name,p.name.lower(),None])
                
            dfProcessStage =  spark.createDataFrame(data = lstOfProcessStage, schema = statusSchema)                    
            objGenHelper.gen_writeToFile_perfom(dfProcessStage,statusFilePath)              
            objGenHelper.gen_readFromFile_perform(statusFilePath).createOrReplaceTempView("processingStageStatus") 
            gl_parameterDictionary[FILES.KPMG_PROCESS_STAGE_STATUS_FILE] = spark.sql("select * from processingStageStatus")

        except Exception as err:
            raise   

    @staticmethod
    def phasewiseExecutionStatusUpdate(statusID, stageID,):
        try:

            statusFilePath = gl_inputParameterPath + FILES.KPMG_PROCESS_STAGE_STATUS_FILE.value
            appStatusFilePath = gl_inputParameterPath + FILES.APP_PROCESS_STAGE_STATUS_FILE.value

            dfStageStatus = DeltaTable.forPath(spark, statusFilePath) 

            if(statusID == LOG_EXECUTION_STATUS.COMPLETED):
                df = gl_parameterDictionary[FILES.KPMG_PROCESS_STAGE_STATUS_FILE].\
                          filter(col("statusID") == LOG_EXECUTION_STATUS.IN_PROGRESS.value ).\
                          select(col("WBTaskTypeID"))
                if(df.take(1)):
                    statusID = LOG_EXECUTION_STATUS.SUCCESS
                    currentStageID = df.collect()[0][0]

                    dfStageStatus.update(
                    condition = "WBTaskTypeID = " + str(currentStageID),
                    set = { "statusID": str(statusID.value),
                            "Status": "'" + str(statusID.name) + "'",
                            "eventTime": "'" + str(datetime.now()) + "'"})
                
            elif(statusID == LOG_EXECUTION_STATUS.FAILED):             
                dfStageStatus.update(
                condition = "WBTaskTypeID = " + str(stageID.value),
                set = { "statusID": str(statusID.value),
                        "Status": "'" + str(statusID.name) + "'",
                        "eventTime": "'" + str(datetime.now()) + "'"})
            else:  
            
                #update current stage's status

                dfStageStatus.update(
                condition = "WBTaskTypeID = " + str(stageID.value),
                set = { "statusID": str(statusID.value),
                        "Status": "'" + str(statusID.name) + "'",
                        "eventTime": "'" + str(datetime.now()) + "'"})
  
                for k,v in gl_processStagewiseOrder.items():
                    if v == stageID:    
                        nextStageID = k + 1
                        break
      
                #update next stage as in-progress
                if(nextStageID <=7):      
                    nextStageID = PROCESS_STAGE(gl_processStagewiseOrder[nextStageID])
    
                    dfStageStatus.update(
                    condition = "WBTaskTypeID = " + str(nextStageID.value),
                    set = { "statusID": str(LOG_EXECUTION_STATUS.IN_PROGRESS.value),
                            "Status": "'" + str(LOG_EXECUTION_STATUS.IN_PROGRESS.name) + "'",
                            "eventTime": "'" + str(datetime.now()) + "'"})
  
            
            objGenHelper.gen_writeSingleCsvFile_perform(df = spark.sql("select * from processingStageStatus")\
                                                  ,targetFile = appStatusFilePath)

        except Exception as err:
            raise
