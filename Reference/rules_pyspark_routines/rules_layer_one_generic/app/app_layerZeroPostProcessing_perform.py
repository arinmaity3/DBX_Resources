# Databricks notebook source
import sys
import traceback

class app_layerZeroPostProcessing_perform():

    def collectAllRawFiles(self,dfSource,fileID,fileType,fileName,executionID):
        try:
            objGenHelper = gen_genericHelper()            
            statusID = 0

            lstOfImportStatus = list()
            [lstOfImportStatus.append([log["logID"],
                      log["fileID"],
                      log["fileName"],
                      log["fileType"],
                      log["tableName"],
                      PROCESS_ID(log["processID"]).value,
                      log["validationID"],
                      log["routineName"],
                      log["statusID"],
                      log["status"],
                      log["startTime"],
                      log["endTime"],
                      log["comments"]])
            for log in list(gl_executionLog.values())]

            dfImportStatus = spark.createDataFrame(data = lstOfImportStatus,schema = gl_logSchema).\
                filter( (col("fileID") == fileID) & (col("statusID") != 1))
          
            logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,fileID,fileName,fileType,VALIDATION_ID.PACKAGE_FAILURE_IMPORT,executionID = executionID) 
                               
            if(dfImportStatus.count() == 0 and dfSource.count() != 0):  
                executionStatus = "Import validations succesfully completed"
                statusID = 1                                
                gView = fileType.lower() + "_" + fileID.replace('-','_').lower()                
                dfSource.createOrReplaceGlobalTempView(gView)  
                print("Layer zero post-import step completed successfully.")                
            else:
                statusID = 0
                executionStatus = "Some L0 validation got failed for file '" + fileID + \
                                  "',that will not be transformed,please check the validation results"
                print(executionStatus)
                executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)                    
        except Exception as e: 
            statusID = 0          
            executionStatus =  objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            print(executionStatus)                        
        finally:               
            if(statusID != 0):
                executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)                 
            layerZeroProcessingStatus.updateExecutionStatus()   

