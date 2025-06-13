# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import col, lower
import uuid

class app_layerOnePreProcessing_perfom():
  def layerOnePreProcessingValidation_perform(self):
    try:      

      global gl_lstOfSourceFiles      
      global gl_isCustomTransform
     
      objGenHelper = gen_genericHelper()
      preProcessingStatus = True
      executionStatus = "" 
      executionStatusID=LOG_EXECUTION_STATUS.SUCCESS
      logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,\
                                validationID = VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION)  

      print("Reading execution logs from gloabl temp view...")

      importLogDetails = {}
      dfExecutionLog = spark.createDataFrame([], gl_logSchema)            
      for f in gl_parameterDictionary["InputParams"].\
               select(lower(col("fileID"))).\
               filter(col("IsCustomTransform")==gl_isCustomTransform).rdd.collect():

          logView = 'executionlog_' + f[0].replace('-','_')    
          try:    
              dfExecutionLog = dfExecutionLog.union(spark.read.table('global_temp.' + logView))    
              spark.catalog.dropGlobalTempView(logView)
          except Exception as e:    
              print("execution log view does not exist for the file:" + f[0])
            
         
      lstOfKeys = gl_logSchema.fieldNames()

      for r in dfExecutionLog.collect():
          lstOfValues = list()
          processID = int(r["processID"])
          statusID = int(r["statusID"])
          lstOfValues = [r["logID"],               
                         r['fileID'],
                         r['fileName'],
                         r['fileType'],
                         r['tableName'],
                         PROCESS_ID(processID),
                         r['validationID'],
                         r['routineName'],
                         LOG_EXECUTION_STATUS(statusID),
                         r['status'],
                         r['startTime'],
                         r['endTime'],
                         r['comments']]
  
          logDetails  = dict(zip(lstOfKeys, lstOfValues))
          importLogDetails[r["logID"]] = logDetails

      dfExecutionLog = objGenHelper.generateExecutionLogFile(importLogDetails,\
                                                            gl_logSchema,\
                                                            gl_executionLogResultPath + "ExecutionLogDetails.csv")

     # logDetails = objGenHelper.gen_executionLog_collect()
      #dfExecutionLog = logDetails[1]
      gl_executionLog.update(importLogDetails)
      print("Execution log repopulated...")
      for f in gl_parameterDictionary["InputParams"].\
               select(lower(col("fileType")),lower(col("fileID"))).\
               filter(col("IsCustomTransform") == gl_isCustomTransform).rdd.collect():
        fType = f[0] 
        fID = f[1]
        glView = fType.lower() + "_" + fID.replace('-','_').lower()
        gl_fileTypes["Input"].add(fType.upper())
        try:
          df = spark.read.table('global_temp.' + glView)
        except:
          print("view: " + glView + " does not exist")
          gl_fileTypes["Failed"].add(fType.upper())
        else:
          gl_lstOfSourceFiles[fID.upper()] = {fType.upper(): df}
          gl_fileTypes["Succeeded"].add(fType.upper())
          #spark.catalog.hdropGlobalTempView(glView)

      if (dfExecutionLog.filter(col("statusID") == 0).count() > 0):
        ls_ErrorRows =  dfExecutionLog.filter((col("statusID") == 0))\
                        .select(col("fileID"),\
                        col("routineName"),col("comments")).collect()
        ls_ErrorMsgs = "".join([ '   File ID: '+ str(each_error_row.__getattr__("fileID")) +', Failed validation: '+str(each_error_row.__getattr__("routineName")) + ',Error Detail: "'+str(each_error_row.__getattr__("comments") +'"')
                                  for each_error_row in ls_ErrorRows])
        executionStatus = "L0 validations failed. Please check import validation results for more details)" + str(ls_ErrorMsgs)             
        return

      executionStatus = "Layer one pre processing completed."           
      return
    except Exception as e:       
      preProcessingStatus = False
      executionStatus = objGenHelper.gen_exceptionDetails_log()    
    finally:
      if(preProcessingStatus == False or not(gl_fileTypes["Succeeded"])):
          executionStatusID=LOG_EXECUTION_STATUS.FAILED
      elif gl_fileTypes["Failed"]:
          executionStatusID=LOG_EXECUTION_STATUS.WARNING
      else:
          executionStatusID=LOG_EXECUTION_STATUS.SUCCESS
      executionLog.add(executionStatusID,logID,executionStatus) 
      return [executionStatusID,executionStatus]


  def layerOnePreProcessingValidationForDataFlow_perform(self):
      try:
            objGenHelper = gen_genericHelper()            
            logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,
                                      validationID = VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION)  

            objGenHelper = gen_genericHelper()

            global gl_lstOfSourceFiles            
            gl_lstOfSourceFiles = {}
            
            #Incase of multiple files with same file type, we have to rebuild the 
            #input paramter with unique file type, otherwise duplicate data will populate 
            #since the data is reading from delta file based on the input parameter

            w = Window.partitionBy("fileType").orderBy(desc("fileType"))
            dfInputParams = gl_parameterDictionary["InputParams"].\
                            withColumn('id',row_number().over(w)).filter(col("id") == 1).persist()

            gl_parameterDictionary.pop("InputParams")
            gl_parameterDictionary["InputParams"] = dfInputParams

            for f in gl_parameterDictionary["InputParams"].collect():
                if(f.isCustomTransform == False):
                    fileID = f.fileID
                    fileType = f.fileType
                    gl_fileTypes["Input"].add(fileType.upper())
                    gl_fileTypes["Succeeded"].add(fileType.upper())
                    
                    if(fileType == 'JET'):
                        dfSource = objGenHelper.gen_readFromFile_perform(gl_CDMLayer1Path + 'fin_L1_TD_Journal.delta')
                    elif(fileType == 'GLA'):
                        dfSource = objGenHelper.gen_readFromFile_perform(gl_CDMLayer1Path + 'fin_L1_MD_GLAccount.delta')
                    elif(fileType == 'GLAB'):
                        dfSource = objGenHelper.gen_readFromFile_perform(gl_CDMLayer1Path + 'fin_L1_TD_GLBalance.delta')
                    elif(fileType == 'GLTB'):
                        dfSource = objGenHelper.gen_readFromFile_perform(gl_CDMLayer1Path + 'fin_L1_TD_GLBalancePeriodic.delta')
                        
                    gl_lstOfSourceFiles[fileID.upper()] = {fileType.upper(): dfSource}

            executionStatus = "Layer one pre processing completed."
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

      except Exception as err:
          executionStatus = objGenHelper.gen_exceptionDetails_log()
          return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

 
