# Databricks notebook source
from datetime import datetime
import inspect
import uuid

class executionLog():
  @staticmethod
  def init (processID,fileID = "",fileName = "",fileType = "",\
            validationID = -1,phaseName = "",className = "",\
            schemaName = "", tableName = "",executionID = "",option=True, **kwargs):
    try:
      global gl_executionLog
      global gl_analysisID     
      global gl_enableTransformaionLog  
      
      validationText  = ""
      writeToSummaryFile = True
      

      logID = uuid.uuid4().hex
      classObject = ""            
      stack = inspect.stack()
      methodName = stack[1][0].f_code.co_name
            
      try:
        routineName = stack[1][0].f_locals["self"].__class__.__name__ + "." + methodName                
      except KeyError as e:
          if(className.strip() == ""):
              routineName = methodName
          else:
              routineName =  className + "." + methodName 
          pass
          
      if(phaseName != ""):
          routineName = phaseName
                         
      if('resultFileType' in kwargs.keys()):
          resultFileType = kwargs['resultFileType']
      else:
          resultFileType = "S"

      if(validationID != -1) & (validationID != None) :
          validationID = VALIDATION_ID(validationID).value
      elif((validationID == None) & (processID in [PROCESS_ID.L1_TRANSFORMATION,
                                                  PROCESS_ID.L2_TRANSFORMATION])):
          validationID = -1

      if not fileID:
          fileID = ""

      lstOfStatusInfo = list()      
         
      if(processID in [PROCESS_ID.TRANSFORMATION_VALIDATION]):          
          validationText = gl_metadataDictionary['dic_ddic_genericValidationProcedures'].\
                           filter((col("processID") == PROCESS_ID.TRANSFORMATION_VALIDATION.value) & \
                                  (col("validationID") == validationID)).\
                           select(col("validationText")).collect()[0][0]
           
      if(processID in [PROCESS_ID.L1_TRANSFORMATION]):
          executionID = gl_parameterDictionary['executionID_TRANSFORMATION']

      
      lstOfkeys = ['logID','fileID','fileName','fileType','processID','validationID',\
                     'schemaName','tableName','routineName','statusID','status',\
                     'startTime','endTime','comments','executionID','validationText']
    
      lstOfValues = [logID,fileID,fileName,fileType,processID,validationID,\
                     schemaName,tableName,\
                     routineName,LOG_EXECUTION_STATUS.IN_PROGRESS.value,\
                     LOG_EXECUTION_STATUS.IN_PROGRESS.name,datetime.now(),\
                     None,'',executionID,validationText]

      logDetails = dict(zip(lstOfkeys, lstOfValues))        
      gl_executionLog[logID] = logDetails

      if(gl_enableTransformaionLog == True):           
           if(processID == PROCESS_ID.IMPORT_VALIDATION):
               lstOfStatusInfo = [str(PROCESS_ID.IMPORT_VALIDATION.value),\
                                  str(logDetails["validationID"]),\
                                  logDetails["routineName"],\
                                  str(LOG_EXECUTION_STATUS.IN_PROGRESS.value),\
                                  LOG_EXECUTION_STATUS.IN_PROGRESS.name,
                                  fileID,fileType,str(datetime.now()),""]
           
           elif(processID == PROCESS_ID.IMPORT):
                              
               lstOfStatusInfo = [logID,fileID,fileName,\
                                  tableName,\
                                  str(LOG_EXECUTION_STATUS.IN_PROGRESS.value),\
                                  LOG_EXECUTION_STATUS.IN_PROGRESS.name,
                                  str('0'),str(datetime.now()),"",executionID,str('')]

           elif(processID == PROCESS_ID.CLEANSING):
              
               lstOfStatusInfo = [logID,str(logDetails["validationID"]),
                                  tableName,str('0'),
                                  str(LOG_EXECUTION_STATUS.IN_PROGRESS.value),\
                                  LOG_EXECUTION_STATUS.IN_PROGRESS.name,
                                  str(datetime.now()),"",executionID]

           elif(processID == PROCESS_ID.DPS):
               lstOfStatusInfo = [logID,str(logDetails["validationID"]),\
                                  tableName,\
                                  str(LOG_EXECUTION_STATUS.IN_PROGRESS.value),\
                                  LOG_EXECUTION_STATUS.IN_PROGRESS.name,
                                  str(datetime.now()),"",executionID]
           
           elif(processID == PROCESS_ID.PRE_TRANSFORMATION):
               lstOfStatusInfo = [logID,str(logDetails["validationID"]),\
                                  routineName,\
                                  str(LOG_EXECUTION_STATUS.IN_PROGRESS.value),\
                                  LOG_EXECUTION_STATUS.IN_PROGRESS.name,
                                  str(datetime.now()),""]

           elif(processID == PROCESS_ID.PRE_TRANSFORMATION_VALIDATION):
               
               lstOfStatusInfo = [str(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION.value),\
                                  str(logDetails["validationID"]),\
                                  logDetails["routineName"],\
                                 str(LOG_EXECUTION_STATUS.IN_PROGRESS.value),\
                                 LOG_EXECUTION_STATUS.IN_PROGRESS.name,
                                 str(datetime.now()),"",executionID]
                                             
           elif(processID == PROCESS_ID.L1_TRANSFORMATION):
               
               lstOfStatusInfo = [logID,\
                                 logDetails["routineName"],\
                                 str(LOG_EXECUTION_STATUS.IN_PROGRESS.value),\
                                 LOG_EXECUTION_STATUS.IN_PROGRESS.name,
                                 str(datetime.now()),"",executionID,
                                 str(logDetails["validationID"])]

           elif(processID == PROCESS_ID.TRANSFORMATION_VALIDATION):

               if(resultFileType == "S"):
                   lstOfStatusInfo = [str(PROCESS_ID.TRANSFORMATION_VALIDATION.value),\
                                 str(logDetails["validationID"]),\
                                 logDetails["routineName"],\
                                 str(LOG_EXECUTION_STATUS.IN_PROGRESS.value),\
                                 LOG_EXECUTION_STATUS.IN_PROGRESS.name,
                                 fileType,fileName,\
                                 str(datetime.now()),"",validationText,\
                                 executionID]
               elif(resultFileType == "E"):
                   lstOfStatusInfo = [logID,validationID,\
                                     logDetails["routineName"],\
                                     str(LOG_EXECUTION_STATUS.IN_PROGRESS.value),\
                                     LOG_EXECUTION_STATUS.IN_PROGRESS.name,\
                                     str(datetime.now()),"",executionID,fileType]

           elif(processID == PROCESS_ID.L2_TRANSFORMATION):
               lstOfStatusInfo = [logID,\
                                 logDetails["routineName"],\
                                 str(LOG_EXECUTION_STATUS.IN_PROGRESS.value),\
                                 LOG_EXECUTION_STATUS.IN_PROGRESS.name,
                                 str(datetime.now()),"",executionID,
                                 str(logDetails["validationID"])]
                 
           if('writeSummaryFile' in kwargs.keys()):
               writeToSummaryFile = kwargs['writeSummaryFile']

           if(writeToSummaryFile == True):               
               transformationStatusLog.transformationStatusUpdate(lstOfStatusInfo = lstOfStatusInfo,\
                                                              processID = processID,\
                                                              executionID = executionID,\
                                                              validationID = validationID,
                                                              resultFileType = resultFileType)                                                       
      return logID
    except Exception as err:
      print(err)
      raise

  @staticmethod
  def add(statusID,logID,errorDescription="",option=True, **kwargs):
    try:       
      global gl_executionLog      
      objGenHelper = gen_genericHelper()
      logDetails = gl_executionLog[logID]
      writeToSummaryFile = True
      
      if('resultFileType' in kwargs.keys()):
          resultFileType = kwargs['resultFileType']
      else:
          resultFileType = "S"
       
      logDetails["endTime"] = datetime.now()
      logDetails["comments"] = errorDescription
      logDetails["statusID"] = LOG_EXECUTION_STATUS(statusID).value   
      logDetails["status"] = LOG_EXECUTION_STATUS(statusID).name
                
      gl_executionLog.pop(logID)
      gl_executionLog[logID] = logDetails
      
      if(gl_enableTransformaionLog == True):
          
          logDetails["comments"] = ('%.2000s' %logDetails["comments"]).replace('\n','').replace('"','')

          if(logDetails["processID"] == PROCESS_ID.IMPORT_VALIDATION):
              lstOfStatusInfo = [str(PROCESS_ID.IMPORT_VALIDATION.value),\
                                 str(logDetails["validationID"]),\
                                 logDetails["routineName"],\
                                 str(LOG_EXECUTION_STATUS(statusID).value),\
                                 LOG_EXECUTION_STATUS(statusID).name,
                                 logDetails["fileID"],logDetails["fileType"],\
                                 str(datetime.now()),logDetails["comments"]]
          
          elif(logDetails["processID"] == PROCESS_ID.IMPORT):
              if(len(kwargs)!=0):
                  importedRows = str(kwargs['importedRows'])
                  mandatoryFieldStatus = str(kwargs['mandatoryFieldStatus'])
              else:
                  importedRows = '0'
                  mandatoryFieldStatus = ''

              lstOfStatusInfo = [logID,logDetails["fileID"],logDetails["fileName"],\
                                  logDetails["tableName"],\
                                  str(LOG_EXECUTION_STATUS(statusID).value),\
                                  LOG_EXECUTION_STATUS(statusID).name,
                                  importedRows,str(datetime.now()),\
                                  logDetails["comments"],logDetails['executionID'],
                                  mandatoryFieldStatus]

          elif(logDetails["processID"] == PROCESS_ID.CLEANSING):
              if(len(kwargs)!=0):
                  duplicateRecords = str(kwargs['duplicateRecords'])
              else:
                  duplicateRecords = '0'

              lstOfStatusInfo = [logID,str(logDetails["validationID"]),\
                                  logDetails["tableName"],\
                                  duplicateRecords,
                                  str(LOG_EXECUTION_STATUS(statusID).value),\
                                  LOG_EXECUTION_STATUS(statusID).name,
                                  str(datetime.now()),logDetails["comments"],
                                  logDetails["executionID"]]

          elif(logDetails["processID"] == PROCESS_ID.DPS):

              lstOfStatusInfo = [logID,str(logDetails["validationID"]),
                                  logDetails["tableName"],\
                                  str(LOG_EXECUTION_STATUS(statusID).value),\
                                  LOG_EXECUTION_STATUS(statusID).name,
                                  str(datetime.now()),logDetails["comments"],
                                  logDetails["executionID"]]

          elif(logDetails["processID"] == PROCESS_ID.PRE_TRANSFORMATION):

              lstOfStatusInfo = [logID,str(logDetails["validationID"]),
                                  logDetails["routineName"],\
                                  str(LOG_EXECUTION_STATUS(statusID).value),\
                                  LOG_EXECUTION_STATUS(statusID).name,
                                  str(datetime.now()),logDetails["comments"]]  
              
          elif(logDetails["processID"] == PROCESS_ID.PRE_TRANSFORMATION_VALIDATION):
          
              lstOfStatusInfo = [str(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION.value),\
                                 str(logDetails["validationID"]),\
                                 logDetails["routineName"],\
                                 str(LOG_EXECUTION_STATUS(statusID).value),\
                                 LOG_EXECUTION_STATUS(statusID).name,                                    
                                 str(datetime.now()),logDetails["comments"],\
                                 logDetails["executionID"]]

          elif(logDetails["processID"] == PROCESS_ID.L1_TRANSFORMATION):

              lstOfStatusInfo = [logID,\
                             logDetails["routineName"],\
                             str(LOG_EXECUTION_STATUS(statusID).value),\
                             LOG_EXECUTION_STATUS(statusID).name,
                             str(datetime.now()),logDetails["comments"],\
                             logDetails["executionID"],
                             str(logDetails["validationID"])]

          elif(logDetails["processID"] == PROCESS_ID.TRANSFORMATION_VALIDATION):

              if(resultFileType == "S"):
                  lstOfStatusInfo = [str(PROCESS_ID.TRANSFORMATION_VALIDATION.value),\
                             str(logDetails["validationID"]),\
                             logDetails["routineName"],\
                             str(LOG_EXECUTION_STATUS(statusID).value),\
                             LOG_EXECUTION_STATUS(statusID).name,
                             logDetails["fileType"],\
                             logDetails["fileName"],\
                             str(datetime.now()),logDetails["comments"],
                             logDetails["validationText"],\
                             logDetails["executionID"]]
              elif(resultFileType == "E"):
                  lstOfStatusInfo = [logID,str(logDetails["validationID"]),\
                                 logDetails["routineName"],\
                                 str(LOG_EXECUTION_STATUS(statusID).value),\
                                 LOG_EXECUTION_STATUS(statusID).name,
                                 str(datetime.now()),logDetails["comments"],\
                                 logDetails["executionID"],\
                                 logDetails["fileType"]]
                  
          elif(logDetails["processID"] == PROCESS_ID.L2_TRANSFORMATION):
              lstOfStatusInfo = [logID,\
                             logDetails["routineName"],\
                             str(LOG_EXECUTION_STATUS(statusID).value),\
                             LOG_EXECUTION_STATUS(statusID).name,
                             str(datetime.now()),logDetails["comments"]\
                             ,logDetails["executionID"],
                             str(logDetails["validationID"])]

          if('writeSummaryFile' in kwargs.keys()):
           writeToSummaryFile = kwargs['writeSummaryFile']

          if(writeToSummaryFile == True):
              transformationStatusLog.transformationStatusUpdate(lstOfStatusInfo,\
                                                        logDetails["processID"],\
                                                        logDetails["executionID"],
                                                        logDetails["validationID"],
                                                        resultFileType = resultFileType)

          
          if('dfDetail' in kwargs.keys()):                
            if(kwargs['dfDetail'] is not None):
              if(kwargs['dfDetail'].first() is not None):
                lstOfResultDetails = list()              
                [lstOfResultDetails.append([r[j] 
                   for j in range(0,len(kwargs['dfDetail'].columns))])
                   for r in kwargs['dfDetail'].collect()]
              
                resultFileType = "D"
                transformationStatusLog.transformationStatusUpdate(lstOfStatusInfo = lstOfResultDetails,\
                                                          processID = logDetails["processID"],\
                                                          executionID = logDetails["executionID"],\
                                                          validationID = logDetails["validationID"],
                                                          resultFileType = resultFileType)                              
          return logDetails
    except Exception as err:
      raise
