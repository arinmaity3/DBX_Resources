# Databricks notebook source
import itertools
import sys
import traceback

def app_SAP_classicOrNewGL_validate(executionID = ""):
  try:
    
    objGenHelper = gen_genericHelper()
    logID = executionLog.init(PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                              validationID = VALIDATION_ID.CLASSIC_OR_NEW_GL,
                              executionID = executionID)
        
    
    lstOfGLTables = ['erp_FAGL_ACTIVEC','erp_GLT0','erp_FAGLFLEXT','erp_FAGL_TLDGRP_MAP','erp_T881','READ_GLT0']    
    lstOfTables = list()
    
    executionStatusID = LOG_EXECUTION_STATUS.STARTED
    executionStatus = ""
    
    #get the record count of GL related tables
    [lstOfTables.append([t.tableName,t.netRecordCount])
    for t in gl_parameterDictionary[FILES.KPMG_ERP_STAGING_RECORD_COUNT].\
                    filter(col("tableName").isin(lstOfGLTables)).\
                    select(col("tableName"),col("netRecordCount")).collect()] 

    dictOfFiles = dict(lstOfTables )
  
    #Check the table erp_FAGL_ACTIVEC loaded and READ_GLT0 ='X' is enabled or not
    try:
      filePath = gl_layer0Staging + "erp_FAGL_ACTIVEC.delta"
      dbutils.fs.ls(filePath)
      dictOfFiles['READ_GLT0'] = objGenHelper.gen_readFromFile_perform(filePath).\
                                 filter(col("READ_GLT0")=="X").count()
    except Exception as e:
      if 'java.io.FileNotFoundException' in str(e):
        dictOfFiles['READ_GLT0'] = 0
        pass

    
    #add all the missing keys in dictionary to avoid key errors
    for k in lstOfGLTables:
      if(k not in dictOfFiles.keys()):
        dictOfFiles[k] = 0      

    #start validation logic

    if((dictOfFiles.get('erp_FAGL_ACTIVEC') == 0) & (dictOfFiles.get('erp_GLT0') == 0)):
      executionStatusID = LOG_EXECUTION_STATUS.FAILED
      executionStatus = "New GL is activated as per system configuration, but the following table(s) is(were) empty FAGL_ACTIVEC,GLT0"  
    elif (dictOfFiles.get('erp_FAGL_ACTIVEC') > 0):
      if (dictOfFiles.get('READ_GLT0') > 0):
        if (dictOfFiles.get('erp_GLT0') == 0 ) :
          executionStatusID = LOG_EXECUTION_STATUS.FAILED
          executionStatus = "Classic GL is activated as per the system configuration,but the the classic GL balance table GLT0 is empty"
        else:
          executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
          isNewGL = 0
          executionStatus = "Classic GL activated"
      elif(dictOfFiles.get('READ_GLT0') == 0):
        lstOfMissngTables = list()
        [lstOfMissngTables.append(k) 
         for k,v in dictOfFiles.items() if (v == 0 and k in ['erp_FAGLFLEXT','erp_FAGL_TLDGRP_MAP','erp_T881'])]
        if(len(lstOfMissngTables) != 0):
            executionStatusID = LOG_EXECUTION_STATUS.FAILED
            executionStatus   = "New GL is activated as per system configuration," \
                              "but the following table(s) is(were) empty '" + ",".join(lstOfMissngTables) + "'"
        else:
            executionStatusID 	= LOG_EXECUTION_STATUS.SUCCESS
            isNewGL	= 1
            executionStatus     = 'New GL activated'
    elif(dictOfFiles.get('erp_GLT0') > 0 ):
            executionStatusID 	= LOG_EXECUTION_STATUS.SUCCESS
            isNewGL			    = 0
            executionStatus    	= 'Classic GL activated'

    
    
    #Upddate the parameter isNewGL to parameter table
    if(executionStatusID == LOG_EXECUTION_STATUS.SUCCESS):
      
      lstOfParameters = list ()

      [lstOfParameters.append([p.Id,
                           p.routineId,
                           p.name,
                           p.parameterValue])
      for p in gl_parameterDictionary['knw_LK_CD_Parameter'].collect()]

      
      lstOfNewParams =  list(itertools.filterfalse(lambda p : p[2] == "isNewGL", lstOfParameters)) 
      maxID = gl_parameterDictionary['knw_LK_CD_Parameter'].select(col("id").cast("int")).\
              agg({"id":"max"}).rdd.collect()[0][0]
      
      lstOfNewParams.append([maxID + 1,'GLOBAL','isNewGL',isNewGL])
      
      dfParameter = spark.createDataFrame(schema = gl_parameterDictionary['knw_LK_CD_Parameter'].schema,\
                                          data = lstOfNewParams).persist()
      
     
      gl_parameterDictionary['knw_LK_CD_Parameter'] = dfParameter          
      objGenHelper.gen_writeSingleCsvFile_perform(df = dfParameter,targetFile = gl_commonParameterPath + "knw_LK_CD_Parameter.csv")

      
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
            
    else:
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
    
  except Exception as err:    
    print(err)
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]                         
