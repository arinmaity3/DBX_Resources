# Databricks notebook source
import sys
import traceback
      
class app_layerOneTransformation_perform():
  def GLALayerOneTransformation_perform(self):
    try:      
      objGenHelper = gen_genericHelper()      
      logID = executionLog.init(PROCESS_ID.TRANSFORMATION_VALIDATION,validationID = VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION)
      transformationStatus = False     
      executionStatus = ""
      
      inputfileTypes = gl_parameterDictionary["InputParams"].\
                       select(upper(col("fileType"))).\
                       filter(col("isCustomTransform")==gl_isCustomTransform).\
                       distinct().rdd.flatMap(lambda x: x).collect()
      
      
      if ((len((gl_lstOfSourceFiles)) > 0) and (len(inputfileTypes)>0)) :
        transformationStatus = True
        global fin_L1_MD_GLAccount,fin_L1_TD_Journal,fin_L1_TD_GLBalance,fin_L1_TD_GLBalancePeriodic
                
        print("Convert raw data files to CDM Layer 1 started...")
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        global gl_fileTypes
        
        fin_L1_MD_GLAccount = objDataTransformation.gen_CDMStructure_get("fin","L1_MD_GLAccount")
        fin_L1_TD_Journal   = objDataTransformation.gen_CDMStructure_get("fin","L1_TD_Journal")
        fin_L1_TD_GLBalance = objDataTransformation.gen_CDMStructure_get("fin","L1_TD_GLBalance")
        fin_L1_TD_GLBalancePeriodic = objDataTransformation.gen_CDMStructure_get("fin","L1_TD_GLBalancePeriodic")
        
        fin_L1_MD_GLAccount.unpersist()
        fin_L1_TD_Journal.unpersist()
        fin_L1_TD_GLBalance.unpersist()
        fin_L1_TD_GLBalancePeriodic.unpersist()

        objCDMLayer = gen_CDMLayer_prepare()
        objCDMLayer.gen_convertToCDMLayer1_prepare()

        for fType in set(inputfileTypes):            
            status = self.__layerOneTempViews_create(fType.upper())
            if(status == False):
              raise
              
        obj_app_layerOnePostProcessing_perform = app_layerOnePostProcessing_perform()
        obj_app_layerOnePostProcessing_perform.app_importResults_write()


        if (('JET' in gl_fileTypes["Succeeded"]) and fin_L1_TD_Journal.count() == 0):
            transformationStatus = False
            print("CDM layer 1 transformation for file type 'JET' has failed.")            
        if ((('GLA' in gl_fileTypes["Succeeded"]) and ('GLTB' in  gl_fileTypes["Succeeded"])) and fin_L1_MD_GLAccount.count() == 0):
            transformationStatus = False
            print("CDM layer 1 transformation for file type 'GLA' has failed.")            
        if ((('GLAB' in gl_fileTypes["Succeeded"]) and ('GLTB' in  gl_fileTypes["Succeeded"])) and  fin_L1_TD_GLBalance.count() == 0):
            transformationStatus = False
            print("CDM layer 1 transformation for file type 'GLAB' has failed.")           
      
        if(transformationStatus == True):
            executionStatus = "Conversion of raw data files to CDM Layer 1 completed sucessfully..."
        else:
            executionStatus = "Conversion of raw data files to CDM Layer 1 failed..."
        print(executionStatus)        
      elif(len(inputfileTypes)==0):
        transformationStatus = True
      else:                
        transformationStatus = False
        executionStatus = "Conversion of raw data files to CDM Layer 1 failed..."
        print(executionStatus)
          
      if(transformationStatus == False):
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)        
      else:
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)      
    except Exception as e:     
      transformationStatus = False
      executionStatus = objGenHelper.gen_exceptionDetails_log() 
      print(executionStatus)                    
    finally:
      print(transformationStatus)
      if(transformationStatus == False):
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)         
        dfExecutionLog =  objGenHelper.generateExecutionLogFile(gl_executionLog,gl_logSchema,\
                                                           gl_executionLogResultPath + "ExecutionLogDetails.csv")
      return [transformationStatus, executionStatus]
    
  def __layerOneTempViews_create(self,fType):
      try:        
        objGenHelper = gen_genericHelper() 
        global fin_L1_MD_GLAccount,fin_L1_TD_Journal,fin_L1_TD_GLBalance,fin_L1_TD_GLBalancePeriodic

        if(fType == 'JET'):
            fin_L1_TD_Journal.createOrReplaceTempView("fin_L1_TD_Journal")
            sqlContext.cacheTable("fin_L1_TD_Journal")
            fin_L1_TD_Journal = spark.sql("SELECT * FROM fin_L1_TD_Journal")
            print("View for JET has been created.")        
        elif (fType == 'GLA'):
            fin_L1_MD_GLAccount.createOrReplaceTempView("fin_L1_MD_GLAccount")
            sqlContext.cacheTable("fin_L1_MD_GLAccount")
            fin_L1_MD_GLAccount = spark.sql("SELECT * FROM fin_L1_MD_GLAccount")
            print("View for GLA has been created.")            
        elif (fType == 'GLAB'):
            fin_L1_TD_GLBalance.createOrReplaceTempView("fin_L1_TD_GLBalance")
            sqlContext.cacheTable("fin_L1_TD_GLBalance")
            fin_L1_TD_GLBalance = spark.sql("SELECT * FROM fin_L1_TD_GLBalance")
            print("View for GLAB has been created.")  
        elif (fType == 'GLTB'):
            fin_L1_TD_GLBalancePeriodic.createOrReplaceTempView("fin_L1_TD_GLBalancePeriodic")
            sqlContext.cacheTable("fin_L1_TD_GLBalancePeriodic")
            fin_L1_TD_GLBalancePeriodic = spark.sql("SELECT * FROM fin_L1_TD_GLBalancePeriodic")
            print("View for GLTB has been created.")  
        return True
      except Exception as e:           
          errorDescription = objGenHelper.gen_exceptionDetails_log()
          print(errorDescription)
          return False
#objLayerOneTransformation = app_layerOneTransformation_perform()
#objLayerOneTransformation.GLALayerOneTransformation_perform()
