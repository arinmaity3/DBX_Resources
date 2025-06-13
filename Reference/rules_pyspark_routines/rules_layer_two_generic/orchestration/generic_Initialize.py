# Databricks notebook source
from pyspark.sql.functions import row_number,lit
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row
from pyspark.sql.types import StructType,StructField, StringType, ShortType

class generic_Initialize():

  def getModulesAndPrepareOrchestration(self):
      try:
          global gl_maxParallelism
          global gl_dfExecutionOrder
          global gl_lstOfExecutableProcs

          logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION,phaseName = "Prepare L2 orchestration")
          lsOfPreTransformationStep = [['Initialize logging', '', -4]
                                       ,['Prepare L2 orchestration','', -3]
                                       ,['Load metadata files','',-2]
                                       ,['Load all the input files','',-1]]
  
          executionOrderSchema = StructType([StructField("objectname",StringType(),True),
                                            StructField("parameters",StringType(),True),
                                            StructField("hierarchy",ShortType(),True)])
  
          dfPreTransformationStep = spark.createDataFrame(data = lsOfPreTransformationStep, schema= executionOrderSchema) 
          gl_dfExecutionOrder = spark.createDataFrame(data = lsOfPreTransformationStep, schema= executionOrderSchema) 
  
          objGenHelper = gen_genericHelper()
          
          objGenHelper.gen_readFromFile_perform(gl_commonParameterPath + "knw_LK_CD_Parameter.csv")\
                        .createOrReplaceTempView("knw_LK_CD_Parameter")  
          sqlContext.cacheTable("knw_LK_CD_Parameter")
          df = spark.sql("select * from knw_LK_CD_Parameter")
          gl_parameterDictionary["knw_LK_CD_Parameter"] = df
          lstOfScopedAnalytics = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'SCOPED_ANALYTICS').upper().split(",")
          
          # ERPSystemID = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID'))
          
          if objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')==None:
            ERPSystemID = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID'))
          else:
            ERPSystemID = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC'))


          gl_dfExecutionOrder = gen_transformationExecutionOrder.\
                              transformationExecutionOrder_get(lstOfScopedAnalytics,ERPSystemID).\
                              union(dfPreTransformationStep).persist()
            
          self.__prepareLogFiles(gl_dfExecutionOrder)
  
          gl_maxParallelism = gl_dfExecutionOrder.\
                             filter(col("hierarchy") >0).\
                             groupBy(col("hierarchy")).\
                             agg(count("hierarchy").alias("maxHierarchy")).\
                             orderBy(col("maxHierarchy").desc()).collect()[0][1]

          if(gl_maxParallelism > sc.defaultParallelism):
                gl_maxParallelism = sc.defaultParallelism
  
          self.__intializeAllOutputDirectories()
                     
          executionStatus = "L2 orchestration scucceded."  
          executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)

      except Exception as err:          
          executionStatus = objGenHelper.gen_exceptionDetails_log()
          executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
              
    
  def __intializeAllOutputDirectories(self):
    
    try:        
        stagingPath = gl_MountPoint + "/"+ gl_analysisPhaseID + "/" + 'staging'
        dbutils.fs.ls(stagingPath)

    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            dbutils.fs.mkdirs(stagingPath)

    global gl_isReProcessing
    if(gl_isReProcessing == True):
        return

    try:              
      objGenHelper = gen_genericHelper()
                 
      for f in dbutils.fs.ls(gl_CDMLayer2Path):
        if(f.isDir()):
          objGenHelper.gen_resultFiles_clean(f.path)               
    except Exception as e:
      if 'java.io.FileNotFoundException' in str(e):
        pass
      else:
        raise

    try:
        for f in dbutils.fs.ls(gl_Layer2Staging):
            if(f.isDir()):
                for d in dbutils.fs.ls(f.path):
                    if(d.isDir()):
                        dbutils.fs.rm(d.path,True)
                    else:
                        dbutils.fs.rm(f.path,True)
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            pass
        else:
            raise

  def __prepareLogFiles(self,dfExecutionOrder):
    try:
      objGenHelper = gen_genericHelper()
      try:        
        objGenHelper.gen_resultFiles_clean(gl_executionLogResultPath + "transformationRules.csv")
      except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
          pass
        else:
          raise
      header = dfExecutionOrder.columns
      logFilePath = gl_executionLogResultPath + "transformationRules.csv"
      objGenHelper.gen_writeSingleCsvFile_perform(df = dfExecutionOrder,targetFile = logFilePath)

    except Exception as err:
      raise
