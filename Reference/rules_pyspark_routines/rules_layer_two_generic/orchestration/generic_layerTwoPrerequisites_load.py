# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,ShortType ,LongType ,DecimalType
from pyspark.sql.functions import row_number,lit ,abs,col, asc,upper,expr,concat
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
import itertools
import warnings
import os
import pathlib

#Knowledge file population need to follow some hierachy, place holder value population etc.
# so it has been treated seperatly and also need to take the knowledge 
# details with latest publish id

def __placeHolderValues_update(self,dfSource,fileName):
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

#The function which is running using the thread pool, will accept the list of files
# and locations and populate the data
def __loadAllFiles(lstOfFiles):
  try:
    objGenHelper = gen_genericHelper()   
    
    lstOfKnowledgeFilesWithLatestPublishID = ['knw_LK_CD_FinancialStructureDetailSnapshot',
                                              'knw_LK_CD_ProcessToKnowledgeAccountsRelationSnapshot',
                                              'knw_LK_CD_KnowledgeAccountSnapshot',
                                              'knw_LK_CD_knowledgeAccountScopingDetail',
                                              'knw_LK_CD_GLAccountToFinancialStructureDetailSnapshot']
    filePath = lstOfFiles[3]    
    fileName = lstOfFiles[1]
    fileType = lstOfFiles[2]
  
    if(fileName == 'dic_ddic_column'):     
      print("Loading..... '" + fileName + "'")
      try:
        df = objGenHelper.gen_readFromFile_perform(filePath).\
             select(col("ERPSystemID"),col("schemaName"),\
                       col("tableName"),col("tableName2"),\
                       col("columnName"),col("dataType"),\
                       col("fieldLength"),col("position").cast("int"),\
                       col("isNullable"),col("isIdentity"),\
                       col("defaultValue"),col("isKey"),\
                       col("fileType"),col("sparkDataType"),\
                       col("isImported").cast("boolean"))
        gl_metadataDictionary[fileName] = df 
        return[True,fileName]
      except Exception as err:
          return[False,fileName]            
    elif (fileName in lstOfKnowledgeFilesWithLatestPublishID):      
      try:
          df = objGenHelper.gen_readFromFile_perform(filePath)
      except Exception as e:
          return[False,fileName]
      
      if (fileName in ['knw_LK_CD_KnowledgeAccountSnapshot',
                        'knw_LK_CD_FinancialStructureDetailSnapshot',
                        'knw_LK_CD_GLAccountToFinancialStructureDetailSnapshot']):
        df =__placeHolderValues_update('',df,fileName)
      if(fileName in lstOfKnowledgeFilesWithLatestPublishID):
        df = df.filter(col("publishID") ==(df.agg({"publishID": "max"}).collect()[0][0]))
          
      df.createOrReplaceTempView(fileName)
      exec("global " + fileName)          
      sqlContext.cacheTable(fileName)
      dfCreate = "{tabName} = spark.sql('select * from {tabName}')".format(tabName= fileName)
      print("Loading..... '" + fileName + "'")
      exec(dfCreate,globals()) 
      return[True,fileName]
    else:      
      exec("global " + fileName)
      query = "{dfName} = objGenHelper.gen_readFromFile_perform('{filePath}')".format(dfName = fileName,filePath = filePath)      
      print("Loading..... '" + fileName + "'")        
      try:
        exec(query,globals())  
        query = "{dfName}.createOrReplaceTempView('{dfName}')".format(dfName = fileName)
        exec(query)
        if(fileType == '/ddic/'):
          query = "gl_metadataDictionary['{fileName}'] = {dfName}".format(fileName = fileName,dfName = fileName)
          exec(query)
        elif(fileType == '/knowledge/'):
          query = "gl_parameterDictionary['{fileName}'] = {dfName}".format(fileName = fileName,dfName = fileName)
          exec(query)
        return[True,fileName]
      except Exception as e:
          print("Unable to load the file '" + fileName +"'" )
          return[False,fileName]        
  except Exception as err:
      raise

#Thread pool execution
def _submitDataLoad(lstOfFiles):
    with ThreadPoolExecutor(max_workers = MAXPARALLELISM.DEFAULT.value) as executor:
        loadStatus = list(executor.map(__loadAllFiles,lstOfFiles))
    return loadStatus

def _getFile(folderPath,fileName) :
    try:
        folderPath = "/dbfs" + folderPath        
        fileList = list()
        [fileList.append(Path(i).suffix)\
           for i in \
           list(itertools.filterfalse(lambda files: not files.upper().\
           startswith(fileName.upper()), os.listdir(folderPath)))]
        
        if '.delta' in fileList:
            return fileName + ".delta"
        elif '.parquet' in fileList:           
            return fileName + ".parquet"
        elif '.csv' in fileList:            
            return fileName + ".csv"
        else:
            return fileName + ".csv"
    except Exception as err:
        raise
# Convert the data frame to list of data to execute the same through thread pool
# Construct the path for each file 
def _prepareFileList(dfSoureFileMapping):
  try:
    lstOfFiles = list()
    gl_ERPSystemID = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL','ERP_SYSTEM_ID'))
    for h in dfSoureFileMapping.select(col("fileName"),col("sourceLocation"),col("sortOrder"))\
              .orderBy(col("sortOrder")).rdd.collect():
      fullPath = ""
      if gl_ERPSystemID!=10 and h.fileName=='gen_L1_MD_Period':
          fullPath = "dbfs:" + gl_CDMLayer1Path + _getFile(gl_CDMLayer1Path,h.fileName)
      elif (h.sourceLocation == '/common-parameters/'):
        fullPath = "dbfs:" + gl_commonParameterPath + _getFile(gl_commonParameterPath,h.fileName)
      elif(h.sourceLocation == '/ddic/'):
        fullPath = "dbfs:" + gl_metadataPath + _getFile(gl_metadataPath,h.fileName)
      elif(h.sourceLocation == '/knowledge/'):
        fullPath = "dbfs:" + gl_knowledgePath +_getFile(gl_knowledgePath,h.fileName)
      elif(h.sourceLocation == '/cdm-l1/'):
        fullPath = "dbfs:" + gl_CDMLayer1Path + _getFile(gl_CDMLayer1Path,h.fileName)
      if(fullPath!=""):
        lstOfFiles.append([h.sortOrder,h.fileName,h.sourceLocation,fullPath])
    return lstOfFiles
  except Exception as err:
    raise

#prepare all the mandatory files details based on the scoped analytics
def _loadAllMandatoryFiles(dfFilesToAuditProcedureMapping,lstOfScopedAnalytics,lstOfFilesToBeExcluded,sortOrderExpression):
  try:
    dfSoureFileMapping = dfFilesToAuditProcedureMapping.\
                    select(col("fileName"),col("sourceLocation"),\
                    col("schemaName"),col("tableName"),\
                    split(col("auditProcedure"),",").alias("auditProcedure"))
    
    dfSoureFileMapping = dfSoureFileMapping.select(col("fileName"),col('sourceLocation'),\
                         col("schemaName"),col("tableName"),\
                         explode(col("auditProcedure")).alias("auditProcedure")).\
                         filter(col("auditProcedure").isin(lstOfScopedAnalytics)).\
                         select(col("fileName"),col("sourceLocation"),\
                         col("schemaName"),col("tableName")).\
                         filter(col("fileName").isin(lstOfFilesToBeExcluded) == False).distinct()   
    
    lstOfMetadatFiles = list()
    
    for f in dbutils.fs.ls(gl_metadataPath):
      if (f.name not in (lstOfFilesToBeExcluded)):
        schemaName = f.name[0:f.name.index('_')]
        tableName = f.name[f.name.index('_')+1:f.name.index('.')]
        lstOfMetadatFiles.extend([[f.name[:f.name.index(".")],"/ddic/",schemaName,tableName]])

              
    dfSoureFileMapping = spark.createDataFrame(data =lstOfMetadatFiles,schema =dfSoureFileMapping.schema).\
                         union(dfSoureFileMapping).withColumn("sortOrder",expr(sortOrderExpression))
    
    return dfSoureFileMapping
  except Exception as err:
    raise

#prepare the optional and other files needed for execution
def _loadAllOptionalFiles(dfFilesToAuditProcedureMapping,lstOfScopedAnalytics,lstOfFilesToBeExcluded,sortOrderExpression,lstOfFilesSucceeded):
  try:
    
    df= dfFilesToAuditProcedureMapping.select(col("fileName"),col("sourceLocation"),\
                    col("schemaName"),col("tableName"),\
                    split(col("optional"),",").alias("optional"),\
                    split(col("requiredForProcessing"),",").alias("requiredForProcessing"))

    df = df.select(col("fileName"),col("sourceLocation"),\
                    col("schemaName"),col("tableName"),\
                    explode(col("optional")).alias("optional"),\
                    lit("1").alias("flag")).\
                    union(df.select(col("fileName"),col("sourceLocation"),\
                    col("schemaName"),col("tableName"),\
                    explode(col("requiredForProcessing")).alias("optional"),\
                    lit("2").alias("flag")))\
                    .filter((col("optional").isin(lstOfScopedAnalytics)) & \
                    (col("fileName").isin(lstOfFilesToBeExcluded) == False) & \
                    (col("fileName").isin(lstOfFilesSucceeded) == False))\
                    .withColumn("sortOrder",expr(sortOrderExpression))    
    return df
    
  except Exception as err:
    raise

#prepare the lists of files needs to be executed based on the hierachy
def _importFiles(lstOfFiles):
  try:
    
    lstOfSourceFiles = itertools.groupby(lstOfFiles, lambda x : x[0])   
    lstOfStatus = list()
    
    for sortOrder,fileName in lstOfSourceFiles:  
      lstOfInputFiles = list()
      [lstOfInputFiles.append(proc) for proc in list(itertools.filterfalse(lambda routine : routine[0] != sortOrder, lstOfFiles))]               
      importStatus = list(_submitDataLoad(lstOfInputFiles))
      lstOfStatus.extend(importStatus)
      
    return lstOfStatus
  except Exception as err:
    raise
    
#Prepare the blank data frames whcih is needed for processing, beacuse of depedencies
def _createDataStructure(lstOfDataStructures):
  try:
    dfName = lstOfDataStructures[0]
    schemaName = lstOfDataStructures[1]
    tableName = lstOfDataStructures[2]
    global objDataTransformation    
    objDataTransformation = gen_dataTransformation()
    
    query = "{dfName} = spark.createDataFrame([], StructType([]))".format(dfName = dfName)
    exec(query,globals())
    query = "{dfName} = objDataTransformation.gen_convertToCDMStructure_generate({fileName},'{schemaName}','{tableName}')[0]"\
              .format(dfName = dfName,fileName = dfName,schemaName = schemaName,tableName = tableName)
    exec(query,globals())
    if(dfName == "fin_L1_TD_Journal"):
        global fin_L1_TD_Journal
        fin_L1_TD_Journal = fin_L1_TD_Journal.withColumn('analysisID',lit(None).cast(StringType()))\
                         .withColumn('transactionIDbyPrimaryKey',lit(None).cast(LongType()))\
                         .withColumn('transactionIDbyJEDocumnetNumber',lit(None).cast(IntegerType()))\
                         .withColumn('documentAmountLC',lit(None).cast(DecimalType(18, 6)))\
                         .withColumn('lineItemCount',lit(None).cast(IntegerType()))\
                         .withColumn('journalSurrogateKey',lit(None).cast(LongType()))

    query = "{dfName}.createOrReplaceTempView('{dfName}')".format(dfName = dfName)
    exec(query)
    print("Create data struture..... '" + dfName + "'") 
    return[True,dfName]
  except Exception as err:       
    return[False,dfName]    
  
## Main logic to load all pre requisite tables, based on metadata
def generic_layerTwoPrerequisites_load():
  try:

    global gl_processID
    global gl_countJET

    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION,phaseName = "Load all the input files")
    gl_countJET = 0

    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    lstOfScopedAnalytics = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'SCOPED_ANALYTICS')    
    if(lstOfScopedAnalytics!=None):
      lstOfScopedAnalytics =lstOfScopedAnalytics.upper().split(",")+ ["*"]
    
    
    lstOfFilesToBeExcluded = ['dic_ddic_executionDependency.csv',
                          'dic_ddic_layerOneToLayerTwoFilesMapping.csv',
                           'knw_LK_CD_Parameter',
                             'validationRoutines.csv']
    
    
    #Sort order is import, means the order of data population have some dependency
    sortOrderExpression = "CASE WHEN fileName = 'dic_ddic_column' THEN CAST(1 AS INT)\
                                WHEN fileName = 'knw_LK_CD_ReportingSetup' THEN CAST(2 AS INT)\
                                WHEN fileName = 'knw_LK_CD_KnowledgeAccountSnapshot' THEN CAST(3 AS INT)\
                                WHEN fileName = 'knw_LK_CD_FinancialStructure' THEN CAST(4 AS INT)\
                                WHEN fileName = 'knw_LK_CD_FinancialStructureDetailSnapshot' THEN CAST(5 AS INT)\
                                WHEN fileName = 'knw_LK_CD_GLAccountToFinancialStructureDetailSnapshot' THEN CAST(6 AS INT)\
                                ELSE CAST(99999 AS INT) END"
    
    filePath = gl_metadataPath + 'dic_ddic_layerOneToLayerTwoFilesMapping.csv'      
    dfFilesToAuditProcedureMapping = objGenHelper.gen_readFromFile_perform(filePath).persist()  
    
    print("Load all mandatory files......")
    dfSoureFileMapping = _loadAllMandatoryFiles(dfFilesToAuditProcedureMapping,lstOfScopedAnalytics,\
                                                lstOfFilesToBeExcluded,sortOrderExpression)    
    lstOfFiles = _prepareFileList(dfSoureFileMapping)   
        
    lstOfFilesFailed = list()
    lstOfFilesSucceeded = list()
    lstOfStatus = _importFiles(lstOfFiles)
    
    [lstOfFilesFailed.append(f[1]) for f in list(itertools.filterfalse(lambda status : status[0] != False, lstOfStatus))]
    [lstOfFilesSucceeded.append(f[1]) for f in list(itertools.filterfalse(lambda status : status[0] != True, lstOfStatus))]
               
    print("Load all optional files......")
    dfOptionalMapping = _loadAllOptionalFiles(dfFilesToAuditProcedureMapping,lstOfScopedAnalytics,\
                             lstOfFilesToBeExcluded,sortOrderExpression,lstOfFilesSucceeded)      
    lstOfFiles = _prepareFileList(dfOptionalMapping.filter(col("flag") == '1'))       
    lstOfStatus = _importFiles(lstOfFiles)    
        
    [lstOfFilesFailed.append(f[1]) for f in list(itertools.filterfalse(lambda status : status[0] != False, lstOfStatus))]    
            
    if(len(lstOfFilesFailed) != 0):
        lstOfFiles = list()
        lstOfStatus = list()        
    
        print("Create data structure needed for execution......")

        [lstOfFiles.extend([[f.fileName,f.schemaName,f.tableName]]) \
        for f in  dfFilesToAuditProcedureMapping.filter((col("sourceLocation") != '/ddic/') & \
        (col("fileName").isin(lstOfFilesFailed))).\
        select(col("fileName"),col("schemaName"),col("tableName")).distinct().collect()]
        
        if(len(lstOfFiles)!=0):
            with ThreadPoolExecutor(max_workers = MAXPARALLELISM.DEFAULT.value) as executor:
                importStatus = list(executor.map(_createDataStructure,lstOfFiles))            
                  
        [lstOfStatus.append(f[1]) for f in list(itertools.filterfalse(lambda status : status[0] != False, importStatus))]
        if(len(lstOfStatus)!=0):
            failedFiles =","
            failedFiles = failedFiles.join(lstOfFilesFailed)
            print('Unable to load the files [' + failedFiles + ']')            
    
    if fin_L1_TD_Journal is not None:
        gl_countJET=fin_L1_TD_Journal.count()
    executionStatus = "Workspace preparation completed.." 
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [True,executionStatus]
    
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()
    print(executionStatus)
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [False,executionStatus]
