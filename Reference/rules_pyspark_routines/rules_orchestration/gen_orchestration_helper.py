# Databricks notebook source
class GenOrchestration():
    @staticmethod
    def paramaterFile_update(validationID):
        try:

            objGenHelper = gen_genericHelper()     
            objDataTransformation = gen_dataTransformation()

            logID = executionLog.init(PROCESS_ID.IMPORT,
                                     validationID = validationID,
                                     tableName = "Update parameter file")

            dfAppParam = gl_parameterDictionary['knw_LK_CD_Parameter']

            dfRoutineParam = spark.sql("select 'METADATA_VERSION'as name, '1' as parameterValue")        
          
            if (len(gl_parameterDictionary['InputParams'].select(col("fileType"))\
                .filter(col("fileType") == 'GLTB').distinct().rdd.collect()) > 0):
                lstParams = [{'name' : 'BALANCE_TYPE','parameterValue': 'GLTB'}]
            else:
                lstParams = [{'name' : 'BALANCE_TYPE','parameterValue': 'GLAB'}]
          
            dfParam = spark.createDataFrame(Row(**x) for x in lstParams)
            dfRoutineParam = dfRoutineParam.union(dfParam)

            lstRoutineParam = dfRoutineParam.select(col("name")).rdd.flatMap(lambda x: x).collect()          
            dfAppParam = dfAppParam.select(col("name"), col("parameterValue")).filter(col("name").isin(lstRoutineParam) == False)
            w = Window().orderBy(lit('name'))
            dfParam = dfAppParam.union(dfRoutineParam).withColumn("routineID",lit("GLOBAL")).withColumn("id", row_number().over(w))
            dfParam = objDataTransformation.gen_CDMColumnOrder_get(dfParam,'knw','LK_CD_Parameter')          
          
            dfParam = dfParam.withColumn("name",upper(col("name")))\
                        .withColumn("routineId",upper(col("routineId")))
            if('knw_LK_CD_Parameter' in gl_parameterDictionary.keys()):
                gl_parameterDictionary.pop('knw_LK_CD_Parameter')

            gl_parameterDictionary['knw_LK_CD_Parameter'] = dfParam   
            
            objGenHelper.gen_writeSingleCsvFile_perform(dfParam,gl_commonParameterPath + "knw_LK_CD_Parameter.csv")

            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,\
                            logID,"Parameter file updated successfully.")

        except Exception as e:  
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            raise
    
    @staticmethod
    def intializeAllOutputDirectories(validationID):
      try:
          objGenHelper = gen_genericHelper()

          logID = executionLog.init(PROCESS_ID.IMPORT,
                                     validationID = validationID,
                                     tableName = "Initialize output directories")

          #CDM  files
          #for f in gl_parameterDictionary["InputParams"].select((col("fileType")))\
          #    .distinct().rdd.collect():
          #    if (f.fileType == "GLA"):
          #        fileName1 = gl_CDMLayer1Path +"fin_L1_MD_GLAccount.csv"
          #        objGenHelper.gen_resultFiles_clean(fileName1)
          #        try:
          #            fileName1 = gl_CDMLayer1Path +"fin_L1_MD_GLAccount.delta"
          #            dbutils.fs.rm(fileName1,True)
          #        except Exception as e:
          #            if 'java.io.FileNotFoundException' in str(e):
          #                pass
          #    elif (f.fileType == 'GLAB'):
          #        fileName1 = gl_CDMLayer1Path +"fin_L1_TD_GLBalance.csv"
          #        objGenHelper.gen_resultFiles_clean(fileName1)
          #        try:
          #            fileName1 = gl_CDMLayer1Path +"fin_L1_TD_GLBalance.delta"
          #            dbutils.fs.rm(fileName1,True)
          #        except Exception as e:
          #            if 'java.io.FileNotFoundException' in str(e):
          #                pass
          #    elif (f.fileType == 'JET'):
          #        fileName1 = gl_CDMLayer1Path +"fin_L1_TD_Journal.csv"
          #        objGenHelper.gen_resultFiles_clean(fileName1)
          #        try:
          #            fileName1 = gl_CDMLayer1Path +"fin_L1_TD_Journal.delta"
          #            dbutils.fs.rm(fileName1,True)
          #        except Exception as e:
          #            if 'java.io.FileNotFoundException' in str(e):
          #                pass
          
          lstCDMFiles = [gl_CDMLayer1Path + 'fin_L1_STG_GLBalanceAccumulated.csv',
                        gl_CDMLayer1Path + 'fin_L1_STG_PYCYBalance.csv']          
          for f in lstCDMFiles:
              try:
                objGenHelper.gen_resultFiles_clean(f)                
                dbutils.fs.rm(f.replace(".csv",".delta"),True)
              except Exception as e:
                if 'java.io.FileNotFoundException' in str(e):
                   print('nothing to clean')
                else:
                  raise
          
          try:
              fileName1 = gl_commonParameterPath +"knw_LK_CD_ReportingSetup.delta"
              dbutils.fs.rm(fileName1,True)
          except Exception as e:
              if 'java.io.FileNotFoundException' in str(e):
                  pass
          
          try:
              dbutils.fs.rm(gl_executionLogResultPath + 'ExecutionLogDetails.csv')
          except Exception as e:
              if 'java.io.FileNotFoundException' in str(e):
                  pass
             
          executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,\
                            logID,"Output directory initialization completed.")

      except Exception as err:
           executionStatus = objGenHelper.gen_exceptionDetails_log()
           executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
           raise  

    @staticmethod
    def prepareFilesForL2Transformation():
        try:
            global gl_dfExecutionOrder
            global gl_maxParallelism

            objGenHelper = gen_genericHelper()
            erpSystemIDGeneric = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC'))

            logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION,                                     
                                     tableName = "Prepare L2 orchestration")

            isReprocessing = False
            routineName = None

            gl_dfExecutionOrder = gen_transformationExecutionOrder.\
                              transformationExecutionOrder_get(gl_lstOfScopedAnalytics,erpSystemIDGeneric)\
                              .persist()

            objGenHelper.gen_writeSingleCsvFile_perform(gl_dfExecutionOrder,
                                                        gl_executionLogResultPathL2 + FILES.TRANSFORMATION_ROUTINE_LIST_FILE.value)

            #check whether it is reprocessing or not  
            try:          
                gl_parameterDictionary['L2TransformationInputParams']= objGenHelper.\
                              gen_readFromFile_perform(gl_executionLogResultPathL2 + "L2TransformationInputParams.csv").\
                              select(col("executionID"),col("routineName"))    

                params = gl_parameterDictionary['L2TransformationInputParams'].\
                          select(col("executionID"),col("routineName")).limit(1).collect()

                executionID = params[0][0]
                routineName = params[0][1]

            except Exception as e:
                if 'java.io.FileNotFoundException' in str(e):
                    pass
            
            if(routineName is not None):
                isReprocessing = True
                mandatoryRoutines = ['gen_dimensionAttributesGLBalance_prepare.glBalanceAttributes_collect',
                                     'gen_dimensionAttributesJournal_prepare.journalAttributes_collect',
                                     'gen_dimensionAttributesKnowledge_prepare.knowledgeAttributes_collect',
                                     'gen_dimensionAttributesPlaceHolderValues_prepare']

                gl_dfExecutionOrder = gl_dfExecutionOrder.alias("A").\
                                join(gl_parameterDictionary['L2TransformationInputParams'].alias("B"),
                                   [lower(col("A.objectName")) == lower(col("B.routineName"))],how='inner').\
                                select(col("A.objectName"),
                                      col("A.parameters"),
                                      col("A.hierarchy")).\
                                union(gl_dfExecutionOrder.alias("C").\
                                select(col("C.objectName"),\
                                       col("C.parameters"),\
                                       col("c.hierarchy"))\
                                .filter(col("C.objectName").isin(mandatoryRoutines))).distinct().persist()
                                      
            gl_maxParallelism = gl_dfExecutionOrder.\
                             filter(col("hierarchy") >0).\
                             groupBy(col("hierarchy")).\
                             agg(count("hierarchy").alias("maxHierarchy")).\
                             orderBy(col("maxHierarchy").desc()).collect()[0][1]

            if(gl_maxParallelism > MAXPARALLELISM.DEFAULT.value):
                gl_maxParallelism = MAXPARALLELISM.DEFAULT.value
                        
            ERPOrchestration.loadAllSourceFiles_load(processID=PROCESS_ID.L2_TRANSFORMATION,
                                         processStage=PROCESS_STAGE.L2_TRANSFORMATION,
                                         isReprocessing = isReprocessing)

            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,\
                            logID,"L2 orchestration completed.")

        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            raise

   

