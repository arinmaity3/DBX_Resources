# Databricks notebook source
import sys
import traceback

class inputParams():
    @staticmethod
    def inputParams_load(processID = None):
        try:
            objGenHelper = gen_genericHelper()

            if(processID == PROCESS_ID.L2_TRANSFORMATION):
                inputParams.updateParameters()

            objGenHelper.gen_readFromFile_perform(gl_commonParameterPath + "knw_LK_CD_Parameter.csv")\
                        .select(col("Id").cast("int"),
                                col("routineId"),
                                col("name"),
                                col("parameterValue"))\
                        .createOrReplaceTempView("knw_LK_CD_Parameter")  
            sqlContext.cacheTable("knw_LK_CD_Parameter")
            df = spark.sql("select * from knw_LK_CD_Parameter")
            gl_parameterDictionary["knw_LK_CD_Parameter"] = df

            #for generic pipeline
            try:
                dbutils.fs.ls(gl_inputParameterPath + "InputParams.csv")
                gl_parameterDictionary['InputParams'] = objGenHelper.gen_readFromFile_perform(\
                                                        filenamepath = gl_inputParameterPath + "InputParams.csv",\
                                                        inferSchema = True).\
                                                        select(col("fileID"),
                                                          col("fileName"),
                                                          col("fileType"),
                                                          col("columnDelimiter"),
                                                          col("textQualifier"),
                                                          col("thousandSeperator"),
                                                          col("decimalSeperator"),
                                                          col("dateFormat"),
                                                          col("executionID"),
                                                          when(col("IsCustomTransform") == 0,False).\
                                                              otherwise(True).cast("boolean").alias("isCustomTransform"))

            except Exception as e:
                if 'java.io.FileNotFoundException' in str(e):
                    pass

            try:
                dbutils.fs.ls(gl_inputParameterPath + "SourceTargetColumnMapping.csv")
                gl_parameterDictionary['SourceTargetColumnMapping'] = objGenHelper.gen_readFromFile_perform(\
                                                        filenamepath = gl_inputParameterPath + "SourceTargetColumnMapping.csv",\
                                                        inferSchema = True)
            except Exception as e:
                if 'java.io.FileNotFoundException' in str(e):
                    pass
        except Exception as err:
            raise

     
    def updateParameters():
        try:            
            dbutils.fs.ls(gl_CDMLayer1Path + 'fin_L1_TD_Journal.delta')
            fin_L1_TD_Journal = objGenHelper.gen_readFromFile_perform(gl_CDMLayer1Path + "fin_L1_TD_Journal.delta")
            fin_L1_TD_Journal.createOrReplaceTempView('fin_L1_TD_Journal')
            jetCount = fin_L1_TD_Journal.count()
            jetnumtr = jetCount//100
            dfLargeAmount = spark.sql(" SELECT amountLC from fin_L1_TD_Journal ORDER BY amountLC DESC limit  "+str(jetnumtr))
            largeAmount = str(dfLargeAmount.agg({"amountLC": "min"}).collect()[0][0])
            
            dfParam = objGenHelper.gen_readFromFile_perform(gl_commonParameterPath + "knw_LK_CD_Parameter.csv")\
                      .select(col("Id").cast("int"),
                                col("routineId"),
                                col("name"),
                                col("parameterValue")).filter(col('name')!='LARGE AMOUNT')
            
            maxParamID  = int(dfParam.agg({"Id": "max"}).collect()[0][0]) + 1
            newRow = list()
            newRow.append([maxParamID,'GLOBAL','LARGE AMOUNT',largeAmount])
            df = spark.createDataFrame(data = newRow, schema = dfParam.schema)
            dfParam = dfParam.union(df)
            parameterFilePath = gl_commonParameterPath + "knw_LK_CD_Parameter.csv"
            objGenHelper.gen_writeSingleCsvFile_perform(df = dfParam,targetFile = parameterFilePath)            
         
        except Exception as err:
            if 'java.io.FileNotFoundException' in str(err):
              pass            
            else:
              raise          
