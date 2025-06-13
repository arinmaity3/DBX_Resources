# Databricks notebook source
import pathlib
from pathlib import Path
import uuid
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from dateutil.parser import parse
from pyspark.sql.functions import row_number,lit ,abs,col, asc,upper,expr,concat,trim,rpad,regexp_replace,date_format,to_timestamp,lpad
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,ShortType ,BooleanType ,TimestampType
from datetime import datetime
import csv
import traceback
import pandas as pd
from pandas import DataFrame
import inspect
import sys
import os
import uuid
import io

class gen_genericHelper():
    """Class that contains generic helper functions."""
    
    def __init__(self):
        global spark
        spark = SparkSession.builder.appName("common functions").getOrCreate()

    def gen_convertToStandardDateFormat_perform(self,dfSource,ls_OfDateColumns,dateFormat):
         try:
              if dateFormat is None:
                 dateFormat = 'YYYY-MM-DD'
              
              if dateFormat == 'yyyyMMdd':
                  date_format_sql = dateFormat
              else:
                  date_format_sql = dateFormat.replace("YYYY","yyyy").replace("DD","d").replace("MM","M")

              dfCommonDataformat = dfSource
              if not ls_OfDateColumns:
                  return dfCommonDataformat
              else: #If date columns list is not empty, convert the columns to std format
                  sqlstr = ""
                  source_table = uuid.uuid4().hex
                  dfCommonDataformat.createOrReplaceTempView(source_table)
                  sqlstr = "".join([",case when nullif(ltrim(rtrim(`"+columnname+"`)),'') is null then `"+columnname+"` else TO_DATE(`"+columnname+"`,'"+date_format_sql+"') end as `"+ columnname+"_derived`" for columnname in  ls_OfDateColumns])   
                  dfCommonDataformat = spark.sql("select * "+ sqlstr +" from " + source_table)
                  for columnname in ls_OfDateColumns:
                      colDerived =columnname+'_derived'
                      dfCommonDataformat = dfCommonDataformat.drop(columnname)
                      dfCommonDataformat = dfCommonDataformat.withColumnRenamed(colDerived,columnname)
              return dfCommonDataformat

         except Exception as err:
             raise

    def gen_convertToStandardTimeFormat_perform(self,dfSource,ls_OfTimeColumns):
        try:
            dfCommonTimeformat = dfSource
            if not ls_OfTimeColumns:
                return dfCommonTimeformat
            else: #If time columns list is not empty, convert the columns to std format
                for columnname in ls_OfTimeColumns:
                    colDerived = columnname+'_derived'
                    colconverted = columnname+'_converted'
                    colintegertime = columnname+'_integertime'
                    collist = [columnname,colconverted,colintegertime]

                    dfCommonTimeformat = dfCommonTimeformat.withColumn(colconverted, \
                                   when((trim(col(columnname)).substr(0,2) == "12") & ((trim(col(columnname)).endswith("AM") == True) | \
                                   ((trim(col(columnname)).endswith("am") == True))), \
                                       F.concat(lit("00"), trim(col(columnname)).substr(F.lit(3),length(trim(columnname)))))\
                                  .when((trim(col(columnname)).substr(0,2) != "12") & (trim(col(columnname)).substr(2,1) != ":") & \
                                 ((trim(col(columnname)).endswith("PM") == True) | (trim(col(columnname)).endswith("pm") == True)), \
                                       F.concat(trim(col(columnname)).substr(0,2).cast("int") \
                                       + 12, trim(col(columnname)).substr(F.lit(3),length(trim(columnname))))) \
                                  .when((trim(col(columnname)).substr(2,1) == ":") & ((trim(col(columnname)).endswith("PM") == True) | \
                                  (trim(col(columnname)).endswith("pm") == True)), \
                                       F.concat(trim(col(columnname)).substr(0,1).cast("int") \
                                       + 12, trim(col(columnname)).substr(F.lit(2),length(trim(columnname))))) \
                                  .when(trim(col(columnname)).isNull(),lit("00:00:00"))
                                  .otherwise(trim(col(columnname))))

                    dfCommonTimeformat = dfCommonTimeformat.withColumn(colintegertime, when(length(trim(colconverted)) == 5, \
                                                                rpad(trim(regexp_replace(colconverted, 'am|pm|AM|PM|:|\.', '')),6,'0'))\
                                                                .when(length(trim(colconverted)) < 5, \
                                                                lpad(rpad(trim(regexp_replace(colconverted, 'am|pm|AM|PM|:|\.', '')),5,'0'),6,'0')) \
                                                                .otherwise(lpad(trim(regexp_replace(colconverted, 'am|pm|AM|PM|:|\.', '')),6,'0')))

                    dfCommonTimeformat = dfCommonTimeformat.withColumn(colDerived,date_format(to_timestamp(colintegertime,"HHmmss"),"HH:mm:ss"))
                    dfCommonTimeformat = dfCommonTimeformat.drop(*collist)
                    dfCommonTimeformat = dfCommonTimeformat.withColumnRenamed(colDerived,columnname)

                return dfCommonTimeformat
        except Exception as err:
            raise

    def gen_convertToStandardNumberFormat_perform(self,dfSource,ls_OfNumColumns,decimalSeperator,thousandSeperator):
         try:             
             dfStdNumberFormat = dfSource             
             if not ls_OfNumColumns:
                 return dfStdNumberFormat
             else: #If numeric columns list is not empty, convert the columns to std format
                 sqlstr = ""
                 source_table = uuid.uuid4().hex
                 dfStdNumberFormat.createOrReplaceTempView(source_table)
                 sqlstr = "".join([",ltrim(rtrim(replace(replace(`"+columnname+"`,'"+thousandSeperator+"',''),'"+decimalSeperator+"','.'))) as `" +columnname+"_derived`" for columnname in  ls_OfNumColumns])   
                 dfStdNumberFormat = spark.sql("select * "+sqlstr+ " from "+ source_table)
                  
                 for columnname in ls_OfNumColumns:
                     colDerived = columnname+'_derived'
                     dfStdNumberFormat = dfStdNumberFormat.drop(columnname)
                     dfStdNumberFormat = dfStdNumberFormat.withColumn(columnname, when(dfStdNumberFormat[colDerived] == '',lit(None)).otherwise(dfStdNumberFormat[colDerived]))
                     dfStdNumberFormat = dfStdNumberFormat.drop(colDerived)                     
             return dfStdNumberFormat

         except Exception as err:
             raise

    def gen_convertToStandardDateFormatExcel_perform(self,dfSource,fileName,fileID,fileType,dateFormat):
         try:
              if dateFormat is None:
                 dateFormat = 'YYYY-MM-DD'

              columnMappingTable = 'columnMapping_table_' + fileID.replace('-','_').lower()
              ddicColumnTable = 'dic_ddic_column_table_' + fileID.replace('-','_').lower()

              gl_parameterDictionary["SourceTargetColumnMapping"].filter((col("fileID")) == fileID).createOrReplaceTempView(columnMappingTable)
              gl_metadataDictionary["dic_ddic_column"].filter(((col("fileType")) == fileType) & ((col("dataType")) == "date")).\
                                        createOrReplaceTempView(ddicColumnTable)

              listDateColumns = spark.sql('select '
                                 'C.sourceColumn '
                                 'from '+ columnMappingTable +' AS C '
                                 'inner join '+ ddicColumnTable +' AS D '
                                 '    on  D.columnName = C.targetColumn').collect()
              cleanedColumnList = [row_of_columnname.sourceColumn.replace('[', '').replace(']', '') for row_of_columnname in listDateColumns]

              pdf = dfSource.toPandas()

              for sourceColumn in cleanedColumnList:
                  pdf[sourceColumn] = pd.to_datetime(pdf[sourceColumn]).dt.date

              sparkDF = spark.createDataFrame(pdf) 

              return sparkDF

         except Exception as err:
             raise
    
    def gen_lk_cd_parameter_get(self,routineID,parameterName):
          try:
              parameterValue = gl_parameterDictionary["knw_LK_CD_Parameter"].select(col("parameterValue")).filter(((col("routineId")) == routineID.upper()) & ((col("name")) == parameterName.upper())).collect()[0][0]
              return parameterValue
          except IndexError:
              return None
          except ValueError as err:
              raise   
    def gen_changeDateFormat_perfom(self,filePath,pandasDF):
           try:
            global gl_lstOfImportValidationSummary
            global gl_lstOfImportValidationDetails
            fileName = os.path.basename(filePath)
            

            df= gl_parameterDictionary["InputParams"].select(col('*'))

            df1 = df.filter(col("fileName") == fileName).select("fileID","fileType","dateFormat")

            
            fileID     = df1.collect()[0][0]
            fileType   = df1.collect()[0][1]
            dtformat   = df1.collect()[0][2]
            
        
            gl_parameterDictionary["SourceTargetColumnMapping"].filter((col("fileID") == fileID)).createOrReplaceTempView("vw_app_date_format_colMapping")
         
       
            gl_metadataDictionary["dic_ddic_column"].select(col("columnName")).filter((col("fileType") == fileType) & (col("dataType") == "date")).createOrReplaceTempView("vw_app_date_format_dic_column")


            column_names_list = spark.sql("SELECT B.sourceColumn \
                                              FROM  vw_app_date_format_dic_column  A \
                                              INNER JOIN vw_app_date_format_colMapping  B \
                                              ON A.columnName = B.targetColumn").collect()

            cleanedColumnList = [row_of_columnname[0].replace('[', '').replace(']', '') for row_of_columnname in column_names_list]
           

      
            dtformat = dtformat.replace("DD", "%d")
            dtformat = dtformat.replace("MM", "%m")
            dtformat = dtformat.replace("YYYY", "%Y")
        
            for columnname in cleanedColumnList:
              pandasDF[columnname] = pd.to_datetime(pandasDF[columnname]).dt.strftime(dtformat)


            gl_metadataDictionary["dic_ddic_column"].select(col("columnName")).filter((col("fileType") == fileType)).createOrReplaceTempView("vw_app_dic_column_All")        
          
            column_names_list_colMapping = spark.sql("SELECT B.sourceColumn \
                                              FROM  vw_app_dic_column_All  A \
                                              INNER JOIN vw_app_date_format_colMapping  B \
                                              ON A.columnName = B.targetColumn").collect()              
 
            column_names_list_excel= pandasDF.columns.values.tolist()
  

            ColumnList_colMapping = [row_of_columnname[0].replace('[', '').replace(']', '') for row_of_columnname in column_names_list_colMapping]
  
            column_names_list_extraInExcel = [x for x in column_names_list_excel if x not in ColumnList_colMapping]
          
            #pandasDF.display() # line to be removed since there is no .display for pandas DF
            
            for columnname in column_names_list_extraInExcel:
              pandasDF[[columnname]] = pandasDF[[columnname]].astype(str) 
            return pandasDF
       
            
           except Exception as e:            
            raise
        
    def gen_readFromFile_perform(self, filenamepath, 
                                 delimiter=',',
                                 encoding='UTF-8',
                                 isPersists = False,
                                 textQualifier='"',
                                 inferSchema = False,
                                 multiLine = False,
                                 ignoreLeadingWhiteSpace = False,
                                 ignoreTrailingWhiteSpace = False,
                                 escape = '\\'):

        """Function that loads from file based on input file extension."""
        try:
            if (isinstance(filenamepath,list)):
              filetype = pathlib.Path(filenamepath[0]).suffix.lower()
            else:
              filetype = pathlib.Path(filenamepath).suffix.lower()

            if (filetype == '.csv'):
                if(isPersists == False ):
                    return spark.read \
                        .option("header", "true") \
                        .option("delimiter", delimiter) \
                        .option("encoding",encoding) \
                        .option("inferSchema", inferSchema)\
                        .option("multiLine",multiLine)\
                        .option("ignoreLeadingWhiteSpace",ignoreLeadingWhiteSpace)\
                        .option("ignoreTrailingWhiteSpace",ignoreTrailingWhiteSpace)\
                        .option("escape",escape)\
                        .csv(filenamepath,quote = textQualifier)
                else:
                    return spark.read \
                        .option("header", "true") \
                        .option("delimiter", delimiter) \
                        .option("encoding",encoding) \
                        .option("inferSchema", inferSchema)\
                        .option("multiLine",multiLine)\
                        .csv(filenamepath,quote = textQualifier)\
                        .cache()
            elif (filetype == '.txt'):
                if(isPersists == False ):
                    return spark.read \
                        .option("header", "true") \
                        .option("delimiter", delimiter) \
                        .option("encoding",encoding) \
                        .option("quote",'') \
                        .option("escape", "\"")\
                        .csv(filenamepath)
                else:
                    return spark.read \
                        .option("header", "true") \
                        .option("delimiter", delimiter) \
                        .option("encoding",encoding) \
                        .option("quote",'') \
                        .option("escape", "\"")\
                        .csv(filenamepath)\
                        .cache()
            elif filetype == '.parquet':
                if(isPersists == False):
                    return spark.read \
                        .option("header", "true") \
                        .parquet(filenamepath) 
                else:
                    return spark.read \
                        .option("header", "true") \
                        .parquet(filenamepath) \
                        .cache()
            elif (filetype == '.xls') or (filetype == '.xlsx') :
                if (filetype == '.xls'):
                    pandasDF = pd.read_excel(io = '/dbfs/'+filenamepath) 
                else:
                    pandasDF = pd.read_excel(io = '/dbfs/'+filenamepath ,engine='openpyxl')

                listdf = list(pandasDF)
                pdf = pd.DataFrame(listdf,columns=['Column_Name'])
                if pdf.empty:
                    emptyDFSchema = StructType([
                                                 StructField("blankData", StringType(),True)
                                               ])
                    return(spark.createDataFrame(spark.sparkContext.emptyRDD(),emptyDFSchema))
                else:
                    if pandasDF.empty:
                        df = spark.createDataFrame(spark.sparkContext.emptyRDD(),StructType([]))
                        for columnname in listdf:
                            df = df.withColumn(columnname, lit(None))
                        
                        return(df)
                    else:
                        return(spark.createDataFrame(pandasDF))
            elif (filetype == ".delta"):
              return spark.read \
                        .format("delta") \
                        .load(filenamepath) 
            else:
                raise ValueError('Invalid path or file extension: '+ filenamepath )
        except Exception as ex:
          raise 

    def gen_writeToFile_perfom(self, df, 
                               filenamepath, 
                               mode='overwrite',
                               isCreatePartitions = False,
                               partitionColumns = "",
                               isOverWriteSchema = False,
                               isCreateDeltaTable = False,
                               tableName = "",
                               isSingleCsvFile = False,
                               isMergeSchema = False,
                               isSelectiveOverwrite = False,
                               overwriteCondition = None):
        """Function that writes the passed dataframe to a file based on the passed file path and filename-with-extension."""        
        try:
            if df is not None:
                filetype = Path(filenamepath).suffix.lower()
                if (filetype == '.csv') or (filetype == '.txt'):
                    if (isSingleCsvFile == True) and (filetype == '.csv'):
                        df.coalesce(1).write.csv(path = filenamepath,mode = mode,quote = None,header = True,emptyValue = '',escape = '"')
                    else:
                        df.write.csv(path = filenamepath,mode = mode,quote = None,header = True,emptyValue = '',escape = '"')
                elif filetype == '.parquet':
                    if((isCreatePartitions == True) and (len(partitionColumns)!=0)):
                      df.write.option("header", True) \
                          .partitionBy(partitionColumns)\
                          .mode(mode) \
                          .parquet(filenamepath)
                    else:
                      df.write.option("header", True) \
                          .mode(mode) \
                          .parquet(filenamepath)
                elif(filetype == ".delta"):
                    if (isSelectiveOverwrite == False):                       
                        if (isCreatePartitions == False):
                            df.write.format("delta")\
                                        .mode(mode)\
                                        .option("overwriteSchema",isOverWriteSchema)\
                                        .option("mergeSchema",isMergeSchema)\
                                        .save(filenamepath)
                        elif((isCreatePartitions == True) and (len(partitionColumns)!=0)):
                            df.write.format("delta")\
                                        .partitionBy(partitionColumns)\
                                        .mode(mode)\
                                        .option("overwriteSchema",isOverWriteSchema)\
                                        .save(filenamepath)
                        elif((isCreatePartitions == True) and (partitionColumns.strip() == "")):
                            #yet to be defined
                            df.write.format("delta")\
                                        .mode(mode)\
                                        .option("overwriteSchema",isOverWriteSchema)\
                                        .save(filenamepath)
                        if ((isCreateDeltaTable == True) & (tableName.strip() != "")) :
                            query = "CREATE TABLE IF NOT EXISTS " + tableName + " USING DELTA LOCATION '" + filenamepath + "'"
                            spark.sql(query)
                    else:
                        if (isCreatePartitions == False):
                            df.write.format("delta")\
                                        .mode(mode)\
                                        .option("overwriteSchema",isOverWriteSchema)\
                                        .option("mergeSchema",isMergeSchema)\
                                        .option("replaceWhere",overwriteCondition)\
                                        .save(filenamepath)
                        elif((isCreatePartitions == True) and (len(partitionColumns)!=0)):
                            df.write.format("delta")\
                                        .partitionBy(partitionColumns)\
                                        .mode(mode)\
                                        .option("overwriteSchema",isOverWriteSchema)\
                                        .option("replaceWhere",overwriteCondition)\
                                        .save(filenamepath)
                        elif((isCreatePartitions == True) and (partitionColumns.strip() == "")):
                            #yet to be defined
                            df.write.format("delta")\
                                        .mode(mode)\
                                        .option("overwriteSchema",isOverWriteSchema)\
                                        .option("replaceWhere",overwriteCondition)\
                                        .save(filenamepath)

                else:
                    raise ValueError('Invalid path or file extension: '+ filenamepath )
        except Exception as ex:
          raise 
               
    def gen_table_exists(self,tableName):
      """Check a table exists or not."""
      try:
        tableList  = sqlContext.tableNames("default") 
        isExists = False
        for tabName in tableList:
          if (tabName.upper() == tableName.upper()):
            isExists = True
            break 
        return isExists
      except Exception as err:
        raise
   
    def gen_writeSingleCsvFile_perform(self,df,targetFile = ''):
        try:
            if isinstance(df,pyspark.sql.DataFrame):
                guid = uuid.uuid4().hex
                sourceFilePath = targetFile.replace('.csv','_'+guid+'.csv')
                sourceFolderPath = sourceFilePath+'/'
                self.gen_writeToFile_perfom(df = df,filenamepath = sourceFilePath,isSingleCsvFile = True)
                for i in dbutils.fs.ls(sourceFilePath):
                    if ((pathlib.Path(i[0]).suffix) == '.csv'):
                        sourceFolderPath = sourceFolderPath + i[1]
                        dbutils.fs.mv(sourceFolderPath,targetFile)
                self.gen_resultFiles_clean(sourceFilePath)
        except Exception as err:
            raise

    def gen_exceptionDetails_log(self):
        try:
            routineName = inspect.stack()[1][3].strip()            
            ex_type, ex_value, ex_traceback = sys.exc_info()             
            stack_trace = list()
            trace_back = traceback.extract_tb(ex_traceback)
            for trace in trace_back:
                stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))      
            errMessage = ("Exception type : %s "% ex_type.__name__ + "\n" + "Exception message : %s" %ex_value + "\n" + "Stack trace : %s" %stack_trace) 
            errMessage = errMessage.replace('\n',' ').replace('\r',' ')[:2000]
            return  "Execution failed at '" + routineName  + "'." +  errMessage
        except Exception as err:
            raise
               
    def gen_executionLog_write(self,isView = False,logFilePath = ""):
        try:
            executionLog = list()
            [executionLog.append(value) for key, value in gl_executionLog.items()]
            dfExecutionLog = spark.createDataFrame(data = executionLog,schema = gl_logSchema)                       
            if(logFilePath == ""):
                dfExecutionLog = dfExecutionLog.select(col("processID"),col("fileID"),col("routineName"),col("validationID"),\
                                                  col("status"),col("startTime"),\
                                                  col("endTime"),col("statusID"),col("comments"))
                dfExecutionLog = dfExecutionLog.sort(col("processID"),col("startTime"),col("endTime")) 
                self.gen_writeSingleCsvFile_perform(dfExecutionLog, gl_executionLogResultPath + "ExecutionLogDetails.csv")
            else:
                dfExecutionLog = dfExecutionLog.select(col("processID"),col("routineName").alias("objectName"),\
                                 col("statusID"),col("status"),col("startTime"),col("endTime"),\
                                 col("comments")).sort(col("startTime"),col("endTime")) 
                self.gen_writeSingleCsvFile_perform(dfExecutionLog, logFilePath)
                    
            if(isView == True):
              dfExecutionLog.display()
            return dfExecutionLog
        except Exception as err:
            raise
    
    

    def file_exist(self,folderPath):
        try:
            dbutils.fs.ls(folderPath)
            return True
        except Exception as e:
            if 'java.io.FileNotFoundException' in str(e):
                return False
            else:
                raise
                
    def gen_resultFiles_clean(self,folderPath):       
        try:                        
            if (self.file_exist(folderPath) == True):              
              for d in dbutils.fs.ls(folderPath):                 
                if (d.isDir()):
                  for f in dbutils.fs.ls(d.path):
                    if(f.isFile()):                                            
                      dbutils.fs.rm(f.path)
                    else:
                      dbutils.fs.rm(d.path)
                else:
                  if(d.isFile()):
                    dbutils.fs.rm(d.path)
            if (self.file_exist(folderPath) == True):
                f1 = dbutils.fs.ls(folderPath)
                if(len(f1) <=0 ):
                    dbutils.fs.rm(folderPath)
              
        except Exception as err:
            raise

    def gen_Directory_clear(self,directoryPath):
      try:
        for f in dbutils.fs.ls(directoryPath):
            if(f.isDir()):
                for d in dbutils.fs.ls(f.path):
                    if(d.isDir()):
                        dbutils.fs.rm(d.path,True)
                    else:
                        dbutils.fs.rm(f.path,True)
            self.gen_resultFiles_clean(f.path)
      except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            pass
        else:
            raise
    def gen_emptyColumns_drop(self,dfSource):
        try:
            records = dfSource.count()
            nullCols = list()
            for c in dfSource.schema.names:
                emptyColCount = dfSource.select(col(c)).filter((col(c).isNull()) | (trim(col(c)) == '')).count() == records
                if(emptyColCount == True):
                    nullCols.append(c)
            if(len(nullCols) > 0):
                dfSource = dfSource.drop(*nullCols)
            return dfSource
        except Exception as err:
            raise 

    def gen_moveFileFromFolder(self,sourceFolder,destinationFolder,newFileName,fileType):
        #move single file from folder
        try:
            objHelper = gen_genericHelper()
            fileType = '.'+fileType.lower()
            if (objHelper.file_exist(sourceFolder) == True):      
              if(([Path(f.name).suffix.lower() == fileType for f in dbutils.fs.ls(sourceFolder)]).count(True) == 1): #only one file exists in the folder        
                for f in dbutils.fs.ls(sourceFolder):          
                  if (Path(f.name).suffix.lower() == fileType): 
                    sourceFile = '/' + f.path.replace(':','')            
                    destFileName = uuid.uuid4().hex + fileType                        
                    Path(sourceFile).rename('/dbfs' + destinationFolder + destFileName)                                        
                    Path('/dbfs' + destinationFolder + destFileName).rename('/dbfs' + destinationFolder + newFileName + fileType)
        except Exception as err:
            raise 
        
    def gen_dynamicPivot(self,**kwargs): 
        try:
            pivotList=""
            orderBycolumnName=""
            cnt=0 
            for key, value in kwargs.items():
                if key.startswith("pivotColumn") and hasattr(value, "rdd")!=True:
                    pivotList+="'"+value+"',cast("+value+" as string) ,"
                    cnt+=1
                elif key.startswith("orderBycolumnName") and hasattr(value, "rdd")!=True:
                    orderBycolumnName+="'"+value+"'"
          
                elif hasattr(value, "rdd")==True: 
                  if orderBycolumnName!="":
                    window = Window().orderBy(lit(orderBycolumnName))
                    df=value.select("*").withColumn("groupSlno",row_number().over(window)).\
                        withColumn("validationObject",lit(kwargs['fileType'])).\
                        withColumn("validationID",lit(kwargs['validationID'])).\
                        withColumn("fileType",lit(kwargs['fileType']))
                  else:
                     df=value.select("*").withColumn("validationObject",lit(kwargs['fileType'])).\
                         withColumn("validationID",lit(kwargs['validationID'])).\
                         withColumn("fileType",lit(kwargs['fileType']))
            return df.select("groupSlno","validationID","validationObject",\
                "fileType",expr("stack("+str(cnt)+","+pivotList[:-1]+") as (resultKey,resultValue)"))  

        except Exception as err:
            raise

    def gen_placeHolderRecords_populate(self,tableName,keysAndValues,dfSource):
        #lstOfValues = {'organizationUnitSurrogateKey':0}
        try:
            analysisID = self.gen_lk_cd_parameter_get('GLOBAL', 'ANALYSISID')
            defaultRow = list()
            for r in gl_metadataDictionary['dic_ddic_column'].\
                  select(col("columnName"),col("dataType")).\
                  filter(lower(col("tableName2")) == tableName.lower()).orderBy(col("position")).rdd.collect():      
              if r.columnName in keysAndValues.keys():
                defaultRow.append(keysAndValues[r.columnName])
              elif (r.columnName.lower() == 'analysisid'):
                defaultRow.append(analysisID)
              elif r.dataType.lower() in ['varchar','char','nvarchar']:
                defaultRow.append('NONE')
              elif r.dataType.lower() in ['int','smallint','tinyint','numeric','float','bigint','decimal']:
                defaultRow.append('0')
              elif r.dataType.lower() in ['date']:
                defaultRow.append('1900-01-01')
              elif r.dataType.lower() in ['time']:
                defaultRow.append('00:00:00')
              elif r.dataType.lower() in ['bit']:
                defaultRow.append(False)
    
            newRow = list()
            newRow.append(defaultRow)
            df = spark.createDataFrame(data = newRow, schema = dfSource.schema)
            dfSource = dfSource.union(df)
            return dfSource    
        except Exception as err:
            raise
   
    def gen_copyFile_perform(self,sourceFileNamePath,targetPath):
        try:
            source = 'cp -a ' + '"/dbfs' + sourceFileNamePath + '"' + ' "/dbfs' + targetPath + '"'
            os.system(source)
        except Exception as err:
            raise

    def gen_writeandCache(self,dfSource,schemaName,tableName,\
                                 targetPath = "", mode = 'overwrite'):
        try:
            global gl_layer0Temp_temp
            
            if(targetPath == ""):
               targetPath = gl_layer0Temp_temp

            fullPath = targetPath + schemaName + "_" + tableName +".delta"
            self.gen_writeToFile_perfom(dfSource,fullPath,mode='overwrite')
            df = self.gen_readFromFile_perform(fullPath)
            df.createOrReplaceTempView(schemaName + "_" + tableName)
            return df
        except Exception as err:
            raise

    def gen_tablesRequiredForScopedAnalytics_get(self,gl_metadataDictionary,ERPSystemID,scopedAnalytics):
        try:
            scope_list = scopedAnalytics.split(",")
            scopedf = spark.createDataFrame(scope_list, StringType())

            allScope = scopedf.union(scopedf.alias('sc') \
                .join (gl_metadataDictionary['dic_ddic_auditProcedureToChildAnalyticsMapping'].alias('ch'), \
                col('sc.value') == col('ch.auditProcedureName'),'inner') \
                .select(col('ch.childauditProcedureName').alias('auditProcedureName')).distinct()).distinct()
            
            erpTables = gl_metadataDictionary['dic_ddic_tablesRequiredForERPPackages'].alias('erp') \
                .join(allScope.alias('sc'),\
                col('erp.auditProcedureName') == col('sc.value'),'inner') \
                .select(col('erp.schemaName').alias('schemaName') \
                ,col('erp.tableName').alias('tableName') \
                ,col('erp.sparkTableName').alias('sparkTableName') \
                ,col('erp.auditProcedureName').alias('auditProcedureName')) \
                .filter(col('erp.ERPSystemID') == ERPSystemID).distinct()

            return erpTables
            
        except Exception as err:
            raise

    def phaseWiseExecutionRoutinesList_prepare(self,processID,dfSource):
        try:  

          if(processID == PROCESS_ID.L1_TRANSFORMATION):
              statusFileFullPath = FILES_PATH.L1_TRANSFORMATION_ROUTINE_LIST_PATH.value + FILES.TRANSFORMATION_ROUTINE_LIST_FILE.value
              dirPath = FILES_PATH.L1_TRANSFORMATION_ROUTINE_LIST_PATH.value
              dbutils.fs.mkdirs(dirPath)
          elif (processID == PROCESS_ID.TRANSFORMATION_VALIDATION):
              dirPath = FILES_PATH.TRANSFORMATION_VALIDATION_ROUTINE_LIST_PATH.value
              statusFileFullPath = FILES_PATH.TRANSFORMATION_VALIDATION_ROUTINE_LIST_PATH.value + FILES.TRANSFORMATION_ROUTINE_LIST_FILE.value
              dfSource = dfSource.select(col("validationID"),
                                  col("routineName")).distinct()\
                                  .orderBy(col("executionOrder"))
              dbutils.fs.mkdirs(dirPath)
          
          self.gen_writeSingleCsvFile_perform(df = dfSource,\
                                              targetFile = statusFileFullPath)  
         
        except Exception as err:
            raise

    def transformationValdation_get(self,ERPSystemID,scopedAnalytics,filetypes = None):
        try:
            objGenHelper = gen_genericHelper()
            glabCount = 1
            jetCount = 1
            glaCount = 1
    
            if(ERPSystemID == 10):
                inputFileTypes = gl_parameterDictionary["InputParams"].select(col("fileType")).filter(col("fileType").isin(filetypes)).distinct().rdd.flatMap(lambda x: x).collect()
            else:
                inputFileTypes = ['GLA','JET','GLAB']

            lstReportsToBeConsiderd = list()
        
            if(fin_L1_TD_GLBalance.first() is None):
              glabCount = 0
            if(fin_L1_TD_Journal.first() is None):
              jetCount = 0
            if(fin_L1_MD_GLAccount.first() is None):
              glaCount = 0
        
            dfTransformationValidationMaster = gl_metadataDictionary['dic_ddic_genericValidationProcedures'].\
                                               select(col("validationID"),col("routineName"),col("methodName"),\
                                               col("parameterValue"),col("resultFile"),col("fileType"),col("executionOrder")).\
                                               filter((col("processID") == PROCESS_ID.TRANSFORMATION_VALIDATION.value) \
                                               & (col("validationID") != VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION.value))
            if(scopedAnalytics != None):
              if (any(item in ['JEA','STRA','TSIT','WDE'] for item in scopedAnalytics) and jetCount != 0):
                lstReportsToBeConsiderd.append(1001)
  
              if (any(item in ['JEA','AA','PA','STRA','TSIT','WDE'] for item in scopedAnalytics) and glabCount != 0  and jetCount != 0):
                lstReportsToBeConsiderd.append(1002)       
  
            dfTransformationValidationMaster = dfTransformationValidationMaster.\
                                              filter((col("fileType").isin(inputFileTypes)) | \
                                              (col("validationID").isin(lstReportsToBeConsiderd)))
            dfTransformationValidationMaster = dfTransformationValidationMaster.\
                                               withColumn("validationID",\
                                               dfTransformationValidationMaster.\
                                               validationID.cast("int"))      

            return dfTransformationValidationMaster

        except Exception as err:
            errorDescription = objGenHelper.gen_exceptionDetails_log().replace('\n',' ').replace('\r',' ')
            print(errorDescription)
            errorDescription = errorDescription[:2000]            
            raise 

    def importValdation_populate(self,gl_ERPSystemID):
        try:
            objGenHelper = gen_genericHelper()
  
            dfImportValidationMaster = gl_metadataDictionary['dic_ddic_genericValidationProcedures'].\
                               select(col("validationID"),col("routineName"),col("methodName") ,col("parameterValue")\
                               ,split(col("applicableInputFileTypes"),",").alias("fileTypes")).\
                               filter((col("processID") == PROCESS_ID.IMPORT_VALIDATION.value) & \
                               (col("validationID") != VALIDATION_ID.PACKAGE_FAILURE_IMPORT.value) & \
                               (col("ERPSystemID") == lit(gl_ERPSystemID)))
            
            dfImportValidationExtended = dfImportValidationMaster.select(dfImportValidationMaster.validationID,\
                                 explode(dfImportValidationMaster.fileTypes).alias("applicableInputFileTypes")) 

            dfImportValidationMaster = dfImportValidationMaster.drop("fileTypes")                   

            dfImportValidation = dfImportValidationMaster.join(dfImportValidationExtended, \
                        [(dfImportValidationMaster.validationID == dfImportValidationExtended.validationID) \
                        & (dfImportValidationExtended.applicableInputFileTypes == inputFileType)],how='inner'). \
                        select(dfImportValidationMaster["*"]).cache()    

            return dfImportValidation

        except Exception as err:
            errorDescription = objGenHelper.gen_exceptionDetails_log().replace('\n',' ').replace('\r',' ')
            print(errorDescription)
            errorDescription = errorDescription[:2000]            
            raise 

    def createPackageExecutionValidationResultFiles(self):
        try:
            for process in PROCESS_ID:
                if(process == PROCESS_ID.IMPORT_VALIDATION):
                    if(PROCESS_STAGE.IMPORT in gl_processStage):
                        transformationStatusLog.createLogFile(processID = PROCESS_ID.IMPORT_VALIDATION,
                                       validationID = VALIDATION_ID.PACKAGE_FAILURE_IMPORT.value,
                                       executionID = 'app_LA_STG_PackageExecutionValidationResult')
                elif(process == PROCESS_ID.TRANSFORMATION_VALIDATION):
                    if(PROCESS_STAGE.POST_TRANSFORMATION_VALIDATION in gl_processStage):
                        transformationStatusLog.createLogFile(processID = PROCESS_ID.TRANSFORMATION_VALIDATION,
                                       validationID = VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION.value,
                                       summaryFileName = 'ValidationSummary-app_LA_STG_PackageExecutionValidationResult.csv',
                                       detailFileName = 'ValidationDetails-app_LA_STG_PackageExecutionValidationResult.csv' )

                elif(process == PROCESS_ID.PRE_TRANSFORMATION_VALIDATION):
                    if(PROCESS_STAGE.PRE_TRANSFORMATION_VALIDATION in gl_processStage):
                        transformationStatusLog.createLogFile(processID = PROCESS_ID.PRE_TRANSFORMATION_VALIDATION,
                                       validationID = VALIDATION_ID.PACKAGE_FAILURE_TRANSFORMATION.value,
                                       executionID = 'app_LA_STG_PackageExecutionValidationResult')                                      

        except Exception as err:
            raise

    def generateExecutionLogFile(self,executionLog,logSchema,targetPath):
      try:          
          lstOfLogs = list()

          [lstOfLogs.append([log["logID"],
                      log["fileID"],
                      log["fileName"],
                      log["fileType"],
                      log["tableName"],
                      PROCESS_ID(log["processID"]).value,
                      log["validationID"],
                      log["routineName"],
                      LOG_EXECUTION_STATUS(log["statusID"]).value,
                      log["status"],
                      log["startTime"],
                      log["endTime"],
                      log["comments"]])
          for log in list(executionLog.values())]

          dfExecutionLog = spark.createDataFrame(data = lstOfLogs,schema = logSchema)
          dfExecutionLog = dfExecutionLog.drop('logID')
          self.gen_writeSingleCsvFile_perform(df = dfExecutionLog,\
                                                            targetFile = targetPath)  
          return dfExecutionLog
      except Exception as err:
          raise
    def isStagingFileEmpty(self,tableName):
        try:          
            lsEmptyTable= gl_parameterDictionary[FILES.KPMG_ERP_STAGING_RECORD_COUNT].\
            filter((col("netRecordCount")==0)&(col("tableName")==tableName)).collect()
            if lsEmptyTable:
                return True
            else:
                return False
        except Exception as err:
            raise
    def enableAccountMapping_check(self):
        try:
          objGenHelper = gen_genericHelper()
          scopedAnalytics=objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'SCOPED_ANALYTICS')
          if scopedAnalytics is None:
                return 0
          else:
                ls_Scoped_auditProcedures=objGenHelper\
                    .gen_tablesRequiredForScopedAnalytics_get(gl_metadataDictionary\
                                                             ,gl_ERPSystemID,scopedAnalytics)\
                    .select(col("auditProcedureName")).distinct().collect()
                ls_Scoped_auditProceduresNeedMapping=\
    		    [row.auditProcedureName  for row in ls_Scoped_auditProcedures 
                            if row.auditProcedureName in ['PEX','GMTR','GTWA','GMT',
    			                                          'LEAD','MAT','KPI','PA','AA'] ]
                if ls_Scoped_auditProceduresNeedMapping:
                    return 1
                elif scopedAnalytics!='JEA':
                    return 0
                else:
                    if   not(objGenHelper.isStagingFileEmpty('erp_GLT0'))\
                      or not(objGenHelper.isStagingFileEmpty('erp_FAGLFLEXT'))\
                      or not(objGenHelper.isStagingFileEmpty('erp_GL_BALANCES'))\
                      or not(objGenHelper.isStagingFileEmpty('fin_L1_TD_GLBalance')):
                         return 1
                    else:
                        return 0

        except Exception as err:
            raise

    def miscellaneousLogWrite(self,targetFilePath,data):
        try:   
            blankDataFrame = [{"Execution directory":''}]
            existingContent = spark.createDataFrame(blankDataFrame)
            if (self.file_exist(targetFilePath + "misc.csv") ==  True):
                existingContent = self.gen_readFromFile_perform(filenamepath = targetFilePath + "misc.csv")
            
            newDataFrame = [{"Execution directory":data}]
            newContent = spark.createDataFrame(newDataFrame)
            miscData = existingContent.union(newContent)
            self.gen_writeSingleCsvFile_perform(df = miscData,targetFile = targetFilePath + "misc.csv")
        except Exception as err:
            raise

    def mandatoryRoutines_get(self,lstOfScopedAnalytics):
        try:
            lstOfDependentRoutines = list()
            if('dic_ddic_auditProcedureToChildAnalyticsMapping' in gl_metadataDictionary.keys()):
                [lstOfDependentRoutines.append(r.childauditProcedureName) \
                    for r in gl_metadataDictionary['dic_ddic_auditProcedureToChildAnalyticsMapping'].\
                        filter(col("auditProcedureName").isin(lstOfScopedAnalytics)).\
                        select(col("childauditProcedureName")).distinct().collect()]

            return lstOfDependentRoutines
        except Exception as err:
            raise

    def gen_database_create(self,dbName=None):
        try:
            spark.sql("create database IF NOT EXISTS "+dbName)  
            print('Database [' + dbName + '] has been created')
        except Exception as e:            
            raise
        
    def gen_database_drop(self,dbName=None):
        try:
            spark.sql("DROP database IF EXISTS "+dbName)  
            print('Database [' + dbName + '] has been dropped')
        except Exception as e:
            raise
    
    def gen_isTableExist(self,dbName=None,schemaTable=None):
        try:
            db_tbl_name = dbName +'.'+schemaTable
            table_exist = False
            try:
                spark.read.table(db_tbl_name)
                table_exist = True        
            except:
                pass
            return table_exist

        except Exception as e:
            raise
        
    def gen_columnsList_get(self,schemaName=None,tableName=None,ERPSystemID=None):
        try:
            columnList=""

            if ERPSystemID == None:
              ERPSystemID = gl_ERPSystemID

            for r in gl_metadataDictionary["dic_ddic_column"]\
                .select(col("tableName2"),\
                col("columnName"),\
                col("position"),\
                when(col("sparkDataType")=="Decimal",concat(col("sparkDataType"),lit("("),coalesce(col("fieldLength"),lit("32,2")),lit(")")))\
                .otherwise(col("sparkDataType")).alias("sparkDataType"))\
                .filter("upper(schemaName) = '" + schemaName.upper() \
                +"' AND  isimported =False"\
                + " AND upper(tableName) = '" + tableName.upper() + "'") \
                .orderBy(col("position").cast("int").asc()).collect():

                columnList = columnList + str(r["columnName"])+str(" ")+str(r["sparkDataType"]) +str(",") + os.linesep

            columnList = columnList[:-2]
            return columnList
        except Exception as e:
            raise
        
    def gen_table_create(self,dbName = None,schemaName = None,tableName = None,
                         isDropAndCreate = False,
                         isDropDeltalakeFile = False,
                         isDeltaLakeTable = False,
                         filePath   = None,
                         isEnableChangeDataFeed = False,
                         isCreatePartition = False,
                         partitionColumn = None):
        try:

            schemaTable = schemaName+"_"+tableName
            if(isDropAndCreate == True):
              self.gen_table_drop(dbName,schemaName,tableName,isDropDeltalakeFile,filePath)
            
            fileName_tblname = filePath+schemaName+"_"+tableName+".delta"

            isTableExists  = self.gen_isTableExist(dbName= dbName,schemaTable=schemaTable)
            isFileExists = True
            try:
              dbutils.fs.ls(fileName_tblname)
            except Exception as e:  
                if 'java.io.FileNotFoundException' in str(e): 
                  pass
                  isFileExists = False
            
            if isFileExists == True and isTableExists== False:
              Query = "CREATE TABLE IF NOT EXISTS "+dbName+"."+schemaName+"_"+tableName+" USING DELTA LOCATION '" + fileName_tblname + "'"
              spark.sql(Query)
            else:
              
              if isTableExists == True:
                  msg = "Table [" + schemaTable +"] already exist in the DB [" + dbName + "]"                
              else:  
                  CDMColumnList = self.gen_columnsList_get(schemaName=schemaName,tableName=tableName)
                  CreateTableScript= "CREATE TABLE "+dbName+"."+schemaTable+"("+ CDMColumnList +")"

                  if isDeltaLakeTable == True:
                      CreateTableScript = CreateTableScript + "USING DELTA LOCATION '"+ filePath + schemaTable+".delta" +"'"
                  
                  if isCreatePartition == True:
                      CreateTableScript = CreateTableScript + " PARTITIONED BY ("+ partitionColumn +")"

                  if isEnableChangeDataFeed == True:
                      CreateTableScript = CreateTableScript + "TBLPROPERTIES (delta.enableChangeDataFeed = true);"

                  spark.sql(CreateTableScript)
                  #print("[" + tableName + "] is created in the database [" + dbName + "]")
            
            spark.sql("SELECT * FROM "+dbName+"."+schemaName+"_"+tableName).createOrReplaceTempView(schemaName+"_"+tableName)

        except Exception as e:
            raise
        
    def gen_table_drop(self,dbName=None,schemaName=None,tableName=None,isDropDeltalakeFile=False,filePath=None):
        try:
            SchemaTable = schemaName+"_"+tableName

            spark.sql("DROP TABLE IF EXISTS "+dbName+"."+SchemaTable)
            print("Table [" + tableName + "] has been dropped from the database [" + dbName +"]")
            if isDropDeltalakeFile == True:
                fileName = schemaName+"_"+tableName+".delta"
                try:
                    dbutils.fs.rm(filePath+fileName,True)
                    print("Delta file [" + fileName + "] has been removed.")
                except:
                    pass

        except Exception as e:
            raise

    def data_existsinFile(self,filePath):
        try:
            filedf = self.gen_readFromFile_perform(filePath)
            if(filedf.first() is None):
                return False
            else:
                return True
        except Exception as err:
            raise

    def gen_all_table_db_drop(self,dbName=None):
      try:

        df_tbl_all = spark.sql("show tables from "+dbName)
        for x in df_tbl_all.collect():
          SchemaTable = x["tableName"]
          spark.sql("DROP TABLE IF EXISTS "+dbName+"."+SchemaTable)

        self.gen_database_drop(dbName=dbName)

      except Exception as e:
        raise
           
    def gen_tableFromDeltaFile_create(self,dbName=None,filePath=None):
      try:

        self.gen_database_create(dbName=dbName)

        for x in dbutils.fs.ls(filePath):
          tblname = x[1].replace(".delta/","")
          fileName_tblname = filePath + x[1].replace("/","")
          file_name, file_extension = os.path.splitext(fileName_tblname)

          if file_extension ==".delta":
            Query = "CREATE TABLE IF NOT EXISTS "+dbName+"."+tblname+" USING DELTA LOCATION '" + fileName_tblname + "'"
            spark.sql(Query)

      except Exception as e:
        raise

    def gen_readFromDeltaTable_perform(self,packageID,dbName,schemaName,tableName):
      try:

        ERPPackageID = list()
        if(len(packageID) == 0):
            ERPPackageID.append(gl_ERPPackageID.lower())
        else:
            #accepting a list of ERP package ids
            ERPPackageID = [p.lower() for p in packageID]

        packages =  "'" + "','".join(ERPPackageID) + "'"

        schema_table_Name = schemaName+"_"+tableName         
        df_app_LA_Watermark = spark.sql("select min(minVersionID) AS minVersionID \
                                         ,max(maxVersionID) AS maxVersionID from app_LA_Watermark \
                                          where tableName = '"+ schema_table_Name + "' \
                                          and lower(packageID) IN (" +  packages  + ")")

        if(df_app_LA_Watermark.collect()[0]["minVersionID"] is not None):
          minVersionID = df_app_LA_Watermark.first()[0]+1
          maxVersionID = df_app_LA_Watermark.first()[1]
          df_result = spark.sql("select * from table_changes('"+gl_databaseName+"."+ schema_table_Name +"'\
                                ,"+str(minVersionID)+","+ str(maxVersionID) +") \
                                where _change_type!='update_preimage'")
          
        else:
          df_result = spark.sql("select * from "+gl_databaseName+"."+ schema_table_Name +" LIMIT 0")

        return(df_result)

      except Exception as e:
          raise

    def gen_getList(self,df,outputType = "S"):
        try:
            l = list(df.select(df.columns[0]).distinct().toPandas()["".join(str(df.columns[0]))])
            if(outputType == 'S'):
                return "'"+"','".join(l)+"'"
            else:
                return l
        except Exception as e:
            raise
