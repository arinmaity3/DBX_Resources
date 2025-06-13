# Databricks notebook source
import pathlib
from pathlib import Path
import uuid
import pyspark
from pyspark.sql import SparkSession
from dateutil.parser import parse
from pyspark.sql.functions import row_number,lit ,abs,col, asc,upper,expr,concat,regexp_replace,when,substring,coalesce,array,lower
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
from pyspark.sql.functions import date_format
from delta.tables import *

class gen_dataTransformation():

    def gen_convertToCDMStructure_get(self,df,isSqlFormat = False):
        """Convert data frame into CDM format."""
        try:
          cdmView = "cdmStructure_"+ uuid.uuid4().hex
          df = df.select(concat(lit("CAST(`"),col("columnName"), lit("` AS "),\
               col("sparkDataType"),\
               expr("case when sparkDataType =  'Decimal' \
               then  concat('(',fieldLength,')') \
               else '' end"),lit(")")).alias("newCol")).\
               orderBy(col("position")).\
               createOrReplaceTempView(cdmView) 
            
          sqlQuery = spark.sql("select concat_ws(',',collect_list(newCol)) from " + cdmView ).rdd.collect()[0]
          spark.sql("DROP VIEW IF EXISTS " + cdmView)
          return sqlQuery[0]
        except Exception as err:
          raise

    def gen_convertToCDMStructure_generate(self, dfSource,
                                           schemaName,
                                           tableName,
                                           isIncludeAnalysisID = False, 
                                           isSqlFormat = False,
                                           isERP = False,
                                           dateTimeFormatting = True):
        """Convert to CDM model based on the schema and tableName."""
        try:
            dictOfDefaultValues = {}
            isCreatePartition = False
            partitionColumns = ""

            dfCDM = gl_metadataDictionary['dic_ddic_column']\
                    .select(col("columnName"),\
                            col("sparkDataType"),\
                            col("fieldLength"),\
                            col("position"),\
                            col("dataType").alias("sqlDataType"),\
                            col("isImported"),\
                            col("defaultValue"),
                            col("isCreatePartition"),
                            col("isPartitionColumn"))\
                            .filter((upper(col("schemaName")) == schemaName.upper()) \
                            & (upper((col("tableName"))) == tableName.upper()))\
                            .orderBy(col("position"))
            if(dfCDM.rdd.isEmpty()):
                err = "CDM data model does not exist for the schema '" + schemaName + "' and table '" + tableName + "'"
                raise KeyError (err)
                
            if (isIncludeAnalysisID == True):
                objGenHelper = gen_genericHelper()
                analysisID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ANALYSISID')
                CDMSchema = StructType([StructField("columnName",StringType(),True),
                                         StructField("sparkDataType",StringType(),True),
                                         StructField("fieldLength",StringType(),True),
                                         StructField("position",IntegerType(),True),
                                         StructField("sqlDataType",StringType(),True),
                                         StructField("isImported",BooleanType(),True),
                                         StructField("defaultValue",StringType(),True),
                                         StructField("isCreatePartition",BooleanType(),True),
                                         StructField("isPartitionColumn",BooleanType(),True)])
                       
                  
                lstAnalysisID  = [['analysisID','String',36,-9999,'String',True,'',False,False]]
                dfAnalysisID = spark.createDataFrame(data = lstAnalysisID,schema = CDMSchema )
                dfCDM = dfCDM.filter(lower(col("columnName")) != "analysisid")
                dfCDM = dfCDM.union(dfAnalysisID)
                dfSource = dfSource.withColumn("analysisID",lit(analysisID))                
             
            isCreatePartition = dfCDM.select(col("isCreatePartition")).first()[0]
            if(isCreatePartition == True):
              objGenHelper = gen_genericHelper()
              partitionColumns =  objGenHelper.gen_getList(dfCDM.filter(col("isPartitionColumn")==True).\
                                                           select(col("columnName")),"L")
              if(len(partitionColumns) == 0):
                partitionColumns = ""
              elif(len(partitionColumns)>1):
                partitionColumns = ",".join(partitionColumns)
                partitionColumns = tuple(map(str,partitionColumns.split(',')))               

            lstOfSourceColumns = [cols.upper() for cols in dfSource.columns]
            if(isERP == True):
                dfCDM,dfSource,dictOfDefaultValues = self.__gen_ERPCDMConversion_prepare(dfCDM,
                                                                                         dfSource,
                                                                                         lstOfSourceColumns,
                                                                                         dateTimeFormatting)

            
            CDMColList = self.gen_convertToCDMStructure_get(dfCDM,isSqlFormat)
            
            for missingCols in dfCDM.filter((upper(col("columnName")).isin(lstOfSourceColumns) == False)).rdd.collect(): 
                if missingCols[1].upper() != 'DECIMAL':
                    dfSource = dfSource.withColumn(missingCols[0],lit(None).cast(missingCols[1]))
                else:
                    dfSource = dfSource.withColumn(missingCols[0],lit("").cast(missingCols[1] + '(' + missingCols[2] +')'))

            if(isERP == True):
                dfSource = dfSource.na.fill(dictOfDefaultValues)

            viewName = uuid.uuid4().hex
            dfSource.createOrReplaceTempView(viewName)
            sqlQuery = "SELECT " + CDMColList + " FROM " + viewName
            dfSource = spark.sql(sqlQuery)            
            return dfSource,isCreatePartition,partitionColumns
        except Exception as err:
            raise

    def __gen_ERPCDMConversion_prepare(self,dfCDM,dfSource,lstOfSourceColumns,dateTimeFormatting = True):
        try:
            objGenHelper = gen_genericHelper()

            lstOfDateColumns = list()
            lstOfTimeColumns = list()
            lstOfCDMColumns = list()            
            lstOfMissingColumns = list()            
            lstOfNewCols = list()
            dictOfDefaultValues = {}

            ##check any new fields present in source file but not in ddic_column metadata, that field
            ##should also needs to consider for ERP import
            [lstOfCDMColumns.append(dtCol.columnName.lower()) \
                   for dtCol in dfCDM.select(col("columnName")).rdd.collect()]
            lstOfMissingColumns = list(set([cols.lower() for cols in dfSource.columns]) - set(lstOfCDMColumns))             
            columnOrder = dfCDM.count()
            
            if(len(lstOfMissingColumns)!=0):
                for cols in lstOfMissingColumns:  
                  missingCol = list()
                  [missingCol.append(i) for i in dfSource.columns if(i.lower() == cols.lower())]                  
                  columnOrder = columnOrder + 1                  
                  lstOfNewCols.append(["".join(missingCol),'String',1000,columnOrder,'nvarchar',False,None,False,False])
                dfNewCols = spark.createDataFrame(data = lstOfNewCols, schema = dfCDM.schema)
                dfCDM = dfCDM.union(dfNewCols)
                           
            if(dateTimeFormatting == True):
                #check if any date or time columns present in source file
                for cols in dfCDM.select(col("columnName"),col("sqlDataType")).\
                            filter((upper(col("columnName")).isin(lstOfSourceColumns) == True) &\
                            ((upper(col("sqlDataType")) =='DATE') | \
                            (upper(col("sqlDataType")) =='DATETIME') | \
                            (upper(col("sqlDataType"))  =='TIME'))).collect():
                    if((cols["sqlDataType"].upper() =='DATE') | (cols["sqlDataType"].upper() =='DATETIME')):                  
                      lstOfDateColumns.append(cols["columnName"])
                    else:
                      lstOfTimeColumns.append(cols["columnName"])
                
                #if date,time column present, do the conversion
                if(len(lstOfDateColumns)!=0):
                    for dateCol in lstOfDateColumns:
                        newCol = dateCol + "_new"    
                        dfSource = dfSource.withColumn(newCol,regexp_replace(dateCol, "[^0-9]", "").substr(0,8))\
                                    .drop(dateCol).withColumnRenamed(newCol,dateCol)

                    dfSource = objGenHelper.gen_convertToStandardDateFormat_perform(dfSource,lstOfDateColumns,"yyyyMMdd")
                if(len(lstOfTimeColumns)!=0):
                    for timeCol in lstOfTimeColumns:
                        newCol_time = timeCol + "_new_time"
                        newCol_time_length = timeCol + "_new_time_length"
                    
                        dfSource = dfSource.withColumn(newCol_time_length,expr('locate(" ", '+timeCol+') - 1'))\
                        .withColumn(newCol_time,expr("case when "+ \
                                                     newCol_time_length +" != -1 then replace(substring\
                                                     ("+timeCol+","+newCol_time_length+"+1),':','')\
                                                     else replace("+ timeCol +",':','') end"))\
                        .drop(timeCol).drop(newCol_time_length).withColumnRenamed(newCol_time,timeCol)

                        # dfSource = dfSource.withColumn(newCol,(date_format(timeCol,'HH:mm:ss')).cast('string'))\
                        #             .drop(timeCol).withColumnRenamed(newCol,timeCol)

                    dfSource = objGenHelper.gen_convertToStandardTimeFormat_perform(dfSource,lstOfTimeColumns)
            
            #keep the CDM metadata based on importted = False or as per the columns in source file
            dfCDM = dfCDM.filter((col("columnName").\
                    isin([cols for cols in dfSource.columns])) | (col("isImported") ==False))
            
            #collect all fields and default values            
            for r in dfCDM.select(col("columnName"),\
                          when(col("defaultValue")=="(N'')","")\
                          .when(col("defaultValue")== "(N'01/01/1900')","1900-01-01")\
                          .otherwise(col("defaultValue")).alias("defaultValue2")).\
                          filter(col("defaultValue").isNotNull()).collect():

                dictOfDefaultValues[r["columnName"]] = r["defaultValue2"]

            return[dfCDM,dfSource,dictOfDefaultValues]
        except Exception as err:
            raise

    def gen_convertToCDMandCache(self,dfSource,
                                 schemaName,
                                 tableName,\
                                 isIncludeAnalysisID = False,\
                                 targetPath = "",
                                 isERP = False,\
                                 mode = 'overwrite',\
                                 isIncludeRowID = False,
                                 dateTimeFormatting = True,
                                 isDPSFlag = False,
                                 md5KeyColumns = None,
                                 md5KeyColumnName = "",
                                 isSelectiveOverwrite = False,
                                 overwriteCondition = None,                                
                                 isOverWriteSchema = False):

        """Convert data frame to CDM model and create delta files"""
        try:
            isSparkCache = 0
            global gl_Layer2Staging
            objGenHelper = gen_genericHelper()

            if (isSparkCache == 1):
                self.gen_convertToCDMStructure_generate\
                        (dfSource
                        ,schemaName
                        ,tableName
                        ,isIncludeAnalysisID
                        ,False
                        ,isERP
                        ,dateTimeFormatting)[0].\
                        createOrReplaceTempView(schemaName+"_"+tableName)
                sqlContext.cacheTable(schemaName+"_"+tableName)
                dfSource = spark.sql("select * from " + schemaName+"_"+tableName).cache()
                return dfSource    
            else:
                if(targetPath == ""):
                    targetPath = gl_Layer2Staging
                
                df,isCreatePartition, partitionColumns = self.gen_convertToCDMStructure_generate\
                                                            (dfSource
                                                             ,schemaName
                                                             ,tableName
                                                             ,isIncludeAnalysisID
                                                             ,False
                                                             ,isERP
                                                             ,dateTimeFormatting)
                
                fullPath = targetPath + schemaName + "_" + tableName +".delta"
                if(isIncludeRowID == True):
                    df = df.withColumn("kpmgRowID", expr("uuid()"))
                if(isDPSFlag == True):                    
                    df = df.withColumn("isShifted",array(lit(0)))
                if(md5KeyColumns is not None):      
                    df = df.withColumn(md5KeyColumnName, expr(self.gen_generateMD5Value(md5KeyColumns))) 

                objGenHelper.gen_writeToFile_perfom(df,
                                                    fullPath,
                                                    mode = mode,
                                                    isMergeSchema = True,
                                                    isSelectiveOverwrite = isSelectiveOverwrite,
                                                    overwriteCondition   = overwriteCondition,
                                                    isCreatePartitions   = isCreatePartition,
                                                    partitionColumns     = partitionColumns,
                                                    isOverWriteSchema    = isOverWriteSchema)

                df = objGenHelper.gen_readFromFile_perform(fullPath)
                df.createOrReplaceTempView(schemaName + "_" + tableName)
                return df
            
        except Exception as err:
            raise
       
    def gen_CDMStructure_get(self,schemaName,tableName):      
      """Create datafame from metadata dic_ddic_column."""
      try:

        cdmView =  uuid.uuid4().hex
        dfFields = gl_metadataDictionary["dic_ddic_column"]\
                  .select(col("fileType"),\
                        col("tableName2"),\
                        col("columnName"),\
                        col("dataType"),\
                        col("fieldLength"),\
                        col("position"),\
                        col("sparkDataType"))\
                  .filter("upper(schemaName) = '" + schemaName.upper() \
                  +"' AND  isimported =False"\
                  + " AND upper(tableName) = '" + tableName.upper() + "'")\
                  .orderBy(col("position").asc())          
        dummyCol =  uuid.uuid4().hex     
        dfCDM = spark.sql("SELECT '' AS " + dummyCol)      
        for field in dfFields.rdd.collect():
          dfCDM = dfCDM.withColumn(field["columnName"],lit(None))     
        dfCDM = dfCDM.drop(dummyCol)  
        dfCDM.createOrReplaceTempView(cdmView)         
        CDMCols = self.gen_convertToCDMStructure_get(dfFields)      
        sqlQuery ="SELECT  " + CDMCols + " FROM " + cdmView + " A WHERE 1= 2"  
        dfCDMStructure = spark.sql(sqlQuery)          
        spark.sql("DROP VIEW IF EXISTS " + cdmView)
        return dfCDMStructure
      except Exception as err:
        raise 

    def gen_CDMColumnOrder_get(self,df,schemaName, tableName):
        try:
            cdmtablename = 'cdm_columns'+schemaName+tableName
            gl_metadataDictionary["dic_ddic_column"]\
                .select(col("fileType"),\
                col("schemaName"),\
                col("tableName"),\
                col("tableName2"),\
                col("columnName"),\
                col("dataType"),\
                col("fieldLength"),\
                col("position"),\
                col("sparkDataType"))\
                .createOrReplaceTempView(cdmtablename)
            dfCDMColumns = spark.sql("SELECT * FROM "+cdmtablename+ \
                            " WHERE upper(schemaName) = '" + schemaName.upper() + \
                            "' AND upper(tableName) = '" + tableName.upper() + "'")
            df = df.select([i.columnName for i in dfCDMColumns.\
                            select(col("columnName")).\
                            orderBy(col("position").asc()).collect()])
            return df
        except Exception as err:
            raise
    
    def gen_generateMD5Value(self,keyColumns):
        try:
            lstOfCols = [f"md5(upper(case when {c} is null then '' else cast({c} as varchar(500)) end))" \
                        for c in keyColumns.split(',')]
            keyColumns = ",".join(lstOfCols) 
            return "md5(concat("+keyColumns+"))"
        except Exception as err:
            raise

    def gen_generateMD5KeyValue(self,dfSource,schemaName,tableName):
        try:
            keyColumns = gl_metadataDictionary['dic_ddic_md5Keys'].\
                             filter( (upper(col("schemaName")) == schemaName.upper())& \
                                    (upper(col("tableName")) == tableName.upper())).\
                             select(col("md5KeyColumns")).first()
            if(keyColumns is not None):
                dfSource = dfSource.withColumn('md5Key', expr(self.gen_generateMD5Value(keyColumns[0]))) 
            return dfSource
        except Exception as err:
            raise err

    def gen_usf_DebitCreditIndicator_Change(self,fileType,df=None):
        try:
            fileType = fileType.upper()
            objGenHelper = gen_genericHelper()
            if  fileType=="GLAB":
                tablename = 'fin_L1_TD_GLBalance'
            elif fileType=='JET':
                tablename = 'fin_L1_TD_Journal'
        
            if df is not None:
                df.createOrReplaceTempView(tablename)

            if fileType=="GLAB":
                Negative = spark.sql("SELECT COUNT(endingBalanceLC) as Negative FROM fin_L1_TD_GLBalance \
                                      WHERE debitCreditIndicator = 'C' AND endingBalanceLC < 0.0").collect()[0][0]
                Positive = spark.sql("SELECT COUNT(endingBalanceLC) AS Positive FROM fin_L1_TD_GLBalance \
                                      WHERE debitCreditIndicator = 'C' AND endingBalanceLC > 0.0").collect()[0][0]
            elif fileType=='JET':
                Negative = spark.sql("SELECT COUNT(amountLC) as Negative FROM fin_L1_TD_Journal \
                                      WHERE debitCreditIndicator = 'C' AND amountLC < 0.0").collect()[0][0]
                Positive = spark.sql("SELECT COUNT(amountLC) AS Positive FROM fin_L1_TD_Journal \
                                      WHERE debitCreditIndicator = 'C' AND amountLC > 0.0").collect()[0][0]

            if Negative >= Positive:
                return "0"
            else:
                return "1"
        except:
            raise

    def gen_UTF8_convert(self,fileName,fileEncoding,filePath,convertedRawFilesPath,inputFileType=None):
        try:
            executionStatus=""
            detectedEncoding = fileEncoding
            if (detectedEncoding is not None and detectedEncoding != 'UTF8'
                and detectedEncoding.upper() != 'ASCII' and inputFileType not in ['xls','xlsx']):
                print("Encoding before conversion: "+detectedEncoding)
                BLOCKSIZE = 1048576 # or some other, desired size in bytes
                #source = 'cp ' + '"/dbfs' + filePath + fileName + '"'
                #os.system(source + ' /tmp')
                targetFile = "/tmp/" + fileName
                file1="/dbfs"+ filePath
                with io.open(file = file1, mode="r", encoding =detectedEncoding) as sourceFile:
                    filePath = convertedRawFilesPath
                    with io.open(file = "/dbfs/"+filePath +"Converted_"+ fileName, mode="w", encoding = "utf-8") as targetFile:
                        while True:
                            contents = sourceFile.read(BLOCKSIZE)
                            if not contents:
                                break
                            targetFile.write(contents)
                newFileName = '"'+ fileName + '"'
                #os.system('rm /tmp/' + newFileName)
                fileName = 'Converted_'+fileName
                fileEncoding = 'UTF8'
                filePath = convertedRawFilesPath + fileName
                print("Encoding after conversion: "+fileEncoding)
                executionStatus =  "File conversion from "+detectedEncoding+" to UTF8 succeeded."
            else:
                print("Conversion was not performed. gl_fileEncoding value:",fileEncoding)
                print("Fileextension:", inputFileType)
                executionStatus =  "No conversion required."
            
            executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
            return [executionStatusID,executionStatus,fileName,filePath,fileEncoding]
        except Exception as err:
            raise
   
    def gen_dm_layerOneTransform_perform(self,L0_schemaName,
                                         L0_tableName,
                                         ERPSystemID,
                                         L1_schemaName,
                                         L1_tableName,
                                         df_l0,
                                         df_l1):
        try:
    
            fullquery = ""
            str_select_query = ""
            
            str_join_query = ""
            str_join_query1 = ""
            str_join_query2 = ""
            str_join_query3 = ""
              
            df_col_L0 = gl_metadataDictionary["dic_ddic_column"].\
                        filter((col("schemaName")== L0_schemaName)\
                        &(col("tableName")== L0_tableName)& \
                        (col("ERPSystemID")== ERPSystemID))\
                        .select("schemaName",\
                                "tableName",\
                                col("columnName").alias("columnName"),\
                                "position",\
                                "isKey",\
                                "isActive").orderBy(col("position"))

            df_col_L1 = gl_metadataDictionary["dic_ddic_column"].\
                        filter((col("schemaName")==L1_schemaName)\
                        &(col("tableName")==L1_tableName))\
                        .select("schemaName",\
                                "tableName",\
                                col("columnName").alias("columnName"),\
                                "position",\
                                "isKey",\
                                "isActive").orderBy(col("position"))
            
            df_col_L1_L0 = df_col_L1.alias("L1").\
                           join(df_col_L0.alias("L0"),\
                           upper(col("L1.columnName"))==upper(col("L0.columnName")),\
                           how="left")\
                           .select(col("L1.columnName").alias("columnName_L1")\
                           ,when(col("L1.columnName")=="currentFlag",lit("Y"))\
                           .when(col("L1.columnName")=="deletedFlag",lit("N"))\
                           .when(col("L1.columnName")=="startEffectiveDate",lit("current_date()"))\
                           .when(col("L1.columnName") == "ERPPackageID",lit(gl_ERPPackageID))\
                           .when(col("L1.columnName") == 'md5Key',lit("md5Key"))\
                           .otherwise(col("L0.columnName")).alias("columnName_L0")\
                           ,when(col("L1.columnName").isin("currentFlag","deletedFlag","ERPPackageID"),"String")\
                           .when(col("L1.columnName").isin("startEffectiveDate"),"Value")\
                           .otherwise("Column").alias("ValueType")\
                           ,col("L1.isKey")\
                           ,when(col("L1.columnName") == 'md5Key',lit(True))\
                           .when(col("L0.isActive").isNull(),lit(False)).otherwise(col("L0.isActive").cast("Boolean")).alias("isActive_L0")\
                           ,col("L1.isActive").alias("isActive_L1"))
    
            for j in df_col_L1_L0.filter(col("isKey")=='X').collect():
              str_join_query2 = str_join_query2 +'(trim(col("L0.' + str(j["columnName_L0"])+'"))=='+'trim(col("L1.'+str(j["columnName_L1"])+'")))&'

            str_join_query2 = str_join_query2[:-1]
    
            for x in df_col_L1_L0.filter((col("columnName_L0").isNotNull())&(col("isActive_L0")==True)).collect():
                if x["ValueType"]=="String":
                  str_select_query = str_select_query+'"'+str(x["columnName_L1"])+'":lit("'+str(x["columnName_L0"])+'")'+',' + os.linesep
                elif x["ValueType"]=="Value":
                  str_select_query = str_select_query+'"'+str(x["columnName_L1"])+'":'+str(x["columnName_L0"])+',' + os.linesep
                else:
                  str_select_query = str_select_query+'"'+str(x["columnName_L1"])+'":trim(col("L0.'+str(x["columnName_L0"])+'"))'+',' + os.linesep

            if df_col_L0.filter(expr("lower(columnName)='engmeminvitestatus'")).first() is not None:
                str_select_query_1 = '"startEffectiveDate":current_date(),\
                                  "currentFlag":lit("Y"),"deletedFlag":"case when ltrim(rtrim(L0.EngMemInviteStatus)) = \'Deleted\' then \'Y\' else \'N\' end",'
            elif df_col_L0.filter(expr("lower(columnName)='isdeleted'")).first() is not None:
                str_select_query_1 = '"startEffectiveDate":current_date(),\
                                  "currentFlag":lit("Y"),"deletedFlag":"case when L0.IsDeleted = True then \'Y\' else \'N\' end",'
            else:
                str_select_query_1 = '"startEffectiveDate":current_date(),\
                                  "currentFlag":lit("Y"),"deletedFlag":lit("N"),'
            str_select_query = str_select_query_1 + str_select_query[:-2]
            str_join_query1 = df_l1+'.alias("L1").merge('+df_l0+'.alias("L0"),'
            str_join_query3 = '&(col("L1.CurrentFlag") == lit("Y")))'
            str_join_query = str_join_query1+str_join_query2+str_join_query3
            
                               
            fullquery =str_join_query+'.whenMatchedUpdate(condition='+"'"+'L0.SYS_CHANGE_OPERATION="U"'+"'"+',set ={'+str_select_query+'})'
            fullquery =fullquery + '.whenNotMatchedInsert(condition='+"'"+'L0.SYS_CHANGE_OPERATION="I"'+"'"+',values = {'+str_select_query+'})'
            fullquery =fullquery + '.whenMatchedUpdate(condition='+"'"+'L0.SYS_CHANGE_OPERATION="D"'
            if df_col_L0.filter(expr("lower(columnName)='isdeleted'")).first() is not None:
                fullquery =fullquery + ' or L0.IsDeleted=True '
            if df_col_L0.filter(expr("lower(columnName)='engmeminvitestatus'")).first() is not None:
                fullquery =fullquery + ' or lower(ltrim(rtrim(L0.EngMemInviteStatus)))="deleted" '
            fullquery =fullquery + "'"+',set ={"L1.endEffectiveDate":current_date(),"L1.deletedFlag":lit("Y")}).execute()'
            
            
            return fullquery
        except Exception as e:
            raise e

    def gen_referentialIntegrity_update(self,schemaName,tableName,dfDelta):
        try:
            
            tempView = 't_' + uuid.uuid4().hex
            dfDelta.createOrReplaceTempView(tempView)
    
            lstOfTables = list()

            [lstOfTables.append([cons.targetTable,cons.deleteAction,
                                 cons.updateAction,
                                 cons.defaultValue,
                                 cons.primaryKey,
                                 cons.foreignKeyColumn])
            for cons in gl_metadataDictionary['dic_ddic_tableConstraints'].\
                        filter((col("primaryKeyTableSchema")==schemaName) & \
                        (col("primaryKeyTable")==tableName)).\
                        select(concat(col("foreignKeySchema"),\
                                lit("_"),col("foreignKeyTable")).alias("targetTable"),\
                       col("deleteAction"),col("updateAction"),\
                       col("defaultValue"),col("primaryKey"),\
                       col("foreignKeyColumn")).rdd.collect()]
    
    
            for t in lstOfTables:
                targetTable = t[0]
                deleteAction = t[1]
                updateAction = t[2]
                value = t[3]
                pkColumn = t[4]
                fkColumn = t[5]    
                
                if(deleteAction == 'SET_DEFAULT'):
                    query = "MERGE INTO {target} {tAlias} using \
                            {source} {sAlias} ON {tAlias}.{fkKeyColumn} = {sAlias}.{pkKeyColumn}\
                            WHEN MATCHED THEN UPDATE set {tAlias}.{fkKeyColumn} = {defaultValue}".\
                            format(target= targetTable
                                   ,tAlias = 'T'
                                   ,source = tempView
                                   ,fkKeyColumn = fkColumn
                                   ,pkKeyColumn = pkColumn
                                   ,defaultValue = value
                                   ,sAlias = 'S')
                elif(deleteAction == 'CASCADE' or updateAction == 'CASCADE'):
                    query = "MERGE INTO {target} {tAlias} using \
                            {source} {sAlias} ON {tAlias}.{fkKeyColumn} = {sAlias}.{pkKeyColumn}\
                             WHEN MATCHED THEN DELETE".\
                             format(target= targetTable
                                   ,tAlias = 'T'
                                   ,source = tempView
                                   ,fkKeyColumn = fkColumn
                                   ,pkKeyColumn = pkColumn                   
                                   ,sAlias = 'S')      
                spark.sql(query)  
        except Exception as err:
            raise
        finally:
            spark.sql("DROP VIEW IF EXISTS " + tempView)  
        
