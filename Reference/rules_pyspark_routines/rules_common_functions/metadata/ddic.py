# Databricks notebook source
import sys
import traceback

class ddicMaster():
    def ddicMaster_load(self):
        try:        
            objGenHelper = gen_genericHelper()

            df = objGenHelper.gen_readFromFile_perform(gl_metadataPath + "dic_ddic_column.delta").\
                     select(col("ERPSystemID"),col("schemaName"),\
                               col("tableName"),col("tableName2"),\
                               col("columnName"),col("dataType"),\
                               col("fieldLength"),col("position").cast("int"),\
                               col("isNullable"),col("isIdentity"),\
                               col("defaultValue"),col("isKey"),\
                               col("fileType"),col("sparkDataType"),\
                               col("isImported").cast("boolean"),col("tableCollation"),\
                               col("isActive"),col("tableLayer"),col("tableCategory"),\
                               col("isCreatePartition").cast("boolean"),\
                               col("isPartitionColumn").cast("boolean"))

            gl_metadataDictionary['dic_ddic_column'] = df
            gl_metadataDictionary['dic_ddic_tablesRequiredForERPPackages'] = objGenHelper.\
                gen_readFromFile_perform(gl_metadataPath + "dic_ddic_tablesRequiredForERPPackages.delta")

            gl_metadataDictionary['dic_ddic_processStagewiseSourceFileMapping'] = objGenHelper.\
                gen_readFromFile_perform(gl_metadataPath + "dic_ddic_processStagewiseSourceFileMapping.delta").\
                    select(col("processStageID").cast("int"),
                          col("processStage"),
                          col("schemaName"),
                          col("tableName"),
                          col("sourceLocation"),
                          col("fileName"),
                          col("auditProcedure"),
                          col("isFolder").cast("boolean"),
                          col("onlyForReProcessing").cast("boolean"),
                          col("executionOrder").cast("int"))

            #load data to cache
            gl_metadataDictionary['dic_ddic_column'].rdd.collect()
        except Exception as err:
            raise

class ddic(ddicMaster):    

    def __init__(self):
        self.ddicMaster_load()

    def ddicDARules_load(self):
        try:                                    
            gl_metadataDictionary['dic_ddic_RemoveDuplicate'] = objGenHelper.\
                gen_readFromFile_perform(gl_metadataPath + "dic_ddic_removeDuplicateExtended.delta")
            
            gl_metadataDictionary['dic_ddic_auditProcedureToChildAnalyticsMapping'] = objGenHelper.\
                gen_readFromFile_perform(gl_metadataPath + "dic_ddic_auditProcedureToChildAnalyticsMapping.csv").persist()
            gl_metadataDictionary['dic_ddic_genericValidationProcedures'] = objGenHelper.\
                gen_readFromFile_perform(gl_metadataPath + "dic_ddic_genericValidationProcedures.delta")
            gl_metadataDictionary['dic_ddic_auditProcedureColumnMapping'] = objGenHelper.\
                gen_readFromFile_perform(gl_metadataPath + "dic_ddic_auditProcedureColumnMapping.csv").persist()       
                        
            #load data to cache            
            gl_metadataDictionary['dic_ddic_RemoveDuplicate'].rdd.collect()
            gl_metadataDictionary['dic_ddic_tablesRequiredForERPPackages'].rdd.collect()
            gl_metadataDictionary['dic_ddic_genericValidationProcedures'].rdd.collect()
            gl_metadataDictionary['dic_ddic_processStagewiseSourceFileMapping'].rdd.collect()

        except Exception as err:
            raise

    def ddicDMLoad(self):
        try:
            gl_metadataDictionary['dic_ddic_tablesAndFields'] = objGenHelper.\
                gen_readFromFile_perform(gl_metadataPath + "dic_ddic_tablesAndFields.csv")

            gl_metadataDictionary['dic_ddic_md5Keys'] = objGenHelper.\
                gen_readFromFile_perform(gl_metadataPath + "dic_ddic_md5Keys.delta")

            gl_metadataDictionary['dic_ddic_selectiveOverwriteSourceTargetMapping'] = objGenHelper.\
                gen_readFromFile_perform(gl_metadataPath + "dic_ddic_selectiveOverwriteSourceTargetMapping.delta")

            gl_metadataDictionary['dic_ddic_tableConstraints'] = objGenHelper.\
                gen_readFromFile_perform(gl_metadataPath + "dic_ddic_tableConstraints.delta")

            gl_metadataDictionary['dic_ddic_layerOneTableMapping'] = objGenHelper.\
                gen_readFromFile_perform(gl_metadataPath + "dic_ddic_layerOneTableMapping.delta")

        except Exception as err:
            raise
