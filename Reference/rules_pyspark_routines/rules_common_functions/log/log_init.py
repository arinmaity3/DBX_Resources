# Databricks notebook source
from pyspark.sql.functions import expr
import pyspark
from pyspark.sql.types import*

gl_executionLog = {}

gl_executionLogSchema = StructType([
                                    StructField("logID",StringType(),False),                                   
                                    StructField("fileID",StringType(),True),
                                    StructField('fileName',StringType(),True),
                                    StructField('fileType',StringType(),True),
                                    StructField("processID",ShortType(),True),
                                    StructField("validationID",ShortType(),True),
                                    StructField("validationStatus",StringType(),True),
                                    StructField("statusDescription",StringType(),True),
                                    StructField("validationBehaviorID",StringType(),True),
                                    StructField("routineName",StringType(),True),
                                    StructField("statusID",ShortType(),True),    
                                    StructField("status",StringType(),True),    
                                    StructField("startTime",TimestampType(),True),
                                    StructField("endTime",TimestampType(),True),
                                    StructField("comments",StringType(),True)                                    
                                  ])

gl_logSchema = StructType([
                            StructField("logID", StringType(),False),
                            StructField("fileID",StringType(),True),
                            StructField("fileName",StringType(),True),
                            StructField("fileType",StringType(),True),
                            StructField("tableName",StringType(),True),
                            StructField("processID",StringType(),True),
                            StructField("validationID",StringType(),True),
                            StructField("routineName",StringType(),True),
                            StructField("statusID",StringType(),True),
                            StructField("status",StringType(),True),
                            StructField("startTime",TimestampType(),True),
                            StructField("endTime",TimestampType(),True),
                            StructField("comments",StringType(),True)
                          ])

gl_ImportValidationSummarySchema = StructType([                                    
                                                StructField("processID",ShortType(),False),                                                
                                                StructField("validationID",ShortType(),False),
                                                StructField("routineName",ShortType(),False),                                                
                                                StructField("validationStatus",StringType(),False),
                                                StructField("statusDescription", StringType(),False),
                                                StructField("fileID", StringType(),False),
                                                StructField("fileType", StringType(),True),                                                
                                                StructField("eventTime", TimestampType(),True),                                               
                                                StructField("errorMessage", StringType(),False)
                                                ])

gl_ImportStatusSchema = StructType([
                                     StructField("id",StringType(),False),
                                     StructField("fileID", StringType(),False),
                                     StructField("fileName", StringType(),False),
                                     StructField("routineName", StringType(),False),
                                     StructField("statusID",ShortType(),False),
                                     StructField("statusDescription",StringType(),False),
                                     StructField("recordCount",ShortType(),False),                                     
                                     StructField("eventTime", TimestampType(),True),                                     
                                     StructField("errorMessage", StringType(),False),
                                     StructField("executionID", StringType(),False),
                                     StructField("mandatoryFieldValidation", StringType(),False)
                                     ])

gl_CleansingStatusSchema = StructType([
                                     StructField("id",StringType(),False),
                                     StructField("validationID",ShortType(),False),
                                     StructField("routineName", StringType(),False),
                                     StructField("duplicateRecords",ShortType(),False),
                                     StructField("statusID",ShortType(),False),
                                     StructField("statusDescription",StringType(),False),                                                                          
                                     StructField("eventTime", TimestampType(),True),                                     
                                     StructField("errorMessage", StringType(),False),
                                     StructField("executionID", StringType(),False)
                                     ])


gl_DPSStatusSchema = StructType([
                                     StructField("id",StringType(),False),
                                     StructField("validationID",ShortType(),False),
                                     StructField("routineName", StringType(),False),                                     
                                     StructField("statusID",ShortType(),False),
                                     StructField("statusDescription",StringType(),False),                                                                          
                                     StructField("eventTime", TimestampType(),True),                                     
                                     StructField("errorMessage", StringType(),False),
                                     StructField("executionID", StringType(),False)
                                     ])


gl_PreTransformationSchema = StructType([
                                     StructField("id",StringType(),False),
                                     StructField("validationID",ShortType(),False),
                                     StructField("routineName", StringType(),False),                                     
                                     StructField("statusID",ShortType(),False),
                                     StructField("statusDescription",StringType(),False),                                                                          
                                     StructField("eventTime", TimestampType(),True),                                     
                                     StructField("errorMessage", StringType(),False),
                                     StructField("executionID", StringType(),False)
                                     ])


gl_ExecutionSummarySchema = StructType([
                                     StructField("id",StringType(),False),
                                     StructField("validationID",ShortType(),False),
                                     StructField("routineName", StringType(),False),                                     
                                     StructField("statusID",ShortType(),False),
                                     StructField("statusDescription",StringType(),False),                                                                          
                                     StructField("eventTime", TimestampType(),True),                                     
                                     StructField("errorMessage", StringType(),False),
                                     StructField("executionID", StringType(),False),
                                     StructField("fileType", StringType(),True)
                                     ])


gl_PreTransValidationSummarySchema = StructType([                                    
                                                StructField("processID",ShortType(),False),                                                
                                                StructField("validationID",ShortType(),False),
                                                StructField("routineName",ShortType(),False),                                                
                                                StructField("validationStatus",StringType(),False),
                                                StructField("statusDescription", StringType(),False),                                                                                           
                                                StructField("eventTime", TimestampType(),True),                                               
                                                StructField("errorMessage", StringType(),False),
                                                StructField("executionID", StringType(),False)
                                                ])

gl_TransformationStatusSchema = StructType([                                    
                                                StructField("id",StringType(),False),                                                
                                                StructField("routineName",StringType(),False),                                                
                                                StructField("statusID",StringType(),False),
                                                StructField("statusDescription", StringType(),False),                                                                                           
                                                StructField("eventTime", TimestampType(),True),                                               
                                                StructField("errorMessage", StringType(),False),
                                                StructField("executionID", StringType(),False),
                                                StructField("validationID",ShortType(),False)
                                                ])


gl_TransformationValidationSummarySchema = StructType([                                    
                                                StructField("processID",ShortType(),False),                                                
                                                StructField("validationID",ShortType(),False),
                                                StructField("routineName",ShortType(),False),                                                
                                                StructField("validationStatus",StringType(),False),
                                                StructField("statusDescription", StringType(),False),                                                
                                                StructField("fileType", StringType(),True),          
                                                StructField("fileID", StringType(),False),
                                                StructField("eventTime", TimestampType(),True),                                               
                                                StructField("comments", StringType(),True),
                                                StructField("validationText", StringType(),True)
                                                ])

gl_ValidationResultDetailSchema = StructType([
                                                StructField("groupSlno",IntegerType(),False),
                                                StructField("validationID",ShortType(),False),                                                
                                                StructField("validationObject",StringType(),False),                                               
                                                StructField("fileType", StringType(),True),
                                                StructField("resultKey", StringType(),False),
                                                StructField("resultValue", StringType(),True)                                               
                                                ])

gl_L2TransformationStatusSchema = StructType([                                    
                                                StructField("id",StringType(),False),                                                
                                                StructField("routine",StringType(),False),                                                
                                                StructField("statusID",StringType(),False),
                                                StructField("statusDescription", StringType(),False),                                                                                           
                                                StructField("statusUpdatedOn", TimestampType(),True),                                               
                                                StructField("comments", StringType(),False)                                                
                                                ])
