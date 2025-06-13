# Databricks notebook source

#global variables

global gl_maxParallelism
global gl_ERPSystemID
global gl_metadataDictionary
global gl_parameterDictionary
global gl_maximumNumberOfValidationDetails
global gl_lstOfScopedAnalytics
global gl_lstOfSourceFiles
global gl_processStage
global gl_ExecutionMode 

#initializtion
gl_metadataDictionary = {}
gl_parameterDictionary = {}
gl_maximumNumberOfValidationDetails = 50
gl_lstOfSourceFiles = {}
gl_fileTypes = {}

#ADLS paths

gl_metadataPath = gl_MountPoint + "/"+ gl_analysisPhaseID + "/metadata/ddic/"
gl_knowledgePath = gl_MountPoint + "/"+ gl_analysisPhaseID + "/metadata/knowledge/"
gl_commonParameterPath = gl_MountPoint + "/"+ gl_analysisPhaseID + "/common-parameters/"
gl_CDMLayer1Path = gl_MountPoint + "/"+ gl_analysisPhaseID + "/cdm-l1/"
gl_CDMLayer2Path = gl_MountPoint + "/"+ gl_analysisPhaseID + "/cdm-l2/"
gl_rawFilesPathERP = gl_MountPoint + "/"+ gl_analysisPhaseID + "/rawFiles/" + gl_ERPPackageID + "/"
gl_rawFilesPath = gl_MountPoint + "/"+ gl_analysisPhaseID + "/rawFiles/"
gl_convertedRawFilesPath = gl_MountPoint  + "/" + gl_analysisPhaseID + "/executions/l0-l1/" + gl_executionGroupID +"/rawfilesConverted/"
gl_convertedRawFilesPathERP = gl_MountPoint  + "/" + gl_analysisPhaseID + "/staging/l0-rawfilesConverted/"
gl_inputParameterPath = gl_MountPoint  + "/" + gl_analysisPhaseID + "/executions/l0-l1/" + gl_executionGroupID +"/"
gl_layer0Staging = gl_MountPoint + "/"+ gl_analysisPhaseID + "/staging/l0-stg/"
gl_executionLogResultPath = gl_MountPoint + "/" + gl_analysisPhaseID + "/executions/l0-l1/" + gl_executionGroupID + "/"
gl_executionLogResultPathL2 = gl_MountPoint + "/" + gl_analysisPhaseID + "/executions/l1-l2/" + gl_executionGroupID + "/"
gl_executionLogBlobFilePath = gl_analysisID + "/" + gl_analysisPhaseID + "/executions/l0-l1/" + gl_executionGroupID + "/"
gl_layer0Temp = gl_MountPoint + "/"+ gl_analysisPhaseID + "/staging/l0-tmp/"
gl_layer0Temp_knw = gl_MountPoint + "/"+ gl_analysisPhaseID + "/staging/l0-tmp/knw/"
gl_layer0Temp_fin = gl_MountPoint + "/"+ gl_analysisPhaseID + "/staging/l0-tmp/fin/"
gl_layer0Temp_gen = gl_MountPoint + "/"+ gl_analysisPhaseID + "/staging/l0-tmp/gen/"
gl_layer0Temp_temp = gl_MountPoint + "/"+ gl_analysisPhaseID + "/staging/l0-tmp/temp/"
gl_reportPath = gl_MountPoint + "/"+ gl_analysisPhaseID + "/cdm-l1/reports/"

gl_Layer2Staging_temp = gl_MountPoint + "/"+ gl_analysisPhaseID + "/staging/l2-temp/"
gl_Layer2Staging = gl_MountPoint + "/"+ gl_analysisPhaseID + "/staging/l2-stg/"
gl_bifurcationPath = gl_MountPoint + "/" + gl_analysisPhaseID + "/staging/l2-stg-bifurcation/"
gl_preBifurcationPath = gl_MountPoint + "/" + gl_analysisPhaseID + "/staging/l2-stg-prebifurcation/"
gl_postBifurcationPath = gl_MountPoint + "/" + gl_analysisPhaseID + "/staging/l2-stg-postbifurcation/"
gl_Layer1Staging = gl_MountPoint + "/"+ gl_analysisPhaseID + "/staging/l1-stg/"

gl_Exploration = gl_MountPoint + "/"+ gl_analysisPhaseID + "/exploration/"

#ADLS DM Specific paths
gl_rawFilesPathDM = gl_rawFilesPathERP + "*/"
gl_ETLParameterPath = gl_MountPoint + "/"+ gl_analysisPhaseID + "/etl-parameters/"
gl_dataExtractionParameterPath = gl_MountPoint + "/" + gl_analysisPhaseID + "/executions/source-l0/" + gl_executionGroupID + "/"
gl_dataExtractionLogPath = gl_MountPoint + "/" + gl_analysisPhaseID + "/executions/source-l0/" + gl_executionGroupID + "/validationresults/dataextraction/"

gl_maxParallelism  = sc.defaultParallelism
