# Databricks notebook source
from pyspark.sql.functions import col,concat,lit,when,substring,length,repeat,row_number,sum,abs,count,expr,countDistinct
import pyspark
from pyspark.sql import SparkSession
from dateutil.parser import parse
from pyspark.sql.functions import row_number,lit ,abs,col, asc,upper
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,ShortType ,BooleanType ,TimestampType,DecimalType,DoubleType,FloatType
from datetime import datetime
from collections import Counter
import inspect
import itertools
from itertools import accumulate,groupby
import builtins as g
import pandas as pd
from collections import namedtuple
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime,timedelta
import re

gl_metadataDictionary = {}
gl_parameterDictionary = {}

gl_processID = 7
gl_metadataPath = gl_MountPoint + "/"+ gl_analysisPhaseID + "/metadata/ddic/"
gl_knowledgePath = gl_MountPoint + "/"+ gl_analysisPhaseID + "/metadata/knowledge/"
gl_commonParameterPath = gl_MountPoint + "/"+ gl_analysisPhaseID + "/common-parameters/"
gl_CDMLayer1Path = gl_MountPoint + "/"+ gl_analysisPhaseID + "/cdm-l1/"
gl_CDMLayer2Path = gl_MountPoint + "/"+ gl_analysisPhaseID + "/cdm-l2/"

gl_routinesCommonFunctionPath = gl_MountPoint + "/"+ gl_analysisPhaseID + "/routines/common-functions/"
gl_routinesL0ToL1GenericPath =  gl_MountPoint + "/"+ gl_analysisPhaseID + "/routines/l0-l1/generic/"
gl_routinesL1ToL2Path =  gl_MountPoint + "/"+ gl_analysisPhaseID + "/routines/l1-l2/generic/"
gl_executionLogResultPath = gl_MountPoint + "/" + gl_analysisPhaseID + "/executions/l1-l2/" + gl_executionGroupID + "/"
gl_executionLogBlobFilePath = gl_analysisID + "/" + gl_analysisPhaseID + "/executions/l1-l2/" + gl_executionGroupID + "/"


##staging/temporary files
gl_Layer2Staging = gl_MountPoint + "/"+ gl_analysisPhaseID + "/staging/l2-stg/"
gl_bifurcationPath = gl_MountPoint + "/" + gl_analysisPhaseID + "/staging/l2-stg-bifurcation/"
gl_preBifurcationPath = gl_MountPoint + "/" + gl_analysisPhaseID + "/staging/l2-stg-prebifurcation/"

gl_countJET=0

global gl_maxParallelism
global gl_dfExecutionOrder
