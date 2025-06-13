# Databricks notebook source
from collections import Counter
import inspect
import itertools
from itertools import accumulate,groupby,zip_longest
import builtins as g
import sys
import csv
import os
import traceback
import pandas as pd
from collections import namedtuple
import pyspark.sql.functions as F
from pyspark.sql.functions import  col,concat,lit,when,substring,length,repeat,row_number,sum,abs,count,expr,countDistinct,lpad
from pyspark.sql.window import Window
from datetime import datetime,timedelta
from dateutil.parser import parse
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,ShortType ,BooleanType ,LongType,DecimalType,DoubleType,FloatType
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
import re
from delta.tables import *

gl_ADJSchema = StructType([
StructField("transactionID", StringType(),False),
StructField("transactionLineIDKey", IntegerType(),False)
   ])

gl_TMPJEBifurcationSchema = StructType([
StructField("journalSurrogateKey", LongType(),False),
StructField("transactionIDbyPrimaryKey",IntegerType(),False),
StructField("transactionID", StringType(),False),
StructField("lineItem", StringType(),False),
StructField("transactionLineIDKey", IntegerType(),False),
StructField("transactionLineID", StringType(),False),
StructField("journalId", StringType(),False),
StructField("accountID", StringType(),False),
# StructField("debitAmount", DecimalType(32,6),False),
# StructField("creditAmount", DecimalType(32,6),False),
StructField("debitAmount", DoubleType(),False),
StructField("creditAmount", DoubleType(),False),
StructField("debitCreditCode", StringType(),False),
# StructField("postingAmount", DecimalType(32,6),False),
StructField("postingAmount", DoubleType(),False),
StructField("patternID", IntegerType(),False),
StructField("patternSource", StringType(),False),
StructField("transactionLine", IntegerType(),False),
StructField("blockSource", StringType(),False),
StructField("isIrregularTransaction", IntegerType(),False)
   ])
gl_SBBSchema = StructType([
StructField("transactionID", StringType(),False),
StructField("transactionID_New", StringType(),False),
StructField("transactionLineIDKey", IntegerType(),False)
   ])
gl_LoopingSchema = StructType([
StructField("transactionID", StringType(),False),
StructField("oneSideTransactionLineIDKey", IntegerType(),False),
StructField("oneSideDebitCreditCode", StringType(),False),
StructField("manySideTransactionLineIDKey", IntegerType(),False),
StructField("amount", StringType(),False),
StructField("loopingStrategy", StringType(),False),
StructField("loopCount", StringType(),False)
])
gl_AggLoopingSchema = StructType([
StructField("transactionID", StringType(),False),
StructField("oneSideTransactionLineIDKey", StringType(),False),
StructField("oneSideDebitCreditCode", StringType(),False),
StructField("manySideTransactionLineIDKey", StringType(),False),
StructField("amount", StringType(),False),
StructField("loopingStrategy", StringType(),False),
StructField("loopCount", StringType(),False)
])
gl_TMPJEBifurcationAggSchema = StructType([
StructField("journalSurrogateKey", LongType(),False),
StructField("transactionIDbyPrimaryKey",IntegerType(),False),
StructField("transactionID", StringType(),False),
StructField("lineItem", StringType(),False),
StructField("transactionLineIDKey", StringType(),False),
StructField("transactionLineID", StringType(),False),
StructField("journalId", StringType(),False),
StructField("accountID", StringType(),False),
StructField("aggregatedInToLineItem", StringType(),False),
StructField("debitAmount", DecimalType(32,6),False),
StructField("creditAmount", DecimalType(32,6),False),
StructField("debitCreditCode", StringType(),False),
StructField("postingAmount", DecimalType(32,6),False),
StructField("patternID", IntegerType(),False),
StructField("transactionLine", IntegerType(),False)
   ])

gl_STGJEBifurcationLinkSchema = StructType([
StructField("journalSurrogateKey", LongType(),False),
StructField("companyCode", StringType(),False),
StructField("transactionIDbyPrimaryKey", IntegerType(),False),
StructField("transactionID", StringType(),False),
StructField("lineItemNumber", StringType(),False),
StructField("fixedAccount", StringType(),False),
StructField("fixedLIType", StringType(),False),
StructField("amount", DecimalType(32,6),False),
StructField("dataType", StringType(),False),
StructField("rule", StringType(),False),
StructField("bifurcationType", StringType(),False),
StructField("journalId", StringType(),False),
StructField("relatedLineItemNumber", StringType(),False),
StructField("relatedAccount", StringType(),False),
StructField("relatedLIType", StringType(),False),
StructField("ruleSequence", ShortType(),False),
StructField("loopCount", StringType(),False),
StructField("direction", StringType(),False),
StructField("aggregatedInToLineItem", StringType(),False),
StructField("lineItem", StringType(),False),
StructField("aggregatedInToLine", IntegerType(),False),
StructField("patternID", ShortType(),False),
StructField("transactionLine", IntegerType(),False),
StructField("patternSource", StringType(),False),
StructField("blockSource", StringType(),False)
   ])
gl_maxLineCount=1000 
gl_SBBMaxLineCount=1000
