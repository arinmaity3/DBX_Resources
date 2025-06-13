# Databricks notebook source
import sys
import traceback
import pandas as pd
from itertools import zip_longest

def ApplyADJ(pdfTransactionColumns):
  pdfTransactionColumns=pdfTransactionColumns.sort_values("transactionLineIDKey")
  lsTransasactionLineIDKeys=pdfTransactionColumns['transactionLineIDKey'].tolist() 
  lsValidPairs=list(zip_longest(lsTransasactionLineIDKeys[1::2],lsTransasactionLineIDKeys[2::2],fillvalue=1))
  lsValidFirsts=[x[0] for x in lsValidPairs]
  lsDelLines=list(set(lsTransasactionLineIDKeys)-set(lsValidFirsts))
  df_DelLines =  pd.DataFrame(lsDelLines, columns = ['transactionLineIDKey'])
  df_ApplyAdjDel = pdfTransactionColumns.merge(df_DelLines,on="transactionLineIDKey")
  df_ApplyAdjDel=df_ApplyAdjDel[["transactionID","transactionLineIDKey"]]
  return df_ApplyAdjDel
