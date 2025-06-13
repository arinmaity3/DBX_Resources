# Databricks notebook source
import sys
import traceback
import pandas as pd
from decimal import Decimal

def ApplySBB(pdfTransactionColumns):
 
      lsOutputBlocks=[]
      blockID=1
      isNewBlocksCreated=0
      
      pdfTransactionColumns=pdfTransactionColumns.sort_values("transactionLineIDKey")
      lsTransasactionLineIDKeys=pdfTransactionColumns['transactionLineIDKey'].tolist() 
      lsSignedAmounts=pdfTransactionColumns['postingAmount'].tolist() 
      lsSignedAmounts=[g.round(Decimal(x),6) for x in lsSignedAmounts]
      lsDebitCreditCodes=pdfTransactionColumns['debitCreditCode'].tolist() 
      # Call SBB function which will return a list:
        # First element of the returned list indicates if further blocks could be identified or not (True/False)
        # Second element of the returned list is a zipped list of (blockIDs,TransactionLineIDKeys,postingAmounts and debitCreditCodes).
      
      SBBOutput =lsShiftedBlock_get(lsTransasactionLineIDKeys,lsSignedAmounts,lsDebitCreditCodes,blockID)
      
      
      # Capture if sub-blocks have been created or not
      if SBBOutput[0]==True:
          isNewBlocksCreated = 1
          lsOutputBlocks = SBBOutput[1]
      lsOutputBlocks=[(x[0],x[1]) for x in lsOutputBlocks]
      df_SBBOutput =  pd.DataFrame(lsOutputBlocks, columns = ['blockID', 'transactionLineIDKey'])
      df_ApplySBBOutput = pdfTransactionColumns.merge(df_SBBOutput,on="transactionLineIDKey")
      if isNewBlocksCreated==1:
          df_ApplySBBOutput["transactionID_New"]=df_ApplySBBOutput["transactionID"] + '-' + df_ApplySBBOutput['blockID'].map(str) 
      else:
#           df_ApplySBBOutput["transactionID_New"]=df_ApplySBBOutput["transactionID"]
           df_ApplySBBOutput=pd.DataFrame(columns = ["transactionID","transactionLineIDKey","transactionID_New"])
      df_ApplySBBOutput=df_ApplySBBOutput[["transactionID","transactionLineIDKey","transactionID_New"]]
      return df_ApplySBBOutput
