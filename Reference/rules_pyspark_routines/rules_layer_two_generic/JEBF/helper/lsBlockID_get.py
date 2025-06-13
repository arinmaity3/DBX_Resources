# Databricks notebook source
from decimal import Decimal
def lsBlockID_get(lsSignedAmounts,blockID=1):
  lsAccAmounts =[g.round(Decimal(str(amnt)), 6) for amnt in list(accumulate(lsSignedAmounts))]
  for x in lsAccAmounts:
    if  x==0.00:
      blockID +=1
      yield blockID-1
    else:
       yield blockID
