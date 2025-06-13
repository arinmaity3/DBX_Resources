# Databricks notebook source
class LoopInputRecord:
  def __init__(self,
               journalSurrogateKey,
               transactionID,
               transactionLineIDKey,
               debitCreditCode,
               amount               
               ):
    self.transactionID=transactionID
    self.journalSurrogateKey=journalSurrogateKey
    self.transactionLineIDKey=transactionLineIDKey
    self.debitCreditCode=debitCreditCode
    self.amount=g.round(Decimal(amount),6)
    self.isResolved=False

# COMMAND ----------

def convert_row_to_LoopInputRecord(row):
    return LoopInputRecord(row['journalSurrogateKey'],
                    row['transactionID'],
                    row['transactionLineIDKey'],
                    row['debitCreditCode'],
                    row['amount'])

def LookDown(ls_records):
    loopCount=0
    LoopDownOutput=[]
    for oneSideIndex,oneSideRecord in enumerate(ls_records):
     if oneSideRecord.isResolved:
       continue
     else:
       tentativeOutput=[]
       loopCount+=1
       amount=g.round(oneSideRecord.amount,6)
       for downIndex in range(oneSideIndex+1,len(ls_records)):
         manySideRecord=ls_records[downIndex]
         if manySideRecord.debitCreditCode!=oneSideRecord.debitCreditCode \
            and manySideRecord.isResolved==False and amount>0:
                  amount=g.round(amount,6)-g.round(manySideRecord.amount,6)
                  tentativeOutput.append((oneSideRecord,manySideRecord))
       if g.round(amount,6)==0:
           oneSideRecord.isResolved=True
           for tup in tentativeOutput:
               manySideRecord = tup[1]
               manySideRecord.isResolved=True
               LoopDownOutput.extend([(oneSideRecord.transactionID,oneSideRecord.transactionLineIDKey,oneSideRecord.debitCreditCode,
                               manySideRecord.transactionLineIDKey,str(g.round(manySideRecord.amount,6)),'DOWN',str(loopCount))])
           loopCount=0
    return LoopDownOutput

def LookUp(ls_records):
    loopCount=0
    LoopUpOutput=[]
    ls_records = [rec for rec in ls_records if rec.isResolved==False]
    for oneSideIndex,oneSideRecord in enumerate(ls_records):
       if oneSideRecord.isResolved:
         continue
       else:
         tentativeOutput=[]
         loopCount+=1
         amount=g.round(oneSideRecord.amount,6)
         for upIndex in range(oneSideIndex-1,-1,-1):
           manySideRecord=ls_records[upIndex]
           if manySideRecord.debitCreditCode!=oneSideRecord.debitCreditCode \
              and manySideRecord.isResolved==False and amount>0:
                    amount=g.round(amount,6)-g.round(manySideRecord.amount,6)
                    tentativeOutput.append((oneSideRecord,manySideRecord))
         if amount==0:
             oneSideRecord.isResolved=True
             for tup in tentativeOutput:
                 manySideRecord = tup[1]
                 manySideRecord.isResolved=True
                 LoopUpOutput.extend([(oneSideRecord.transactionID,oneSideRecord.transactionLineIDKey,oneSideRecord.debitCreditCode,
                                 manySideRecord.transactionLineIDKey,str(g.round(manySideRecord.amount,6)),'UP',str(loopCount))])
             loopCount=0
    return LoopUpOutput



def LookDownUp(ls_records):
    loopCount=0
    LoopDownUpOutput=[]
    ls_records = [rec for rec in ls_records if rec.isResolved==False]
    for oneSideIndex,oneSideRecord in enumerate(ls_records):
#     print(oneSideRecord.transactionLineIDKey,oneSideRecord.debitCreditCode,oneSideRecord.amount,oneSideRecord.isResolved)
      if oneSideRecord.isResolved:
        continue
      else:
        tentativeOutput=[]
        loopCount+=1
        downIndex=oneSideIndex+1
        upIndex=oneSideIndex
        amount=g.round(oneSideRecord.amount,6)
        direction_flag=1
#         print("DownIndex ",downIndex,"upIndex ",upIndex,"amount ",amount)
        while ((downIndex<len(ls_records) or upIndex>=0) and g.round(amount,6)>0): 
#               print("DownIndex ",downIndex,"upIndex ",upIndex,"amount ",g.round(amount,6),"direction_flag ",direction_flag)
              if direction_flag==1:
                  if downIndex<len(ls_records):
                    manySideRecord=ls_records[downIndex]
                    if manySideRecord.debitCreditCode!=oneSideRecord.debitCreditCode and manySideRecord.isResolved==False :
                      amount=g.round(amount,6)-g.round(manySideRecord.amount,6)
                      tentativeOutput.append((oneSideRecord,manySideRecord))
                      upIndex-=1
                      direction_flag=-1 
                    else:
                       downIndex+=1
                  else:
                      upIndex-=1
                      direction_flag=-1 
              elif direction_flag==-1:
                  if upIndex>=0:
                    manySideRecord=ls_records[upIndex]
                    if manySideRecord.debitCreditCode!=oneSideRecord.debitCreditCode and manySideRecord.isResolved==False :
                        amount=g.round(amount,6)-g.round(manySideRecord.amount,6)
                        tentativeOutput.append((oneSideRecord,manySideRecord))
                        downIndex+=1
                        direction_flag=1 
                    else:
                      upIndex-=1
                  else:
                      downIndex+=1
                      direction_flag=1 
#               print("End of Loop: DownIndex ",downIndex,"upIndex ",upIndex,"amount ",g.round(amount,6),"direction_flag ",direction_flag)
        if g.round(amount,6)==0:
            oneSideRecord.isResolved=True
            for tup in tentativeOutput:
                manySideRecord = tup[1]
                manySideRecord.isResolved=True
                LoopDownUpOutput.extend([(oneSideRecord.transactionID,oneSideRecord.transactionLineIDKey,oneSideRecord.debitCreditCode,
                                manySideRecord.transactionLineIDKey,str(g.round(manySideRecord.amount,6)),'DOWN_UP',str(loopCount))])
            loopCount=0
    return LoopDownUpOutput

def ApplyLoopingStrategy(pdf_Input):
    ls_records = [convert_row_to_LoopInputRecord(row) for index,row in pdf_Input.sort_values("transactionLineIDKey").iterrows()]
    LoopOutput=[]
    LoopOutput.extend(LookDown(ls_records))
    LoopOutput.extend(LookUp(ls_records))
    LoopOutput.extend(LookDownUp(ls_records))
    dfLoopOutput =  pd.DataFrame(LoopOutput, columns = ['transactionID', 'oneSideTransactionLineIDKey','oneSideDebitCreditCode','manySideTransactionLineIDKey','amount','loopingStrategy','loopCount'])
    return dfLoopOutput
