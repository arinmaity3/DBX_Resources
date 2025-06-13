# Databricks notebook source
from decimal import Decimal
def lsShiftedBlock_get(lsTransasactionLineIDKeys,lsSignedAmounts,lsDebitCreditCodes,blockID):
    lsOutputBlocks=[]
    lsBlockForNextIteration=[]
    iterationCount=0
    lsSignedAmounts=[g.round(amnt, 6) for amnt in lsSignedAmounts]
    i=1
    LineByLineCntr=1
    maxIteration=len(lsTransasactionLineIDKeys)
    while True:
        # Check if the block has just one debit or credit line
        if 1 in list(Counter(lsDebitCreditCodes).values()):
            print('Single Debit or Credit case. No use shifting.')
            if iterationCount==0:
                # If the input transaction itself has just one debit/credit then nothing to return here.
                return [False]
            else:
                # The outputblocks constructed over the previous iterations will be returned here.
                lsOutputBlocks.extend(lsBlockForNextIteration)
                if len(set([x[0] for x in lsOutputBlocks]))>1:
                  return [True,lsOutputBlocks]
                else:
                  return [False]
        elif g.sum(lsSignedAmounts)==0:
            print('Proceed to SBB')
            lsLineIndices = list(range(len(lsTransasactionLineIDKeys)))
            endIndex = len(lsLineIndices)-1
            # Find out at which line will the lsSignedAmounts give new blocks
            lsSignedAmountsReversedForDescAccum = list(reversed(lsSignedAmounts))
            lsAccAmounts =[g.round(Decimal(str(amnt)), 6) for amnt in list(accumulate(lsSignedAmounts))]
            lsAccAmountsDesc =[g.round(Decimal(str(amnt)), 6) for amnt in list(accumulate(lsSignedAmountsReversedForDescAccum))]
            lsAccAmountsDesc.reverse()
            lsAccAmountsDescOppSign=[-1*amnt for amnt in lsAccAmountsDesc]
            for currLineIndex,accamount in enumerate(lsAccAmounts) :
                lsMatched=0
                if currLineIndex >= endIndex:
                  break
                elif g.round(lsSignedAmounts[currLineIndex]+lsSignedAmounts[currLineIndex+1],6)==0.00:
                  lsMatched=1
                  break
                elif accamount in lsAccAmountsDescOppSign[currLineIndex+2:]:
                  lsMatched=2
                  break
            if lsMatched==0:
                print("lsMatched=",lsMatched,"iterationCount=",iterationCount)
                if iterationCount==0:
                    # If no shifting has been done yet, and if no index could be identified for
                    #  successful shifting, then nothing to return here.
                    return [False]
                else:
                    # If some shifting has been done, and if no index could be identified for further shifting,
                    #  then the outputblocks constructed over the previous iterations will be returned here.
                    print("lsMatched=",lsMatched,"iterationCount=",iterationCount)
                    lsOutputBlocks.extend(lsBlockForNextIteration)
                    if len(set([x[0] for x in lsOutputBlocks]))>1:
                      return [True,lsOutputBlocks]
                    else:
                      return [False]
            elif lsMatched==1:
                # Each shifting corresponds to an iteration.
                iterationCount+=1
#                 print(transactionID)
                print("lsMatched=",lsMatched,"iterationCount=",iterationCount)
                #Handle case if that is a O-O subblock:
                
                 # The currLineIndex at which lsMatched returned matchingline 
                          #is the index at which shifting can be done to successfully produce sub-blocks.
                lsLineIndices=list(range(currLineIndex,endIndex+1))
                lsLineIndices.extend(list(range(0,currLineIndex)))
            elif lsMatched==2:
                iterationCount+=1
                print("lsMatched=",lsMatched,"iterationCount=",iterationCount)
                #(The currLineIndex at which lsMatched returned matchingline )+ 1 is the index
                     #at which shifting can be done to successfully produce sub-blocks.
                #So Shift to that location.
                lsLineIndices.extend(list(range(currLineIndex+1)))
                del lsLineIndices[:currLineIndex+1]
#                 print("Before shifting")
                
            #Align all the lists based on new indexing.
            lsSignedAmounts = [lsSignedAmounts[i] for i in lsLineIndices]
            lsDebitCreditCodes = [lsDebitCreditCodes[i] for i in lsLineIndices]
            lsTransasactionLineIDKeys = [lsTransasactionLineIDKeys[i] for i in lsLineIndices]

            #Identify blocks
#                 print("After shifting, pass into blockbuilding:")

            lsBlocks = list(lsBlockID_get(lsSignedAmounts,blockID))

            lsShiftedWithBlocks = list(zip(lsBlocks,lsTransasactionLineIDKeys,lsSignedAmounts,lsDebitCreditCodes))
            #Output all except the last block
            maxBlockID = g.max(lsBlocks)
            for key, group in groupby(lsShiftedWithBlocks, lambda x: x[0]):
                lsSplitByBlocks=list(group)
                if lsSplitByBlocks[0][0]!=maxBlockID:
                    # All blocks except the last can already be moved to output-list
                    lsOutputBlocks.extend(lsSplitByBlocks)
                else:
                    lsBlockForNextIteration = lsSplitByBlocks
            lsTransasactionLineIDKeys = [tup[1] for tup in lsBlockForNextIteration]
            lsSignedAmounts = [tup[2] for tup in lsBlockForNextIteration]
            lsDebitCreditCodes = [tup[3] for tup in lsBlockForNextIteration]
            blockID = maxBlockID
            # The 3 shifted lists above and the maxBlockID from this iteration will go into the next iteration.
            
        else:
            print("g.sum(lsSignedAmounts)=",g.sum(lsSignedAmounts))
            lsLineIndices = list(range(len(lsTransasactionLineIDKeys)))
            
            while LineByLineCntr < len(lsTransasactionLineIDKeys):
                firstIndex=lsLineIndices[0]
                lsLineIndices=lsLineIndices[1:]
                lsLineIndices.append(firstIndex)
                firstAmnt=lsSignedAmounts[0]
                lsSignedAmounts=lsSignedAmounts[1:]
                lsSignedAmounts.append(firstAmnt)
                lsBlocks = list(lsBlockID_get(lsSignedAmounts,blockID))
                maxBlockID = g.max(lsBlocks)
                if maxBlockID>blockID:
                    lsDebitCreditCodes = [lsDebitCreditCodes[i] for i in lsLineIndices]
                    lsTransasactionLineIDKeys = [lsTransasactionLineIDKeys[i] for i in lsLineIndices]
                    print(lsTransasactionLineIDKeys[0])
                    lsShiftedWithBlocks = list(zip(lsBlocks,lsTransasactionLineIDKeys,lsSignedAmounts,lsDebitCreditCodes))
                    for key, group in groupby(lsShiftedWithBlocks, lambda x: x[0]):
                        lsSplitByBlocks=list(group)
                        if lsSplitByBlocks[0][0]!=maxBlockID:
                            # All blocks except the last can already be moved to output-list
                            lsOutputBlocks.extend(lsSplitByBlocks)
                        else:
                            lsBlockForNextIteration = lsSplitByBlocks
                    lsTransasactionLineIDKeys = [tup[1] for tup in lsBlockForNextIteration]
                    lsSignedAmounts = [tup[2] for tup in lsBlockForNextIteration]
                    lsDebitCreditCodes = [tup[3] for tup in lsBlockForNextIteration]
                    blockID = maxBlockID
                    break
                else:
                    LineByLineCntr+=1
            if LineByLineCntr >=len(lsTransasactionLineIDKeys):
                lsOutputBlocks.extend(lsBlockForNextIteration)
                if len(set([x[0] for x in lsOutputBlocks]))>1:
                  return [True,lsOutputBlocks]
                else:
                  return [False]  
            else:
                iterationCount+=1
        if i> maxIteration:
            lsOutputBlocks.extend(lsBlockForNextIteration)
            if len(set([x[0] for x in lsOutputBlocks]))>1:
                  return [True,lsOutputBlocks]
            else:
                  return [False]  
        else:
            i+=1
                    
