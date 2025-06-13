# Databricks notebook source
import sys
import traceback
from datetime import datetime,time
from pyspark.sql.functions import  col,concat,lit,when,lead,lag,max,min,sum,substring
from pyspark.sql.window import Window
import pyspark.sql.functions as F

def fin_SAP_L0_STG_reversalDocument_determine(): 
  try:
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    global fin_SAP_L0_STG_reversalDocument
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)

    case1 = when (concat(col('BKPF.GJAHR'),col('BKPF.BELNR')) < concat(col('BKPF.STJAH'),col('BKPF.STBLG')),col('BKPF.STJAH'))\
                  .otherwise(col("BKPF.GJAHR"))

    case2 = when (concat(col('BKPF.GJAHR'),col('BKPF.BELNR')) < concat(col('BKPF.STJAH'),col('BKPF.STBLG')),col('BKPF.STBLG'))\
                  .otherwise(col("BKPF.BELNR"))

    dr_Rank= Window.orderBy(col("BKPF.MANDT"),col("BKPF.BUKRS"),lit(case1),lit(case2))
    
    creationDT = concat(substring(col('BKPF.CPUDT'), 1, 10),lit(' '),substring(col('BKPF.CPUTM'), 1, 2),lit(':'),\
                        substring(substring(col('BKPF.CPUTM'), 3, 4),1,2),lit(':'),substring(col('BKPF.CPUTM'),-2,2))

    fin_SAP_L0_STG_reversalDocument = erp_BKPF.alias('BKPF')\
                                  .filter(col('STBLG') != '')\
                                  .select(F.dense_rank().over(dr_Rank).alias("reversalID"),col('MANDT'),col('BUKRS'),col('GJAHR'),col('BELNR'),\
                                          col('STBLG'),col('XREVERSAL'),lit(creationDT).alias('creationDate'),col('BUDAT').alias('postingDT'),\
                                          col('STJAH').alias('postingDate'),col('STJAH'),col('STGRD').alias('reversalReason'),\
                                          when (col('BKPF.XREVERSAL').isNull(),lit(0))\
                                               .otherwise(col('BKPF.XREVERSAL')).alias('reversalType')).sort(col('reversalID'))
        
    ReversalCTE1 = fin_SAP_L0_STG_reversalDocument.alias('slsr')\
                              .filter(col('reversalType') == lit(0))\
                              .groupBy('slsr.reversalID')\
                              .agg(
                                    count('slsr.BELNR').alias("cnt_STBLG")\
                                  )\
                              .where(col("cnt_STBLG") > 2)


    reversalType1 = when(col('cte1.reversalID').isNotNull(),lit(3))\
                .otherwise(col('slsr1.reversalType'))

    fin_SAP_L0_STG_reversalDocument = fin_SAP_L0_STG_reversalDocument.alias('slsr1')\
                                    .join(ReversalCTE1.alias('cte1'),\
                                              (col('slsr1.reversalID').eqNullSafe(col('cte1.reversalID'))),how='left')\
                                  .select(col('slsr1.reversalID'),col('MANDT'),col('BUKRS'),col('GJAHR'),col('BELNR'),col('STBLG'),col('XREVERSAL'),\
                                          col('creationDate'),col('postingDT'),col('postingDate'),\
                                          col('STJAH'),col('reversalReason'),lit(reversalType1).alias('reversalType'))
    
    ReversalCTE2 = fin_SAP_L0_STG_reversalDocument.alias('slsr')\
                              .filter(col('reversalType') == lit(0))\
                              .groupBy('slsr.reversalID')\
                              .agg(
                                    max('slsr.reversalReason').alias("maxReversal")\
                                    ,min('slsr.reversalReason').alias("minReversal")\
                                    ,count('slsr.reversalReason').alias("countReversal")\
                                  )

    fin_SAP_L0_TMP_reversalDocument = ReversalCTE2.alias('rcte2')\
                                     .filter(((col("rcte2.maxReversal")!="") &\
                                           (col("rcte2.minReversal")=="")) &\
                                           ((col("rcte2.countReversal")==2)))\
                                     .select(col('rcte2.reversalID'),col('rcte2.maxReversal'),col('rcte2.minReversal'),col('rcte2.countReversal'))
      
    reversalType2 = when((col('lsrd.reversalReason')!= "") & (col('ltrd.reversalID').isNotNull()),lit(1))\
                    .otherwise(lit(2))

    fin_SAP_L0_STG_reversalDocument= fin_SAP_L0_STG_reversalDocument.alias('lsrd')\
            .join(fin_SAP_L0_TMP_reversalDocument.alias('ltrd'),\
                  (col('lsrd.reversalID').eqNullSafe(col('ltrd.reversalID'))),how='left')\
            .select(col('lsrd.reversalID'),col('MANDT'),col('BUKRS'),col('GJAHR'),col('BELNR'),col('STBLG'),col('XREVERSAL'),\
                                          col('creationDate'),col('postingDT'),col('postingDate'),\
                                          col('STJAH'),col('reversalReason'),lit(reversalType2).alias('reversalType'))
    
    ReversalCTE3 = fin_SAP_L0_STG_reversalDocument.alias('slsr3')\
                  .filter(col('reversalType') == 0)\
                  .groupBy('slsr3.reversalID')\
                  .agg(
                           count('slsr3.BELNR').alias('cnt_STBLG')\
                       )\
                  .where(col('cnt_STBLG') == 1)
       
    reversalType3 = when((col('revd0.reversalReason')!= "") & (col('rcte3.reversalID').isNotNull()),lit(1))\
                                .otherwise(lit(2))
    
    
                    
    fin_SAP_L0_STG_reversalDocument = fin_SAP_L0_STG_reversalDocument.alias('revd0')\
                                      .join(ReversalCTE3.alias('rcte3'),(col('revd0.reversalID').eqNullSafe(col('rcte3.reversalID'))),how='left')\
                                      .select(col('revd0.reversalID'),col('MANDT'),col('BUKRS'),col('GJAHR'),col('BELNR'),col('STBLG'),col('XREVERSAL'),\
                                          col('creationDate'),col('postingDT'),col('postingDate'),\
                                          col('STJAH'),col('reversalReason'),lit(reversalType3).alias('reversalType'))

    fin_SAP_L0_TMP_reversalIDCount =  fin_SAP_L0_STG_reversalDocument.alias('revd0')\
                                          .filter(col('reversalType') == 0)\
                                          .groupBy('revd0.reversalID')\
                                          .agg(
                                                   count('revd0.BELNR').alias('cnt_STBLG')\
                                               )\
                                          .where(col('cnt_STBLG') == 2)
    
    
    sub1 = fin_SAP_L0_TMP_reversalIDCount.select(col('reversalID')).distinct().collect()
    sub1 = [x["reversalID"] for x in sub1]

    windowSpec  = Window.partitionBy("srd.reversalID").orderBy("srd.reversalID","srd.creationDate","srd.postingDate","srd.BELNR")
    ReversalCTE4 = fin_SAP_L0_STG_reversalDocument.alias('srd')\
                  .filter((col('srd.reversalID').isin(sub1)) &  (col('reversalType') == 0 ))\
                  .select(F.row_number().over(windowSpec).alias("ID"),col('srd.reversalID'),col('srd.BELNR'),col('srd.creationDate'),col('srd.postingDate'),\
                         col('srd.reversalType'))
    
    reversalType4 = when(col('revd0.reversalType') == 0, when((col('rcte4.ID')!= 1) & (col('rcte4.reversalID').isNotNull()),lit(1))\
                                                              .otherwise(lit(2)))\
                    .otherwise(col('revd0.reversalType'))


    fin_SAP_L0_STG_reversalDocument = fin_SAP_L0_STG_reversalDocument.alias('revd0')\
                  .filter(col('revd0.reversalType') == 0)\
                  .join(ReversalCTE4.alias('rcte4'),((col("revd0.reversalID") == col("rcte4.reversalID")) \
                                                        & (col("revd0.BELNR")   == col("rcte4.BELNR"))\
                                                        & (col("revd0.creationDate")   == col("rcte4.creationDate"))\
                                                        & (col("revd0.postingDate") == col("rcte4.postingDate"))), "left")\
                  .select(col('revd0.reversalID'),col('revd0.MANDT'),col('revd0.BUKRS'),col('revd0.GJAHR'),col('revd0.BELNR'),col('revd0.STBLG'),col('revd0.XREVERSAL'),\
                                          col('revd0.creationDate'),col('revd0.postingDT'),col('revd0.postingDate'),\
                                          col('revd0.STJAH'),col('revd0.reversalReason'),lit(reversalType4).alias('reversalType'))
    
    
    fin_SAP_L0_STG_reversalDocument =  objDataTransformation.gen_convertToCDMandCache \
          (fin_SAP_L0_STG_reversalDocument,'fin','SAP_L0_STG_reversalDocument',targetPath=gl_layer0Temp_fin)
      
    executionStatus = "fin_SAP_L0_STG_reversalDocument populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
