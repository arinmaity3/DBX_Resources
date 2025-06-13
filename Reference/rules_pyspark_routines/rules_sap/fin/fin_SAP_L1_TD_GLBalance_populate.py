# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,when,DataFrame
from functools import reduce



def fin_SAP_L1_TD_GLBalance_populate():
  try:
    
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    global fin_L1_TD_GLBalance

    
    erpSAPSystemID = int(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID')) 
    #clientCodeActivated = erp_FAGL_ACTIVEC.agg({"MANDT": "max"}).collect()[0][0]
    
    clientCodeActivated = erp_FAGL_ACTIVEC.filter((col("ACTIVE")!="") & (col("READ_GLT0")=="")).agg({"MANDT": "max"}).collect()[0][0]

    when(col("glb.financialPeriod")=='-1', col('glb.beginningBalanceLC')) \
                               .otherwise(lit('0'))

    if (erp_FAGLFLEXT.first() is not None):
       is_GLT_FAGLFLEXT = "FAGLFLEXT"
    else:
        is_GLT_FAGLFLEXT = "GLT0"
    
    
    if (objGenHelper.file_exist(gl_layer0Staging + "erp_FAGLFLEXT.delta") is not None) & (objGenHelper.file_exist(gl_layer0Staging + "erp_FAGL_TLDGRP_MAP.delta") is not None) & \
    (objGenHelper.file_exist(gl_layer0Staging + "erp_T881.delta") is not None) & (is_GLT_FAGLFLEXT == "FAGLFLEXT"):
      
      ledgerInGeneralLedger = erp_FAGL_TLDGRP_MAP.alias('fagl_tldgrp_map_erp')\
                                              .join(erp_T881.alias('t881_erp'),((col("fagl_tldgrp_map_erp.RLDNR") == col("t881_erp.RLDNR"))\
                                                                                             & (col("t881_erp.XLEADING") == lit('X')) \
                                                                                             & (col("fagl_tldgrp_map_erp.MANDT") == lit(clientCodeActivated)) \
                                                                                             & (col("fagl_tldgrp_map_erp.MANDT") == col("t881_erp.MANDT")) ),"inner")
      ledgerInGeneralLedgeragg = ledgerInGeneralLedger.agg({'fagl_tldgrp_map_erp.RLDNR': 'max'})
      ledgerInGeneralLedgerAccountingJoin=ledgerInGeneralLedgeragg\
                .withColumn("ledgerInGeneralLedgerAccountingJoin",(when(col('max(RLDNR)').isNull() ,lit('0L')).otherwise(col('max(RLDNR)')))).drop("max(RLDNR)").collect()[0][0]


      LDGRP_nullif = expr("nullif(erp_BKPF.LDGRP,'')")
      LDGRP = when((lit(LDGRP_nullif).isNull()),lit(ledgerInGeneralLedgerAccountingJoin)).otherwise(lit(LDGRP_nullif))       
      erp_BKPF1 = erp_BKPF.alias('erp_BKPF')\
                .select(lit(LDGRP).alias('LDGRP')).distinct()   
      ledgerInGeneralLedger1 = ledgerInGeneralLedger.alias('ss')\
                              .join(erp_BKPF1.alias('erp_BKPF1'),((col('ss.LDGRP') == col('erp_BKPF1.LDGRP'))),"inner")\
    
      ledgerInGeneralLedger1agg = ledgerInGeneralLedger1.agg({'erp_BKPF1.LDGRP': 'max'})  
      ledgerInGeneralLedgerAccounting = ledgerInGeneralLedger1agg\
                                          .withColumn("ledgerInGeneralLedgerAccounting",(when(col('max(LDGRP)').isNull() ,lit('')).otherwise(col('max(LDGRP)')))).drop('max(LDGRP)').collect()[0][0]
    else:
      ledgerInGeneralLedgerAccounting = '0L'
      
    if ((ledgerInGeneralLedgerAccounting != "") & (erp_FAGLFLEXT.first() is not None)):
      
      fin_SAP_L0_TMP_GLBalance = erp_FAGLFLEXT.alias('fagl')\
                                          .join (erp_T001.alias('t001'),((col("t001.MANDT") == col("fagl.RCLNT"))\
                                                                                         & (col("t001.BUKRS") == col("fagl.RBUKRS") )),"inner")\
                                          .filter((col("fagl.RRCTY") == lit('0')) & (col('fagl.RVERS') == lit('001'))\
                                                  &(col('fagl.RLDNR') == lit(ledgerInGeneralLedgerAccounting)))\
                                          .groupBy(col('fagl.RBUKRS').alias('companyCode'),col('fagl.RYEAR').alias('fiscalYear')\
                                                   ,col('fagl.RACCT').alias('accountNumber'),col('fagl.DRCRK').alias('debitCreditIndicator')\
                                                   ,col('fagl.RTCUR').alias('documentCurrency'),col('t001.WAERS').alias('localCurrency'))\
                                          .agg( sum('fagl.HSLVT').alias("beginningBalanceLC")\
                                                ,sum('fagl.TSLVT').alias("beginningBalanceDC")\
                                              ,sum('fagl.HSLVT').alias('-1')\
                                              ,sum('fagl.HSL01').alias('1')\
                                              ,sum('fagl.HSL02').alias('2')\
                                              ,sum('fagl.HSL03').alias('3')\
                                              ,sum('fagl.HSL04').alias('4')\
                                              ,sum('fagl.HSL05').alias('5')\
                                              ,sum('fagl.HSL06').alias('6')\
                                              ,sum('fagl.HSL07').alias('7')\
                                              ,sum('fagl.HSL08').alias('8')\
                                              ,sum('fagl.HSL09').alias('9')\
                                              ,sum('fagl.HSL10').alias('10')\
                                              ,sum('fagl.HSL11').alias('11')\
                                              ,sum('fagl.HSL12').alias('12')\
                                              ,sum('fagl.HSL13').alias('13')\
                                              ,sum('fagl.HSL14').alias('14')\
                                              ,sum('fagl.HSL15').alias('15')\
                                              ,sum('fagl.HSL16').alias('16')\
                                               )

      fin_SAP_L0_TMP_GLBalance.createOrReplaceTempView("fin_SAP_L0_TMP_GLBalance")


      fin_SAP_L0_TMP_GLBalance1 = erp_FAGLFLEXT.alias('fagl')\
                                          .join (erp_T001.alias('t001'),((col("t001.MANDT") == col("fagl.RCLNT"))\
                                                                                         & (col("t001.BUKRS") == col("fagl.RBUKRS") )),"inner")\
                                          .filter((col("fagl.RRCTY") == lit('0')) & (col('fagl.RVERS') == lit('001')) \
                                                  &(col('fagl.RLDNR') == lit(ledgerInGeneralLedgerAccounting)) )\
                                          .groupBy(col('fagl.RBUKRS').alias('companyCode'),col('fagl.RYEAR').alias('fiscalYear')\
                                                   ,col('fagl.RACCT').alias('accountNumber'),col('fagl.DRCRK').alias('debitCreditIndicator')\
                                                   ,col('fagl.RTCUR').alias('documentCurrency'),col('t001.WAERS').alias('localCurrency'))\
                                          .agg( sum('fagl.HSLVT').alias("beginningBalanceLC")\
                                                ,sum('fagl.TSLVT').alias("beginningBalanceDC")\
                                                ,sum('fagl.TSLVT').alias('-1')\
                                                ,sum('fagl.TSL01').alias('1')\
                                                ,sum('fagl.TSL02').alias('2')\
                                                ,sum('fagl.TSL03').alias('3')\
                                                ,sum('fagl.TSL04').alias('4')\
                                                ,sum('fagl.TSL05').alias('5')\
                                                ,sum('fagl.TSL06').alias('6')\
                                                ,sum('fagl.TSL07').alias('7')\
                                                ,sum('fagl.TSL08').alias('8')\
                                                ,sum('fagl.TSL09').alias('9')\
                                                ,sum('fagl.TSL10').alias('10')\
                                                ,sum('fagl.TSL11').alias('11')\
                                                ,sum('fagl.TSL12').alias('12')\
                                                ,sum('fagl.TSL13').alias('13')\
                                                ,sum('fagl.TSL14').alias('14')\
                                                ,sum('fagl.TSL15').alias('15')\
                                                ,sum('fagl.TSL16').alias('16')\
                                               )
      fin_SAP_L0_TMP_GLBalance1.createOrReplaceTempView("fin_SAP_L0_TMP_GLBalance1")
      
    else :

         fin_SAP_L0_TMP_GLBalance = erp_GLT0.alias('glt0')\
                                          .join (erp_T001.alias('t001'),((col("t001.MANDT") == col("glt0.RCLNT"))\
                                                                                         & (col("t001.BUKRS") == col("glt0.BUKRS") )),"inner")\
                                          .filter(((col("glt0.RLDNR") == lit('00')) | (col('glt0.RLDNR') == lit('0'))) & (col('glt0.RRCTY') == lit('0')))\
                                          .groupBy(col('glt0.BUKRS').alias('companyCode'),col('glt0.RYEAR').alias('fiscalYear')\
                                                   ,col('glt0.RACCT').alias('accountNumber'),col('glt0.DRCRK').alias('debitCreditIndicator')\
                                                   ,col('glt0.RTCUR').alias('documentCurrency'),col('t001.WAERS').alias('localCurrency'))\
                                          .agg( sum('glt0.HSLVT').alias("beginningBalanceLC")\
                                                ,sum('glt0.TSLVT').alias("beginningBalanceDC")\
                                              ,sum('glt0.HSLVT').alias('-1')\
                                              ,sum('glt0.HSL01').alias('1')\
                                              ,sum('glt0.HSL02').alias('2')\
                                              ,sum('glt0.HSL03').alias('3')\
                                              ,sum('glt0.HSL04').alias('4')\
                                              ,sum('glt0.HSL05').alias('5')\
                                              ,sum('glt0.HSL06').alias('6')\
                                              ,sum('glt0.HSL07').alias('7')\
                                              ,sum('glt0.HSL08').alias('8')\
                                              ,sum('glt0.HSL09').alias('9')\
                                              ,sum('glt0.HSL10').alias('10')\
                                              ,sum('glt0.HSL11').alias('11')\
                                              ,sum('glt0.HSL12').alias('12')\
                                              ,sum('glt0.HSL13').alias('13')\
                                              ,sum('glt0.HSL14').alias('14')\
                                              ,sum('glt0.HSL15').alias('15')\
                                              ,sum('glt0.HSL16').alias('16')\
                                               )
         fin_SAP_L0_TMP_GLBalance.createOrReplaceTempView("fin_SAP_L0_TMP_GLBalance")

        
         fin_SAP_L0_TMP_GLBalance1 = erp_GLT0.alias('glt0')\
                                          .join (erp_T001.alias('t001'),((col("t001.MANDT") == col("glt0.RCLNT"))\
                                                                                         & (col("t001.BUKRS") == col("glt0.BUKRS") )),"inner")\
                                          .filter(((col("glt0.RLDNR") == lit('00')) | (col('glt0.RLDNR') == lit('0'))) & (col('glt0.RRCTY') == lit('0')))\
                                          .groupBy(col('glt0.BUKRS').alias('companyCode'),col('glt0.RYEAR').alias('fiscalYear')\
                                                   ,col('glt0.RACCT').alias('accountNumber'),col('glt0.DRCRK').alias('debitCreditIndicator')\
                                                   ,col('glt0.RTCUR').alias('documentCurrency'),col('t001.WAERS').alias('localCurrency'))\
                                          .agg( sum('glt0.HSLVT').alias("beginningBalanceLC")\
                                                ,sum('glt0.TSLVT').alias("beginningBalanceDC")\
                                              ,sum(when(col("glt0.TSLVT").isNull(), lit(0)).otherwise(col("glt0.TSLVT"))).alias('-1')\
                                              ,sum(when(col("glt0.TSL01").isNull(), lit(0)).otherwise(col("glt0.TSL01"))).alias('1')\
                                              ,sum(when(col("glt0.TSL02").isNull(), lit(0)).otherwise(col("glt0.TSL02"))).alias('2')\
                                              ,sum(when(col("glt0.TSL03").isNull(), lit(0)).otherwise(col("glt0.TSL03"))).alias('3')\
                                              ,sum(when(col("glt0.TSL04").isNull(), lit(0)).otherwise(col("glt0.TSL04"))).alias('4')\
                                              ,sum(when(col("glt0.TSL05").isNull(), lit(0)).otherwise(col("glt0.TSL05"))).alias('5')\
                                              ,sum(when(col("glt0.TSL06").isNull(), lit(0)).otherwise(col("glt0.TSL06"))).alias('6')\
                                              ,sum(when(col("glt0.TSL07").isNull(), lit(0)).otherwise(col("glt0.TSL07"))).alias('7')\
                                              ,sum(when(col("glt0.TSL08").isNull(), lit(0)).otherwise(col("glt0.TSL08"))).alias('8')\
                                              ,sum(when(col("glt0.TSL09").isNull(), lit(0)).otherwise(col("glt0.TSL09"))).alias('9')\
                                              ,sum(when(col("glt0.TSL10").isNull(), lit(0)).otherwise(col("glt0.TSL10"))).alias('10')\
                                              ,sum(when(col("glt0.TSL11").isNull(), lit(0)).otherwise(col("glt0.TSL11"))).alias('11')\
                                              ,sum(when(col("glt0.TSL12").isNull(), lit(0)).otherwise(col("glt0.TSL12"))).alias('12')\
                                              ,sum(when(col("glt0.TSL13").isNull(), lit(0)).otherwise(col("glt0.TSL13"))).alias('13')\
                                              ,sum(when(col("glt0.TSL14").isNull(), lit(0)).otherwise(col("glt0.TSL14"))).alias('14')\
                                              ,sum(when(col("glt0.TSL15").isNull(), lit(0)).otherwise(col("glt0.TSL15"))).alias('15')\
                                              ,sum(when(col("glt0.TSL16").isNull(), lit(0)).otherwise(col("glt0.TSL16"))).alias('16')\
                                               )
         fin_SAP_L0_TMP_GLBalance1.createOrReplaceTempView("fin_SAP_L0_TMP_GLBalance1")

          
    unpivotv1 = fin_SAP_L0_TMP_GLBalance.select("companyCode","fiscalYear","accountNumber","debitCreditIndicator","beginningBalanceLC",lit("0.00").alias("beginningBalanceDC"),\
                                                "documentCurrency","localCurrency",lit("0.00").alias("amountDC"),\
                     expr("stack(17,'-1',`-1`,'1',`1`,'2',`2`,'3',`3`,'4',`4`,'5',`5`,'6',`6`,'7',`7`,'8',`8`,'9',`9`,'10',`10`,'11',`11`,'12',`12`,'13',`13`,'14',`14`,\
                                                '15',`15`,'16',`16`) as (financialPeriod,amountLC)")).filter((col("amountLC") != "0.000000"))

    unpivotv1select = unpivotv1.select(col('companyCode'),col('fiscalYear'),col('financialPeriod'),col('accountNumber'),col('debitCreditIndicator'),col('beginningBalanceLC'),\
                                       col('beginningBalanceDC'),col('documentCurrency'),col('amountLC'),col('amountDC'),col('localCurrency'))
     
    unpivotv2 = fin_SAP_L0_TMP_GLBalance1.select("companyCode","fiscalYear","accountNumber","debitCreditIndicator",lit("0.00").alias("beginningBalanceLC"),"beginningBalanceDC",\
                                                 "documentCurrency","localCurrency",lit("0.00").alias("amountLC"),\
                     expr("stack(17,'-1',`-1`,'1',`1`,'2',`2`,'3',`3`,'4',`4`,'5',`5`,'6',`6`,'7',`7`,'8',`8`,'9',`9`,'10',`10`,'11',`11`,'12',`12`,'13',`13`,'14',`14`,\
                                                '15',`15`,'16',`16`) as (financialPeriod,amountDC)")).filter((col("amountDC") != "0.000000") )

    unpivotv2select = unpivotv2.select(col('companyCode'),col('fiscalYear'),col('financialPeriod'),col('accountNumber'),col('debitCreditIndicator'),col('beginningBalanceLC'),\
                                       col('beginningBalanceDC'),col('documentCurrency'),col('amountLC'),col('amountDC'),col('localCurrency'))
    
    unpivotv3 = fin_SAP_L0_TMP_GLBalance.select("companyCode","fiscalYear","accountNumber","debitCreditIndicator","beginningBalanceLC",lit("0.00").alias("beginningBalanceDC")\
                                                ,"documentCurrency","localCurrency",lit("0.00").alias("amountDC"),\
                     expr("stack(17,'-1',`-1`,'1',`1`,'2',`2`,'3',`3`,'4',`4`,'5',`5`,'6',`6`,'7',`7`,'8',`8`,'9',`9`,'10',`10`,'11',`11`,'12',`12`,'13',`13`,'14',`14`,\
                     '15',`15`,'16',`16`) as (financialPeriod,amountLC)")).filter((col("amountLC") == "0.000000")  & (col('financialPeriod') == '-1')  & (col('fiscalYear') == '2007') )

    unpivotv3select = unpivotv3.select(col('companyCode'),col('fiscalYear'),col('financialPeriod'),col('accountNumber'),col('debitCreditIndicator'),col('beginningBalanceLC')\
                                       ,col('beginningBalanceDC'),col('documentCurrency'),col('amountLC'),col('amountDC'),col('localCurrency'))
    
    
    unpivotv4 = fin_SAP_L0_TMP_GLBalance1.select("companyCode","fiscalYear","accountNumber","debitCreditIndicator",lit("0.00").alias("beginningBalanceLC"),"beginningBalanceDC",\
                                                 "documentCurrency","localCurrency",lit("0.00").alias("amountLC"),\
                     expr("stack(17,'-1',`-1`,'1',`1`,'2',`2`,'3',`3`,'4',`4`,'5',`5`,'6',`6`,'7',`7`,'8',`8`,'9',`9`,'10',`10`,'11',`11`,'12',`12`,'13',`13`,'14',`14`,\
                     '15',`15`,'16',`16`) as (financialPeriod,amountDC)")).filter((col("amountDC") == "0.000000") & (col('financialPeriod') == '-1')  & (col('fiscalYear') == '2007') )

    unpivotv4select = unpivotv4.select(col('companyCode'),col('fiscalYear'),col('financialPeriod'),col('accountNumber'),col('debitCreditIndicator'),col('beginningBalanceLC'),\
                                       col('beginningBalanceDC'),col('documentCurrency'),col('amountLC'),col('amountDC'),col('localCurrency'))
    
    
    dfs = [unpivotv1select,unpivotv2select,unpivotv3select,unpivotv4select]
    fin_SAP_L0_TMP_GLBalance_Unpivot = reduce(DataFrame.unionAll, dfs)

    fin_SAP_L0_TMP_GLBalance_Unpivot.createOrReplaceTempView("fin_SAP_L0_TMP_GLBalance_Unpivot")
    
    beginningBalanceLC = when(col("glb.financialPeriod")=='-1', col('glb.beginningBalanceLC')) \
                               .otherwise(lit('0'))

    beginningBalanceDC = when(col("glb.financialPeriod")=='-1', col('glb.beginningBalanceDC')) \
                               .otherwise(lit('0'))

    dr_wcnt= Window.partitionBy("companyCode","accountNumber","debitCreditIndicator","fiscalYear")\
            .orderBy("companyCode","accountNumber","debitCreditIndicator","fiscalYear",col("financialPeriod").cast('int'))

    fin_SAP_L0_TMP_GLBalance2  = fin_SAP_L0_TMP_GLBalance_Unpivot.alias('glb')\
                                          .groupBy(col('glb.companyCode').alias('companyCode'),col('glb.fiscalYear').alias('fiscalYear'),col('glb.financialPeriod').alias('financialPeriod'),\
                                                   col('glb.accountNumber').alias('accountNumber'),col('glb.debitCreditIndicator').alias('debitCreditIndicator'),\
                                                   col('glb.documentCurrency').alias('documentCurrency'),col('glb.localCurrency').alias('localCurrency'))\
                                          .agg( sum(lit(beginningBalanceLC)).alias('beginningBalanceLC'),\
                                                sum(lit(beginningBalanceDC)).alias('beginningBalanceDC'),sum(col('amountLC')).alias("endingBalanceCalulatedLC"),\
                                                sum(col('amountDC')).alias("endingBalanceCalulatedDC"),F.dense_rank().over(dr_wcnt).alias("ID"))
    fin_SAP_L0_TMP_GLBalance2.createOrReplaceTempView("fin_SAP_L0_TMP_GLBalance2")
    
    ID = col('ID') + 1

    fnperiod = when(col('glb2.financialPeriod') == '-1',lit(0))\
            .otherwise(col('glb2.financialPeriod'))

    sfjoin = fin_SAP_L0_TMP_GLBalance2.alias('glb2')\
         .select(col('companyCode'),col('fiscalYear'),(lit(fnperiod) +1).alias('financialPeriod'),\
                 col('accountNumber'),col('debitCreditIndicator'),col('documentCurrency'),\
                 col('localCurrency'),col('endingBalanceCalulatedLC'),col('endingBalanceCalulatedDC'),lit(ID).alias('ID'))
    
    
    beginingBalanceCalculatedLC = when(col('glb3.financialPeriod') == '-1',col('glb3.beginningBalanceLC'))\
            .otherwise(when(col('glb4.endingBalanceCalulatedLC').isNull(),lit(0))\
                       .otherwise(col('glb4.endingBalanceCalulatedLC')))

    beginingBalanceCalculatedDC = when(col('glb3.financialPeriod') == '-1',col('glb3.beginningBalanceDC'))\
            .otherwise(when(col('glb4.endingBalanceCalulatedDC').isNull(),lit(0))\
                       .otherwise(col('glb4.endingBalanceCalulatedDC')))


    amountLC = when(col('glb3.financialPeriod') == '-1',lit(0))\
                .otherwise((when(col('glb4.endingBalanceCalulatedLC').isNull(),lit(0))\
                           .otherwise(col('glb4.endingBalanceCalulatedLC'))) - (when(col('glb3.endingBalanceCalulatedLC').isNull(),lit(0))\
                       .otherwise(col('glb3.endingBalanceCalulatedLC'))))

    amountDC = when(col('glb3.financialPeriod') == '-1',lit(0))\
            .otherwise((when(col('glb4.endingBalanceCalulatedDC').isNull(),lit(0))\
                       .otherwise(col('glb4.endingBalanceCalulatedDC'))) - (when(col('glb3.endingBalanceCalulatedDC').isNull(),lit(0))\
                       .otherwise(col('glb3.endingBalanceCalulatedDC'))))


    fin_SAP_L0_TMP_GLBalanceFinal = fin_SAP_L0_TMP_GLBalance2.alias('glb3')\
         .join(sfjoin.alias('glb4'),((col("glb3.companyCode") == col("glb4.companyCode"))\
                                            & (col("glb3.fiscalYear") == col("glb4.fiscalYear")) & (col("glb3.accountNumber") == col("glb4.accountNumber"))\
                                            & (col("glb3.documentCurrency") == col("glb4.documentCurrency")) & (col("glb3.localCurrency") == col("glb4.localCurrency"))\
                                            & (col("glb3.debitCreditIndicator") == col("glb4.debitCreditIndicator")) & (col("glb3.ID") == col("glb4.ID"))),"left")\
         .select(col('glb3.companyCode'),col('glb3.fiscalYear'),col('glb3.accountNumber'),col('glb3.documentCurrency'),col('glb3.localCurrency'),col('glb3.debitCreditIndicator'),\
                col('glb3.financialPeriod'),lit(beginingBalanceCalculatedLC).alias('beginingBalanceCalculatedLC'),lit(beginingBalanceCalculatedDC).alias('beginingBalanceCalculatedDC'),\
                 lit(amountLC).alias('amountLC'),lit(amountDC).alias('amountDC'),col('glb3.endingBalanceCalulatedLC'),col('glb3.endingBalanceCalulatedDC').alias('endingBalanceCalulatedDC'))

    fin_SAP_L0_TMP_GLBalanceFinal.createOrReplaceTempView("fin_SAP_L0_TMP_GLBalanceFinal")
    
    
    financialPeriod = when(col('glBal.financialPeriod') == '-1',lit(0))\
            .otherwise(col('glBal.financialPeriod'))


    fin_L1_TD_GLBalance = fin_SAP_L0_TMP_GLBalanceFinal.alias('glBal')\
                       .join(knw_LK_CD_ReportingSetup.alias('rptSetup'),((col("glBal.companyCode") == col("rptSetup.companyCode"))),"inner")\
                       .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdtDrCr'),((col("bdtDrCr.businessDatatype") == lit("Debit Credit Indicator"))\
                                            & (col("bdtDrCr.sourceSystemValue") == col("glBal.debitCreditIndicator")) & (col("bdtDrCr.targetLanguageCode") == col("rptSetup.KPMGDataReportingLanguage"))\
                                            & (col("bdtDrCr.sourceERPSystemID") == lit(erpSAPSystemID))),"left")\
                       .select(col('glBal.companyCode').alias('companyCode'),col('glBal.fiscalYear').alias('fiscalYear'),lit(financialPeriod).alias("financialPeriod"),\
                              col('glBal.accountNumber').alias('accountNumber'),col('bdtDrCr.targetSystemValue').alias('debitCreditIndicator'),col('glBal.beginingBalanceCalculatedLC').alias('beginningBalanceLC'),\
                              col('glBal.beginingBalanceCalculatedDC').alias('beginningBalanceDC'),col('glBal.amountLC').alias('amountLC'),col('glBal.amountDC').alias('amountDC'),\
                              col('glBal.endingBalanceCalulatedLC').alias('endingBalanceLC'),col('glBal.endingBalanceCalulatedDC').alias('endingBalanceDC'),col('glBal.documentCurrency').alias('documentCurrency'),\
                              col('glBal.localCurrency').alias('localCurrency'),lit('').alias('segment01'),lit('').alias('segment02'),lit('').alias('segment03'),lit('').alias('segment04'),lit('').alias('segment05'))
    
    debitCreditIndicatorChange = objDataTransformation.gen_usf_DebitCreditIndicator_Change('GLAB',fin_L1_TD_GLBalance)

    fin_L1_TD_GLBalance = fin_L1_TD_GLBalance.select(col("companyCode")\
        ,col("fiscalYear")\
        ,col("financialPeriod")\
        ,col("accountNumber")\
        ,col("debitCreditIndicator")\
        ,col("beginningBalanceLC")\
        ,col("beginningBalanceDC")\
        ,col("amountLC")\
        ,col("amountDC")\
        ,when(((col('debitCreditIndicator') == "C")&(debitCreditIndicatorChange==lit(1))),(col('endingBalanceLC')* -1))\
        .   otherwise(col('endingBalanceLC')).alias("endingBalanceLC")\
        ,when(((col('debitCreditIndicator') == "C")&(debitCreditIndicatorChange==lit(1))),(col('endingBalanceDC')* -1))\
            .otherwise(col('endingBalanceDC')).alias("endingBalanceDC")\
        ,col("documentCurrency")\
        ,col("localCurrency")\
        ,col("segment01")\
        ,col("segment02")\
        ,col("segment03")\
        ,col("segment04")\
        ,col("segment05"))


    fin_L1_TD_GLBalance =  objDataTransformation.gen_convertToCDMandCache \
          (fin_L1_TD_GLBalance,'fin','L1_TD_GLBalance',targetPath=gl_CDMLayer1Path)
    
    executionStatus = "L1_TD_GLBalance populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
  
