# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import expr,min,sum,col,lit,when,lag
from pyspark.sql.window import Window

def fin_ORA_L1_TD_GLBalance_populate(): 
    try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
        global fin_L1_TD_GLBalance        
        finYear = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'FIN_YEAR')
        
        startPeriod= erp_GL_BALANCES.alias("a")\
            .join(gen_ORA_L0_STG_ClientCCIDAccount.alias("b"),\
                  expr("a.CODE_COMBINATION_ID=b.codeCombinationId"),"inner")\
           .join(fin_L0_STG_ClientBusinessStructure.alias("c"),\
                  expr("a.LEDGER_ID=c.ledger_Id AND\
                        b.balancingSegmentValue=c.balancing_Segment"),"inner")\
           .filter(expr("a.PERIOD_YEAR="+finYear))\
           .groupBy()\
           .agg(expr("min(PERIOD_NUM)").alias("minPeriod")).collect()[0].minPeriod
        
        glbal=erp_GL_BALANCES.alias("gb")\
           .join(erp_GL_CODE_COMBINATIONS.alias("gcc"),\
                 expr("gb.CODE_COMBINATION_ID = gcc.CODE_COMBINATION_ID AND gcc.SUMMARY_FLAG = 'N'"),"inner")\
           .join(gen_ORA_L0_STG_ClientCCIDAccount.alias("lsc"),\
                 expr("gcc.CODE_COMBINATION_ID = lsc.codeCombinationId"),"inner")\
           .join(erp_GL_LEDGERS.alias("gl"),\
                expr("gb.LEDGER_ID = gl.LEDGER_ID"),"inner")
        
        glbal_TransFlag_NullOrEmpty=glbal\
            .filter(expr("gb.ACTUAL_FLAG='A' AND gb.CURRENCY_CODE<>'STAT'\
                        AND (ISNULL(TRANSLATED_FLAG) OR TRANSLATED_FLAG='')\
                        AND gb.CURRENCY_CODE=gl.CURRENCY_CODE"))
        
        glbal_TransFlag_R=glbal\
            .filter(expr("gb.ACTUAL_FLAG='A' AND gb.CURRENCY_CODE<>'STAT'\
                        AND TRANSLATED_FLAG='R'\
                        AND gb.CURRENCY_CODE<>gl.CURRENCY_CODE"))
        strSelectExpr_D='''"gb.LEDGER_ID"\
                      ,"lsc.balancingSegmentValue"\
                      ,"gb.PERIOD_NAME"\
                      ,"gb.PERIOD_YEAR"\
                      ,"gb.PERIOD_NUM"\
                      ,"lsc.naturalAccountNumber"\
                      ,"gcc.CODE_COMBINATION_ID"\
                      ,"CAST(gb.BEGIN_BALANCE_DR_BEQ AS NUMERIC(32,6)) AS beginningBalanceLC"\
                      ,"CAST(gb.BEGIN_BALANCE_DR AS NUMERIC(32,6)) AS beginningBalanceDC"\
                      ,"0 AS amountLC"\
                      ,"0 AS amountDC	"\
                      ,"CAST(gb.BEGIN_BALANCE_DR_BEQ AS NUMERIC(32,6)) AS endingBalanceLC"\
                      ,"CAST(gb.BEGIN_BALANCE_DR AS NUMERIC(32,6)) AS endingBalanceDC"\
                      ,"gl.CURRENCY_CODE AS localCurrency"\
                      ,"gb.CURRENCY_CODE AS documentCurrency"'''
        strSelectExpr_C='''"gb.LEDGER_ID"\
                      ,"lsc.balancingSegmentValue"\
                      ,"gb.PERIOD_NAME"\
                      ,"gb.PERIOD_YEAR"\
                      ,"gb.PERIOD_NUM"\
                      ,"lsc.naturalAccountNumber"\
                      ,"gcc.CODE_COMBINATION_ID"\
                      ,"CAST(gb.BEGIN_BALANCE_CR_BEQ AS NUMERIC(32,6)) AS beginningBalanceLC"\
                      ,"CAST(gb.BEGIN_BALANCE_CR AS NUMERIC(32,6)) AS beginningBalanceDC"\
                      ,"0 AS amountLC"\
                      ,"0 AS amountDC	"\
                      ,"CAST(gb.BEGIN_BALANCE_CR_BEQ AS NUMERIC(32,6)) AS endingBalanceLC"\
                      ,"CAST(gb.BEGIN_BALANCE_CR AS NUMERIC(32,6)) AS endingBalanceDC"\
                      ,"gl.CURRENCY_CODE AS localCurrency"\
                      ,"gb.CURRENCY_CODE AS documentCurrency"'''
        strSelectExpr_D_Net='''"gb.LEDGER_ID"\
                      ,"lsc.balancingSegmentValue"\
                      ,"gb.PERIOD_NAME"\
                      ,"gb.PERIOD_YEAR"\
                      ,"gb.PERIOD_NUM"\
                      ,"lsc.naturalAccountNumber"\
                      ,"gcc.CODE_COMBINATION_ID"\
                      ,"CAST(gb.BEGIN_BALANCE_DR_BEQ AS NUMERIC(32,6)) AS beginningBalanceLC"\
                      ,"CAST(gb.BEGIN_BALANCE_DR AS NUMERIC(32,6)) AS beginningBalanceDC"\
                      ,"CAST(gb.PERIOD_NET_DR_BEQ AS NUMERIC(32,6)) AS endingBalanceLC"\
                      ,"CAST(gb.PERIOD_NET_DR AS NUMERIC(32,6)) AS endingBalanceDC"\
                      ,"gl.CURRENCY_CODE AS localCurrency"\
                      ,"gb.CURRENCY_CODE AS documentCurrency"'''
        strSelectExpr_C_Net='''"gb.LEDGER_ID"\
                      ,"lsc.balancingSegmentValue"\
                      ,"gb.PERIOD_NAME"\
                      ,"gb.PERIOD_YEAR"\
                      ,"gb.PERIOD_NUM"\
                      ,"lsc.naturalAccountNumber"\
                      ,"gcc.CODE_COMBINATION_ID"\
                      ,"CAST(gb.BEGIN_BALANCE_CR_BEQ AS NUMERIC(32,6)) AS beginningBalanceLC"\
                      ,"CAST(gb.BEGIN_BALANCE_CR AS NUMERIC(32,6)) AS beginningBalanceDC"\
                      ,"CAST(gb.PERIOD_NET_CR_BEQ AS NUMERIC(32,6)) AS endingBalanceLC"\
                      ,"CAST(gb.PERIOD_NET_CR AS NUMERIC(32,6)) AS endingBalanceDC"\
                      ,"gl.CURRENCY_CODE AS localCurrency"\
                      ,"gb.CURRENCY_CODE AS documentCurrency"'''
        glbal_D_1=eval(f"glbal_TransFlag_NullOrEmpty.selectExpr({strSelectExpr_D})")
        glbal_D_2=eval(f"glbal_TransFlag_R.selectExpr({strSelectExpr_D})")
        glbal_C_1=eval(f"glbal_TransFlag_NullOrEmpty.selectExpr({strSelectExpr_C})")
        glbal_C_2=eval(f"glbal_TransFlag_R.selectExpr({strSelectExpr_C})")
        glbal_D_Net_1=eval(f"glbal_TransFlag_NullOrEmpty.filter(expr('CAST(gb.PERIOD_NET_DR AS NUMERIC(32,6))<>0')).selectExpr({strSelectExpr_D_Net})")
        glbal_D_Net_2=eval(f"glbal_TransFlag_R.filter(expr('CAST(gb.PERIOD_NET_DR_BEQ AS NUMERIC(32,6))<>0')).selectExpr({strSelectExpr_D_Net})")
        glbal_C_Net_1=eval(f"glbal_TransFlag_NullOrEmpty.filter(expr('CAST(gb.PERIOD_NET_CR AS NUMERIC(32,6))<>0')).selectExpr({strSelectExpr_C_Net})")
        glbal_C_Net_2=eval(f"glbal_TransFlag_R.filter(expr('CAST(gb.PERIOD_NET_CR_BEQ AS NUMERIC(32,6))<>0')).selectExpr({strSelectExpr_C_Net})")
        
        a=glbal_D_1.union(glbal_D_2).distinct()
        fin_L1_TD_GLBalance_Insert1=a.alias("a").join(knw_LK_CD_ReportingSetup.alias("rep"),\
           expr("rep.ledgerID=a.ledger_Id AND rep.balancingSegment = a.balancingSegmentValue"),"inner")\
           .filter(col("PERIOD_NUM")==lit(startPeriod))\
           .groupBy("rep.companyCode","PERIOD_YEAR","PERIOD_NUM","naturalAccountNumber","localCurrency","documentCurrency")\
           .agg(sum(col("beginningBalanceLC")).alias("beginningBalanceLC")\
                ,sum(col("beginningBalanceDC")).alias("beginningBalanceDC")\
               ,sum(col("amountLC")).alias("amountLC")\
               ,sum(col("amountDC")).alias("amountDC")\
               ,sum(col("endingBalanceLC")).alias("endingBalanceLC")\
               ,sum(col("endingBalanceDC")).alias("endingBalanceDC"))\
            .selectExpr("rep.companyCode		    as companyCode"\
                        ,"PERIOD_YEAR				as fiscalYear"\
                        ,"0						as financialPeriod"\
                        ,"naturalAccountNumber	as accountNumber"\
                        ,"'D'						as debitCreditIndicator"\
                        ,"beginningBalanceLC     as beginningBalanceLC"\
                        ,"beginningBalanceDC     as beginningBalanceDC"\
                        ,"amountLC     as amountLC"\
                        ,"amountDC     as amountDC"\
                        ,"endingBalanceLC     as endingBalanceLC"\
                        ,"endingBalanceDC     as endingBalanceDC"\
                        ,"localCurrency			as localCurrency"\
                        ,"documentCurrency		as documentCurrency")
        
        a=glbal_C_1.union(glbal_C_2).distinct()
        fin_L1_TD_GLBalance_Insert2=a.alias("a").join(knw_LK_CD_ReportingSetup.alias("rep"),\
           expr("rep.ledgerID=a.ledger_Id AND rep.balancingSegment = a.balancingSegmentValue"),"inner")\
           .filter(col("PERIOD_NUM")==lit(startPeriod))\
           .groupBy("rep.companyCode","PERIOD_YEAR","PERIOD_NUM","naturalAccountNumber","localCurrency","documentCurrency")\
           .agg(sum(col("beginningBalanceLC")).alias("beginningBalanceLC")\
                ,sum(col("beginningBalanceDC")).alias("beginningBalanceDC")\
               ,sum(col("amountLC")).alias("amountLC")\
               ,sum(col("amountDC")).alias("amountDC")\
               ,sum(col("endingBalanceLC")).alias("endingBalanceLC")\
               ,sum(col("endingBalanceDC")).alias("endingBalanceDC"))\
            .selectExpr("rep.companyCode		    as companyCode"\
                        ,"PERIOD_YEAR				as fiscalYear"\
                        ,"0						as financialPeriod"\
                        ,"naturalAccountNumber	as accountNumber"\
                        ,"'C'						as debitCreditIndicator"\
                        ,"beginningBalanceLC     as beginningBalanceLC"\
                        ,"beginningBalanceDC     as beginningBalanceDC"\
                        ,"amountLC     as amountLC"\
                        ,"amountDC     as amountDC"\
                        ,"endingBalanceLC     as endingBalanceLC"\
                        ,"endingBalanceDC     as endingBalanceDC"\
                        ,"localCurrency			as localCurrency"\
                        ,"documentCurrency		as documentCurrency")
        fin_L1_TD_GLBalance=fin_L1_TD_GLBalance_Insert1.union(fin_L1_TD_GLBalance_Insert2)
        
        
        a=glbal_D_Net_1.union(glbal_D_Net_2).distinct()
        fin_L1_TD_GLBalance_Insert3=a.alias("a").join(knw_LK_CD_ReportingSetup.alias("rep"),\
           expr("rep.ledgerID=a.ledger_Id AND rep.balancingSegment = a.balancingSegmentValue"),"inner")\
           .groupBy("rep.companyCode","PERIOD_YEAR","PERIOD_NUM","naturalAccountNumber","localCurrency","documentCurrency")\
           .agg(sum(col("beginningBalanceLC")).alias("beginningBalanceLC")\
                ,sum(col("beginningBalanceDC")).alias("beginningBalanceDC")\
               ,(sum(col("beginningBalanceLC"))-sum(col("endingBalanceLC"))).alias("amountLC")\
               ,(sum(col("beginningBalanceDC"))-sum(col("endingBalanceDC"))).alias("amountDC")\
               ,sum(col("endingBalanceLC")).alias("endingBalanceLC")\
               ,sum(col("endingBalanceDC")).alias("endingBalanceDC"))\
            .selectExpr("rep.companyCode		    as companyCode"\
                        ,"PERIOD_YEAR				as fiscalYear"\
                        ,"PERIOD_NUM						as financialPeriod"\
                        ,"naturalAccountNumber	as accountNumber"\
                        ,"'D'						as debitCreditIndicator"\
                        ,"beginningBalanceLC     as beginningBalanceLC"\
                        ,"beginningBalanceDC     as beginningBalanceDC"\
                        ,"amountLC     as amountLC"\
                        ,"amountDC     as amountDC"\
                        ,"endingBalanceLC     as endingBalanceLC"\
                        ,"endingBalanceDC     as endingBalanceDC"\
                        ,"localCurrency			as localCurrency"\
                        ,"documentCurrency		as documentCurrency")
        fin_L1_TD_GLBalance=fin_L1_TD_GLBalance.union(fin_L1_TD_GLBalance_Insert3)
        
        a=glbal_C_Net_1.union(glbal_C_Net_2).distinct()
        fin_L1_TD_GLBalance_Insert4=a.alias("a").join(knw_LK_CD_ReportingSetup.alias("rep"),\
           expr("rep.ledgerID=a.ledger_Id AND rep.balancingSegment = a.balancingSegmentValue"),"inner")\
           .groupBy("rep.companyCode","PERIOD_YEAR","PERIOD_NUM","naturalAccountNumber","localCurrency","documentCurrency")\
           .agg(sum(col("beginningBalanceLC")).alias("beginningBalanceLC")\
                ,sum(col("beginningBalanceDC")).alias("beginningBalanceDC")\
               ,(sum(col("beginningBalanceLC"))-sum(col("endingBalanceLC"))).alias("amountLC")\
               ,(sum(col("beginningBalanceDC"))-sum(col("endingBalanceDC"))).alias("amountDC")\
               ,sum(col("endingBalanceLC")).alias("endingBalanceLC")\
               ,sum(col("endingBalanceDC")).alias("endingBalanceDC"))\
            .selectExpr("rep.companyCode		    as companyCode"\
                        ,"PERIOD_YEAR				as fiscalYear"\
                        ,"PERIOD_NUM						as financialPeriod"\
                        ,"naturalAccountNumber	as accountNumber"\
                        ,"'C'						as debitCreditIndicator"\
                        ,"beginningBalanceLC     as beginningBalanceLC"\
                        ,"beginningBalanceDC     as beginningBalanceDC"\
                        ,"amountLC     as amountLC"\
                        ,"amountDC     as amountDC"\
                        ,"endingBalanceLC     as endingBalanceLC"\
                        ,"endingBalanceDC     as endingBalanceDC"\
                        ,"localCurrency			as localCurrency"\
                        ,"documentCurrency		as documentCurrency")
        fin_L1_TD_GLBalance=fin_L1_TD_GLBalance.union(fin_L1_TD_GLBalance_Insert4)
        
        windowSpec=Window.partitionBy("companyCode","accountNumber","fiscalYear","debitCreditIndicator","documentCurrency").orderBy("financialPeriod")
        LagEndingBalanceLC=\
         when(col("financialPeriod")==0,\
             col("beginningBalanceLC"))\
        .otherwise(when(lag("endingBalanceLC",1).over(windowSpec).isNull(),0)\
                  .otherwise(lag("endingBalanceLC",1).over(windowSpec)))
        
        LagEndingBalanceDC=\
        when(col("financialPeriod")==0,\
             col("beginningBalanceDC"))\
        .otherwise(when(lag("endingBalanceDC",1).over(windowSpec).isNull(),0)\
                  .otherwise(lag("endingBalanceDC",1).over(windowSpec)))
        		  
        fin_L1_TD_GLBalance=fin_L1_TD_GLBalance\
        .withColumn("beginningBalanceLC",lit(LagEndingBalanceLC))\
        .withColumn("beginningBalanceDC",lit(LagEndingBalanceDC))
        
        fin_gldc1SumEndingBalanceLC=fin_L1_TD_GLBalance\
        .filter(expr("documentCurrency = localCurrency")).alias("gldc2")\
        .join(fin_L1_TD_GLBalance.alias("gldc1"),\
         (col("gldc2.companyCode")==col("gldc1.companyCode"))\
        &(col("gldc2.accountNumber")==col("gldc1.accountNumber"))\
        &(col("gldc2.fiscalYear")==col("gldc1.fiscalYear"))\
        &(col("gldc2.debitCreditIndicator")==col("gldc1.debitCreditIndicator"))\
        &(col("gldc2.documentCurrency")!=col("gldc1.documentCurrency"))\
        &(col("gldc2.financialPeriod")==col("gldc1.financialPeriod")),"inner")\
        .groupBy("gldc2.companyCode"\
        		,"gldc2.accountNumber"\
        		,"gldc2.fiscalYear"\
        		,"gldc2.debitCreditIndicator"\
        		,"gldc2.documentCurrency"\
        		,"gldc2.financialPeriod")\
        .agg(expr("sum(gldc1.endingBalanceLC)").alias("SumEndingBalanceLC"))\
        .selectExpr("gldc2.companyCode"\
              ,"gldc2.accountNumber"\
              ,"gldc2.fiscalYear"\
              ,"gldc2.debitCreditIndicator"\
              ,"gldc2.documentCurrency"\
              ,"gldc2.financialPeriod"\
        	  ,"SumEndingBalanceLC")
        fin_L1_TD_GLBalance=fin_L1_TD_GLBalance.alias("gldc2")\
        .join(fin_gldc1SumEndingBalanceLC.alias("gldc1"),\
        (expr("gldc2.companyCode	  = gldc1.companyCode \
        AND gldc2.accountNumber		  = gldc1.accountNumber \
        AND gldc2.fiscalYear		  = gldc1.fiscalYear \
        AND gldc2.debitCreditIndicator= gldc1.debitCreditIndicator \
        AND gldc2.documentCurrency	  = gldc1.documentCurrency \
        AND gldc2.financialPeriod	  = gldc1.financialPeriod")),"left")\
        .selectExpr( "gldc2.*"\
        			,"case when gldc2.documentCurrency = gldc2.localCurrency\
        			       then case when isnull(gldc1.SumEndingBalanceLC) then 0\
                      				 else gldc1.SumEndingBalanceLC end\
        				   else  gldc2.endingBalanceLC end as endingBalanceLCNew")
        fin_L1_TD_GLBalance=fin_L1_TD_GLBalance.withColumn("endingBalanceLC",\
                                      expr("case when documentCurrency = localCurrency \
                                                 then endingBalanceDC-endingBalanceLCNew \
                                                 else endingBalanceLC end"))\
                           .drop("endingBalanceLCNew")

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
        ,col("localCurrency")\
        ,col("documentCurrency"))
        
        fin_L1_TD_GLBalance =  objDataTransformation.gen_convertToCDMandCache \
         (fin_L1_TD_GLBalance,'fin','L1_TD_GLBalance',targetPath=gl_CDMLayer1Path)
        
        executionStatus = "L1_TD_GLBalance populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
