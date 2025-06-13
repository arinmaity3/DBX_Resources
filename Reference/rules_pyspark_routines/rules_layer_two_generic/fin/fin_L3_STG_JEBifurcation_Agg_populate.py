# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType
import sys
import traceback
def fin_L3_STG_JEBifurcation_Agg_populate():
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    
    fin_L2_FACT_GLBifurcation_Agg=spark.sql("""
        SELECT
            t33.organizationUnitSurrogateKey
            ,t33.fixedGLAccountSurrogateKey
            ,t33.relatedGLAccountSurrogateKey
            ,t33.mainOffsetGLAccountSurrogateKey
            ,t33.postingDateSurrogateKey
            ,t33.documentClassificationSurrogateKey
            ,t33.documentNumberWithCompanyCode
            ,t33.footPrintSurrogateKey
            ,t33.glAccountCombinationSurrogateKey
            ,t35.transactionNo
            ,t32.isMultiBPViewAssignment
            ,t32.fixedDebitCreditTypeID
            ,t34.documentNumber
            ,COUNT(*) AS Count
            ,Sum(t33.amountCr) as amountCR
            ,Sum(t33.amountDr) as amountDR
            ,Sum(mainOffsetAmount) as mainOffsetAmount
            ,Sum(amountDrCr) as amountDrCr
            ,Sum(amountDr + amountCr) as amountDRCRTotal
            ,finPer.financialPeriod
            -- SELECT tOP 10 * 
        FROM
        fin_L2_DIM_JEBifurcation_Base AS t32

        INNER JOIN
        (
            SELECT
                organizationUnitSurrogateKey
                ,fixedGLAccountSurrogateKey
                ,relatedGLAccountSurrogateKey
                ,mainOffsetGLAccountSurrogateKey
                ,postingDateSurrogateKey
                ,documentClassificationSurrogateKey
                ,glAccountCombinationSurrogateKey
                ,documentNumberWithCompanyCode
                ,footPrintSurrogateKey
                ,transactionLinkID
                ,exceptLineItemLinkID
                ,detailLinkID        
                ,amountCR
                ,amountDR
                ,mainOffsetAmount
                ,amountDrCr
            from fin_L2_FACT_GLBifurcation_Raw
            WHERE glJEBifurcationId > 0
            --AND x.filepath(1) = XXXXXXXXXX

        ) AS t33 
        on t32.exceptLineItemLinkID = t33.exceptLineItemLinkID

        INNER JOIN
        (
            SELECT
                 detailLinkID
                ,documentNumber
            FROM 
            fin_L2_DIM_DocumentDetail_Complete            
        ) AS t34
        ON t33.detailLinkID= t34.detailLinkID

        INNER JOIN
        (
            SELECT
                 transactionLinkID                 
                ,transactionNo
            FROM fin_L2_DIM_JEBifurcation_Transaction_Base 
        ) AS t35 
        ON t33.transactionLinkID= t35.transactionLinkID

        INNER JOIN fin_L2_DIM_FinancialPeriod as finPer 
        on t33.postingDateSurrogateKey = finPer.financialPeriodSurrogateKey

        GROUP BY
            t33.organizationUnitSurrogateKey
            ,t33.fixedGLAccountSurrogateKey
            ,t33.relatedGLAccountSurrogateKey
            ,t33.mainOffsetGLAccountSurrogateKey
            ,t33.postingDateSurrogateKey
            ,finPer.financialPeriod
            ,t33.documentClassificationSurrogateKey
            ,t33.documentNumberWithCompanyCode
            ,t33.footPrintSurrogateKey
            ,t33.glAccountCombinationSurrogateKey
            ,t35.transactionNo
            ,t32.isMultiBPViewAssignment
            ,t32.fixedDebitCreditTypeID
            ,t34.documentNumber
        """)    
    
    fin_L2_FACT_GLBifurcation_Agg = objDataTransformation.gen_convertToCDMStructure_generate(fin_L2_FACT_GLBifurcation_Agg,\
				'fin','L2_FACT_GLBifurcation_Agg',True)[0]
    
    objGenHelper.gen_writeToFile_perfom(fin_L2_FACT_GLBifurcation_Agg,gl_CDMLayer2Path + "fin_L2_FACT_GLBifurcation_Agg.parquet" \
                                       ,isCreatePartitions = True,partitionColumns = "financialPeriod")
    
    executionStatus = "fin_L2_FACT_GLBifurcation_Agg populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as e:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus) 
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

