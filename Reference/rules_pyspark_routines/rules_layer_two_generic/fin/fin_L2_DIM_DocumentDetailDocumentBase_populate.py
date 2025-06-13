# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number,floor,trim
from dateutil.parser import parse

class fin_L2_DIM_DocumentDetailDocumentBase_populate():
    @staticmethod
 
    def dwhvwDIMDocumentDetailDocumentBase_populate():
        try:
            logID = executionLog.init(processID = PROCESS_ID.L2_TRANSFORMATION,className = __class__.__name__)
            objGenHelper = gen_genericHelper()
            objDataTransformation = gen_dataTransformation()
            global fin_L2_DIM_DocumentDetail_DocumentBase
            global dwh_vw_DIM_DocumentDetail_DocumentBase
            
            fin_L2_DIM_DocumentDetail_DocumentBase = fin_L2_FACT_Journal.alias('jrnl') \
                   .join(gen_L2_DIM_Currency.alias('cur'),\
                   (col('jrnl.documentCurrencySurrogateKey').eqNullSafe(col('cur.currencySurrogateKey'))),how='left')\
                   .select(col('cur.currencySurrogateKey').alias('currencySurrogateKey')\
                   ,col('cur.currencyISOCode').alias('documentCurrency')\
                   ,col('jrnl.documentReversalFiscalYear').alias('documentReversalFiscalYear')\
                   ,col('jrnl.consistentEndingNumberOfDigit').alias('consistentEndingNumberOfDigit')\
                   ,col('jrnl.transactionType').alias('transactionType')\
                   ,col('jrnl.transactionTypeDescription').alias('transactionTypeDescription')\
                   ,col('jrnl.firstDigitOfAmount').alias('firstDigitOfAmount')\
                   ,col('jrnl.postPeriodDays').alias('postPeriodDays')\
                   ,col('jrnl.reversalID').alias('reversalID')\
                   ).distinct()
            
            w = Window().orderBy(lit(''))
            fin_L2_DIM_DocumentDetail_DocumentBase = fin_L2_DIM_DocumentDetail_DocumentBase.withColumn( "documentBaseID" , row_number().over(w))
           
            fin_L2_DIM_DocumentDetail_DocumentBase  = objDataTransformation.gen_convertToCDMandCache \
                (fin_L2_DIM_DocumentDetail_DocumentBase,'fin','L2_DIM_DocumentDetail_DocumentBase',False)
            
            dwh_vw_DIM_DocumentDetail_DocumentBase = objDataTransformation.gen_convertToCDMStructure_generate \
                (fin_L2_DIM_DocumentDetail_DocumentBase,'dwh','vw_DIM_DocumentDetail_DocumentBase',True)[0]
            keysAndValues = {'documentBaseID':-1,'currencySurrogateKey':0,'documentCurrency':'NONE',\
                'documentReversalFiscalYear':'NONE','consistentEndingNumberOfDigit':0,'transactionType':'NONE',\
                'transactionTypeDescription':'NONE','firstDigitOfAmount':0,'postPeriodDays':0,'reversalID':0}
            dwh_vw_DIM_DocumentDetail_DocumentBase = objGenHelper.gen_placeHolderRecords_populate \
                ('dwh.vw_DIM_DocumentDetail_DocumentBase',keysAndValues,dwh_vw_DIM_DocumentDetail_DocumentBase)

            dwh_vw_DIM_DocumentDetail_DocumentBase  = objDataTransformation.gen_convertToCDMandCache \
                (dwh_vw_DIM_DocumentDetail_DocumentBase,'dwh','vw_DIM_DocumentDetail_DocumentBase',False)

            objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_DocumentDetail_DocumentBase,gl_CDMLayer2Path \
                + "fin_L2_DIM_DocumentDetail_DocumentBase.parquet" )
        
            executionStatus = "fin_L2_DIM_DocumentDetail_DocumentBase populated sucessfully"
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    @staticmethod
    def dwhvwDIMDocumentDetailDocumentCore_populate():
        try:
            logID = executionLog.init(processID = PROCESS_ID.L2_TRANSFORMATION,className = __class__.__name__)
            objGenHelper = gen_genericHelper()
            objDataTransformation = gen_dataTransformation()
            global fin_L2_DIM_DocumentDetail_DocumentCore
            global dwh_vw_DIM_DocumentDetail_DocumentCore
            
            fin_L2_DIM_DocumentDetail_DocumentCore = fin_L2_FACT_Journal.alias('jrnl') \
                                      .select(col('jrnl.documentNumber').alias('documentNumber')\
                                      ,col('jrnl.reverseDocumentNumber').alias('reverseDocumentNumber')\
                                      ,col('jrnl.referenceDocumentNumber').alias('referenceDocumentNumber')\
                                      ,col('jrnl.referenceSubledgerDocumentNumber').alias('referenceSubledgerDocumentNumber')\
                                      ).distinct()
            
            w = Window().orderBy(lit(''))
            fin_L2_DIM_DocumentDetail_DocumentCore = fin_L2_DIM_DocumentDetail_DocumentCore.withColumn( "documentCoreID" , row_number().over(w))
            
            fin_L2_DIM_DocumentDetail_DocumentCore  = objDataTransformation.gen_convertToCDMandCache \
                (fin_L2_DIM_DocumentDetail_DocumentCore,'fin','L2_DIM_DocumentDetail_DocumentCore',False)
            
            dwh_vw_DIM_DocumentDetail_DocumentCore = objDataTransformation.gen_convertToCDMStructure_generate \
                (fin_L2_DIM_DocumentDetail_DocumentCore,'dwh','vw_DIM_DocumentDetail_DocumentCore',True)[0]
            keysAndValues = {'documentCoreID':-1,'documentNumber':'NONE','reverseDocumentNumber':'NONE',\
                'referenceDocumentNumber':'NONE','referenceSubledgerDocumentNumber':'NONE' }
            dwh_vw_DIM_DocumentDetail_DocumentCore = objGenHelper.gen_placeHolderRecords_populate \
                ('dwh.vw_DIM_DocumentDetail_DocumentCore',keysAndValues,dwh_vw_DIM_DocumentDetail_DocumentCore)

            dwh_vw_DIM_DocumentDetail_DocumentCore  = objDataTransformation.gen_convertToCDMandCache \
                (dwh_vw_DIM_DocumentDetail_DocumentCore,'dwh','vw_DIM_DocumentDetail_DocumentCore',False)

            objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_DocumentDetail_DocumentCore,gl_CDMLayer2Path \
                + "fin_L2_DIM_DocumentDetail_DocumentCore.parquet" )
            
            executionStatus = "fin_L2_DIM_DocumentDetail_DocumentCore populated sucessfully"
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    @staticmethod
    def dwhvwDIMDocumentDetailDocumentLineItem_populate():
        try:
            logID = executionLog.init(processID = PROCESS_ID.L2_TRANSFORMATION,className = __class__.__name__)
            objGenHelper = gen_genericHelper()
            objDataTransformation = gen_dataTransformation()
            global fin_L2_DIM_DocumentDetail_DocumentLineItem
            global dwh_vw_DIM_DocumentDetailDocumentLineItem

            fin_L2_DIM_DocumentDetail_DocumentLineItem = fin_L2_FACT_Journal.alias('jrnl') \
                                      .select(col('jrnl.documentLineNumber').alias('documentLineNumber')\
                                      ,col('jrnl.documentLineNumberSort').alias('documentLineNumberSort')\
                                      ).distinct()
            
            w = Window().orderBy(lit(''))
            fin_L2_DIM_DocumentDetail_DocumentLineItem = fin_L2_DIM_DocumentDetail_DocumentLineItem.withColumn( "documentLineItemID" , row_number().over(w))
            
            fin_L2_DIM_DocumentDetail_DocumentLineItem  = objDataTransformation.gen_convertToCDMandCache \
                (fin_L2_DIM_DocumentDetail_DocumentLineItem,'fin','L2_DIM_DocumentDetail_DocumentLineItem',False)
            
            dwh_vw_DIM_DocumentDetailDocumentLineItem = objDataTransformation.gen_convertToCDMStructure_generate \
                (fin_L2_DIM_DocumentDetail_DocumentLineItem,'dwh','vw_DIM_DocumentDetail_DocumentLineItem',True)[0]
            keysAndValues = {'documentLineItemID':-1,'documentLineNumber':'NONE','documentLineNumberSort':'NONE'}
            dwh_vw_DIM_DocumentDetailDocumentLineItem = objGenHelper.gen_placeHolderRecords_populate \
                ('dwh.vw_DIM_DocumentDetail_DocumentLineItem',keysAndValues,dwh_vw_DIM_DocumentDetailDocumentLineItem)

            dwh_vw_DIM_DocumentDetailDocumentLineItem  = objDataTransformation.gen_convertToCDMandCache \
                (dwh_vw_DIM_DocumentDetailDocumentLineItem,'dwh','vw_DIM_DocumentDetail_DocumentLineItem',False)
            
            objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_DocumentDetailDocumentLineItem,gl_CDMLayer2Path \
                + "fin_L2_DIM_DocumentDetail_DocumentLineItem.parquet" )
            
            executionStatus = "fin_L2_DIM_DocumentDetail_DocumentLineItem populated sucessfully"
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    @staticmethod
    def dwhvwDIMDocumentDetailDocumentDetail_populate():
        try:
            logID = executionLog.init(processID = PROCESS_ID.L2_TRANSFORMATION,className = __class__.__name__)
            objGenHelper = gen_genericHelper()
            objDataTransformation = gen_dataTransformation()
            global fin_L2_DIM_DocumentDetail_DocumentDetail
            global dwh_vw_DIM_DocumentDetail_DocumentDetail
            
            fin_L2_DIM_DocumentDetail_DocumentDetail = fin_L2_FACT_Journal.alias('jrnl') \
                                      .select(col('jrnl.assignmentNumber').alias('assignmentNumber')\
                                      ,col('jrnl.clearingDocumentNumber').alias('clearingDocumentNumber')\
                                      ,col('jrnl.customDate').alias('customDate')\
                                      ).distinct()
            
            w = Window().orderBy(lit(''))
            fin_L2_DIM_DocumentDetail_DocumentDetail = fin_L2_DIM_DocumentDetail_DocumentDetail.withColumn( "DocumentDetailID" , row_number().over(w))
            
            fin_L2_DIM_DocumentDetail_DocumentDetail  = objDataTransformation.gen_convertToCDMandCache \
                (fin_L2_DIM_DocumentDetail_DocumentDetail,'fin','L2_DIM_DocumentDetail_DocumentDetail',False)
            
            defaultDate='1901-01-01'
            defaultDate = parse(defaultDate).date()
            
            dwh_vw_DIM_DocumentDetail_DocumentDetail = objDataTransformation.gen_convertToCDMStructure_generate \
                (fin_L2_DIM_DocumentDetail_DocumentDetail,'dwh','vw_DIM_DocumentDetail_DocumentDetail',isIncludeAnalysisID=True,isSqlFormat=True)[0]
            keysAndValues = {'documentDetailID':-1,'assignmentNumber':'NONE','clearingDocumentNumber':'NONE','customDate':defaultDate}
            dwh_vw_DIM_DocumentDetail_DocumentDetail = objGenHelper.gen_placeHolderRecords_populate \
                ('dwh.vw_DIM_DocumentDetail_DocumentDetail',keysAndValues,dwh_vw_DIM_DocumentDetail_DocumentDetail)

            dwh_vw_DIM_DocumentDetail_DocumentDetail  = objDataTransformation.gen_convertToCDMandCache \
                (dwh_vw_DIM_DocumentDetail_DocumentDetail,'dwh','vw_DIM_DocumentDetail_DocumentDetail',False)
            
            objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_DocumentDetail_DocumentDetail,gl_CDMLayer2Path \
                + "fin_L2_DIM_DocumentDetail_DocumentDetail.parquet" )

            executionStatus = "fin_L2_DIM_DocumentDetail_DocumentDetail populated sucessfully"
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    @staticmethod
    def dwhvwDIMDocumentDetailDocumentDescription_populate():
        try:
            logID = executionLog.init(processID = PROCESS_ID.L2_TRANSFORMATION,className = __class__.__name__)
            objGenHelper = gen_genericHelper()
            objDataTransformation = gen_dataTransformation()
            global fin_L2_DIM_DocumentDetail_DocumentDescription
            global dwh_vw_DIM_DocumentDetail_DocumentDescription
            
            fin_L2_DIM_DocumentDetail_DocumentDescription = fin_L2_FACT_Journal.alias('jrnl') \
                                      .select(col('jrnl.documentHeaderDescription').alias('documentHeaderDescription')\
                                      ).distinct()
            
            w = Window().orderBy(lit(''))
            fin_L2_DIM_DocumentDetail_DocumentDescription = fin_L2_DIM_DocumentDetail_DocumentDescription.withColumn( "documentDescriptionID" , row_number().over(w))
            
            fin_L2_DIM_DocumentDetail_DocumentDescription  = objDataTransformation.gen_convertToCDMandCache \
                (fin_L2_DIM_DocumentDetail_DocumentDescription,'fin','L2_DIM_DocumentDetail_DocumentDescription',False)
            
            dwh_vw_DIM_DocumentDetail_DocumentDescription = objDataTransformation.gen_convertToCDMStructure_generate \
                (fin_L2_DIM_DocumentDetail_DocumentDescription,'dwh','vw_DIM_DocumentDetail_DocumentDescription',True)[0]
            keysAndValues = {'documentDescriptionID':-1,'documentHeaderDescription':'NONE'}
            dwh_vw_DIM_DocumentDetail_DocumentDescription = objGenHelper.gen_placeHolderRecords_populate \
                ('dwh.vw_DIM_DocumentDetail_DocumentDescription',keysAndValues,dwh_vw_DIM_DocumentDetail_DocumentDescription)

            dwh_vw_DIM_DocumentDetail_DocumentDescription  = objDataTransformation.gen_convertToCDMandCache \
                (dwh_vw_DIM_DocumentDetail_DocumentDescription,'dwh','vw_DIM_DocumentDetail_DocumentDescription',False)
            
            objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_DocumentDetail_DocumentDescription,gl_CDMLayer2Path \
                + "fin_L2_DIM_DocumentDetail_DocumentDescription.parquet" )

            executionStatus = "fin_L2_DIM_DocumentDetail_DocumentDescription populated sucessfully"
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    @staticmethod
    def dwhvwDIMDocumentDetailDocumentLineItemDescription_populate():
        try:
            logID = executionLog.init(processID = PROCESS_ID.L2_TRANSFORMATION,className = __class__.__name__)
            objGenHelper = gen_genericHelper()
            objDataTransformation = gen_dataTransformation()
            global fin_L2_DIM_DocumentDetail_DocumentLineItemDescription
            global dwh_vw_DIM_DocumentDetail_DocumentLineItemDescription
            
            fin_L2_DIM_DocumentDetail_DocumentLineItemDescription = fin_L2_FACT_Journal.alias('jrnl') \
                                      .select(col('jrnl.documentLineDescription').alias('documentLineDescription')\
                                      ).distinct()
            
            w = Window().orderBy(lit(''))
            fin_L2_DIM_DocumentDetail_DocumentLineItemDescription = fin_L2_DIM_DocumentDetail_DocumentLineItemDescription.withColumn( "documentLineItemDescriptionID" , row_number().over(w))
            
            fin_L2_DIM_DocumentDetail_DocumentLineItemDescription  = objDataTransformation.gen_convertToCDMandCache \
                (fin_L2_DIM_DocumentDetail_DocumentLineItemDescription,'fin','L2_DIM_DocumentDetail_DocumentLineItemDescription',False)
            
            dwh_vw_DIM_DocumentDetail_DocumentLineItemDescription = objDataTransformation.gen_convertToCDMStructure_generate \
                (fin_L2_DIM_DocumentDetail_DocumentLineItemDescription,'dwh','vw_DIM_DocumentDetail_DocumentLineItemDescription',True)[0]
            keysAndValues = {'documentLineItemDescriptionID':-1,'documentLineDescription':'NONE'}
            dwh_vw_DIM_DocumentDetail_DocumentLineItemDescription = objGenHelper.gen_placeHolderRecords_populate \
                ('dwh.vw_DIM_DocumentDetail_DocumentLineItemDescription',keysAndValues,dwh_vw_DIM_DocumentDetail_DocumentLineItemDescription)

            dwh_vw_DIM_DocumentDetail_DocumentLineItemDescription  = objDataTransformation.gen_convertToCDMandCache \
                (dwh_vw_DIM_DocumentDetail_DocumentLineItemDescription,'dwh','vw_DIM_DocumentDetail_DocumentLineItemDescription',False)
            
            objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_DocumentDetail_DocumentLineItemDescription,gl_CDMLayer2Path \
                + "fin_L2_DIM_DocumentDetail_DocumentLineItemDescription.parquet" )

            executionStatus = "fin_L2_DIM_DocumentDetail_DocumentLineItemDescription populated sucessfully"
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

    @staticmethod
    def dwhvwDIMDocumentDetailDocumentCustomText_populate():
        try:
            logID = executionLog.init(processID = PROCESS_ID.L2_TRANSFORMATION,className = __class__.__name__)
            objGenHelper = gen_genericHelper()
            objDataTransformation = gen_dataTransformation()
            global fin_L2_DIM_DocumentDetail_DocumentCustomText
            global dwh_vw_DIM_DocumentDetail_DocumentCustomText
            
            fin_L2_DIM_DocumentDetail_DocumentCustomText = fin_L2_FACT_Journal.alias('jrnl') \
                                      .select(col('jrnl.customText').alias('customText')\
                                      ,col('jrnl.customText02').alias('customText02')\
                                      ,col('jrnl.customText03').alias('customText03')\
                                      ,col('jrnl.customText04').alias('customText04')\
                                      ,col('jrnl.customText05').alias('customText05')\
                                      ,col('jrnl.customText06').alias('customText06')\
                                      ,col('jrnl.customText07').alias('customText07')\
                                      ,col('jrnl.customText08').alias('customText08')\
                                      ,col('jrnl.customText09').alias('customText09')\
                                      ,col('jrnl.customText10').alias('customText10')\
                                      ,col('jrnl.customText11').alias('customText11')\
                                      ,col('jrnl.customText12').alias('customText12')\
                                      ,col('jrnl.customText13').alias('customText13')\
                                      ,col('jrnl.customText14').alias('customText14')\
                                      ,col('jrnl.customText15').alias('customText15')\
                                      ,col('jrnl.customText16').alias('customText16')\
                                      ,col('jrnl.customText17').alias('customText17')\
                                      ,col('jrnl.customText18').alias('customText18')\
                                      ,col('jrnl.customText19').alias('customText19')\
                                      ,col('jrnl.customText20').alias('customText20')\
                                      ,col('jrnl.customText21').alias('customText21')\
                                      ,col('jrnl.customText22').alias('customText22')\
                                      ,col('jrnl.customText23').alias('customText23')\
                                      ,col('jrnl.customText24').alias('customText24')\
                                      ,col('jrnl.customText25').alias('customText25')\
                                      ,col('jrnl.customText26').alias('customText26')\
                                      ,col('jrnl.customText27').alias('customText27')\
                                      ,col('jrnl.customText28').alias('customText28')\
                                      ,col('jrnl.customText29').alias('customText29')\
                                      ,col('jrnl.customText30').alias('customText30')\
                                      ).distinct()
            
            w = Window().orderBy(lit(''))
            fin_L2_DIM_DocumentDetail_DocumentCustomText = fin_L2_DIM_DocumentDetail_DocumentCustomText.withColumn( "documentCustomTextID" , row_number().over(w))
            
            fin_L2_DIM_DocumentDetail_DocumentCustomText  = objDataTransformation.gen_convertToCDMandCache \
                (fin_L2_DIM_DocumentDetail_DocumentCustomText,'fin','L2_DIM_DocumentDetail_DocumentCustomText',False)
            
            dwh_vw_DIM_DocumentDetail_DocumentCustomText = objDataTransformation.gen_convertToCDMStructure_generate \
                (fin_L2_DIM_DocumentDetail_DocumentCustomText,'dwh','vw_DIM_DocumentDetail_DocumentCustomText',True)[0]
            keysAndValues = {'documentCustomTextID':-1}
            dwh_vw_DIM_DocumentDetail_DocumentCustomText = objGenHelper.gen_placeHolderRecords_populate \
                ('dwh.vw_DIM_DocumentDetail_DocumentCustomText',keysAndValues,dwh_vw_DIM_DocumentDetail_DocumentCustomText)

            dwh_vw_DIM_DocumentDetail_DocumentCustomText  = objDataTransformation.gen_convertToCDMandCache \
                (dwh_vw_DIM_DocumentDetail_DocumentCustomText,'dwh','vw_DIM_DocumentDetail_DocumentCustomText',False)
            
            objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_DocumentDetail_DocumentCustomText,gl_CDMLayer2Path \
                + "fin_L2_DIM_DocumentDetail_DocumentCustomText.parquet" )
            
            executionStatus = "fin_L2_DIM_DocumentDetail_DocumentCustomText populated sucessfully"
            executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

        except Exception as err:
            executionStatus = objGenHelper.gen_exceptionDetails_log()
            executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
            return [LOG_EXECUTION_STATUS.FAILED,executionStatus]
