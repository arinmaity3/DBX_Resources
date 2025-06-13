# Databricks notebook source
import sys
import traceback
from pyspark.sql import Row
from pyspark.sql.functions import col,expr,lower,trim,concat,max,coalesce
from pyspark.sql.types import StructType,StructField,StringType


def gen_L1_STG_ClearingBox_MapBusinessData_populate():
    try:
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        global gen_L1_STG_ClearingBoxBusinessDataMapping

        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        
        erpSAPSystemID = objGenHelper\
            .gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID')
        erpGENSystemID =  objGenHelper\
            .gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')
        df_reportingLanguage=knw_LK_CD_ReportingSetup\
                .select(col('KPMGDataReportingLanguage'))\
                .alias('languageCode').distinct()

        targetLanguageCode=df_reportingLanguage.first()[0]
        if targetLanguageCode is None:
            targetLanguageCode='EN'
      
        
        gen_L1_STG_ClearingBoxBusinessDataMapping=\
        knw_LK_GD_BusinessDatatypeValueMapping.alias("lkbd")\
        .filter(expr("lkbd.sourceERPSystemID = "+erpSAPSystemID+" \
         AND lkbd.businessDatatype NOT IN \
         ( \
         'GL Posting Key' \
         ,'GL Special Indicator' \
         ,'GL Account Type' \
         ,'IR Transaction Type' \
         ,'GL Document Type' \
         ,'SD Document Type' \
         ) \
         AND lower(lkbd.businessDatatype) NOT LIKE '%heuristic' \
         AND lkbd.targetLanguageCode = targetLanguageCode"))\
        .groupBy("lkbd.businessDatatype"\
        ,"lkbd.sourceSystemValue")\
        .agg(expr("max(lkbd.businessDatatypeDescription) \
        as businessDatatypeDescription")\
        ,expr("max(lkbd.sourceERPSystemID) as sourceERPSystemID")\
        ,expr("max(lkbd.sourceSystemValueDescription) \
        as sourceSystemValueDescription")\
        ,expr("max(lkbd.targetERPSystemID) as targetERPSystemID")\
        ,expr("max(lkbd.targetSystemValue) as targetSystemValue")\
        ,expr("max(lkbd.targetSystemValueDescription)\
       as targetSystemValueDescription")\
        ,expr("max(lkbd.targetLanguageCode) as targetLanguageCode"))
        
        gen_L1_STG_ClearingBoxBusinessDataMapping_1=\
        knw_LK_GD_BusinessDatatypeValueMapping.alias("lkbd")\
        .join(gen_L1_STG_ClearingBoxBusinessDataParameter.alias("stgbdp1"),\
        expr("( stgbdp1.clearingBoxParamName = 'GL Posting Key'\
         AND stgbdp1.clearingBoxParamType = 'business_data'\
         AND lower(stgbdp1.clearingBoxParamSourceValue) \
         = lower(lkbd.sourceSystemValue) )"),"left")\
        .filter(expr("lkbd.sourceERPSystemID = "+erpSAPSystemID+" \
        AND lkbd.businessDatatype				= 'Posting Key' \
        AND lkbd.targetLanguageCode			= '"+targetLanguageCode+"'"))\
        .groupBy("lkbd.businessDatatype"\
        ,"lkbd.sourceSystemValue")\
        .agg(expr("max(lkbd.sourceERPSystemID) as sourceERPSystemID")\
        ,expr("max(lkbd.sourceSystemValueDescription) \
        as sourceSystemValueDescription")\
        ,expr("max(lkbd.targetERPSystemID) as targetERPSystemID")\
        ,expr("(CASE WHEN ISNULL(max(stgbdp1.clearingBoxParamTargetValue)) THEN  \
        'TBD' ELSE max(stgbdp1.clearingBoxParamTargetValue) END)\
       as targetSystemValue")\
        ,expr("(CASE WHEN ISNULL(\
        max(stgbdp1.clearingBoxParamTargetDescription)) THEN \
        'To be defined' ELSE max(stgbdp1.clearingBoxParamTargetDescription) \
        END) as targetSystemValueDescription")\
        ,expr("max(lkbd.targetLanguageCode) as targetLanguageCode"))\
        .selectExpr('"GL Posting Key" as businessDatatype', 'sourceSystemValue',\
                    '"GL Posting Key" as businessDatatypeDescription', \
                    'sourceERPSystemID', 'sourceSystemValueDescription', \
                    'targetERPSystemID', 'targetSystemValue', \
                    'targetSystemValueDescription', 'targetLanguageCode')
        
        gen_L1_STG_ClearingBoxBusinessDataMapping=\
        gen_L1_STG_ClearingBoxBusinessDataMapping\
        .union(gen_L1_STG_ClearingBoxBusinessDataMapping_1)
        
        gen_L1_TMP_ClearingBoxBusinessDataMissingValue=\
        fin_L1_TD_Journal.select("postingKey").distinct()\
        .union(\
        fin_L1_STG_AccountReceivableComplete.select("postingKey")\
        .distinct())\
        .union(\
        fin_L1_TD_AccountPayable.select("accountPayablePostingKey")\
        .distinct())\
        .union(\
        fin_L1_TD_AccountGRIR.select("accountGRIR_ERPSourcePostingKey")\
        .distinct())\
        .select("postingKey").distinct().alias("erp_table")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1"),\
        expr("( erp_table.postingKey = cbbdm1.sourceSystemValue\
         AND cbbdm1.businessDatatype = 'GL Posting Key' )"),"left")\
        .filter(expr("cbbdm1.sourceSystemValue IS NULL"))\
        .selectExpr("postingKey as clearingBoxMissingValue").distinct()
        
        
        knw_LK_GD_BusinessDatatypeValueMapping_GLPostingKey=\
        knw_LK_GD_BusinessDatatypeValueMapping.alias("lkbd")\
        .filter(expr("lkbd.businessDatatype ='GL Posting Key'"))\
        .groupBy()\
        .agg(expr("max(lkbd.businessDatatype) as businessDatatype")\
        ,expr("max(lkbd.businessDatatypeDescription) \
        as businessDatatypeDescription")\
        ,expr("max(lkbd.sourceERPSystemID) as sourceERPSystemID")\
        ,expr("max(lkbd.targetERPSystemID) as targetERPSystemID")\
        ,expr("max(lkbd.targetLanguageCode) as targetLanguageCode"))
        
        
        
        gen_L1_STG_ClearingBoxBusinessDataMapping_2=\
        gen_L1_TMP_ClearingBoxBusinessDataMissingValue.alias("tcbdmv")\
        .join(knw_LK_GD_BusinessDatatypeValueMapping_GLPostingKey\
        .alias("lkbd2"),how="cross")\
        .join(gen_L1_STG_ClearingBoxBusinessDataParameter.alias("stgbdp1"),\
        expr("( stgbdp1.clearingBoxParamName ='GL Posting Key'\
         AND stgbdp1.clearingBoxParamType ='business_data'\
         AND lower(stgbdp1.clearingBoxParamSourceValue) = \
         lower(tcbdmv.clearingBoxMissingValue) )"),"left")\
        .selectExpr("(CASE WHEN ISNULL(lkbd2.businessDatatype) \
        THEN 'GL Posting Key' ELSE lkbd2.businessDatatype END) \
        as businessDatatype"\
        ,"(CASE WHEN ISNULL(lkbd2.businessDatatypeDescription) \
        THEN 'GL Posting Key' ELSE lkbd2.businessDatatypeDescription END) \
        as businessDatatypeDescription"\
        ,"(CASE WHEN ISNULL(lkbd2.sourceERPSystemID) THEN "+erpSAPSystemID+" \
        ELSE lkbd2.sourceERPSystemID END) as sourceERPSystemID"\
        ,"tcbdmv.clearingBoxMissingValue as sourceSystemValue"\
        ,"'to be defined' as sourceSystemValueDescription"\
        ,"(CASE WHEN ISNULL(lkbd2.targetERPSystemID) THEN "+erpGENSystemID+" \
        ELSE lkbd2.targetERPSystemID END) as targetERPSystemID"\
        ,"(CASE WHEN ISNULL((stgbdp1.clearingBoxParamTargetValue)) \
        THEN 'TBD' ELSE (stgbdp1.clearingBoxParamTargetValue) END) \
        as targetSystemValue"\
        ,"(CASE WHEN ISNULL((stgbdp1.clearingBoxParamTargetDescription))\
        THEN 'To be defined' ELSE\
        (stgbdp1.clearingBoxParamTargetDescription) END) \
        as targetSystemValueDescription"\
        ,"(lkbd2.targetLanguageCode) as targetLanguageCode")
        
        gen_L1_STG_ClearingBoxBusinessDataMapping=\
        gen_L1_STG_ClearingBoxBusinessDataMapping.union(\
        gen_L1_STG_ClearingBoxBusinessDataMapping_2)
        
        
        Row_GLSpecialIndicator=Row(
         businessDatatype			 = 'GL Special Indicator'
        ,sourceSystemValue            = ''
        ,businessDatatypeDescription  = 'GL Special Indicator'
        ,sourceERPSystemID            = erpSAPSystemID
        ,sourceSystemValueDescription = 'Normal'
        ,targetERPSystemID            = erpGENSystemID
        ,targetSystemValue            = 'Normal'
        ,targetSystemValueDescription = 'Normal'
        ,targetLanguageCode           = targetLanguageCode)
        df_GLSpecialIndicator=spark.createDataFrame\
        ([Row_GLSpecialIndicator],\
        gen_L1_STG_ClearingBoxBusinessDataMapping.schema)
        
        gen_L1_STG_ClearingBoxBusinessDataMapping=\
        gen_L1_STG_ClearingBoxBusinessDataMapping.union(\
        df_GLSpecialIndicator)
        
        
        gen_L1_STG_ClearingBoxBusinessDataMapping_4=\
        gen_L1_STG_ClearingBoxData.alias("cbd")\
        .join(gen_L1_STG_ClearingBoxBusinessDataParameter\
        .alias("stgbdp1"),\
        expr("( stgbdp1.clearingBoxParamName = 'GL Special Indicator'\
         AND stgbdp1.clearingBoxParamType = 'business_data'\
         AND lower(stgbdp1.clearingBoxParamSourceValue) = \
         lower(cbd.sourceSystemValue) )"),"left")\
        .filter(expr("cbd.sourceType='FinancialAccountingPoll'"))\
        .selectExpr("'GL Special Indicator' as businessDatatype"\
        ,"'GL Special Indicator' as businessDatatypeDescription"\
        ,"cbd.sourceERPSystemID as sourceERPSystemID"\
        ,"cbd.sourceSystemValue as sourceSystemValue"\
        ,"cbd.sourceSystemValueDescription as sourceSystemValueDescription"\
        ,"cbd.targetERPSystemID as targetERPSystemID"\
        ,"(CASE WHEN ISNULL((stgbdp1.clearingBoxParamTargetValue))\
        THEN 'TBD' ELSE (stgbdp1.clearingBoxParamTargetValue) END) \
        as targetSystemValue"\
        ,"(CASE WHEN ISNULL((stgbdp1.clearingBoxParamTargetDescription))\
        THEN 'To be defined' ELSE\
        (stgbdp1.clearingBoxParamTargetDescription) END) \
        as targetSystemValueDescription"\
        ,"cbd.targetLanguageCode as targetLanguageCode")\
        .select(col('businessDatatype'),col('sourceSystemValue'),\
                col('businessDatatypeDescription'),col('sourceERPSystemID'),\
                col('sourceSystemValueDescription'),col('targetERPSystemID'),\
                col('targetSystemValue'),col('targetSystemValueDescription'),\
                col('targetLanguageCode'))
        
        gen_L1_STG_ClearingBoxBusinessDataMapping=\
        gen_L1_STG_ClearingBoxBusinessDataMapping.union(\
        gen_L1_STG_ClearingBoxBusinessDataMapping_4)
        
        gen_L1_STG_ClearingBoxBusinessDataMapping_5=\
        gen_L1_STG_ClearingBoxData.alias("cbd")\
        .join(gen_L1_STG_ClearingBoxBusinessDataParameter.alias("stgbdp1"),\
        expr("( stgbdp1.clearingBoxParamName ='SD Document Type'\
         AND stgbdp1.clearingBoxParamType ='business_data'\
         AND stgbdp1.clearingBoxParamSourceValue = \
         cbd.sourceSystemValue )"),"left")\
        .filter(expr("cbd.sourceType='DomainFixedValues_SDDocumentType'"))\
        .selectExpr("cbd.businessDatatype as businessDatatype"\
        ,"cbd.businessDatatypeDescription as businessDatatypeDescription"\
        ,"cbd.sourceERPSystemID as sourceERPSystemID"\
        ,"cbd.sourceSystemValue as sourceSystemValue"\
        ,"cbd.sourceSystemValueDescription as sourceSystemValueDescription"\
        ,"cbd.targetERPSystemID as targetERPSystemID"\
        ,"(CASE WHEN ISNULL((stgbdp1.clearingBoxParamTargetValue))\
        THEN 'TBD' ELSE\
        (stgbdp1.clearingBoxParamTargetValue) END) as targetSystemValue"\
        ,"(CASE WHEN ISNULL((stgbdp1.clearingBoxParamTargetDescription))\
        THEN 'To be defined' ELSE \
        (stgbdp1.clearingBoxParamTargetDescription) END) \
        as targetSystemValueDescription"\
        ,"cbd.targetLanguageCode as targetLanguageCode")\
        .select(col('businessDatatype'),col('sourceSystemValue'),\
                col('businessDatatypeDescription'),col('sourceERPSystemID'),\
                col('sourceSystemValueDescription'),col('targetERPSystemID'),\
                col('targetSystemValue'),col('targetSystemValueDescription'),\
                col('targetLanguageCode'))
        
        gen_L1_STG_ClearingBoxBusinessDataMapping=\
        gen_L1_STG_ClearingBoxBusinessDataMapping.union(\
        gen_L1_STG_ClearingBoxBusinessDataMapping_5)
        
        
        gen_L1_STG_ClearingBoxBusinessDataMapping_6=\
        gen_L1_STG_ClearingBoxData.alias("cbd")\
        .join(gen_L1_STG_ClearingBoxBusinessDataParameter.alias("stgbdp1"),\
        expr("( stgbdp1.clearingBoxParamName ='GL Account Type'\
         AND stgbdp1.clearingBoxParamType ='business_data'\
         AND lower(stgbdp1.clearingBoxParamSourceValue) = \
         lower(cbd.sourceSystemValue) )"),"left")\
        .filter(expr("sourceType='DomainFixedValues_GLAccountType'"))\
        .selectExpr("cbd.businessDatatype as businessDatatype"\
        ,"cbd.businessDatatypeDescription as businessDatatypeDescription"\
        ,"cbd.sourceERPSystemID as sourceERPSystemID"\
        ,"cbd.sourceSystemValue as sourceSystemValue"\
        ,"cbd.sourceSystemValueDescription as sourceSystemValueDescription"\
        ,"cbd.targetERPSystemID as targetERPSystemID"\
        ,"(CASE WHEN ISNULL((stgbdp1.clearingBoxParamTargetValue)) THEN 'TBD'\
        ELSE (stgbdp1.clearingBoxParamTargetValue) END) as targetSystemValue"\
        ,"(CASE WHEN ISNULL((stgbdp1.clearingBoxParamTargetDescription)) THEN \
        'To be defined' ELSE \
        (stgbdp1.clearingBoxParamTargetDescription) END) \
        as targetSystemValueDescription"\
        ,"cbd.targetLanguageCode as targetLanguageCode")\
        .select(col('businessDatatype'),col('sourceSystemValue'),\
                col('businessDatatypeDescription'),col('sourceERPSystemID'),\
                col('sourceSystemValueDescription'),col('targetERPSystemID'),\
                col('targetSystemValue'),col('targetSystemValueDescription'),\
                col('targetLanguageCode'))
        
        
        gen_L1_STG_ClearingBoxBusinessDataMapping=\
        gen_L1_STG_ClearingBoxBusinessDataMapping.union(\
        gen_L1_STG_ClearingBoxBusinessDataMapping_6)
        
        uni_schema= StructType([StructField("sourceSystemValue"
                                            ,StringType(),False)
                                ,StructField("sourceSystemValueDescription"
                                            ,StringType(),False)])
        ls_uni=[['RP','Invoice receipt'			]
        ,['KP','Account maintenance'			]
        ,['KS','Account maintenance reversal'	]
        ,['PR','Price change'					]
        ,['BL','Material debit'					]
        ,['PF','Std cost estim. release'		]
        ,['RD','Logistics invoice'				]
        ,['ML','Material ledger settlement'		]
        ,['MI','Material ledger initialization'	]
        ,['RS','Logistics invoice, cancel'		]]
        uni=spark.createDataFrame(ls_uni,uni_schema)
        gen_L1_STG_ClearingBoxBusinessDataMapping_7=\
        uni.alias("uni")\
        .join(gen_L1_STG_ClearingBoxBusinessDataParameter.alias("stgbdp1"),\
        expr("( stgbdp1.clearingBoxParamName ='IR Transaction Type'\
         AND stgbdp1.clearingBoxParamType ='business_data'\
         AND lower(stgbdp1.clearingBoxParamSourceValue) =\
        lower(uni.sourceSystemValue) )"),"left")\
        .selectExpr("'IR Transaction Type' as businessDatatype"\
                    ,"uni.sourceSystemValue as sourceSystemValue"\
        ,"'IR Transaction Type' as businessDatatypeDescription"\
        ,erpSAPSystemID+" as sourceERPSystemID"\
        ,"uni.sourceSystemValueDescription as sourceSystemValueDescription"\
        ,erpGENSystemID+" as targetERPSystemID"\
        ,"(CASE WHEN ISNULL((stgbdp1.clearingBoxParamTargetValue)) THEN\
        'TBD' ELSE (stgbdp1.clearingBoxParamTargetValue) END) \
        as targetSystemValue"\
        ,"(CASE WHEN ISNULL((stgbdp1.clearingBoxParamTargetDescription)) THEN\
        'To be defined' ELSE \
        (stgbdp1.clearingBoxParamTargetDescription) END) \
        as targetSystemValueDescription"\
        ,"'"+targetLanguageCode+"' as targetLanguageCode")
        
        
        gen_L1_STG_ClearingBoxBusinessDataMapping=\
        gen_L1_STG_ClearingBoxBusinessDataMapping.union(\
        gen_L1_STG_ClearingBoxBusinessDataMapping_7)
        
        TmpTargetLanguageCode= gen_L1_MD_DocType.alias("doty1")\
        .groupBy()\
        .agg(expr("coalesce(max(CASE WHEN \
        doty1.languageCode = '"+targetLanguageCode+"'	\
        THEN doty1.languageCode ELSE NULL END)\
        ,max(CASE WHEN	doty1.languageCode = 'EN' \
        THEN doty1.languageCode ELSE NULL END) \
        ,max(CASE WHEN	doty1.languageCode = 'DE'\
        THEN doty1.languageCode ELSE NULL END)\
        ,max(doty1.languageCode)) as TmpTargetLanguageCode")).first() 
        if TmpTargetLanguageCode is not None:
          TmpTargetLanguageCode=TmpTargetLanguageCode.TmpTargetLanguageCode
          
        gen_L1_STG_ClearingBoxBusinessDataMapping_8=\
        gen_L1_MD_DocType.alias("doty1")\
        .join(gen_L1_STG_ClearingBoxBusinessDataParameter.alias("stgbdp1"),\
        expr("( stgbdp1.clearingBoxParamName ='GL Document Type'\
         AND stgbdp1.clearingBoxParamType ='business_data'\
         AND lower(stgbdp1.clearingBoxParamSourceValue) = \
         lower(doty1.documentType) )"),"left")\
        .filter(expr("doty1.languageCode ='"+ TmpTargetLanguageCode+"'"))\
        .selectExpr("'GL Document Type' as businessDatatype"\
        ,"'GL Document Type' as businessDatatypeDescription"\
        ,erpSAPSystemID+" as sourceERPSystemID"\
        ,"doty1.documentType as sourceSystemValue"\
        ,"doty1.documentTypeDescription as sourceSystemValueDescription"\
        ,erpGENSystemID+" as targetERPSystemID"\
        ,"(CASE WHEN ISNULL((stgbdp1.clearingBoxParamTargetValue))\
        THEN 'TBD' ELSE (stgbdp1.clearingBoxParamTargetValue) END)\
        as targetSystemValue"\
        ,"(CASE WHEN ISNULL((stgbdp1.clearingBoxParamTargetDescription)) \
        THEN 'To be defined' ELSE (stgbdp1.clearingBoxParamTargetDescription)\
        END) as targetSystemValueDescription"\
        ,"doty1.languageCode as targetLanguageCode")\
        .select(col('businessDatatype'),col('sourceSystemValue'),\
                col('businessDatatypeDescription'),col('sourceERPSystemID'),\
                col('sourceSystemValueDescription'),col('targetERPSystemID'),\
                col('targetSystemValue'),col('targetSystemValueDescription'),\
                col('targetLanguageCode'))  
        
        gen_L1_STG_ClearingBoxBusinessDataMapping=\
        gen_L1_STG_ClearingBoxBusinessDataMapping.union(\
        gen_L1_STG_ClearingBoxBusinessDataMapping_8)
        
        
        TmpTargetLanguageCode= gen_L1_MD_Transaction.alias("doty1")\
        .groupBy()\
        .agg(expr("coalesce(max(CASE WHEN \
        doty1.languageCode = '"+targetLanguageCode+"'\
       THEN doty1.languageCode ELSE NULL END)\
        ,max(CASE WHEN \
        doty1.languageCode = 'EN'	THEN doty1.languageCode ELSE NULL END)\
        ,max(CASE WHEN	doty1.languageCode = 'DE'\
        THEN doty1.languageCode ELSE NULL END)\
        ,max(doty1.languageCode)) as TmpTargetLanguageCode")).first() 
        if TmpTargetLanguageCode is not None:
          TmpTargetLanguageCode=TmpTargetLanguageCode.TmpTargetLanguageCode
          
        gen_L1_TMP_ClearingBoxBusinessDataMissingValue=\
        fin_L1_TD_Journal.select(col("transactionCode")\
                                 .alias("clearingBoxMissingValue")).distinct()
        
        gen_L1_STG_ClearingBoxBusinessDataMapping_9=\
        gen_L1_TMP_ClearingBoxBusinessDataMissingValue.alias("tcbdmv")\
        .join(gen_L1_STG_ClearingBoxBusinessDataParameter.alias("stgbdp1"),\
        expr("( stgbdp1.clearingBoxParamName ='GL Transaction Code'\
         AND stgbdp1.clearingBoxParamType ='business_data'\
         AND lower(stgbdp1.clearingBoxParamSourceValue) = \
         lower(tcbdmv.clearingBoxMissingValue) ) "),"left")\
        .join(gen_L1_MD_Transaction.alias("TSTCT"),\
        expr("( tcbdmv.clearingBoxMissingValue = TSTCT.transactionType\
         AND TSTCT.languageCode = '"+TmpTargetLanguageCode+"' )"),"left")\
        .selectExpr("'GL Transaction Code' as businessDatatype"\
        ,"tcbdmv.clearingBoxMissingValue as sourceSystemValue"\
        ,"'GL Transaction Code' as businessDatatypeDescription"\
        ,erpSAPSystemID+" as sourceERPSystemID"\
        ,"transactionTypeDescription as sourceSystemValueDescription"\
        ,erpGENSystemID+" as targetERPSystemID"\
        ,"(CASE WHEN ISNULL((stgbdp1.clearingBoxParamTargetValue)) \
        THEN 'TBD' ELSE\
        (stgbdp1.clearingBoxParamTargetValue) END) as targetSystemValue"\
        ,"(CASE WHEN ISNULL((stgbdp1.clearingBoxParamTargetDescription)) \
        THEN 'To be defined' \
        ELSE (stgbdp1.clearingBoxParamTargetDescription) \
        END) as targetSystemValueDescription"\
        ,"(CASE WHEN ISNULL(TSTCT.languageCode) THEN \
        '"+TmpTargetLanguageCode+"' ELSE TSTCT.languageCode END)\
       as targetLanguageCode")
        
        
        gen_L1_STG_ClearingBoxBusinessDataMapping=\
        gen_L1_STG_ClearingBoxBusinessDataMapping.union(\
        gen_L1_STG_ClearingBoxBusinessDataMapping_9)
        
        
        gen_L1_STG_ClearingBoxBusinessDataMapping_10=\
        gen_L1_STG_ClearingBoxBusinessDataParameter.alias("stgbdp1")\
        .filter(expr("lower(stgbdp1.clearingBoxParamName)\
       LIKE '%heuristic' \
         AND stgbdp1.clearingBoxParamType = 'business_data_heuristic'"))\
        .selectExpr("stgbdp1.clearingBoxParamName as businessDatatype"\
                    ,"stgbdp1.clearingBoxParamSourceValue \
                    as sourceSystemValue"\
        ,"stgbdp1.clearingBoxParamName as businessDatatypeDescription"\
        ,erpSAPSystemID+" as sourceERPSystemID"\
        ,"stgbdp1.clearingBoxParamTargetDescription\
       as sourceSystemValueDescription"\
        ,erpGENSystemID+" as targetERPSystemID"\
        ,"(CASE WHEN ISNULL((stgbdp1.clearingBoxParamTargetValue)) \
        THEN 'TBD' ELSE (stgbdp1.clearingBoxParamTargetValue) END)\
       as targetSystemValue"\
        ,"(CASE WHEN ISNULL((stgbdp1.clearingBoxParamTargetDescription)) \
        THEN 'To be defined' ELSE \
        (stgbdp1.clearingBoxParamTargetDescription) END)\
        as targetSystemValueDescription"\
        ,"'"+targetLanguageCode+"' as targetLanguageCode")
        
        gen_L1_STG_ClearingBoxBusinessDataMapping=\
        gen_L1_STG_ClearingBoxBusinessDataMapping.union(\
        gen_L1_STG_ClearingBoxBusinessDataMapping_10)
        
        gen_L1_STG_ClearingBoxBusinessDataMapping=\
        gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1")\
        .join(gen_L1_STG_ClearingBoxBusinessDataMapping.alias("cbbdm1_2"),\
        expr("( lower(cbbdm1.sourceSystemValue) =\
       lower(cbbdm1_2.sourceSystemValue)\
         AND lower(cbbdm1.businessDatatype) =\
         concat(lower(cbbdm1_2.businessDatatype) ,\
         ' heuristic') )"),"left")\
        .groupBy("cbbdm1.sourceERPSystemID"\
        ,"cbbdm1.businessDatatypeDescription"\
        ,"cbbdm1.businessDatatype"\
        ,"cbbdm1.targetSystemValue"\
        ,"cbbdm1.targetSystemValueDescription"\
        ,"cbbdm1.targetLanguageCode"\
        ,"cbbdm1.targetERPSystemID"\
        ,"cbbdm1.sourceSystemValue")\
        .agg(expr("max(case when cbbdm1_2.sourceSystemValue \
        is not null then cbbdm1_2.sourceSystemValueDescription \
        else cbbdm1.sourceSystemValueDescription end) \
        as sourceSystemValueDescription"))
        
        
        
        gen_L1_STG_ClearingBoxBusinessDataMapping =\
           objDataTransformation.gen_convertToCDMandCache \
        (gen_L1_STG_ClearingBoxBusinessDataMapping,'gen',\
        'L1_STG_ClearingBoxBusinessDataMapping',True)
        
        executionStatus = "gen_L1_STG_ClearingBoxBusinessDataMapping populated sucessfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus]




