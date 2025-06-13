# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  col,explode,split,expr,max,lower,trim,concat_ws,collect_list

def gen_L1_STG_ClearingBoxBusinessDataParameter_populate():
  try:
    logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
    global gen_L1_STG_ClearingBoxBusinessDataParameter
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()

    gen_L1_TMP_ClearingBoxPreparedParameter=\
    knw_LK_GD_ClearingBoxInitBusinessDataParameter\
    .filter(col("clearingBoxParamType")=='business_data')\
    .alias("lkcbdp")\
    .withColumn(
      "clearingBoxParamSourceValue", 
      explode(split(col("lkcbdp.clearingBoxParamSourceValueList"),",")))\
    .selectExpr("clearingBoxParamType","clearingBoxParamName","clearingBoxParamTargetValue"\
        ,"clearingBoxParamTargetDescription","ltrim(rtrim(clearingBoxParamSourceValue)) as clearingBoxParamSourceValue")

    arv=fin_L1_STG_AccountReceivableComplete.alias("acrv1")\
    .join(gen_L1_TMP_ClearingBoxPreparedParameter.alias("tcpp1"),\
    expr("( acrv1.postingKey = tcpp1.clearingBoxParamSourceValue \
     AND tcpp1.clearingBoxParamTargetValue IN('Incoming payment', 'Outgoing payment')\
     AND tcpp1.clearingBoxParamName = 'GL Posting Key'\
     AND tcpp1.clearingBoxParamType ='business_data' ) "),"inner")\
    .join(gen_L1_TMP_ClearingBoxPreparedParameter.alias("tcpp1_2"),\
    expr("( acrv1.documentType = tcpp1_2.clearingBoxParamSourceValue \
     AND tcpp1_2.clearingBoxParamTargetValue IN('Payment')\
     AND tcpp1_2.clearingBoxParamName = 'GL Document Type'\
     AND tcpp1_2.clearingBoxParamType = 'business_data' )"),"inner")\
    .filter(expr("acrv1.dataOrigin = 'Closed AR'"))\
    .selectExpr("acrv1.documentType as clearingBoxGLDocumentTypeERP").distinct()
   
    bsak=fin_L1_TD_AccountPayable.alias("apa1")\
    .join(gen_L1_TMP_ClearingBoxPreparedParameter.alias("tcpp1"),\
    expr("( apa1.accountPayablePostingKey = tcpp1.clearingBoxParamSourceValue\
     AND tcpp1.clearingBoxParamTargetValue IN('Incoming payment', 'Outgoing payment')\
     AND tcpp1.clearingBoxParamName = 'GL Posting Key'\
     AND tcpp1.clearingBoxParamType ='business_data' ) "),"inner")\
    .join(gen_L1_TMP_ClearingBoxPreparedParameter.alias("tcpp1_2"),\
    expr("( apa1.accountPayableDocumentType = tcpp1_2.clearingBoxParamSourceValue\
     AND tcpp1_2.clearingBoxParamTargetValue IN('Payment')\
     AND tcpp1_2.clearingBoxParamName = 'GL Document Type'\
     AND tcpp1_2.clearingBoxParamType = 'business_data' )"),"inner")\
    .filter(expr("apa1.accountPayableSourceERPTable = 'BSAK'"))\
    .selectExpr("ltrim(rtrim(apa1.accountPayableDocumentType)) as clearingBoxGLDocumentTypeERP").distinct()

    gen_L1_TMP_ClearingBoxBusinessDataDocumentTypeSL=\
    arv.union(bsak).distinct()

    CTE_documentType=gen_L1_MD_DocType.alias("doty1")\
    .filter(expr("ltrim(rtrim(doty1.languageCode)) IN	('EN', 'DE')"))\
    .filter(lower(col('doty1.documentTypeDescription')).like('%bank%'))\
    .select(col("doty1.documentType")).distinct()\
    .union(\
    gen_L1_MD_DocType.alias("doty1")\
    .filter(expr("ltrim(rtrim(doty1.languageCode)) IN	('EN')"))\
    .filter(lower(col('doty1.documentTypeDescription')).like('%cash%'))\
    .select(col("doty1.documentType")).distinct())\
    .union(\
    gen_L1_MD_DocType.alias("doty1")\
    .filter(expr("ltrim(rtrim(doty1.languageCode)) IN	('DE')"))\
    .filter(lower(col('doty1.documentTypeDescription')).like('%kasse%'))\
    .select(col("doty1.documentType")).distinct())\
    .union(\
    gen_L1_MD_DocType.alias("doty1")\
    .filter(trim(col("doty1.documentType"))\
    .isin([row[0].strip() for row in gen_L1_TMP_ClearingBoxBusinessDataDocumentTypeSL\
    .selectExpr("clearingBoxGLDocumentTypeERP").distinct().collect()]))\
    .select(col("doty1.documentType")).distinct()).distinct()

    gen_L1_TMP_ClearingBoxHeuristicBusinessDataParameter=\
    CTE_documentType.alias("CTE_dt")\
    .filter(expr("CTE_dt.documentType is not null and CTE_dt.documentType <> ''"))\
    .selectExpr("'business_data_heuristic' as clearingBoxParamType"\
    ,"'GL Document Type Heuristic' as clearingBoxParamName"\
    ,"'Bank/Cash Heuristic' as clearingBoxParamTargetValue"\
    ,"'Bank/Cash Heuristic' as clearingBoxParamTargetDescription"\
    ,"CTE_dt.documentType as clearingBoxParamSourceValue")

    gen_L1_TMP_ClearingBoxHeuristicBusinessDataParameter=\
    gen_L1_TMP_ClearingBoxHeuristicBusinessDataParameter.alias("thbdp1")\
    .orderBy("clearingBoxParamSourceValue")\
    .groupBy("clearingBoxParamType")\
    .agg(max("thbdp1.clearingBoxParamName").alias("clearingBoxParamName")\
        ,max("thbdp1.clearingBoxParamTargetValue").alias("clearingBoxParamTargetValue")\
        ,max("thbdp1.clearingBoxParamTargetDescription").alias("clearingBoxParamTargetDescription")\
        ,concat_ws(',',collect_list("thbdp1.clearingBoxParamSourceValue")).alias("clearingBoxParamSourceValueList"))
    gen_L1_TMP_ClearingBoxInitBusinessDataParameter=\
    knw_LK_GD_ClearingBoxInitBusinessDataParameter.alias("lkcbdp")\
    .selectExpr("lkcbdp.clearingBoxParamType"\
    ,"lkcbdp.clearingBoxParamName"\
    ,"lkcbdp.clearingBoxParamTargetValue"\
    ,"lkcbdp.clearingBoxParamTargetDescription"\
    ,"lkcbdp.clearingBoxParamSourceValueList")\
    .union(gen_L1_TMP_ClearingBoxHeuristicBusinessDataParameter.alias("thbdp1")\
    .selectExpr("thbdp1.clearingBoxParamType"\
    ,"thbdp1.clearingBoxParamName"\
    ,"thbdp1.clearingBoxParamTargetValue"\
    ,"thbdp1.clearingBoxParamTargetDescription"\
    ,"thbdp1.clearingBoxParamSourceValueList"))

    gen_L1_TMP_ClearingBoxHeuristicBusinessDataParameter=\
        gen_L1_TMP_ClearingBoxHeuristicBusinessDataParameter\
        .filter(col("clearingBoxParamType")=='business_data_heuristic')\
        .alias("tbdp1")\
        .withColumn(
          "clearingBoxParamSourceValue", 
          explode(split(col("tbdp1.clearingBoxParamSourceValueList"),",")))\
        .selectExpr("clearingBoxParamType"\
        ,"clearingBoxParamName"\
        ,"clearingBoxParamTargetValue"\
        ,"clearingBoxParamTargetDescription"\
        ,"ltrim(rtrim(clearingBoxParamSourceValue)) as clearingBoxParamSourceValue").distinct()

    gen_L1_STG_ClearingBoxBusinessDataParameter=\
    gen_L1_TMP_ClearingBoxPreparedParameter.alias("lkcbdp")\
    .selectExpr("lkcbdp.clearingBoxParamType"\
    ,"lkcbdp.clearingBoxParamName"\
    ,"lkcbdp.clearingBoxParamTargetValue"\
    ,"lkcbdp.clearingBoxParamTargetDescription"\
    ,"lkcbdp.clearingBoxParamSourceValue")\
    .union(gen_L1_TMP_ClearingBoxHeuristicBusinessDataParameter\
    .selectExpr("clearingBoxParamType"\
    ,"clearingBoxParamName"\
    ,"clearingBoxParamTargetValue"\
    ,"clearingBoxParamTargetDescription"\
    ,"clearingBoxParamSourceValue"))

    gen_L1_STG_ClearingBoxBusinessDataParameter = objDataTransformation.gen_convertToCDMandCache \
    (gen_L1_STG_ClearingBoxBusinessDataParameter,'gen','L1_STG_ClearingBoxBusinessDataParameter',True)

    executionStatus = "gen_L1_STG_ClearingBoxBusinessDataParameter populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]

  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


