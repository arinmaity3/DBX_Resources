# Databricks notebook source
from pyspark.sql.functions import concat,col,expr,count,collect_list,lit,trim,concat_ws,length,substring,pow,when,array_sort
from pyspark.sql.types import DecimalType
import re
from delta.tables import *
import sys
import traceback
def app_decimalPointShiftingMetadata_prepare():
    try:

        objGenHelper = gen_genericHelper()
        logID = executionLog.init(PROCESS_ID.DPS,\
                                      validationID = VALIDATION_ID.DPS,
                                      tableName = 'Prepare metadata for DPS') 

        global gl_ls_app_SAP_L0_TMP_ERPTablesRequiredForSelectedAnalytics
        lsTablesNames=[]
        dic_ddic_column_erp=gl_metadataDictionary["dic_ddic_column"].filter(col("schemaName")=='erp')
        knw_LK_GD_CurrencyShiftingStaticJoins=gl_metadataDictionary["knw_LK_GD_CurrencyShiftingStaticJoins"]
        
        scopedAnalytics=objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'SCOPED_ANALYTICS')
        dic_ddic_tablesRequiredForERPPackages=objGenHelper\
            .gen_tablesRequiredForScopedAnalytics_get(gl_metadataDictionary,gl_ERPSystemID,scopedAnalytics)
        gl_ls_app_SAP_L0_TMP_ERPTablesRequiredForSelectedAnalytics=\
                    dic_ddic_tablesRequiredForERPPackages.select(col("tableName")).distinct().collect()
        gl_ls_app_SAP_L0_TMP_ERPTablesRequiredForSelectedAnalytics=\
            [x["tableName"] for x in gl_ls_app_SAP_L0_TMP_ERPTablesRequiredForSelectedAnalytics]
        
        app_SAP_L0_TMP_TransformCurrencyTableListInit=erp_DD03L\
            .alias("DD03L")\
            .filter(~(col("TABNAME").rlike('^_'))\
                   &(col("TABNAME").isin(gl_ls_app_SAP_L0_TMP_ERPTablesRequiredForSelectedAnalytics)))\
            .filter(expr("DD03L.FIELDNAME NOT LIKE '.%' \
                         AND DD03L.FIELDNAME NOT LIKE '/%' \
                         AND DD03L.TABNAME NOT LIKE '/%' \
                         AND DD03L.AS4LOCAL <> 'N'" ))\
            .selectExpr("DD03L.TABNAME AS tablename").distinct()
        
        app_SAP_L0_TMP_TransformCurrencyBasetableReducedList = erp_DD03L.alias("DD03L")\
         .join(app_SAP_L0_TMP_TransformCurrencyTableListInit.alias("LST"),\
               col("LST.tablename")== col("DD03L.TABNAME"),"inner")\
         .join(erp_DD04T.alias("DD04T"),((col("DD03L.ROLLNAME")== col("DD04T.ROLLNAME"))\
        								&(col("DD04T.DDLANGUAGE")== 'D')),"left")\
        .select(col("DD03L.TABNAME").alias("tableName")\
        ,col("DD04T.DDTEXT").alias("textFieldName")\
        ,col("DD03L.FIELDNAME").alias("fieldName")\
        ,col("DD03L.KEYFLAG").alias("keyFlag")\
        ,col("DD03L.DOMNAME").alias("domName")\
        ,col("DD03L.CHECKTABLE").alias("checkTable")\
        ,col("DD03L.REFTABLE").alias("referenceTable")\
        ,col("DD03L.REFFIELD").alias("referenceField")\
        ,col("DD03L.ROLLNAME").alias("rollName")\
        ,col("DD03L.POSITION").alias("position")\
        ,col("DD03L.DATATYPE").alias("dataType"))
        
        app_SAP_L0_TMP_TransformCurrencyBasetableReducedList= \
             app_SAP_L0_TMP_TransformCurrencyBasetableReducedList\
            .withColumn("domname",expr("CASE WHEN tableName = 'CDHDR' \
                                            AND fieldname='USERNAME' \
                                            THEN 'USNAM' ELSE domname END"))\
            .withColumn("CHECKTABLE",expr("CASE WHEN tableName='TKA01' \
                                                THEN '' ELSE CHECKTABLE END"))\
            .filter("tableName NOT IN ('COSP', 'COSS')") \
            .filter(~((col("tableName")=='EKPO')&(col("fieldName")=='ZZWEINEUEBSTN'))) \
            .filter(~(col("DD03L.TABNAME").rlike('^_')))\
            .filter(expr("DD03L.FIELDNAME NOT LIKE '.%' \
                                              AND DD03L.FIELDNAME NOT LIKE '/%' \
                                              AND DD03L.TABNAME NOT LIKE '/%' \
                                              AND DD03L.AS4LOCAL <> 'N'" ))
        
        app_SAP_L0_TMP_TransformCurrencyInformationSchemaTablesAndColumns=\
        	dic_ddic_column_erp.selectExpr("tableName","columnName","position")\
            .filter((col("isImported")==lit(False))|(col("isImported").isNull()))\
        	.orderBy(col("position"))
        
        app_SAP_L0_TMP_TransformCurrencyDefinitionList = erp_DD03L.alias("A")\
            .join(app_SAP_L0_TMP_TransformCurrencyInformationSchemaTablesAndColumns.alias("T"),\
                  (col("A.TABNAME")== col("T.tableName"))\
                 &(col("A.FIELDNAME")==col("T.columnName")),"inner")\
            .filter(expr("A.DATATYPE = 'CURR' AND A.AS4LOCAL <> 'N'"))\
            .filter(~(col("A.TABNAME").rlike('^_')))\
            .filter(expr("A.FIELDNAME NOT LIKE '.%' AND A.FIELDNAME NOT LIKE '/%'"))\
            .selectExpr("A.TABNAME	AS tableName"\
                        ,"A.FIELDNAME	AS fieldName"\
                       ,"CASE WHEN (A.TABNAME IN ('FAGLFLEXT', 'FAGLFLEXA') \
                                AND LEFT(A.FIELDNAME,1) = 'H') THEN 'T001' \
                                ELSE A.REFTABLE END AS currencyTableName"\
                        ,"CASE WHEN (A.TABNAME IN ('FAGLFLEXT', 'FAGLFLEXA') \
                        AND LEFT(A.FIELDNAME,1) = 'H') THEN 'WAERS'\
                        ELSE A.REFFIELD END AS currencyFieldName")
        
        app_SAP_L0_TMP_TransformCurrencyFilteredTablesAndColumns=\
             app_SAP_L0_TMP_TransformCurrencyDefinitionList.alias("cdef")\
            .join(app_SAP_L0_TMP_TransformCurrencyInformationSchemaTablesAndColumns.alias("istc1")\
            ,(col("istc1.tableName")==col("cdef.tableName"))\
            &(col("istc1.columnName")==col("cdef.fieldName")),"inner")\
            .join(app_SAP_L0_TMP_TransformCurrencyInformationSchemaTablesAndColumns.alias("istc2")\
            ,(col("istc2.tableName")==col("cdef.currencyTableName"))\
            &(col("istc2.columnName")==col("cdef.currencyFieldName")),"inner")\
            .selectExpr("cdef.tableName","cdef.fieldName"\
            ,"cdef.currencyTableName","cdef.currencyFieldName")
        
        ls_TableNames=app_SAP_L0_TMP_TransformCurrencyFilteredTablesAndColumns\
           .select(col("tableName")).distinct().collect()
        ls_TableNames=[x["tableName"] for x in ls_TableNames]
        ls_CurrencyTableNames=app_SAP_L0_TMP_TransformCurrencyFilteredTablesAndColumns\
            .select(col("currencyTableName")).distinct().collect()
        ls_CurrencyTableNames=[x["currencyTableName"] for x in ls_CurrencyTableNames]
        ls_FinalTableNames=list(set().union\
            (ls_TableNames\
            ,ls_CurrencyTableNames\
            ,['T000','T001','T001K','T001L','T001W','T030','T030U','GLT0','FAGLFLEXT','FAGLFLEXA']))
        
        app_SAP_L0_TMP_TransformCurrencyBaseTableValues=\
        app_SAP_L0_TMP_TransformCurrencyBasetableReducedList\
          .filter(col("tableName").isin(ls_FinalTableNames))\
          .selectExpr("tableName"\
                      ,"textFieldName"\
                      ,"fieldName"\
                      ,"keyFlag"\
                      ,"domName"\
                      ,"checkTable"\
                      ,"referenceTable"\
                      ,"referenceField AS referenceFieldName"\
                      ,"rollName"\
                      ,"position"\
                      ,"dataType")
        
        gegen=app_SAP_L0_TMP_TransformCurrencyBaseTableValues\
            .select(col("tableName").alias("v_tabname")).distinct()
        app_SAP_L0_TMP_TransformCurrencyLinkedColumn=\
            app_SAP_L0_TMP_TransformCurrencyBaseTableValues.alias("basis")\
            .join(gegen.alias("gegen"),\
                 (col("gegen.v_tabname")!=col("basis.tableName")),"left")\
            .join(app_SAP_L0_TMP_TransformCurrencyBaseTableValues.alias("tftv"),\
                 (col("tftv.tableName")==col("gegen.V_TABNAME"))\
        &(col("tftv.domName")==col("basis.domName"))\
        &(col("tftv.dataType")==col("basis.dataType"))
        &((col("tftv.fieldName")==col("basis.fieldName"))\
          |(col("tftv.rollName") ==col("basis.rollName"))\
          )\
        &((col("tftv.keyFlag")== col("basis.keyFlag"))\
          |(col("tftv.checkTable")== col("basis.tableName"))\
         ),"left").filter(col("basis.keyFlag")=='X')\
        .selectExpr("basis.tableName AS tableName"\
        ,"basis.fieldName AS fieldName"\
        ,"basis.textFieldName AS fieldText"\
        ,"basis.keyFlag AS keyFlag"\
        ,"gegen.V_TABNAME AS v_tableName"\
        ,"tftv.fieldName AS v_fieldname"\
        ,"tftv.textFieldName AS v_field_text"\
        ,"CASE WHEN (tftv.tableName IS NOT NULL AND basis.domName <> 'BUZEI') \
               THEN 'X'  ELSE '' END AS partnerExists"\
        ,"basis.domName AS domName"\
        ,"basis.position AS position"\
        ,"basis.dataType AS dataType")
        
        app_SAP_L0_TMP_TransformCurrencyReachability=\
            app_SAP_L0_TMP_TransformCurrencyLinkedColumn.filter(expr("KEYFLAG = 'X'"))\
            .groupBy("tableName","V_tableName")\
            .agg(expr("count(*)"))
        
        app_SAP_L0_TMP_TransformCurrencyReachability=\
            app_SAP_L0_TMP_TransformCurrencyLinkedColumn.filter(expr("KEYFLAG = 'X'"))\
            .groupBy("tableName","V_tableName")\
            .agg(expr("count(*)").alias("wholeCount")\
                 ,expr("CASE WHEN (MIN(partnerExists) = MAX(partnerExists) \
                              AND MAX(partnerExists)  ='X')\
                             THEN 'X' ELSE '' END AS reachableYN")\
                )\
             .filter(expr("MIN(partnerExists)=MAX(partnerExists) AND MAX(partnerExists)='X'"))\
             .selectExpr("v_tableName as tableStart","tableName as tableEnd","reachableYN","wholeCount")\
             
        app_SAP_L0_TMP_TransformCurrencyTableJoinList=\
        app_SAP_L0_TMP_TransformCurrencyLinkedColumn.alias("lnk")\
        .join(app_SAP_L0_TMP_TransformCurrencyReachability.alias("reach"),\
             (col("reach.tableStart")==col("lnk.V_tableName"))\
            &(col("reach.tableEnd")==col("lnk.tableName")),"inner").orderBy(col("lnk.position"))
        app_SAP_L0_TMP_TransformCurrencyTableJoinList=\
        app_SAP_L0_TMP_TransformCurrencyTableJoinList.groupBy("V_tableName","tableName")\
            .agg(concat_ws(' ',collect_list(concat(lit(' AND ['), trim(col("tableName")), \
                                                   lit('].['),trim(col("fieldName")),lit('] = ['),\
                                                   trim(col("v_tableName")),lit('].['),trim(col("V_FIELDNAME")),\
                                                   lit(']')))).alias("sql"))\
            .select(trim(col("v_tableName")).alias("tableStart"),trim(col("tableName"))\
                 .alias("tableEnd"),col("sql"))
        app_SAP_L0_TMP_TransformCurrencyTableJoinList=\
            app_SAP_L0_TMP_TransformCurrencyTableJoinList\
                          .withColumn("joinString",concat(lit('JOIN [erp].['),col("tableEnd"),\
                                                          lit('] ON ('),col("sql").substr(lit(6),\
                                                          length(col("sql"))),lit(')')))\
                          .drop(col("sql"))
        
        staticJoinsNotPresentin_JoinList=\
        knw_LK_GD_CurrencyShiftingStaticJoins.alias("S")\
            .join(app_SAP_L0_TMP_TransformCurrencyTableJoinList.alias("J"),\
                  (col("S.tableStart")==col("J.tableStart"))\
                 &(col("S.tableEnd")==col("J.tableEnd")),"leftanti")\
            .selectExpr("tableStart","tableEnd","joinString")
        app_SAP_L0_TMP_TransformCurrencyTableJoinList=\
            app_SAP_L0_TMP_TransformCurrencyTableJoinList\
             .union(staticJoinsNotPresentin_JoinList)
        
        staticJoinsNotpresentInCurrencyReachability=\
                     knw_LK_GD_CurrencyShiftingStaticJoins.alias("cssj")\
        			.join(app_SAP_L0_TMP_TransformCurrencyReachability.alias("R"),\
        			 (col("cssj.tableStart")==col("R.tableStart"))\
        			&(col("cssj.tableEnd")==col("R.tableEnd")),"leftanti")\
        		     .select(col("cssj.tableStart").alias("tableStart")\
                            ,col("cssj.tableEnd").alias("tableEnd")\
                            ,col("cssj.isAccessible").alias("reachableYN")\
                            ,col("cssj.totalCount").alias("wholeCount"))
        app_SAP_L0_TMP_TransformCurrencyReachability=\
            app_SAP_L0_TMP_TransformCurrencyReachability\
              .union(staticJoinsNotpresentInCurrencyReachability)
        
        reach1= app_SAP_L0_TMP_TransformCurrencyReachability.alias("a")\
                               .join(app_SAP_L0_TMP_TransformCurrencyReachability.alias("b"),
                                    col("a.tableEnd")==col("b.tableStart"),"inner")\
                               .selectExpr("a.tableStart as a_tableStart",
                                           "a.tableEnd as a_tableEnd","b.tableEnd as b_tableEnd",\
                                           "a.wholeCount+b.wholeCount as wholeCount")
        reach1=reach1.groupBy("a_tableStart","a_tableEnd","b_tableEnd")\
                     .agg(expr("SUM(wholeCount)").alias("wholeCount"))\
                     .filter(expr("a_tableStart<>b_tableEnd"))\
                     .selectExpr("a_tableStart as tableStart",\
                                 "a_tableEnd AS stopPosition1",\
                                 "cast(NULL as varchar(128)) as stopPosition2",\
                                 "cast(NULL as varchar(128)) as stopPosition3",\
                                 "b_tableEnd as tableEnd","wholeCount")
        
        reach2= app_SAP_L0_TMP_TransformCurrencyReachability.alias("a")\
                               .join(app_SAP_L0_TMP_TransformCurrencyReachability.alias("b"),
                                    col("a.tableEnd")==col("b.tableStart"),"inner")\
                               .join(app_SAP_L0_TMP_TransformCurrencyReachability.alias("c"),
                                    col("b.tableEnd")==col("c.tableStart"),"inner")\
                               .selectExpr("a.tableStart as a_tableStart",\
                                           "a.tableEnd as a_tableEnd",\
                                           "b.tableEnd as b_tableEnd",\
                                           "c.tableEnd as c_tableEnd",\
                                           "a.wholeCount+b.wholeCount+c.wholeCount as wholeCount")
        reach2=reach2.groupBy("a_tableStart","a_tableEnd","b_tableEnd","c_tableEnd")\
                     .agg(expr("SUM(wholeCount)").alias("wholeCount"))\
                     .filter(expr("a_tableStart <> b_tableEnd \
                               AND a_tableStart <> c_tableEnd \
                               AND a_tableEnd	<> c_tableEnd"))\
                     .selectExpr("a_tableStart as tableStart",\
                                 "a_tableEnd AS stopPosition1",\
                                 "b_tableEnd as stopPosition2",\
                                 "cast(NULL as varchar(128)) as stopPosition3",\
                                 "c_tableEnd as tableEnd","wholeCount")
        
        reach3= app_SAP_L0_TMP_TransformCurrencyReachability.alias("a")\
                               .join(app_SAP_L0_TMP_TransformCurrencyReachability.alias("b"),
                                    col("a.tableEnd")==col("b.tableStart"),"inner")\
                               .join(app_SAP_L0_TMP_TransformCurrencyReachability.alias("c"),
                                    col("b.tableEnd")==col("c.tableStart"),"inner")\
                                .join(app_SAP_L0_TMP_TransformCurrencyReachability.alias("d"),
                                    col("c.tableEnd")==col("d.tableStart"),"inner")\
                               .selectExpr("a.tableStart as a_tableStart",\
                                           "a.tableEnd as a_tableEnd",\
                                           "b.tableEnd as b_tableEnd",\
                                           "c.tableEnd as c_tableEnd",\
                                           "d.tableEnd as d_tableEnd",\
                                           "a.wholeCount+b.wholeCount+c.wholeCount+ d.wholeCount as wholeCount")
        reach3=reach3.groupBy("a_tableStart","a_tableEnd","b_tableEnd","c_tableEnd","d_tableEnd")\
                     .agg(expr("SUM(wholeCount)").alias("wholeCount"))\
                     .filter(expr("a_tableStart <> b_tableEnd \
                               AND a_tableStart <> c_tableEnd \
                               AND a_tableStart <> d_tableEnd \
                               AND a_tableEnd	<> c_tableEnd\
                               AND a_tableEnd	<> d_tableEnd\
                               AND b_tableEnd	<> c_tableEnd\
                               AND b_tableEnd	<> d_tableEnd" ))\
                     .selectExpr("a_tableStart as tableStart",\
                                 "a_tableEnd AS stopPosition1",\
                                 "b_tableEnd as stopPosition2",\
                                 "c_tableEnd as stopPosition3",\
                                 "d_tableEnd as tableEnd","wholeCount")
        
        app_SAP_L0_TMP_TransformCurrencyTableConnectionGraph_Prepare=app_SAP_L0_TMP_TransformCurrencyReachability\
                   .selectExpr("tableStart",\
                               "cast(NULL as varchar(128)) as stopPosition1",\
                               "cast(NULL AS varchar(128)) as stopPosition2",\
                               "cast(NULL AS varchar(128)) as stopPosition3",\
                             "tableEnd","wholeCount")
        app_SAP_L0_TMP_TransformCurrencyTableConnectionGraph_Prepare=\
            app_SAP_L0_TMP_TransformCurrencyTableConnectionGraph_Prepare.\
                union(reach1).union(reach2).union(reach3)
        
        app_SAP_L0_TMP_TransformCurrencyMinJoinFilter=\
            app_SAP_L0_TMP_TransformCurrencyTableConnectionGraph_Prepare\
            .groupBy("tableStart","tableEnd")\
            .agg(expr("MIN(wholeCount)").alias("minCount"))
        
        app_SAP_L0_TMP_TransformCurrencyTableConnections_Prepare=\
            app_SAP_L0_TMP_TransformCurrencyTableConnectionGraph_Prepare.alias("stamm")\
        			.join(app_SAP_L0_TMP_TransformCurrencyMinJoinFilter.alias("filter"),\
                      (col("stamm.tableStart")== col("filter.tableStart"))\
                     &(col("stamm.tableEnd")== col("filter.tableEnd"))\
                     &(col("stamm.wholeCount")== col("filter.minCount")),"inner")\
                .join(app_SAP_L0_TMP_TransformCurrencyTableJoinList.alias("join0"),\
                       (col("join0.tableStart")== col("stamm.tableStart"))\
                       &(col("join0.tableEnd")== col("stamm.tableEnd")),"left")\
                .join(app_SAP_L0_TMP_TransformCurrencyTableJoinList.alias("join1"),\
                       (col("join1.tableStart")== col("stamm.tableStart"))\
                       &(col("join1.tableEnd")== col("stamm.stopPosition1")),"left")\
                 .join(app_SAP_L0_TMP_TransformCurrencyTableJoinList.alias("join2"),\
                       (col("join2.tableStart")== col("stamm.stopPosition1"))\
                       &(col("join2.tableEnd")== col("stamm.tableEnd")),"left")\
                 .join(app_SAP_L0_TMP_TransformCurrencyTableJoinList.alias("join3"),\
                       (col("join3.tableStart")== col("stamm.stopPosition1"))\
                       &(col("join3.tableEnd")== col("stamm.stopPosition2")),"left")\
                 .join(app_SAP_L0_TMP_TransformCurrencyTableJoinList.alias("join4"),\
                       (col("join4.tableStart")== col("stamm.stopPosition2"))\
                       &(col("join4.tableEnd")== col("stamm.tableEnd")),"left")\
                 .join(app_SAP_L0_TMP_TransformCurrencyTableJoinList.alias("join5"),\
                       (col("join5.tableStart")== col("stamm.stopPosition2"))\
                       &(col("join5.tableEnd")== col("stamm.stopPosition3")),"left")\
                 .join(app_SAP_L0_TMP_TransformCurrencyTableJoinList.alias("join6"),\
                       (col("join6.tableStart")== col("stamm.stopPosition3"))\
                       &(col("join6.tableEnd")== col("stamm.tableEnd")),"left")\
        .selectExpr(\
          "stamm.tableStart    AS tableStart"\
          ,"stamm.stopPosition1 AS stopPosition1"\
          ,"stamm.stopPosition2 AS stopPosition2"\
          ,"stamm.stopPosition3 AS stopPosition3"\
          ,"stamm.tableEnd      AS tableEnd"\
          ,"concat(stamm.tableStart ,\
             CASE WHEN stamm.stopPosition1 IS NOT NULL THEN concat(' - ' , stamm.stopPosition1) ELSE '' END\
            ,CASE WHEN stamm.stopPosition2 IS NOT NULL THEN concat(' - ' , stamm.stopPosition2) ELSE '' END\
            ,CASE WHEN stamm.stopPosition3 IS NOT NULL THEN concat(' - ' , stamm.stopPosition3) ELSE '' END\
            ,concat(' - ',stamm.tableEnd)) AS joinChain"\
          ,"concat(CASE WHEN join0.joinString IS NOT NULL THEN concat('   ',join0.joinString) ELSE '' END \
                  ,CASE WHEN join1.joinString IS NOT NULL THEN concat('   ',join1.joinString) ELSE '' END \
                  ,CASE WHEN join2.joinString IS NOT NULL THEN concat('   ',join2.joinString) ELSE '' END \
                  ,CASE WHEN join3.joinString IS NOT NULL THEN concat('   ',join3.joinString) ELSE '' END \
                  ,CASE WHEN join4.joinString IS NOT NULL THEN concat('   ',join4.joinString) ELSE '' END \
                  ,CASE WHEN join5.joinString IS NOT NULL THEN concat('   ',join5.joinString) ELSE '' END \
                  ,CASE WHEN join6.joinString IS NOT NULL THEN concat('   ',join6.joinString) ELSE '' END)\
            as joinQuery")
        
        joinQueryEnd=concat(lit(' JOIN [erp].[TCURX] ON ([TCURX].[CURRKEY] = ['),\
                            trim(col("defs.currencyTableName")),lit('].['),\
                            trim(col("defs.currencyFieldName")),lit('])'))
        joinQueryStart=when(col("conns.joinQuery").isNull(),lit(''))\
                       .otherwise(col("conns.joinQuery"))
        app_SAP_L0_TMP_TransformCurrencyQueryComponent=\
            app_SAP_L0_TMP_TransformCurrencyFilteredTablesAndColumns.alias("defs")\
            .join(app_SAP_L0_TMP_TransformCurrencyTableConnections_Prepare.alias("conns"),\
        	 (col("defs.tableName")==col("conns.tableStart"))\
        	&(col("defs.currencyTableName")==col("conns.tableEnd")),"left")\
           .select(col("defs.tableName"),col("defs.fieldName"),\
                   col("defs.currencyTableName"),col("defs.currencyFieldName"),\
                   concat(lit(joinQueryStart),lit(joinQueryEnd))\
                   .alias("joinComponent"))
        
        app_L0_STG_DecimalPointShiftingQueryList=\
            app_SAP_L0_TMP_TransformCurrencyQueryComponent\
               .filter(col("tableName").isin(gl_ls_app_SAP_L0_TMP_ERPTablesRequiredForSelectedAnalytics))\
               .groupBy("tableName","currencyTableName","currencyFieldName","joinComponent")\
               .agg(array_sort(collect_list("fieldName")).alias("fieldNames"))
        app_L0_STG_DecimalPointShiftingQueryList.createOrReplaceTempView("app_L0_STG_DecimalPointShiftingQueryList")
        spark.sql("select *,row_number() over(partition by tableName order by fieldNames) \
                as updateNum from app_L0_STG_DecimalPointShiftingQueryList")\
            .createOrReplaceTempView("app_L0_STG_DecimalPointShiftingQueryList")
        spark.sql("select *,concat_ws(',',collect_list(updateNum) over(partition by tableName))  \
            as isFullyShifted from app_L0_STG_DecimalPointShiftingQueryList")\
        .createOrReplaceTempView("app_L0_STG_DecimalPointShiftingQueryList")
        sqlContext.cacheTable("app_L0_STG_DecimalPointShiftingQueryList")
        lsTablesNames=app_L0_STG_DecimalPointShiftingQueryList.select(col("tableName")).distinct().collect()
        if lsTablesNames:
            executionStatus = 'Metadata preparation completed for decimal point shifting.'
        else:
            executionStatus = 'DPS Metadata is empty. So no list of tables on which to perform decimal point shifting.'
        executionStatusID = LOG_EXECUTION_STATUS.SUCCESS   
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)
        return [executionStatusID,executionStatus,
                lsTablesNames,
                app_L0_STG_DecimalPointShiftingQueryList] 

    except Exception as err:
        executionStatus = objGenHelper.gen_exceptionDetails_log()       
        executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
        return [LOG_EXECUTION_STATUS.FAILED,executionStatus,None] 


