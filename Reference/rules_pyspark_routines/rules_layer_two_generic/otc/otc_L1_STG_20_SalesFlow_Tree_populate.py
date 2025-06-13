# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import expr,element_at,col,size,array_union,array_contains,struct
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType
from pyspark.sql.functions import expr
from delta.tables import *

def otc_L1_STG_20_SalesFlow_Tree_populate():
  try:
        objGenHelper = gen_genericHelper()
        objDataTransformation = gen_dataTransformation()
        logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
        
        SFTDeltaPath=gl_Layer2Staging+"/L1_STG_20_SalesFlow_Tree/"
        objGenHelper.gen_Directory_clear(SFTDeltaPath)
        
        
        clientProcessID_OTC='1'
        unknownDocTypeClient='%'
        includeBranch	    =1	
        includeParentBranch =1
        includeTree		    =1
        includeTreeParent	=1
        maxChildID			=0
        
        erpSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID')
        startDate = parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'START_DATE')).date()
        endDate = parse(objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'END_DATE')).date()

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181115_0922
        sourceTypeSchema =StructType([StructField("sourceTypeID",IntegerType(),True),\
                                     StructField("sourceTypeName",StringType(),True),\
                                     StructField("isArtificial",IntegerType(),True)])
        hierarchyTypeSchema =StructType([StructField("hierarchyTypeID",IntegerType(),True),\
                                     StructField("hierarchyTypeName",StringType(),True)])
        linkTypeSchema =StructType([StructField("linkTypeID",IntegerType(),True),\
                                     StructField("linkTypeName",StringType(),True)])
        ls_SourceType=[(1,'Order',0)\
                    ,(2,'Shipping',0)\
                    ,(3,'Billing',0)\
                    ,(4,'Artificial | 1st Hierarchy',1)\
                    ,(5,'Artificial | 2nd Hierarchy',1)\
                    ,(6,'Artificial | 3rd Hierarchy',1)\
                    ,(7,'Artificial | External',1)\
                    ,(8,'Purchase Order',0)]

        ls_HierarchyType=[(1,'2nd Hierarchy')
                    ,(2,'3rd Hierarchy')
                    ,(3,'2nd | 3rd Hierarchy')]
        ls_LinkType=[ (0,'NONE')
                    ,(1,'2nd Hierarchy')
                    ,(2,'3rd Hierarchy')]
        otc_L1_STG_20_SalesFlowSourceType=spark.createDataFrame(data=ls_SourceType,schema=sourceTypeSchema)
        otc_L1_TMP_20_HierarchyType=spark.createDataFrame(data=ls_HierarchyType,schema=hierarchyTypeSchema)
        otc_L1_TMP_20_LinkType=spark.createDataFrame(data=ls_LinkType,schema=linkTypeSchema)

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20190118_0944
        otc_L1_TMP_26_SalesFlowInitTreeDistinctSalesDocumentTypesWithDescription=\
        knw_vw_LK_GD_ClientKPMGDocumentType\
        .selectExpr("documentTypeClientID as documentTypeClientID"\
        ,"documentTypeClient as documentTypeClient"\
        ,"documentTypeKPMGShort as documentTypeKPMGShort"\
        ,"documentTypeKPMG as documentTypeKPMG"\
        ,"documentTypeKPMGInterpreted as documentTypeKPMGInterpreted"\
        ,"documentTypeKPMGGroup as documentTypeKPMGGroup"\
        ,"documentTypeKPMGGroupID as documentTypeKPMGGroupID"\
        ,"documentTypeKPMGShortID as documentTypeKPMGShortID"\
        ,"DENSE_RANK() OVER (ORDER BY lower(documentTypeKPMGInterpreted) ) as documentTypeKPMGInterpretedID"\
        ,"documentTypeKPMGID as documentTypeKPMGID"\
        ,"documentTypeKPMGGroupOrder as documentTypeKPMGGroupOrder"\
        ,"documentTypeKPMGOrderOTC as documentTypeKPMGOrderOTC"\
        ,"ROW_NUMBER()OVER(ORDER BY (SELECT NULL)) as rowNum")

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181119_0917
        dfBilling=\
        otc_L1_TD_Billing.alias("billing")\
        .join(otc_L1_STG_20_SalesFlowSourceType.alias("srctyp"),\
        expr("( srctyp.sourceTypeName = 'Billing' )"),"inner")\
        .selectExpr("NULL as ID"\
        ,"NULL as parentID"\
        ,"billing.billingDocumentCompanyCode as companyCode"\
        ,"billing.billingDocumentNumber as documentNumber"\
        ,"billing.billingDocumentLineItem as documentLineItem"\
        ,"billing.billingDocumentTypeClient as documentTypeClient"\
        ,"billing.billingDocumentPredecessorDocumentNumber as predecessorDocumentNumber"\
        ,"billing.billingDocumentPredecessorDocumentLineItem as predecessorDocumentLineItem"\
        ,"billing.billingDocumentPredecessorDocumentTypeClient as predecessorDocumentTypeClient"\
        ,"billing.billingDocumentPredecessorOrderingDocumentNumber as predecessorOrderingDocumentNumber"\
        ,"(CASE WHEN ISNULL(billing.billingDocumentPredecessorOrderingDocumentLineItem) THEN '000000' ELSE billing.billingDocumentPredecessorOrderingDocumentLineItem END) as predecessorOrderingDocumentLineItem"\
        ,"billing.billingDocumentPredecessorOrderingDocumentTypeClient as predecessorOrderingDocumentTypeClient"\
        ,"billing.billingDocumentOriginatingDocumentNumber as originatingDocumentNumber"\
        ,"billing.billingDocumentOriginatingDocumentLineItem as originatingDocumentLineItem"\
        ,"billing.billingDocumentLineItemHigherbom as lineItemHigherBOM"\
        ,"billing.billingDocumentProduct as documentProduct"\
        ,"billing.billingDocumentProductArtificialID as productArtificialID"\
        ,"NULL as documentProduct2"\
        ,"NULL as predecessorOrderingDocumentNumberHeader"\
        ,"NULL as predecessorOrderingDocumentTypeHeader"\
        ,"srctyp.sourceTypeID as sourceTypeID"\
        ,"billing.billingDocumentTypeClientID as documentTypeClientID")

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181119_0923
        dfShipping=\
        otc_L1_TD_SalesShipment.alias("shipping")\
        .join(otc_L1_STG_20_SalesFlowSourceType.alias("st"),\
        expr("( st.sourceTypeName = 'Shipping' )"),"inner")\
        .selectExpr("NULL as ID"\
        ,"NULL as parentID"\
        ,"shipping.shippingDocumentCompanycode as companyCode"\
        ,"shipping.shippingDocumentNumber as documentNumber"\
        ,"shipping.shippingDocumentLineItem as documentLineItem"\
        ,"shipping.shippingDocumentTypeClient as documentTypeClient"\
        ,"shipping.shippingDocumentPredecessorDocumentNumber as predecessorDocumentNumber"\
        ,"shipping.shippingDocumentPredecessorDocumentLineItem as predecessorDocumentLineItem"\
        ,"shipping.shippingDocumentPredecessorDocumentTypeClient as predecessorDocumentTypeClient"\
        ,"NULL as predecessorOrderingDocumentNumber"\
        ,"NULL as predecessorOrderingDocumentLineItem"\
        ,"NULL as predecessorOrderingDocumentTypeClient"\
        ,"NULL as originatingDocumentNumber"\
        ,"NULL as originatingDocumentLineItem"\
        ,"shipping.shippingDocumentLineItemHigherBatch as lineItemHigherBOM"\
        ,"shipping.shippingDocumentProduct as documentProduct"\
        ,"shipping.shippingDocumentProductArtificialID as productArtificialID"\
        ,"NULL as documentProduct2"\
        ,"NULL as predecessorOrderingDocumentNumberHeader"\
        ,"NULL as predecessorOrderingDocumentTypeHeader"\
        ,"st.sourceTypeID as sourceTypeID"\
        ,"shipping.shippingDocumentTypeClientID as documentTypeClientID")

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181119_0934
        dfOrder=\
        otc_L1_TD_SalesOrder.alias("ordering")\
        .join(otc_L1_STG_20_SalesFlowSourceType.alias("st"),\
        expr("( st.sourceTypeName = 'Order' )"),"inner")\
        .selectExpr("NULL as ID"\
        ,"NULL as parentID"\
        ,"ordering.orderingDocumentCompanyCode as companyCode"\
        ,"ordering.orderingDocumentNumber as documentNumber"\
        ,"ordering.orderingDocumentLineItem as documentLineItem"\
        ,"ordering.orderingDocumentTypeClient as documentTypeClient"\
        ,"ordering.orderingDocumentPredecessorDocumentNumber as predecessorDocumentNumber"\
        ,"ordering.orderingDocumentPredecessorDocumentLineItem as predecessorDocumentLineItem"\
        ,"ordering.orderingDocumentPredecessorDocumentTypeClient as predecessorDocumentTypeClient"\
        ,"NULL as predecessorOrderingDocumentNumber"\
        ,"NULL as predecessorOrderingDocumentLineItem"\
        ,"NULL as predecessorOrderingDocumentTypeClient"\
        ,"ordering.orderingDocumentOriginatingDocumentNumber as originatingDocumentNumber"\
        ,"ordering.orderingDocumentOriginatingDocumentLineItem as originatingDocumentLineItem"\
        ,"ordering.orderingDocumentLineItemHigherBOM as lineItemHigherBOM"\
        ,"ordering.orderingDocumentProduct as documentProduct"\
        ,"ordering.orderingDocumentProductArtificialID as productArtificialID"\
        ,"NULL as documentProduct2"\
        ,"ordering.orderingDocumentPredecessorDocumentNumberHeader as predecessorOrderingDocumentNumberHeader"\
        ,"ordering.orderingDocumentPredecessorDocumentTypeHeader as predecessorOrderingDocumentTypeHeader"\
        ,"st.sourceTypeID as sourceTypeID"\
        ,"ordering.orderingDocumentTypeClientID as documentTypeClientID")



        dfUnion=dfBilling.union(dfShipping).union(dfOrder)

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181119_0906
        otc_L1_TMP_21_InitChainBaseChain=\
        dfUnion\
        .selectExpr("row_number() OVER (ORDER BY (SELECT NULL)) as childID"\
        ,"cast(ID as int) as  ID"\
        ,"cast(parentID as int) as parentID"\
        ,"companyCode as companyCode"\
        ,"documentNumber as documentNumber"\
        ,"documentLineItem as documentLineItem"\
        ,"documentTypeClient as documentTypeClient"\
        ,"NULLIF(predecessorDocumentNumber, '') as predecessorDocumentNumber"\
        ,"predecessorDocumentLineItem as predecessorDocumentLineItem"\
        ,"predecessorDocumentTypeClient as predecessorDocumentTypeClient"\
        ,"NULLIF(predecessorOrderingDocumentNumber, '') as predecessorOrderingDocumentNumber"\
        ,"predecessorOrderingDocumentLineItem as predecessorOrderingDocumentLineItem"\
        ,"predecessorOrderingDocumentTypeClient as predecessorOrderingDocumentTypeClient"\
        ,"NULLIF(originatingDocumentNumber, '') as originatingDocumentNumber"\
        ,"originatingDocumentLineItem as originatingDocumentLineItem"\
        ,"(CASE WHEN ISNULL(lineItemHigherBOM) THEN '000000' ELSE lineItemHigherBOM END) as lineItemHigherBOM"\
        ,"documentProduct as documentProduct"\
        ,"productArtificialID as productArtificialID"\
        ,"cast(documentProduct2 as string) as documentProduct2"\
        ,"predecessorOrderingDocumentNumberHeader as predecessorOrderingDocumentNumberHeader"\
        ,"predecessorOrderingDocumentTypeHeader as predecessorOrderingDocumentTypeHeader"\
        ,"sourceTypeID as sourceTypeID"\
        ,"documentTypeClientID as documentTypeClientID")

        
        otc_L1_TMP_21_InitChainBaseChain.write.format("delta").mode("overwrite")\
        .save(SFTDeltaPath+"otc_L1_TMP_21_InitChainBaseChain.delta")
        otc_L1_TMP_21_InitChainBaseChain=spark.read.format("delta")\
        .load(SFTDeltaPath+"otc_L1_TMP_21_InitChainBaseChain.delta")

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181119_1059
        if otc_L1_TMP_21_InitChainBaseChain.first() is not None:
          maxChildID=otc_L1_TMP_21_InitChainBaseChain.groupBy().agg(expr("max(childID) as maxChildID")).select("maxChildID").collect()[0][0]
        else:
          maxChildID=0
        maxChildID=str(maxChildID)

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181119_1109
        y=\
        otc_L1_TMP_21_InitChainBaseChain.alias("bc")\
        .join((otc_L1_TMP_21_InitChainBaseChain\
        .select("documentNumber","documentLineItem")).alias("x"),\
        expr("( x.documentNumber = bc.predecessorDocumentNumber\
         AND x.documentLineItem = bc.predecessorDocumentLineItem )"),"left")\
        .filter(expr("bc.predecessorDocumentNumber IS NOT NULL \
         AND x.documentNumber IS NULL"))\
        .groupBy("bc.predecessorDocumentNumber"\
        ,"bc.predecessorDocumentLineItem")\
        .agg(expr("min(predecessorDocumentTypeClient) as documentTypeClient")\
        ,expr("count(DISTINCT(predecessorDocumentTypeClient)) as numberOfVBTYPs")\
        ,expr("min(documentProduct) as documentProduct")\
        ,expr("min(productArtificialID) as productArtificialID")\
        ,expr("count(*) as numberOfChildren"))\
        .selectExpr("predecessorDocumentNumber     as documentNumber"\
        ,"predecessorDocumentLineItem   as documentLineItem"\
        ,"documentTypeClient","numberOfVBTYPs","documentProduct","productArtificialID","numberOfChildren")

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181119_1102
        otc_L1_TMP_21_InitChainBaseChain_Artificial_1stHierarchy=\
        y.alias("y")\
        .join(otc_L1_STG_20_SalesFlowSourceType.alias("st"),\
        expr("( st.sourceTypeName = 'Artificial | 1st Hierarchy' ) "),"inner")\
        .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias("docType"),\
        expr("( y.documentTypeClient  = docType.documentTypeClient \
         AND docType.documentTypeClientProcessID = "+clientProcessID_OTC+"\
         AND docType.isReversed = 0\
         AND docType.ERPSystemID = '1' ) "),"left")\
        .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias("docType1"),\
        expr("( docType1.documentTypeClient = '"+unknownDocTypeClient+"'\
         AND docType1.documentTypeClientProcessID = "+clientProcessID_OTC+"\
         AND docType1.isReversed = 0\
         AND docType1.ERPSystemID = '1' )"),"left")\
        .selectExpr("row_number() OVER (ORDER BY (SELECT NULL))+ "+maxChildID+" as childID"\
        ,"documentNumber as documentNumber"\
        ,"documentLineItem as documentLineItem"\
        ,"CASE WHEN numberOfVBTYPs = 1 THEN y.documentTypeClient ELSE '%' END as documentTypeClient"\
        ,"CASE WHEN numberOfChildren = 1 THEN documentProduct ELSE NULL END as documentProduct"\
        ,"CASE WHEN numberOfChildren = 1 THEN productArtificialID ELSE NULL END as productArtificialID"\
        ,"st.sourceTypeID as sourceTypeID"\
        ,"CASE WHEN numberOfVBTYPs = 1 THEN docType.documentTypeClientID ELSE docType1.documentTypeClientID END as documentTypeClientID")

        otc_L1_TMP_21_InitChainBaseChain_Artificial_1stHierarchy.write.format("delta").mode("append")\
        .save(SFTDeltaPath+"otc_L1_TMP_21_InitChainBaseChain.delta")

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181119_1427
        otc_L1_TMP_21_InitChainBaseChainFirstHierarchyRelation=\
        otc_L1_TMP_21_InitChainBaseChain.alias("a")\
        .join(otc_L1_TMP_21_InitChainBaseChain.alias("b"),\
        expr("a.predecessorDocumentNumber = b.documentNumber\
         AND a.predecessorDocumentLineItem = b.documentLineItem "),"inner")\
        .join(otc_L1_STG_20_SalesFlowSourceType.alias("c"),\
        expr("b.sourceTypeID = c.sourceTypeID"),"inner")\
        .filter(expr("c.sourceTypeName = 'Artificial | 1st Hierarchy'"))\
        .selectExpr("b.documentNumber as documentNumber"\
        ,"b.documentLineItem as documentLineItem"\
        ,"b.documentTypeClient as documentTypeClient"\
        ,"a.predecessorOrderingDocumentNumber as predecessorOrderingDocumentNumber"\
        ,"a.predecessorOrderingDocumentLineItem as predecessorOrderingDocumentLineItem"\
        ,"a.predecessorOrderingDocumentTypeClient as predecessorOrderingDocumentTypeClient"\
        ,"a.companyCode as companyCode"\
        ,"concat(ltrim((CASE WHEN ISNULL(a.predecessorOrderingDocumentNumber) THEN '' ELSE a.predecessorOrderingDocumentNumber END)),ltrim((CASE WHEN ISNULL(a.predecessorOrderingDocumentLineItem) THEN '' ELSE a.predecessorOrderingDocumentLineItem END)),ltrim((CASE WHEN ISNULL(a.predecessorOrderingDocumentTypeClient) THEN '' ELSE a.predecessorOrderingDocumentTypeClient END)))	AS predecessorOrderingDocumentNumberFull").distinct()

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181119_1606
        otc_L1_TMP_21_ArtificialDocumentsWithSinglePredecessorOrderingDocument=\
        otc_L1_TMP_21_InitChainBaseChainFirstHierarchyRelation\
        .filter(expr("predecessorOrderingDocumentNumberFull <>''"))\
        .groupBy("documentNumber"\
        ,"documentLineItem")\
        .agg(expr("count(DISTINCT predecessorOrderingDocumentNumberFull) AS countOfpredecessorOrderingDocuments"))\
        .filter("countOfpredecessorOrderingDocuments=1")

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181119_1755
        otc_L1_TMP_21_InitChainBaseChain_Ordering_upd=\
        otc_L1_TMP_21_InitChainBaseChain.alias("a")\
        .join(otc_L1_TMP_21_ArtificialDocumentsWithSinglePredecessorOrderingDocument.alias("dwspod"),\
        expr("a.documentNumber = dwspod.documentNumber\
         AND a.documentLineItem = dwspod.documentLineItem "),"inner")\
        .join(otc_L1_TMP_21_InitChainBaseChainFirstHierarchyRelation.alias("cbcfhr"),\
        expr("a.documentNumber = cbcfhr.documentNumber\
         AND a.documentLineItem = cbcfhr.documentLineItem "),"inner")\
        .join(otc_L1_STG_20_SalesFlowSourceType.alias("d"),\
        expr("a.sourceTypeID = d.sourceTypeID and d.sourceTypeName='Artificial | 1st Hierarchy'"),"inner")\
        .selectExpr("cbcfhr.predecessorOrderingDocumentNumber"\
        ,"cbcfhr.predecessorOrderingDocumentLineItem"\
        ,"cbcfhr.predecessorOrderingDocumentTypeClient"\
        ,"a.documentNumber"\
        ,"a.documentLineItem")


        otc_L1_TMP_21_InitChainBaseChain_Delta = DeltaTable.forPath\
                                                (spark, SFTDeltaPath+"otc_L1_TMP_21_InitChainBaseChain.delta")
        otc_L1_TMP_21_InitChainBaseChain_Delta.alias("del")\
                                .merge(source=otc_L1_TMP_21_InitChainBaseChain_Ordering_upd.alias("upd"),condition="del.documentNumber = upd.documentNumber\
                                and del.documentLineItem = upd.documentLineItem") \
                                .whenMatchedUpdate(set = {"predecessorDocumentNumber": "upd.predecessorOrderingDocumentNumber"\
                                                          ,"predecessorDocumentLineItem": "upd.predecessorOrderingDocumentLineItem"\
                                                          ,"predecessorDocumentTypeClient": "upd.predecessorOrderingDocumentTypeClient"}).execute() 


        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181120_1103
        if otc_L1_TMP_21_InitChainBaseChain.first() is not None:
          maxChildID=otc_L1_TMP_21_InitChainBaseChain.groupBy().agg(expr("max(childID) as maxChildID")).select("maxChildID").collect()[0][0]
        else:
          maxChildID=0
        maxChildID=str(maxChildID)
        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181120_1106
        y=\
        otc_L1_TMP_21_InitChainBaseChain.alias("bc")\
        .join((otc_L1_TMP_21_InitChainBaseChain\
        .select("documentNumber","documentLineItem","documentTypeClientID")).alias("x"),\
        expr("( x.documentNumber = bc.predecessorOrderingDocumentNumber\
         AND x.documentLineItem = bc.predecessorOrderingDocumentLineItem )"),"left")\
        .filter(expr("bc.predecessorOrderingDocumentNumber IS NOT NULL \
         AND x.documentNumber IS NULL"))\
        .groupBy("predecessorOrderingDocumentNumber"\
        ,"predecessorOrderingDocumentLineItem")\
        .agg(expr("count(*) as numberOfChildren")\
        ,expr("min(bc.predecessorOrderingDocumentTypeClient) as documentTypeClient")\
        ,expr("count(DISTINCT(bc.predecessorOrderingDocumentTypeClient)) as numberOfAUTYPs")\
        ,expr("min(bc.documentProduct) as documentProduct")\
        ,expr("min(bc.productArtificialID) as productArtificialID"))\
        .selectExpr("predecessorOrderingDocumentNumber     as documentNumber"\
        ,"predecessorOrderingDocumentLineItem   as documentLineItem"\
        ,"documentTypeClient","numberOfAUTYPs","documentProduct","productArtificialID","numberOfChildren")

        otc_L1_TMP_21_InitChainBaseChain_Artificial_2ndHierarchy=\
        y.alias("y")\
        .join(otc_L1_STG_20_SalesFlowSourceType.alias("st"),\
        expr("st.sourceTypeName = 'Artificial | 2nd Hierarchy' "),"inner")\
        .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias("docType"),\
        expr("( y.documentTypeClient = docType.documentTypeClient\
         AND docType.documentTypeClientProcessID = "+clientProcessID_OTC+"\
         AND docType.isReversed = 0\
         AND docType.ERPSystemID = '1' ) "),"left")\
        .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias("docType1"),\
        expr("( docType1.documentTypeClient = '"+unknownDocTypeClient+"'\
         AND docType1.documentTypeClientProcessID = "+clientProcessID_OTC+"\
         AND docType1.isReversed = 0\
         AND docType1.ERPSystemID = '1' )"),"left")\
        .selectExpr("row_number() OVER (ORDER BY (SELECT NULL))+ "+maxChildID+" as childID"\
        ,"documentNumber as documentNumber"\
        ,"documentLineItem as documentLineItem"\
        ,"y.documentTypeClient as documentTypeClient"\
        ,"CASE WHEN numberOfAUTYPs = 1 THEN NULL ELSE '%' END as predecessorOrderingDocumentTypeClient"\
        ,"CASE WHEN numberOfChildren = 1 THEN y.documentProduct ELSE NULL END as documentProduct"\
        ,"CASE WHEN numberOfChildren = 1 THEN y.productArtificialID ELSE NULL END as productArtificialID"\
        ,"st.sourceTypeID as sourceTypeID"\
        ,"CASE WHEN numberOfAUTYPs = 1 THEN docType.documentTypeClientID ELSE docType1.documentTypeClientID END as documentTypeClientID")

        otc_L1_TMP_21_InitChainBaseChain_Artificial_2ndHierarchy.write.format("delta").mode("append").save(SFTDeltaPath+"otc_L1_TMP_21_InitChainBaseChain.delta")


        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181120_1324
        if otc_L1_TMP_21_InitChainBaseChain.first() is not None:
          maxChildID=otc_L1_TMP_21_InitChainBaseChain.groupBy().agg(expr("max(childID) as maxChildID")).select("maxChildID").collect()[0][0]
        else:
          maxChildID=0
        maxChildID=str(maxChildID)
        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181120_1325
        y=\
        otc_L1_TMP_21_InitChainBaseChain.alias("bc")\
        .join((otc_L1_TMP_21_InitChainBaseChain\
        .select("documentNumber","documentLineItem")).alias("x"),\
        expr("( x.documentNumber = bc.originatingDocumentNumber\
         AND x.documentLineItem = bc.originatingDocumentLineItem )"),"left")\
        .filter(expr("bc.originatingDocumentNumber IS NOT NULL \
         AND x.documentNumber IS NULL"))\
        .groupBy("originatingDocumentNumber"\
        ,"originatingDocumentLineItem")\
        .agg(expr("count(*) as numberOfChildren")\
        ,expr("min(documentProduct) as documentProduct")\
        ,expr("min(productArtificialID) as productArtificialID"))\
        .selectExpr("originatingDocumentNumber     as documentNumber"\
        ,"originatingDocumentLineItem   as documentLineItem"\
        ,"'%'                            as documentTypeClient"\
        ,"documentProduct","productArtificialID","numberOfChildren")


        otc_L1_TMP_21_InitChainBaseChain_Artificial_3rdHierarchy=\
        y.alias("y")\
        .join(otc_L1_STG_20_SalesFlowSourceType.alias("st"),\
        expr("st.sourceTypeName = 'Artificial | 3rd Hierarchy' "),"inner")\
        .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias("docType1"),\
        expr("( docType1.documentTypeClient = '"+unknownDocTypeClient+"'\
         AND docType1.documentTypeClientProcessID = "+clientProcessID_OTC+"\
         AND docType1.isReversed = 0\
         AND docType1.ERPSystemID = '1' )"),"left")\
        .selectExpr("row_number() OVER (ORDER BY (SELECT NULL))+ "+maxChildID+" as childID"\
        ,"documentNumber as documentNumber"\
        ,"documentLineItem as documentLineItem"\
        ,"CASE WHEN y.numberOfChildren = 1 THEN y.documentProduct ELSE NULL END as documentProduct"\
        ,"CASE WHEN y.numberOfChildren = 1 THEN y.productArtificialID ELSE NULL END as productArtificialID"\
        ,"st.sourceTypeID as sourceTypeID"\
        ,"docType1.documentTypeClientID as documentTypeClientID")

        otc_L1_TMP_21_InitChainBaseChain_Artificial_3rdHierarchy.write.format("delta").mode("append").save(SFTDeltaPath+"otc_L1_TMP_21_InitChainBaseChain.delta")

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181120_1429
        if otc_L1_TMP_21_InitChainBaseChain.first() is not None:
          maxChildID=otc_L1_TMP_21_InitChainBaseChain.groupBy().agg(expr("max(childID) as maxChildID")).select("maxChildID").collect()[0][0]
        else:
          maxChildID=0
        maxChildID=str(maxChildID)

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181120_1430
        y=\
        otc_L1_TMP_21_InitChainBaseChain.alias("BC")\
        .filter(expr("BC.predecessorDocumentTypeClient = '2'"))\
        .selectExpr("concat('X',BC.documentNumber) as documentNumber"\
        ,"concat('X',BC.documentLineItem) as documentLineItem"\
        ,"'2' as documentTypeClient"\
        ,"BC.documentProduct as documentProduct"\
        ,"BC.productArtificialID as productArtificialID"\
        ,"4 as documentTypeClientID").distinct()


        otc_L1_TMP_21_InitChainBaseChain_ArtificialExternal=\
        y.alias("y")\
        .join(otc_L1_STG_20_SalesFlowSourceType.alias("ST"),\
        expr("( ST.sourceTypeName = 'Artificial | External' )"),"inner")\
        .selectExpr("row_number() OVER (ORDER BY (SELECT NULL))+ "+maxChildID+" as childID"\
        ,"documentNumber as documentNumber"\
        ,"documentLineItem as documentLineItem"\
        ,"documentTypeClient as documentTypeClient"\
        ,"documentProduct as documentProduct"\
        ,"productArtificialID as productArtificialID"\
        ,"documentTypeClientID as documentTypeClientID")

        otc_L1_TMP_21_InitChainBaseChain_ArtificialExternal.write.format("delta").\
        mode("append").save(SFTDeltaPath+"otc_L1_TMP_21_InitChainBaseChain.delta")

        otc_L1_TMP_21_InitChainBaseChain_Pur_upd=\
        otc_L1_TMP_21_InitChainBaseChain.alias("inbc1")\
        .join(ptp_L1_TD_PurchaseOrder.alias("puro1"),\
        expr("( inbc1.documentNumber = puro1.purchaseOrderNumber\
         AND inbc1.documentLineItem = puro1.purchaseOrdeLineItem2\
         AND inbc1.documentTypeClient = 'V' ) "),"inner")\
        .join(otc_L1_STG_20_SalesFlowSourceType.alias("sfsp1"),\
        expr("( sfsp1.sourceTypeName = 'Purchase Order' )"),"inner")\
        .selectExpr("sfsp1.sourceTypeID"\
        ,"puro1.purchaseOrderMaterialNumber"\
        ,"inbc1.documentNumber"\
        ,"inbc1.documentLineItem").cache()

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181120_1520
        otc_L1_TMP_21_InitChainBaseChain_Delta = DeltaTable.forPath\
                    (spark, SFTDeltaPath+"otc_L1_TMP_21_InitChainBaseChain.delta")
        otc_L1_TMP_21_InitChainBaseChain_Delta.alias("del")\
            .merge(source=otc_L1_TMP_21_InitChainBaseChain_Pur_upd.alias("upd")\
                   ,condition="del.documentNumber = upd.documentNumber\
                           and del.documentLineItem = upd.documentLineItem") \
            .whenMatchedUpdate(set = {"sourceTypeID": "upd.sourceTypeID"\
                                      ,"documentProduct2": "upd.purchaseOrderMaterialNumber"\
                                     }).execute() 



        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181120_1642

        otc_L1_TMP_21_InitChainBaseChain.alias("P")\
        .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias("ckdt1"),\
        expr("( ckdt1.documentTypeClient = P.documentTypeClient\
         AND ckdt1.documentTypeClientProcessID = "+clientProcessID_OTC+"\
         AND ckdt1.ERPSystemID = '1' )"),"left")\
        .filter(expr("P.predecessorDocumentNumber IS NULL \
         and ckdt1.documentTypeKPMGShort='purchase_order'"))\
        .selectExpr("p.documentNumber as po_documentNumber"\
        ,"p.documentLineItem as po_documentLineItem"\
        ,"P.documentNumber as documentNumber"\
        ,"P.documentLineItem as documentLineItem"\
        ,"P.predecessorDocumentNumber as predecessorDocumentNumber"\
        ,"P.predecessorDocumentLineItem as predecessorDocumentLineItem"\
        ,"P.predecessorOrderingDocumentNumber as predecessorOrderingDocumentNumber"\
        ,"P.predecessorOrderingDocumentLineItem as predecessorOrderingDocumentLineItem"\
        ,"P.predecessorOrderingDocumentTypeClient as predecessorOrderingDocumentTypeClient"\
        ,"P.productArtificialID as productArtificialID"\
        ,"0 as documentLevel").write.format("delta").mode("overwrite").save(SFTDeltaPath+"cte_purchase_order.delta")


        cte_purchase_order=spark.read.format("delta").load(SFTDeltaPath+"cte_purchase_order.delta")
        while(True):
              cte_purchase_order=\
              cte_purchase_order.alias("MyCTE")\
              .join(otc_L1_TMP_21_InitChainBaseChain.alias("BC"),\
              expr("( MyCTE.documentNumber = BC.predecessorDocumentNumber and MyCTE.documentLineItem = BC.predecessorDocumentLineItem )"),"inner")\
              .selectExpr("MyCTE.po_documentNumber as po_documentNumber"\
              ,"MyCTE.po_documentLineItem as po_documentLineItem"\
              ,"BC.documentNumber as documentNumber"\
              ,"BC.documentLineItem as documentLineItem"\
              ,"BC.predecessorDocumentNumber as predecessorDocumentNumber"\
              ,"BC.predecessorDocumentLineItem as predecessorDocumentLineItem"\
              ,"BC.predecessorOrderingDocumentNumber as predecessorOrderingDocumentNumber"\
              ,"BC.predecessorOrderingDocumentLineItem as predecessorOrderingDocumentLineItem"\
              ,"BC.predecessorOrderingDocumentTypeClient as predecessorOrderingDocumentTypeClient"\
              ,"BC.productArtificialID as productArtificialID"\
              ,"MyCTE.documentLevel + 1 as documentLevel").cache()
              cte_purchase_order.write.format("delta").mode("append").save(SFTDeltaPath+"cte_purchase_order.delta")
              if cte_purchase_order.first() is None:
                break
        otc_L1_TMP_21_InitChainBaseChain_Intermediate=spark.read.format("delta").load(SFTDeltaPath+"cte_purchase_order.delta")


        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181121_1142
        xx=\
        otc_L1_TMP_21_InitChainBaseChain_Intermediate.alias("CTE")\
        .join(knw_vw_LK_GD_ClientKPMGDocumentType.alias("ckdt2"),\
        expr("( ckdt2.documentTypeClient = predecessorOrderingDocumentTypeClient\
         AND ckdt2.documentTypeClientProcessID = "+clientProcessID_OTC+"\
         AND ckdt2.ERPSystemID = '1' )"),"left")\
        .filter(expr("predecessorOrderingDocumentNumber IS NOT NULL \
         AND ckdt2.documentTypeKPMGShort='sales_order'"))\
        .groupBy("po_documentNumber"\
        ,"po_documentLineItem")\
        .agg(expr("min(documentLevel) as min_doc_level"))

        otc_L1_TMP_21_InitChainBaseChain_salesorder_upd=\
        otc_L1_TMP_21_InitChainBaseChain.alias("inbc1")\
        .join(otc_L1_TMP_21_InitChainBaseChain_Intermediate.alias("CTE"),\
        expr("inbc1.documentNumber = CTE.po_documentNumber and inbc1.documentLineItem = CTE.po_documentLineItem "),"inner")\
        .join(xx.alias("xx"),\
        expr("( CTE.po_documentNumber = xx.po_documentNumber and CTE.po_documentLineItem = xx.po_documentLineItem and CTE.documentLevel = xx.min_doc_level )"),"inner")\
        .selectExpr("CTE.predecessorOrderingDocumentNumber"\
        ,"CTE.predecessorOrderingDocumentLineItem "\
        ,"CTE.predecessorOrderingDocumentTypeClient"\
        ,"CTE.po_documentNumber"\
        ,"CTE.po_documentLineItem").cache()

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181121_1052
        otc_L1_TMP_21_InitChainBaseChain_Delta = DeltaTable.forPath\
                                                (spark, SFTDeltaPath+"otc_L1_TMP_21_InitChainBaseChain.delta")
        otc_L1_TMP_21_InitChainBaseChain_Delta.alias("del")\
                                .merge(source=otc_L1_TMP_21_InitChainBaseChain_salesorder_upd.alias("upd"),condition="del.documentNumber = upd.po_documentNumber\
                                and del.documentLineItem = upd.po_documentLineItem") \
                                .whenMatchedUpdate(set = {"predecessorDocumentNumber": "upd.predecessorOrderingDocumentNumber"\
                                                          ,"predecessorDocumentLineItem": "upd.predecessorOrderingDocumentLineItem"\
                                                          ,"predecessorDocumentTypeClient": "upd.predecessorOrderingDocumentTypeClient"\
                                                         }).execute() 
        otc_L1_TMP_21_InitChainBaseChain=\
        otc_L1_TMP_21_InitChainBaseChain\
        .withColumn("predecessorDocumentNumber",expr("case when predecessorDocumentTypeClient='2' then concat('X',documentNumber) else predecessorDocumentNumber end"))\
        .withColumn("predecessorDocumentLineItem",expr("case when predecessorDocumentTypeClient='2' then concat('X',documentLineItem) else predecessorDocumentLineItem end"))

        otc_L1_TMP_21_InitChainBaseChain=\
        otc_L1_TMP_21_InitChainBaseChain\
        .withColumn("isCircularReverenceVictim",expr("case when documentNumber=predecessorDocumentNumber \
        and documentLineItem = predecessorDocumentLineItem then true else false end"))\
        .withColumn("predecessorDocumentNumber",expr("case when isCircularReverenceVictim=true then NULL else predecessorDocumentNumber end"))\
        .withColumn("predecessorDocumentLineItem",expr("case when isCircularReverenceVictim=true then NULL else predecessorDocumentLineItem end"))\
        .withColumn("predecessorDocumentTypeClient",expr("case when isCircularReverenceVictim=true then NULL else predecessorDocumentTypeClient end"))

        otc_L1_TMP_21_InitChainBaseChain.alias("upd")\
        .join(otc_L1_TMP_21_InitChainBaseChain.alias("pre"),\
              expr("upd.predecessorDocumentNumber=pre.documentNumber and upd.predecessorDocumentLineItem=pre.documentLineItem"),"inner")\
        .selectExpr("upd.documentNumber","upd.documentLineItem","upd.predecessorDocumentNumber","upd.predecessorDocumentLineItem",\
                    "array_union(array(struct(upd.predecessorDocumentNumber as doc,upd.predecessorDocumentLineItem as line)),array(struct(pre.predecessorDocumentNumber as doc,pre.predecessorDocumentLineItem as line))) as Hierarchy","pre.predecessorDocumentNumber as NewPreDoc","pre.predecessorDocumentLineItem as NewPreDocLine","case when \
                    pre.predecessorDocumentNumber = upd.documentNumber and \
                    pre.predecessorDocumentLineItem = upd.documentLineItem then true else false end as isCircRef","false as isHierSame")\
        .write.format("delta").mode("overwrite").save(SFTDeltaPath+"Hierarchytable.delta")

        level_cnt=1
        HierarchytableDelta=DeltaTable.forPath(spark, SFTDeltaPath+"Hierarchytable.delta")  
        while(level_cnt<10):
                Hierarchytable=spark.read.format("delta").load(SFTDeltaPath+"Hierarchytable.delta").filter("isCircRef=false and isHierSame=false")
                NextLevelHierarchy=\
                Hierarchytable.alias("upd")\
                .join(Hierarchytable.alias("pre"),\
                     ((col("upd.NewPreDoc")==col("pre.documentNumber"))\
                     &(col("upd.NewPreDocLine")==col("pre.documentLineItem"))))\
                .selectExpr("upd.documentNumber","upd.documentLineItem",\
                            "upd.predecessorDocumentNumber","upd.predecessorDocumentLineItem",\
                            "array_union(upd.Hierarchy,array(struct(pre.predecessorDocumentNumber as doc,pre.predecessorDocumentLineItem as line))) as Hierarchy",\
                "pre.predecessorDocumentNumber as NewPreDoc","pre.predecessorDocumentLineItem as NewPreDocLine",\
                            "case when pre.predecessorDocumentNumber = upd.documentNumber \
                             and pre.predecessorDocumentLineItem = upd.documentLineItem then true else false end as isCircRef").cache()
                if NextLevelHierarchy.first() is None:
                  break

                HierarchytableDelta.alias("hier")\
               .merge(source=NextLevelHierarchy.alias("nxt_lvl"),\
                   condition="hier.documentNumber=nxt_lvl.documentNumber and hier.documentLineItem=nxt_lvl.documentLineItem")\
               .whenMatchedUpdate(set={"hier.Hierarchy": "nxt_lvl.Hierarchy",
                                      "hier.NewPreDoc": "nxt_lvl.NewPreDoc",
                                      "hier.NewPreDocLine": "nxt_lvl.NewPreDocLine",
                                      "hier.isCircRef": "nxt_lvl.isCircRef",
                                      "hier.isHierSame": "case when nxt_lvl.Hierarchy = hier.Hierarchy then true else false end"}).execute()
                level_cnt+=1
            
        circularReferenceVictim=spark.read.format("delta").load(SFTDeltaPath+"Hierarchytable.delta").filter("isCircRef=true")\
          .selectExpr("documentNumber","documentLineItem","array_sort(array_union(Hierarchy,array_union(array(struct(documentNumber as doc,documentLineItem as line)),\
          array(struct(predecessorDocumentNumber as doc,predecessorDocumentLineItem as line))))) as Hier_Sorted")\
        .withColumn("First",element_at(col("Hier_Sorted"),1))\
        .withColumn("circularReferenceVictimDocumentNumber",col("First").doc)\
        .withColumn("circularReferenceVictimDocumentLineItem",col("First").line)\
        .filter(expr("struct(documentNumber as doc,documentLineItem as line)=First"))\

        otc_circularReferenceVictimDocLine=circularReferenceVictim\
        .select("circularReferenceVictimDocumentNumber","circularReferenceVictimDocumentLineItem").distinct()

        otc_L1_TMP_21_InitChainBaseChain=\
        otc_L1_TMP_21_InitChainBaseChain.alias("bc")\
        .join(otc_circularReferenceVictimDocLine.alias("bc2"),\
        expr("( bc.documentNumber = bc2.circularReferenceVictimDocumentNumber\
         AND bc.documentLineItem = bc2.circularReferenceVictimDocumentLineItem )"),"left")\
        .selectExpr("case when bc2.circularReferenceVictimDocumentNumber is not null then NULL else predecessorDocumentNumber end as predecessorDocumentNumber"\
        ,"case when bc2.circularReferenceVictimDocumentNumber is not null then NULL else predecessorDocumentLineItem end as predecessorDocumentLineItem"\
        ,"case when bc2.circularReferenceVictimDocumentNumber is not null then true else isCircularReverenceVictim end as isCircularReverenceVictim"\
        ,"bc.documentNumber"\
        ,"bc.predecessorOrderingDocumentTypeHeader"\
        ,"bc.originatingDocumentLineItem"\
        ,"bc.sourceTypeID"\
        ,"bc.parentID"\
        ,"bc.predecessorOrderingDocumentNumberHeader"\
        ,"bc.documentLineItem"\
        ,"bc.predecessorOrderingDocumentNumber"\
        ,"bc.documentTypeClient"\
        ,"bc.originatingDocumentNumber"\
        ,"bc.documentProduct2"\
        ,"bc.documentTypeClientID"\
        ,"bc.predecessorDocumentTypeClient"\
        ,"bc.documentProduct"\
        ,"bc.companyCode"\
        ,"bc.predecessorOrderingDocumentTypeClient"\
        ,"bc.productArtificialID"\
        ,"bc.ID"\
        ,"bc.predecessorOrderingDocumentLineItem"\
        ,"bc.lineItemHigherBOM"\
        ,"bc.childID").cache()

        x=\
        otc_L1_TMP_21_InitChainBaseChain\
        .select("documentNumber","documentLineItem").distinct()\
        .union(\
              otc_L1_TMP_21_InitChainBaseChain\
        .select("predecessorDocumentNumber","predecessorDocumentLineItem").distinct())\
        .union(\
              otc_L1_TMP_21_InitChainBaseChain\
        .select("originatingDocumentNumber","originatingDocumentLineItem").distinct())\
        .union(\
              otc_L1_TMP_21_InitChainBaseChain\
        .select("predecessorOrderingDocumentNumber","predecessorOrderingDocumentLineItem").distinct())\

        otc_L1_TMP_21_InitChainID=\
        x.distinct().selectExpr("documentNumber","documentLineItem","row_number() OVER (ORDER BY documentNumber ASC, documentLineItem ASC) as ID").cache()

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181121_1431
        otc_L1_TMP_21_InitChainBaseChain=\
        otc_L1_TMP_21_InitChainBaseChain.alias("inbc1")\
        .join(otc_L1_TMP_21_InitChainID.alias("inca1"),\
        expr("( inbc1.documentNumber = inca1.documentNumber\
         AND ( inbc1.documentLineItem = inca1.documentLineItem OR inbc1.documentLineItem IS NULL ) )"),"left")\
        .selectExpr("case when inca1.documentNumber is not null then inca1.ID else inbc1.ID end as ID"\
        ,"inbc1.documentNumber"\
        ,"inbc1.predecessorOrderingDocumentTypeHeader"\
        ,"inbc1.originatingDocumentLineItem"\
        ,"inbc1.sourceTypeID"\
        ,"inbc1.parentID"\
        ,"inbc1.predecessorDocumentNumber"\
        ,"inbc1.predecessorOrderingDocumentNumberHeader"\
        ,"inbc1.documentLineItem"\
        ,"inbc1.predecessorDocumentLineItem"\
        ,"inbc1.predecessorOrderingDocumentNumber"\
        ,"inbc1.documentTypeClient"\
        ,"inbc1.originatingDocumentNumber"\
        ,"inbc1.documentProduct2"\
        ,"inbc1.documentTypeClientID"\
        ,"inbc1.isCircularReverenceVictim"\
        ,"inbc1.predecessorDocumentTypeClient"\
        ,"inbc1.documentProduct"\
        ,"inbc1.companyCode"\
        ,"inbc1.predecessorOrderingDocumentTypeClient"\
        ,"inbc1.productArtificialID"\
        ,"inbc1.predecessorOrderingDocumentLineItem"\
        ,"inbc1.lineItemHigherBOM"\
        ,"inbc1.childID")


        otc_L1_TMP_21_InitChainBaseChain=\
        otc_L1_TMP_21_InitChainBaseChain.alias("inbc1")\
        .join(otc_L1_TMP_21_InitChainID.alias("inca1"),\
        expr("( inbc1.predecessorDocumentNumber = inca1.documentNumber\
         AND inbc1.predecessorDocumentLineItem = inca1.documentLineItem  )"),"left")\
        .selectExpr("case when inca1.documentNumber is not null then inca1.ID else inbc1.parentID end as parentID"\
        ,"inbc1.documentNumber"\
        ,"inbc1.predecessorOrderingDocumentTypeHeader"\
        ,"inbc1.originatingDocumentLineItem"\
        ,"inbc1.sourceTypeID"\
        ,"inbc1.ID"\
        ,"inbc1.predecessorDocumentNumber"\
        ,"inbc1.predecessorOrderingDocumentNumberHeader"\
        ,"inbc1.documentLineItem"\
        ,"inbc1.predecessorDocumentLineItem"\
        ,"inbc1.predecessorOrderingDocumentNumber"\
        ,"inbc1.documentTypeClient"\
        ,"inbc1.originatingDocumentNumber"\
        ,"inbc1.documentProduct2"\
        ,"inbc1.documentTypeClientID"\
        ,"inbc1.isCircularReverenceVictim"\
        ,"inbc1.predecessorDocumentTypeClient"\
        ,"inbc1.documentProduct"\
        ,"inbc1.companyCode"\
        ,"inbc1.predecessorOrderingDocumentTypeClient"\
        ,"inbc1.productArtificialID"\
        ,"inbc1.predecessorOrderingDocumentLineItem"\
        ,"inbc1.lineItemHigherBOM"\
        ,"inbc1.childID").cache()

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181213_1050

        lstNonUniqueIDs=otc_L1_TMP_21_InitChainBaseChain.groupBy("ID")\
        .agg(expr("count(*) as cnt"))\
        .filter("cnt>1")\
        .collect()
        lstNonUniqueIDs=[f"'{row.ID}'" for row in lstNonUniqueIDs]
        strNonUniqueIDs=",".join(lstNonUniqueIDs)
        if strNonUniqueIDs=='':
          strNonUniqueIDs="''"
        x=\
        otc_L1_TMP_21_InitChainBaseChain\
        .filter(expr("ID IN ("+strNonUniqueIDs+")"))\
        .selectExpr("dense_rank() OVER (PARTITION BY ID ORDER BY childID) as myRank"\
        ,"childID as childID"\
        ,"ID as ID"\
        ,"parentID as parentID"\
        ,"documentNumber as documentNumber"\
        ,"documentLineItem as documentLineItem"\
        ,"documentTypeClient as documentTypeClient"\
        ,"productArtificialID as productArtificialID")

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181213_1055
        maxChildID=[row.maxChildID for row in otc_L1_TMP_21_InitChainID.groupBy()\
        .agg(expr("max(ID) as maxChildID")).collect()]
        if maxChildID==[]:
          maxChildID='0'
        else:
          maxChildID=str(maxChildID[0])


        otc_L1_TMP_21_InitChainBaseChainNewID=\
        x\
        .filter(expr("myRank>1"))\
        .selectExpr("row_number() OVER (ORDER BY (SELECT NULL)) +"+maxChildID+" as myNewID"\
        ,"myRank as myRank"\
        ,"childID as childID"\
        ,"ID as ID"\
        ,"parentID as parentID"\
        ,"documentNumber as documentNumber"\
        ,"documentLineItem as documentLineItem"\
        ,"documentTypeClient as documentTypeClient"\
        ,"productArtificialID as productArtificialID").cache()

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181213_1057
        otc_L1_TMP_21_InitChainBaseChain_IDNew_upd=\
        otc_L1_TMP_21_InitChainBaseChain.alias("inbc1")\
        .join(otc_L1_TMP_21_InitChainBaseChainNewID.alias("inid1"),\
        expr("( inid1.childID = inbc1.childID )"),"inner")\
        .selectExpr("myNewID"\
        ,"inbc1.ID").cache()

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181213_1101
        otc_L1_TMP_21_InitChainBaseChain_parentIDNew_upd=\
        otc_L1_TMP_21_InitChainBaseChain.alias("inbc1")\
        .join(otc_L1_TMP_21_InitChainBaseChainNewID.alias("inid1"),\
        expr("( inid1.ID = inbc1.parentID AND inid1.productArtificialID=inbc1.productArtificialID )"),"inner")\
        .selectExpr("myNewID"\
        ,"inbc1.ID").cache()


        otc_L1_TMP_21_InitChainBaseChain_Delta.alias("del")\
                                .merge(source=otc_L1_TMP_21_InitChainBaseChain_IDNew_upd.alias("upd"),condition="del.ID = upd.ID")\
                                .whenMatchedUpdate(set = {"ID": "upd.myNewID"\
                                                         }).execute() 

        otc_L1_TMP_21_InitChainBaseChain_Delta.alias("del")\
                                .merge(source=otc_L1_TMP_21_InitChainBaseChain_parentIDNew_upd.alias("upd"),condition="del.ID = upd.ID")\
                                .whenMatchedUpdate(set = {"parentID": "upd.myNewID"\
                                                         }).execute() 



        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181121_1740

        otc_L1_TMP_21_InitChainBaseChain.alias("P")\
        .filter(expr("P.predecessorDocumentNumber IS NULL"))\
        .selectExpr("P.ID as transactionID"\
        ,"P.ID as ID"\
        ,"P.parentID as parentID"\
        ,"0 as documentLevel").write.format("delta").mode("overwrite").save(SFTDeltaPath+"cte_null_predecessor_hierarchy.delta")

        
        otc_L1_TMP_21_InitChainBaseChain.write.format("delta").mode("overwrite").option("mergeSchema",True).save(SFTDeltaPath+"otc_L1_TMP_21_InitChainBaseChain.delta")
        otc_L1_TMP_21_InitChainBaseChain=spark.read.format("delta").load(SFTDeltaPath+"otc_L1_TMP_21_InitChainBaseChain.delta")

        cte_null_predecessor_hierarchy=spark.read.format("delta").load(SFTDeltaPath+"cte_null_predecessor_hierarchy.delta")
        i=0
        while(True):
                i+=1
                cte_null_predecessor_hierarchy=\
                cte_null_predecessor_hierarchy.alias("MyCTE")\
                .join(otc_L1_TMP_21_InitChainBaseChain.alias("BC"),\
                expr("( BC.parentID = MyCTE.ID )"),"inner")\
                .selectExpr("MyCTE.transactionID as transactionID"\
                ,"BC.ID as ID"\
                ,"BC.parentID as parentID"\
                ,"MyCTE.documentLevel + 1 as documentLevel").cache()
                cte_null_predecessor_hierarchy.write.format("delta").mode("append")\
                    .save(SFTDeltaPath+"cte_null_predecessor_hierarchy.delta")
                if cte_null_predecessor_hierarchy.first() is None:
                    break
          
            
        cte_null_predecessor_hierarchy=spark.read.format("delta").load(SFTDeltaPath+"cte_null_predecessor_hierarchy.delta")
        cte_null_predecessor_hierarchy.groupBy("documentLevel")\
        .agg(expr("count(*)")).display()



        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181121_1749
        CTE=spark.read.format("delta").load(SFTDeltaPath+"cte_null_predecessor_hierarchy.delta")
        otc_L1_TMP_21_InitChainResultSet1=\
        CTE.alias("CTE")\
        .join(otc_L1_TMP_21_InitChainBaseChain.alias("BC"),\
        expr("CTE.ID = BC.ID"),"inner")\
        .selectExpr("CTE.transactionID as transactionID"\
        ,"CTE.ID as ID"\
        ,"CTE.parentID as parentID"\
        ,"BC.companyCode as companyCode"\
        ,"BC.documentNumber as documentNumber"\
        ,"BC.documentLineItem as documentLineItem"\
        ,"BC.documentTypeClient as documentTypeClient"\
        ,"BC.predecessorDocumentNumber as predecessorDocumentNumber"\
        ,"BC.predecessorDocumentLineItem as predecessorDocumentLineItem"\
        ,"BC.predecessorDocumentTypeClient as predecessorDocumentTypeClient"\
        ,"BC.predecessorOrderingDocumentNumber as predecessorOrderingDocumentNumber"\
        ,"BC.predecessorOrderingDocumentLineItem as predecessorOrderingDocumentLineItem"\
        ,"BC.predecessorOrderingDocumentTypeClient as predecessorOrderingDocumentTypeClient"\
        ,"BC.originatingDocumentNumber as originatingDocumentNumber"\
        ,"BC.originatingDocumentLineItem as originatingDocumentLineItem"\
        ,"BC.lineItemHigherBOM as lineItemHigherBOM"\
        ,"BC.documentProduct as documentProduct"\
        ,"BC.productArtificialID as productArtificialID"\
        ,"BC.predecessorOrderingDocumentNumberHeader as predecessorOrderingDocumentNumberHeader"\
        ,"BC.predecessorOrderingDocumentTypeHeader as predecessorOrderingDocumentTypeHeader"\
        ,"BC.sourceTypeID as sourceTypeID"\
        ,"BC.isCircularReverenceVictim as isCircularReverenceVictim"\
        ,"CTE.documentLevel as documentLevel"\
        ,"BC.documentTypeClientID as documentTypeClientID").distinct()

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181121_1809
        rr=otc_L1_TMP_21_InitChainBaseChain\
        .selectExpr("documentNumber as documentNumber"\
        ,"documentLineItem as documentLineItem")\
        .exceptAll(otc_L1_TMP_21_InitChainResultSet1\
        .selectExpr("documentNumber as documentNumber"\
        ,"documentLineItem as documentLineItem"))

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181121_1805
        otc_L1_TMP_21_InitChainBaseChainDelta=\
        otc_L1_TMP_21_InitChainBaseChain.alias("bc")\
        .join(rr.alias("rr"),\
        expr("( rr.documentNumber = bc.documentNumber\
         AND rr.documentLineItem = bc.documentLineItem )"),"inner")\
        .selectExpr("bc.childID as childID"\
        ,"bc.ID as ID"\
        ,"bc.parentID as parentID"\
        ,"bc.companyCode as companyCode"\
        ,"bc.documentNumber as documentNumber"\
        ,"bc.documentLineItem as documentLineItem"\
        ,"bc.documentTypeClient as documentTypeClient"\
        ,"bc.predecessorDocumentNumber as predecessorDocumentNumber"\
        ,"bc.predecessorDocumentLineItem as predecessorDocumentLineItem"\
        ,"bc.predecessorDocumentTypeClient as predecessorDocumentTypeClient"\
        ,"bc.predecessorOrderingDocumentNumber as predecessorOrderingDocumentNumber"\
        ,"bc.predecessorOrderingDocumentLineItem as predecessorOrderingDocumentLineItem"\
        ,"bc.predecessorOrderingDocumentTypeClient as predecessorOrderingDocumentTypeClient"\
        ,"bc.originatingDocumentNumber as originatingDocumentNumber"\
        ,"bc.originatingDocumentLineItem as originatingDocumentLineItem"\
        ,"bc.lineItemHigherBOM as lineItemHigherBOM"\
        ,"bc.documentProduct as documentProduct"\
        ,"bc.productArtificialID as productArtificialID"\
        ,"bc.documentProduct2 as documentProduct2"\
        ,"bc.predecessorOrderingDocumentNumberHeader as predecessorOrderingDocumentNumberHeader"\
        ,"bc.predecessorOrderingDocumentTypeHeader as predecessorOrderingDocumentTypeHeader"\
        ,"bc.sourceTypeID as sourceTypeID"\
        ,"bc.isCircularReverenceVictim as isCircularReverenceVictim"\
        ,"bc.documentTypeClientID as documentTypeClientID")

        
        otc_L1_TMP_21_InitChainBaseChainDelta.write.format("delta").mode("overwrite")\
            .option("mergeSchema",True).save(SFTDeltaPath+"otc_L1_TMP_21_InitChainBaseChainDelta.delta")
        otc_L1_TMP_21_InitChainBaseChainDelta=spark.read.format("delta").load(SFTDeltaPath+"otc_L1_TMP_21_InitChainBaseChainDelta.delta")

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181122_0923
        X=otc_L1_TMP_21_InitChainBaseChainDelta.alias("t")\
        .join(otc_L1_TMP_21_InitChainBaseChainDelta.alias("parent")\
        ,expr("parent.ID=t.parentID"),"leftanti")\
        .selectExpr("ID")

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181122_0921
        
        otc_L1_TMP_21_InitChainBaseChainDelta.alias("P")\
        .join(X.alias("X"),expr("X.ID = P.ID"),"inner")\
        .selectExpr("P.ID as transactionID"\
        ,"P.ID as ID"\
        ,"P.parentID as parentID"\
        ,"P.companyCode as companyCode"\
        ,"P.documentNumber as documentNumber"\
        ,"P.documentLineItem as documentLineItem"\
        ,"P.documentTypeClient as documentTypeClient"\
        ,"P.predecessorDocumentNumber as predecessorDocumentNumber"\
        ,"P.predecessorDocumentLineItem as predecessorDocumentLineItem"\
        ,"P.predecessorDocumentTypeClient as predecessorDocumentTypeClient"\
        ,"P.predecessorOrderingDocumentNumber as predecessorOrderingDocumentNumber"\
        ,"P.predecessorOrderingDocumentLineItem as predecessorOrderingDocumentLineItem"\
        ,"P.predecessorOrderingDocumentTypeClient as predecessorOrderingDocumentTypeClient"\
        ,"P.originatingDocumentNumber as originatingDocumentNumber"\
        ,"P.originatingDocumentLineItem as originatingDocumentLineItem"\
        ,"P.lineItemHigherBOM as lineItemHigherBOM"\
        ,"P.documentProduct as documentProduct"\
        ,"p.productArtificialID as productArtificialID"\
        ,"P.predecessorOrderingDocumentNumberHeader as predecessorOrderingDocumentNumberHeader"\
        ,"P.predecessorOrderingDocumentTypeHeader as predecessorOrderingDocumentTypeHeader"\
        ,"P.sourceTypeID as sourceTypeID"\
        ,"P.isCircularReverenceVictim as isCircularReverenceVictim"\
        ,"p.documentTypeClientID as documentTypeClientID"\
        ,"0 as documentLevel").write.format("delta").mode("overwrite").save(SFTDeltaPath+"CTEDelta.delta")

        CTEDelta=spark.read.format("delta").load(SFTDeltaPath+"CTEDelta.delta")

        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181122_0935
        #Technical_otc_usp_L1_STG_20_SalesFlow_Tree_Populate_sbreuer_20181213_1153
        i=0
        while(True):
                CTEDelta=\
                CTEDelta.alias("MyCTE")\
                .join(otc_L1_TMP_21_InitChainBaseChainDelta.alias("bc"),\
                expr("( bc.parentID = MyCTE.ID )"),"inner")\
                .selectExpr("MyCTE.transactionID as transactionID"\
                      ,"bc.ID as ID"\
                      ,"bc.parentID as parentID"\
                      ,"bc.companyCode as companyCode"\
                      ,"bc.documentNumber as documentNumber"\
                      ,"bc.documentLineItem as documentLineItem"\
                      ,"bc.documentTypeClient as documentTypeClient"\
                      ,"bc.predecessorDocumentNumber as predecessorDocumentNumber"\
                      ,"bc.predecessorDocumentLineItem as predecessorDocumentLineItem"\
                      ,"bc.predecessorDocumentTypeClient as predecessorDocumentTypeClient"\
                      ,"bc.predecessorOrderingDocumentNumber as predecessorOrderingDocumentNumber"\
                      ,"bc.predecessorOrderingDocumentLineItem as predecessorOrderingDocumentLineItem"\
                      ,"bc.predecessorOrderingDocumentTypeClient as predecessorOrderingDocumentTypeClient"\
                      ,"bc.originatingDocumentNumber as originatingDocumentNumber"\
                      ,"bc.originatingDocumentLineItem as originatingDocumentLineItem"\
                      ,"bc.lineItemHigherBOM as lineItemHigherBOM"\
                      ,"bc.documentProduct as documentProduct"\
                      ,"bc.productArtificialID as productArtificialID"\
                      ,"bc.predecessorOrderingDocumentNumberHeader as predecessorOrderingDocumentNumberHeader"\
                      ,"bc.predecessorOrderingDocumentTypeHeader as predecessorOrderingDocumentTypeHeader"\
                      ,"bc.sourceTypeID as sourceTypeID"\
                      ,"bc.isCircularReverenceVictim as isCircularReverenceVictim"\
                      ,"bc.documentTypeClientID as documentTypeClientID"\
                      ,"MyCTE.documentLevel+1 as documentLevel").cache()
                CTEDelta.write.format("delta").mode("append").save(SFTDeltaPath+"CTEDelta.delta")
                if CTEDelta.first() is None:
                    break
        otc_L1_TMP_21_InitChainResultSet2=spark.read.format("delta").load(SFTDeltaPath+"CTEDelta.delta")
        executionStatus = "otc_L1_STG_20_SalesFlow_Tree_populate completed successfully"
        executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
        return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

