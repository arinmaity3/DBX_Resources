# Databricks notebook source
from pyspark.sql.functions import concat,col,expr,count,collect_list,lit,trim,concat_ws,length,substring,pow,when,max,array_union,array_contains
from pyspark.sql.types import DecimalType
import re
from delta.tables import *

def cleanupSql(sqljoinComponent):
  return sqljoinComponent.strip().replace("[","").replace("]","").replace("erp.","erp_")

def sqlToPysparkJoinComponent(sqljoinComponent):
    ls_sqljoinComponents=sqljoinComponentSplitList(sqljoinComponent)
    ls_pysparkJoinComponents=[[sqlTopysparkJoinDF(x),f'expr("{y}")'] for x,y in ls_sqljoinComponents]
    pysparkJoinComponent=["".join(x)+',"inner")' for x in ls_pysparkJoinComponents]
    pysparkJoinComponent="".join(pysparkJoinComponent)
    return pysparkJoinComponent

def sqljoinComponentSplitList(sqljoinComponent):
  '''sqljoinComponent="    JOIN [erp].[BKPF] ON ([BKPF].[MANDT] = [BSEG].[MANDT] AND [BKPF].[BUKRS] = [BSEG].[BUKRS] AND [BKPF].[BELNR] = [BSEG].[BELNR] AND [BKPF].[GJAHR] = [BSEG].[GJAHR])   JOIN [erp].[TCURX] ON ([TCURX].[CURRKEY] = [BKPF].[HWAE2])"
  '''
  sqljoinComponent=sqljoinComponent.split("JOIN")
  sqljoinComponent=[x.strip() for x in sqljoinComponent]
  sqljoinComponent=sqljoinComponent[1:]
  sqljoinComponent=[x.split(" ON ") for x in sqljoinComponent]
  return sqljoinComponent


def sqlTopysparkJoinDF(erpjoinDFName):
  return '.join('+erpjoinDFName+'.alias("'+erpjoinDFName[4:]+'"),'


def getTableName1ColsFromsqlJoinComponent(tableName1,sqljoinComponent):
    ls_tableName1Cols=re.findall(tableName1+r'.[^ \)]+[ \)]',sqljoinComponent)
    ls_tableName1Cols=[x.replace(')','').replace(' ','') for x in ls_tableName1Cols]
    return ls_tableName1Cols


def pysparkSelectColumns(tableName1,sqljoinComponent):
    ls_tableName1Cols=getTableName1ColsFromsqlJoinComponent(tableName1,sqljoinComponent)
    ls_selectCols=[f'col("{x}")' for x in ls_tableName1Cols]
    ls_groupByCols=ls_selectCols
    groupByCols=f".groupBy({','.join(ls_groupByCols)})"
    ls_selectCols.append('col("TCURX.CURRDEC")')
    aggMaxCol='.agg(max("TCURX.CURRDEC").alias("CURRDEC"))'
    selectCols=f".select({','.join(ls_selectCols)}){groupByCols}{aggMaxCol}.alias('upd')),"
    return selectCols


def pysparkMergeCondition(tableName1,sqljoinComponent):
    ls_tableName1Cols=getTableName1ColsFromsqlJoinComponent(tableName1,sqljoinComponent)
    ls_erpDeltaCols=[x.replace(tableName1,"erpDelta") for x in ls_tableName1Cols]
    ls_updCols=[x.replace(tableName1,"upd") for x in ls_tableName1Cols]
    ls_MergeCondition=[f"{x}={y}" for x,y in list(zip(ls_erpDeltaCols,ls_updCols))]
    mergeCondition=" AND ".join(ls_MergeCondition)
    mergeCondition=f'condition="{mergeCondition}")'
    return mergeCondition


def pysparkWhenMatchedUpdate(ls_fieldNames,updateNum):
    ls_setfields=[f'"{fieldName}": "CASE WHEN {fieldName} IS NULL ' \
	                     f'OR upd.CURRDEC IS NULL THEN {fieldName} ' \
						 f'ELSE cast({fieldName}*pow(10,(2-upd.CURRDEC)) as decimal(32,6)) END"'\
                   for fieldName in ls_fieldNames ]
    ls_setfields.extend([f'"isShifted":"array_union(isShifted,array({updateNum}))"'])
    setfields=",".join(ls_setfields)
    whenMatchedUpdate = (f'.whenMatchedUpdate(set = {{{setfields}}})')
    return whenMatchedUpdate


def prepareAndExecuteMergeQuery(erpDeltafilepath,tableName,ls_fieldNames,sqljoinComponent,updateNum):
    try:
        import re
        
        sqljoinComponent=cleanupSql(sqljoinComponent)
        setDelta=f"erpDelta = DeltaTable.forPath(spark, '{erpDeltafilepath}')"        
        exec(setDelta)
        
        mergeStart=f'erpDelta.alias("erpDelta").merge(source=(erpDelta.toDF().alias("{tableName}")'
        joinComponent=sqlToPysparkJoinComponent(sqljoinComponent)
        filterComponent=f'.filter((col("TCURX.CURRDEC")!=lit(2))'\
                                f'&((col("{tableName}.isShifted").isNull())'\
                                 f'|(col("{tableName}.isShifted")==array(lit(0)))'\
                                 f'|(~array_contains(col("{tableName}.isShifted"),{updateNum}))))'
        selectCols=pysparkSelectColumns(tableName,sqljoinComponent)
        mergeCondition=pysparkMergeCondition(tableName,sqljoinComponent)
        whenMatchedUpdate = pysparkWhenMatchedUpdate(ls_fieldNames,updateNum)
        
        fullquery=f"{mergeStart}{joinComponent}{filterComponent}{selectCols}{mergeCondition}{whenMatchedUpdate}.execute()"
        
        exec(fullquery)
    except Exception as err:
        lstablenames=re.findall(r"name '.*' is not defined",str(err))
        if lstablenames:
          nonexistentTable= lstablenames[0].replace("name '",'').replace("' is not defined",'').replace("erp_",'')
          
          if nonexistentTable not in gl_ls_app_SAP_L0_TMP_ERPTablesRequiredForSelectedAnalytics:
            pass
          else:
            raise
        else:
           raise
  
