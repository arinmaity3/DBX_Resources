# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import lit,col,when,to_date

def gen_ORA_L1_MD_User_populate(): 
  try:
    objGenHelper = gen_genericHelper()
    objDataTransformation = gen_dataTransformation()
    logID = executionLog.init(PROCESS_ID.L1_TRANSFORMATION,None,None,None,None)
    global gen_L1_MD_User
    
    erp_PER_ALL_PEOPLE_F.createOrReplaceTempView("erp_PER_ALL_PEOPLE_F")
    t2_DF = spark.sql("SELECT ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY EFFECTIVE_START_DATE DESC) as ORDER_KEY \
                               ,PERSON_ID  as    PERSON_ID     \
                               ,FULL_NAME as FULL_NAME\
                               ,BUSINESS_GROUP_ID  as BUSINESS_GROUP_ID\
                            FROM erp_PER_ALL_PEOPLE_F")


    EMPLOYEEID_nullif = expr("NULLIF(t1.EMPLOYEE_ID,0)")
    isnull_EMPLOYEEID = when(lit(EMPLOYEEID_nullif).isNull(),lit(0)).otherwise(lit(EMPLOYEEID_nullif))
    CUSTOMERID_nullif = expr("NULLIF(t1.CUSTOMER_ID,0)")
    isnull_CUSTOMERID = when(lit(CUSTOMERID_nullif).isNull(),lit(0)).otherwise(lit(CUSTOMERID_nullif))
    SUPPLIERID_nullif = expr("NULLIF(t1.SUPPLIER_ID,0)")
    isnull_SUPPLIERID = when(lit(SUPPLIERID_nullif).isNull(),lit(0)).otherwise(lit(SUPPLIERID_nullif))


    userGroup_case = when((lit(isnull_EMPLOYEEID) == lit(0)) & 
                      (lit(isnull_CUSTOMERID) == lit(0)) & 
                      (lit(isnull_SUPPLIERID) == lit(0)), lit('Unknown'))\
                .when(lit(isnull_EMPLOYEEID) != lit(0),lit('Employee'))\
                .when((lit(isnull_EMPLOYEEID) == lit(0))& \
                     ((lit(isnull_CUSTOMERID) != lit(0)) | (lit(isnull_CUSTOMERID) != lit(0))),lit('Not Employee'))\
                .otherwise('#NA#')
    gen_L1_MD_User = erp_FND_USER.alias('t1')\
                 .join(t2_DF.alias('t2'),((col('t1.EMPLOYEE_ID').eqNullSafe(col('t2.PERSON_ID')))\
                                          &(col('t2.ORDER_KEY')==lit(1))),how='left')\
                 .select(lit(0).alias('companyCode')\
                        ,when(col('t1.USER_ID').cast('String').isNull(),lit('#NA#'))\
                           .otherwise(col('t1.USER_ID').cast('String')).alias('userName')\
                        ,when(col('t2.BUSINESS_GROUP_ID').cast('String').isNull(),lit('#NA#'))\
                           .otherwise(col('t2.BUSINESS_GROUP_ID').cast('String')).alias('userDepartment')\
                        ,when(col('t1.USER_NAME').isNull(),lit('#NA#'))
                           .otherwise(col('t1.USER_NAME')).alias('employeeName')\
                        ,lit(userGroup_case).alias('userGroup')\
                        ,lit('#NA#').alias('userType')\
                        ,lit('#NA#').alias('timeZone')\
                        ,col('t1.CREATED_BY').alias('createdBy')\
                        ,to_date(col('t1.CREATION_DATE')).alias('createdOn')\
                        ,when(col('t1.USER_LOCK')=='N',lit(0)).otherwise(lit(1)).alias('isUserLocked')\
                        ,lit(None).alias('segment01')\
                        ,lit(None).alias('segment02')\
                        ,lit(None).alias('segment03')\
                        ,lit(None).alias('segment04')\
                        ,lit(None).alias('segment05'))\
                 .distinct()  

    gen_L1_MD_User =  objDataTransformation.gen_convertToCDMandCache \
          (gen_L1_MD_User,'gen','L1_MD_User',targetPath=gl_CDMLayer1Path)
       
    executionStatus = "L1_MD_User populated sucessfully"
    executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
    return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
  except Exception as err:
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus]

