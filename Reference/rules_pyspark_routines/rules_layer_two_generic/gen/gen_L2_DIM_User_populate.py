# Databricks notebook source
import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number
from functools import reduce
from pyspark.sql import DataFrame

import sys
import traceback
from pyspark.sql.functions import  lit,col,row_number
from functools import reduce
from pyspark.sql import DataFrame

def gen_L2_DIM_User_populate():  
    """ Populate L2_DIM_User """
    try:
      
      logID = executionLog.init(PROCESS_ID.L2_TRANSFORMATION)
      global gen_L2_DIM_User
      w = Window().orderBy(lit('userSurrogateKey'))
      objGenHelper = gen_genericHelper()
      objDataTransformation = gen_dataTransformation()

      erpGENSystemID = objGenHelper.gen_lk_cd_parameter_get('GLOBAL', 'ERP_SYSTEM_ID_GENERIC')
      
      if erpGENSystemID is None:
          erpGENSystemID = 'NULL'
    
      L1_TMP_UserCollect = spark.createDataFrame(gl_lstOfUserCollect,userSchemaCollect).\
                           select(col("companyCode"),col("userName")).distinct()
      #.withColumn("userSurrogateKey", row_number().over(w))\
      df_L2_DIM_User = L1_TMP_UserCollect.alias("User").\
                        join(gen_L2_DIM_Organization.alias("orgUnit")\
                        ,[col("User.companyCode") == col("orgUnit.companyCode")],how='inner').\
                        select(col("orgUnit.organizationUnitSurrogateKey"),
                              col("User.userName"),lit("#NA#").alias("userDepartment"),\
                              col("User.userName").alias("employeeName"),\
                              col("orgUnit.companyCode").alias("companyCode"))\
                        .withColumn("userGroup", lit("#NA#"))\
                        .withColumn("userType", lit("#NA#"))\
                        .withColumn("userTimeZone", lit("#NA#"))\
                        .withColumn("createdBy", lit("#NA#"))\
                        .withColumn("createdOn", lit("1900-01-01"))\
                        .withColumn("isUserLocked", lit(False))\
                        .withColumn("userTypeDetermination", lit("#NA#"))\
                        .withColumn("userTypeDeterminationDescription", lit("#NA#"))
      
      gen_L2_DIM_User,status=L2_User_AddedDetails(df_L2_DIM_User,gl_CDMLayer1Path)
      
      gen_L2_DIM_User=gen_L2_DIM_User.withColumn("userSurrogateKey", row_number().over(w))\
        
      #gen_L2_DIM_User.display()
      gen_L2_DIM_User = objDataTransformation.gen_convertToCDMandCache \
          (gen_L2_DIM_User,'gen','L2_DIM_User',False)
      
      #prepare the dwh view corresponding for User
      #dwh_vw_DIM_User = gen_L2_DIM_User.withColumn("userTypeText",lit("#NA#"))\
      #                                  .withColumn("userTypeDescription",lit("#NA#"))

      bdtvmjoin1 = when(col('rep.KPMGDataReportingLanguage').isNull(), lit('EN') ) \
                              .otherwise(col('rep.KPMGDataReportingLanguage'))

      dwh_vw_DIM_User = gen_L2_DIM_User.alias('usr')\
                        .join(gen_L2_DIM_Organization.alias('org2')\
                            ,(col('usr.organizationUnitSurrogateKey') == (col('org2.organizationUnitSurrogateKey'))),how='inner')\
                        .join(knw_LK_CD_ReportingSetup.alias('rep')\
                            ,(col('rep.companyCode')	==	col('org2.companyCode')) \
                            & (col('rep.clientCode') == (col('org2.clientID'))),how ='inner') \
                        .join(knw_LK_GD_BusinessDatatypeValueMapping.alias('bdtvm')\
                            ,(col('bdtvm.targetLanguageCode') == lit(bdtvmjoin1)) \
                            & (col('bdtvm.sourceSystemValue') == (col('usr.userType')))  \
                            & (col('bdtvm.businessDatatype') == lit('User Type')) \
                            & (col('bdtvm.targetERPSystemID') == lit(erpGENSystemID)),how =('left')) \
                        .select(\
                            col('usr.employeeName').alias('employeeName')\
                            ,col('usr.isUserLocked').alias('isUserLocked')\
                            ,col('usr.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
                            ,col('usr.userDepartment').alias('userDepartment')\
                            ,col('usr.userGroup').alias('userGroup')\
                            ,col('usr.userName').alias('userName')\
                            ,col('usr.userSurrogateKey').alias('userSurrogateKey')\
                            ,col('usr.userTimeZone').alias('userTimeZone')\
                            ,col('usr.userType').alias('userType')\
                            ,concat(col('usr.userType'),lit(' ('),(when(col('bdtvm.targetSystemValueDescription').isNull(),'#NA#')\
                                .otherwise(col('bdtvm.targetSystemValueDescription'))),(lit(')'))).alias('userTypeDescription')\
                            ,when(col('bdtvm.targetSystemValueDescription').isNull(),'#NA#')\
                                .otherwise(col('bdtvm.targetSystemValueDescription')).alias('userTypeText')\
                            )
            
      dwh_vw_DIM_User = objDataTransformation.gen_convertToCDMStructure_generate(dwh_vw_DIM_User,'dwh','vw_DIM_User',True)[0]
      
      keysAndValues = {"organizationUnitSurrogateKey" : 0,"userSurrogateKey" :0,"isUserLocked" : -1}
      dwh_vw_DIM_User = objGenHelper.gen_placeHolderRecords_populate('dwh.vw_DIM_User',keysAndValues,dwh_vw_DIM_User)  
      keysAndValues = {"organizationUnitSurrogateKey" : 0,"userSurrogateKey" :-1,"isUserLocked" : -1,"userName":"#NA#"}
      dwh_vw_DIM_User = objGenHelper.gen_placeHolderRecords_populate('dwh.vw_DIM_User',keysAndValues,dwh_vw_DIM_User) 
                  
      objGenHelper.gen_writeToFile_perfom(dwh_vw_DIM_User,gl_CDMLayer2Path + "gen_L2_DIM_User.parquet" )
      
      executionStatus = "L2_DIM_User populated sucessfully"
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,executionStatus)  
      return [LOG_EXECUTION_STATUS.SUCCESS,executionStatus]
      
    except Exception as e:
      executionStatus = objGenHelper.gen_exceptionDetails_log()
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
      return [LOG_EXECUTION_STATUS.FAILED,executionStatus]


def L2_User_AddedDetails(df_L2_user,fileLocation):
  try:
    objGenHelper = gen_genericHelper()

    status="Update L2 User additional details"
    fileFullName=fileLocation+"gen_L1_MD_User.delta"
    
    if objGenHelper.file_exist(fileFullName) == True:
      gen_L1_MD_User = objGenHelper.gen_readFromFile_perform(fileFullName) 
      
      df_l2DimUserAddedInfo= df_L2_user.alias('l2user')\
          .join(gen_L1_MD_User.alias('l1user'),\
            (col('l2user.userName').eqNullSafe(col('l1user.userName'))),how ='left')\
          .select(col('l2user.organizationUnitSurrogateKey').alias('organizationUnitSurrogateKey')\
              ,col('l2user.userName').alias('userName')\
              ,coalesce(col('l1user.userDepartment'),col('l2user.userDepartment')).alias("userDepartment")\
              ,coalesce(col('l1user.employeeName'),col('l2user.employeeName')).alias("employeeName")\
              ,coalesce(col('l1user.userGroup'),col('l2user.userGroup')).alias("userGroup")\
              ,coalesce(col('l1user.userType'),col('l2user.userType')).alias("userType")\
              ,coalesce(col('l1user.timeZone'),col('l2user.userTimeZone')).alias("userTimeZone")\
              ,coalesce(col('l1user.createdBy'),col('l2user.createdBy')).alias("createdBy")\
              ,coalesce(col('l1user.createdOn'),col('l2user.createdOn')).alias("createdOn")\
              ,coalesce(col('l1user.isUserLocked'),col('l2user.isUserLocked')).alias("isUserLocked")\
              ,coalesce(col('l1user.userTypeDetermination'),col('l2user.userTypeDetermination')).alias("userTypeDetermination")\
              ,coalesce(col('l1user.userTypeDeterminationDescription'),col('l2user.userTypeDeterminationDescription'))\
                    .alias("userTypeDeterminationDescription")\
                 )  
      status="Successfully update L2 User additional details"
    else:
      #df_l2DimUserAddedInfo=df_L2_user.drop('companyCode')
      status="The file L1 User does not exists in specified location: "+fileLocation
      df_l2DimUserAddedInfo=df_L2_user
    
    return df_l2DimUserAddedInfo ,status
  
  except Exception as err:
    raise 
   
