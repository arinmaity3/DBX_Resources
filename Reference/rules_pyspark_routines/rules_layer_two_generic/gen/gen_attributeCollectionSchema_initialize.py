# Databricks notebook source
from pyspark.sql.functions import expr
import pyspark
from pyspark.sql import SparkSession
from dateutil.parser import parse
from pyspark.sql.functions import row_number,lit ,abs,col, asc,upper
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,ShortType ,BooleanType ,TimestampType



#region Global lists of dimension attributes

gl_lstOfOrganizationCollect = list()
gl_lstOfGLAccountCollect = list()
gl_lstOfPostingKeyCollect = list()
gl_lstOfReferenceTransactionCollect = list()
gl_lstOfDocTypeCollect = list()
gl_lstOfUserCollect= list()
gl_lstOfCalendarDateCollect= list()

gl_lstOfVendorCollect = list()
gl_lstOfCurrencyCollect = list()
gl_lstOfProductCollect = list()
gl_lstOfBusinessTransactionCollect = list()
gl_lstOfCustomerCollect = list()
gl_lstOfTransactionCollect = list()
gl_lstOfFinancialPeriodCollect = list()

#endregion

#region Organization 

organizationSchemaCollect = StructType([StructField("companyCode",StringType(),True),\
                                 StructField("key",StringType(),True)])

#endregion 

#region financailPeriod

financailPerodSchemaCollect = StructType([StructField("companyCode",StringType(),True)\
                                  ,StructField("fiscalYear",StringType(),True)\
                                  ,StructField("calendarDate",DateType(),True)\
                                  ,StructField("financialPeriod",StringType(),True)\
                                  ,StructField("key",StringType(),True)])

#endregion

#region Posting key
  
postingKeySchemaCollect = StructType([StructField("companyCode",StringType(),True)\
                               ,StructField("postingKey",StringType(),True)\
                               ,StructField("key",StringType(),True)])

#endregion 

#region Product

productSchemaCollect = StructType([StructField("companyCode",StringType(),True),\
                             StructField("productNumber",StringType(),True),\
                             StructField("productArtificialID",StringType(),True),\
                             StructField("key",StringType(),True)])
#endregion 


#region ReferenceTransaction 

referenceTransactionSchemaCollect = StructType([StructField("companyCode",StringType(),True),\
                                        StructField("referenceTransactionCode",StringType(),True),\
                                        StructField("key",StringType(),True)])

#endregion 

#region BusinessTransaction  
businessTransactionSchemaCollect = StructType([StructField("companyCode",StringType(),True),\
                                        StructField("businessTransactionCode",StringType(),True),\
                                        StructField("key",StringType(),True)])
#endregion 

#region CalendarDate
calendarDateSchemaCollect = StructType([StructField("companyCode",StringType(),True)\
                                 ,StructField("calendarDate",DateType(),True)\
                                 ,StructField("key",StringType(),True)])
#endregion  

#region Currency
currencySchemaCollect = StructType([StructField("companyCode",StringType(),True),\
                             StructField("currencyCode",StringType(),True),\
                             StructField("key",StringType(),True)])
#endregion 

#region Currency
docTypeSchemaCollect = StructType([StructField("companyCode",StringType(),True),\
                           StructField("documentType",StringType(),True),\
                           StructField("key",StringType(),True)])
#endregion 

#region Transaction
transactionSchemaCollect = StructType([StructField("companyCode",StringType(),True),\
                                StructField("transactionType",StringType(),True),\
                                StructField("key",StringType(),True)])

#endregion 

#region User
userSchemaCollect = StructType([StructField("companyCode",StringType(),True),\
                                StructField("userName",StringType(),True),\
                                StructField("key",StringType(),True)])
#endregion

#region Vendor
vendorSchemaCollect = StructType([StructField("companyCode",StringType(),True),\
                           StructField("vendorNumber",StringType(),True),\
                           StructField("key",StringType(),True)])
#endregion

#region Customer
customerSchemaCollect = StructType([StructField("companyCode",StringType(),True),\
                            StructField("customerNumber",StringType(),True),\
                            StructField("key",StringType(),True)])
#endregion

#region GLAccount
accountSchemaCollect = StructType([StructField("companyCode",StringType(),True),\
                            StructField("accountNumber",StringType(),True),\
                            StructField("key",StringType(),True)])
#endregion


dimensionSchemCollection = {'gl_lstOfProductCollect': 'productSchemaCollect'
                            ,'gl_lstOfTransactionCollect' : 'transactionSchemaCollect'
                            ,'gl_lstOfPostingKeyCollect' : 'postingKeySchemaCollect'
                            ,'gl_lstOfReferenceTransactionCollect' : 'referenceTransactionSchemaCollect' 
                            ,'gl_lstOfBusinessTransactionCollect': 'businessTransactionSchemaCollect' 
                            #,'gl_lstOfCalendarDateCollect' : 'calendarDateSchemaCollect' 
                            #,'gl_lstOfCurrencyCollect': 'currencySchemaCollect'
                            ,'gl_lstOfDocTypeCollect' : 'docTypeSchemaCollect'                            
                            ,'gl_lstOfUserCollect': 'userSchemaCollect'
                            ,'gl_lstOfVendorCollect' : 'vendorSchemaCollect'
                            ,'gl_lstOfCustomerCollect' : 'customerSchemaCollect'
                            ,'gl_lstOfGLAccountCollect' : 'accountSchemaCollect'
                            #,'gl_lstOfFinancialPeriodCollect' : 'financailPerodSchemaCollect'
                          }
