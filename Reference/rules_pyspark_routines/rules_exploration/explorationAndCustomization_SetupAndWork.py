# Databricks notebook source
# MAGIC %md # Data Exploration and Customization Notebook
# MAGIC ##History
# MAGIC |KPMG Clara version|Comment|
# MAGIC |------------|-------------|
# MAGIC |2022.2.4|Release version|

# COMMAND ----------

# MAGIC %md ## Section 1: Setup environment for Exploration & Customization
# MAGIC >This section must be executed to enable use of the other sections.

# COMMAND ----------

# MAGIC %md #### Section 1.1: Install the Azure Blob Storage Library and the Azure Identity client for your credentials

# COMMAND ----------

pip install azure-storage-blob

# COMMAND ----------

pip install azure-identity

# COMMAND ----------

# MAGIC %md #### Section 1.2: Define location parameters for the Mount Point
# MAGIC >Insert the value from the analysis details pop-up in the widgets on top of the screen.

# COMMAND ----------

dbutils.widgets.text("ContainerName", "")
dbutils.widgets.text("SubDirectory", "")
dbutils.widgets.text("StorageAccount", "")

# COMMAND ----------

gl_analysisID = dbutils.widgets.get("ContainerName")
gl_analysisPhaseID = dbutils.widgets.get("SubDirectory")
gl_executionGroupID = ""
storageAccountName = dbutils.widgets.get("StorageAccount")
gl_ERPPackageID = ""

# COMMAND ----------

projectId = gl_analysisPhaseID.split('/')[0]
gl_analysisPhaseID = gl_analysisPhaseID.split('/',1)[1]

gl_MountPoint = '/mnt/' + storageAccountName + '-' + gl_analysisID + '-' + projectId

# COMMAND ----------

# MAGIC %md
# MAGIC >Within this Notebook user can also execute other Notebook directly. For this purpose, user needs to use **%run** command and specify the path to the Notebook that is supposed to be executed.
# MAGIC
# MAGIC >Below the notebooks required in this Exploration and Customization section:
# MAGIC > - GlobalVariables: Azure Data Lake shortcut path
# MAGIC > - Gen_constants: Class for specific validation review
# MAGIC > - GenericHelper: Function for read and write files
# MAGIC > - Gen_montADLSContainer: Function for mounting point

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/parameters/gen_globalVariables

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/parameters/gen_constants

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/helper/gen_commonFunctions

# COMMAND ----------

# MAGIC %run ../kpmg_rules_common_functions/helper/gen_mountADLSContainerToWorkspace

# COMMAND ----------

# MAGIC %md
# MAGIC >Mount point initialization

# COMMAND ----------

gen_mountADLSContainer.gen_mountContainerToWorkspace_perform(gl_analysisID, storageAccountName, gl_MountPoint, projectId)

# COMMAND ----------

objGenHelper = gen_genericHelper() 

# COMMAND ----------

# MAGIC %md #### To delete a mount point (below should be executed only when mountpoint deletion is required - uncomment to use it)

# COMMAND ----------

#dbutils.fs.unmount(gl_MountPoint)

# COMMAND ----------

# MAGIC %md ## Section 2: How to display and modify data?

# COMMAND ----------

# MAGIC %md
# MAGIC ####Section 2.1: Display the Mount point being used with following structure storageName-containerName-projectID which has been created in Section 1.

# COMMAND ----------

gl_MountPoint

# COMMAND ----------

# MAGIC %md #### Section 2.2: List of the files in the Azure Data Lake Storage (ADLS)
# MAGIC
# MAGIC >User can get a list of items of any folder by using corresponding command and a path to the folder as shown below:

# COMMAND ----------

display(dbutils.fs.ls(gl_rawFilesPath))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Section 2.3: Read data from ADLS
# MAGIC >To read file user has to use the gen_readFromFile_perform function and indicate the file path within the ADLS.
# MAGIC
# MAGIC >The file path can use shortcut provided in the *kpmg_rules_common_functions/parameters/gen_globalVariables* file (e.g. gl_CDMLayer1Path is gl_MountPoint + "/"+ gl_analysisPhaseID + "/cdm-l1/").
# MAGIC
# MAGIC >Otherwise, user will have to provide the full path to the file using the form: *Gl_Mountpoint + gl_analysisPhaseID* (e.g '/mnt/audeusdevsalcw02-wbdata-p-d564905f-4e50-ed11-b11b-000d3a104194' + "/" + 'a-b5ea4cc6-4e50-ed11-b11b-000d3a104194/h-b7ea4cc6-4e50-ed11-b11b-000d3a104194'). User can leverage the section 2.2 to get the full path.

# COMMAND ----------

fin_L1_TD_GLBalance = objGenHelper.gen_readFromFile_perform(gl_CDMLayer1Path + "fin_L1_TD_GLBalance.delta")

# COMMAND ----------

display(fin_L1_TD_GLBalance)

# COMMAND ----------

# MAGIC %md #### Section 2.4: Write data to ADLS (only to Exploration folder or authorized folders)

# COMMAND ----------

fin_L1_TD_GLBalance.write.format("delta").save(gl_Exploration +"Session1/" +"fin_L1_TD_GLBalance.delta")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Section 2.5: Modify data stored as Delta file on ADLS
# MAGIC
# MAGIC >Any Delta file can be modified (update, delete, upsert) as described below. Python API or Spark SQL API can be leveraged for this.
# MAGIC
# MAGIC >Additional information available: https://docs.delta.io/0.4.0/delta-update.html

# COMMAND ----------

# MAGIC %md #####Example with Python API
# MAGIC 1. Import python library to read delta files
# MAGIC 2. Read the file and define the file path

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

deltaTable_fin_L1_TD_GLBalance = DeltaTable.forPath(spark, gl_Exploration +"Session1/" +"fin_L1_TD_GLBalance.delta")

# COMMAND ----------

# MAGIC %md ######Update entrie(s) in file
# MAGIC 1. Update command with filter conditions and value to be set.
# MAGIC 2. Review the modification by displaying the file

# COMMAND ----------

deltaTable_fin_L1_TD_GLBalance.update(
    condition = "accountNumber = 3001 and financialPeriod = 0",
    set = {"companyCode": "'A001_updated'"}
)

# COMMAND ----------

deltaTable_fin_L1_TD_GLBalance.toDF().display()

# COMMAND ----------

# MAGIC %md ###### Delete entrie(s) in file
# MAGIC 1. Delete command with filter condition
# MAGIC 2. Review the modification by displaying the file

# COMMAND ----------

deltaTable_fin_L1_TD_GLBalance.delete(
    condition = "accountNumber = 3001 and financialPeriod = 0"
)

# COMMAND ----------

deltaTable_fin_L1_TD_GLBalance.toDF().display()

# COMMAND ----------

# MAGIC %md ##### Example with Spark SQL API
# MAGIC 1. Identify the file path of the delta file to be modified
# MAGIC 2. Create table based on the delta file (copy the file path with file name from the output of step 1 into step2 and needs to be between ``)
# MAGIC 3. Display data with select statement.
# MAGIC 4. Overwrite changes into delta file 

# COMMAND ----------

gl_CDMLayer1Path + "fin_L1_TD_Journal.delta"

# COMMAND ----------

# MAGIC %md
# MAGIC User needs to drop the table if previously created

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS fin_L1_TD_Journal

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS fin_L1_TD_Journal
# MAGIC AS SELECT * FROM delta.`filePathName/fileName`;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from fin_L1_TD_Journal

# COMMAND ----------

# MAGIC %md ###### Update entrie(s) in file
# MAGIC 1. Update command with filter conditions and value to be set
# MAGIC 2. Review the modification by displaying the file (either sql statement or spark.table(tablename).display)

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fin_L1_TD_Journal
# MAGIC SET companyCode = 'A001_Updated'
# MAGIC where documentNumber = 'Test1' and lineItem in (1,2)

# COMMAND ----------

spark.table("fin_L1_TD_Journal").display()

# COMMAND ----------

# MAGIC %md ###### Delete entrie(s) in file
# MAGIC 1. Delete command with filter condition
# MAGIC 2. Review the modification by displaying the file

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE 
# MAGIC FROM fin_L1_TD_Journal
# MAGIC where documentNumber = 'Test1' and lineItem = 2

# COMMAND ----------

spark.table("fin_L1_TD_Journal").display()

# COMMAND ----------

# MAGIC %md
# MAGIC > In order to apply changes done on the table (with the above sql steps), user has to overwrite existing delta file.

# COMMAND ----------

spark.table("fin_L1_TD_Journal").write.format("delta").mode("overwrite").save(gl_CDMLayer1Path + "fin_L1_TD_Journal.delta")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Section 2.6: Modify data stored as parquet, txt or csv file on ADLS
# MAGIC
# MAGIC To modify data stored in non-Delta format (e.g., parquet, txt or csv) user should follow the below steps:
# MAGIC 1. Read txt-file from ADLS into a DataFrame
# MAGIC 2. Write obtained DataFrame to exploration folder as Delta file
# MAGIC 3. Read obtained Delta file into a new DataFrame
# MAGIC 4. Modify Delta file using one of the approaches described above (with Python API or Spark SQL) or use additional one: TempView
# MAGIC 5. Overwrite non-Delta source file with modifed Delta file

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Read txt-file from ADLS into a DataFrame**

# COMMAND ----------

txt_JET_file = objGenHelper.gen_readFromFile_perform(gl_rawFilesPath + "CTD 2022 - JET.txt", delimiter='#|#')

# COMMAND ----------

txt_JET_file.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Write obtained DataFrame to exploration folder as Delta file**

# COMMAND ----------

txt_JET_file.write.mode("overwrite").format("delta").save(gl_Exploration +"/Session2/" +"delta_JET_file.delta")

# COMMAND ----------

# MAGIC %md
# MAGIC **3. Read obtained Delta file into a new DataFrame**

# COMMAND ----------

delta_JET_file = objGenHelper.gen_readFromFile_perform(gl_Exploration +"/Session2/" +"delta_JET_file.delta")

# COMMAND ----------

delta_JET_file.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **4. Modify Delta file using one of the approaches described above**
# MAGIC >User can leverage Spark SQL API or Python API as explained earlier. 
# MAGIC
# MAGIC >An additional approach is to use the TempView based approach explained in the next steps.

# COMMAND ----------

# MAGIC %md
# MAGIC **TempView based approach** to modify Delta file
# MAGIC 1. Use the createOrReplaceTempView on the file defined in the read (within the dataframe) section
# MAGIC 2. Display data
# MAGIC 3. Update command with filter condition and value to be set
# MAGIC 4. Review the modification by displaying the file (either sql statement or spark.table(tablename).display())

# COMMAND ----------

delta_JET_file.createOrReplaceTempView("delta_JET_file")

# COMMAND ----------

spark.table('delta_JET_file').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta_JET_file
# MAGIC SET companyCode = 'A001_Updated'
# MAGIC where documentNumber = 'Test1' and lineItem in (1,2)

# COMMAND ----------

spark.table('delta_JET_file').display()

# COMMAND ----------

# MAGIC %md
# MAGIC  **5.To apply changes done, user has to overwrite txt-file with modifed Delta file**

# COMMAND ----------

spark.table('delta_JET_file').write.options(delimiter='#|#').csv(path = gl_rawFilesPath + "CTD 2022 - JET.txt", mode='overwrite', quote = None,header = True,emptyValue = '',escape = '"')

# COMMAND ----------

# MAGIC %md
# MAGIC **Review the results**

# COMMAND ----------

txt_JET_ModifiedFile = objGenHelper.gen_readFromFile_perform(gl_rawFilesPath + "CTD 2022 - JET.txt", delimiter='#|#')
txt_JET_ModifiedFile.display()

# COMMAND ----------

# MAGIC %md ## Section 3: How to customise Code (New & Existing)?

# COMMAND ----------

# MAGIC %md #### Section 3.1: Modify an existing Stored Procedure
# MAGIC >Source code can be found in the version specific folder
# MAGIC
# MAGIC >List the files used in the snipset and create the respective views

# COMMAND ----------

fin_L1_TD_GLBalance = objGenHelper.gen_readFromFile_perform(gl_CDMLayer1Path + "fin_L1_TD_GLBalance.delta")
fin_L1_TD_Journal = objGenHelper.gen_readFromFile_perform(gl_CDMLayer1Path + "fin_L1_TD_Journal.delta")

# COMMAND ----------

fin_L1_TD_GLBalance.createOrReplaceTempView('fin_L1_TD_GLBalance')
fin_L1_TD_Journal.createOrReplaceTempView('fin_L1_TD_Journal')

# COMMAND ----------

# MAGIC %md
# MAGIC > Add the snipset to be changed, modify it and display/validate the output.

# COMMAND ----------

df1 = spark.sql('SELECT DISTINCT companyCode FROM fin_L1_TD_Journal UNION SELECT DISTINCT companyCode FROM fin_L1_TD_GLBalance')
df1.createOrReplaceTempView("vw_companyCode")
df2 = spark.sql('SELECT DISTINCT "000"	AS	clientCode, CASE WHEN ISNULL(CompanyCode) THEN "N/A" ELSE CompanyCode END	AS	clientName,CASE WHEN ISNULL(CompanyCode) THEN "N/A" ELSE CompanyCode END	AS	companyCode,CASE WHEN ISNULL(CompanyCode) THEN "N/A" ELSE CompanyCode END	AS	reportingGroup,"EN"	AS	reportingLanguageCode,"EN"	AS	clientDataReportingLanguage,"EN"	AS	clientDataSystemReportingLanguage,"EN"	AS	KPMGDataReportingLanguage FROM vw_companyCode')
df2.createOrReplaceTempView("vw_tmp_reportingsetup")

# COMMAND ----------

spark.table('vw_tmp_reportingsetup').display()

# COMMAND ----------

# MAGIC %md
# MAGIC >After modifying SP user needs to apply the change in the SP within the version specific folder and move it to respective ERP, L0, L1, L2 folder in Databricks Workspace.

# COMMAND ----------

# MAGIC %md #### Section 3.2: Add a new Stored Procedure
# MAGIC >User has to identify in which section of the execution dependency the new SP should be added by first reading the file and then display it.
# MAGIC 1. Read the file from ADLS into a DataFrame
# MAGIC 2. Display content of the file
# MAGIC 3. Write obtained DataFrame to exploration folder as a Delta file
# MAGIC 4. Read obtained Delta file into a new DataFrame
# MAGIC 5. Modify Delta file using on of the approaches described above (with Python API or Spark SQL or TempView). Here TempView will be used
# MAGIC 6. Display content of the new Delta file
# MAGIC 7. Update command with filter condition and values to be set
# MAGIC 8. Review the modification by displaying the file (either sql statement or spark.table(tablename).display())
# MAGIC 9. Overwrite existing file based on the modified Delta file

# COMMAND ----------

dic_ddic_executionDependency_csv = objGenHelper.gen_readFromFile_perform(gl_metadataPath + "dic_ddic_executionDependency.csv")

# COMMAND ----------

dic_ddic_executionDependency_csv.display()

# COMMAND ----------

dic_ddic_executionDependency_csv.write.mode("overwrite").format("delta").save(gl_Exploration +"/Session3/" +"dic_ddic_executionDependency.delta")

# COMMAND ----------

dic_ddic_executionDependency_delta = objGenHelper.gen_readFromFile_perform(gl_Exploration +"Session3/" +"dic_ddic_executionDependency.delta")

# COMMAND ----------

dic_ddic_executionDependency_delta.createOrReplaceTempView("dic_ddic_executionDependency_delta")

# COMMAND ----------

spark.table("dic_ddic_executionDependency_delta").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dic_ddic_executionDependency_delta values
# MAGIC ('10', 'GEN', 'ptp_L2_DIM_Vendor_NewSP_populate', 'gen_L2_DIM_Organization_populate', 'PA,JEA,LEAD,MAT,GTWA,KPI,GMT,PEX,GMTR,SAPA,TODA,WDE,AA', null, 139)

# COMMAND ----------

spark.table("dic_ddic_executionDependency_delta").display()

# COMMAND ----------

objGenHelper.gen_writeSingleCsvFile_perform(df = dic_ddic_executionDependency_delta,targetFile = gl_metadataPath + "dic_ddic_executionDependency.csv")
