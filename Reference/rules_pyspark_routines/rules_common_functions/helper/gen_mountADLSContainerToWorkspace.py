# Databricks notebook source
import itertools

class gen_mountADLSContainer():

    def __isCredentialPassThroughEnabled():
        try:
            tag = list(itertools.filterfalse(lambda config : \
                config[0] != 'spark.databricks.passthrough.enabled',sc.getConf().getAll()))  
            if ((len(tag) == 0) or (tag[0][1] == 'false')):
              return False            
            else:
                return True
        except Exception as err:
            raise err

    def __mountADLSUsingServicePrincipal(analysisID, storageAccountName, gl_MountPoint):
        try:
            listOfMountPoints = [item.mountPoint for item in dbutils.fs.mounts()]
            
            if not(gl_MountPoint in listOfMountPoints):                
                #if (key.strip() == ""):               
                print('Mounting using key vault and secret.')
                # Application (Client) ID
                applicationId = dbutils.secrets.get(scope="rules-scope",key="ClientId")
                # Application (Client) Secret Key
                authenticationKey = dbutils.secrets.get(scope="rules-scope",key="ClientSecret")
                # Directory (Tenant) ID
                tenantId = dbutils.secrets.get(scope="rules-scope",key="TenantId")
    
                endpoint = "https://login.microsoftonline.com/" + tenantId + "/oauth2/token"
                source = "abfss://" + analysisID + "@" + storageAccountName + ".dfs.core.windows.net/"
    
                # Connecting using Service Principal secrets and OAuth
                configs = {"fs.azure.account.auth.type": "OAuth",
                           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                           "fs.azure.account.oauth2.client.id": applicationId,
                           "fs.azure.account.oauth2.client.secret": authenticationKey,
                           "fs.azure.account.oauth2.client.endpoint": endpoint}
    
                dbutils.fs.mount(
                source = source,
                mount_point = gl_MountPoint,
                extra_configs = configs)

            else:
                print("ADLS: " + storageAccountName + " is already mounted with the same mount point name:" + gl_MountPoint)

        except Exception as err:
            raise err
    
    def __mountADLSUsingADLSCredentailPassThrough(analysisID, storageAccountName, gl_MountPoint, folder=''):
        try:
            listOfMountPoints = [item.mountPoint for item in dbutils.fs.mounts()]
            if not(gl_MountPoint in listOfMountPoints):
                configs = {
                            "fs.azure.account.auth.type": "CustomAccessToken",
                            "fs.azure.account.custom.token.provider.class": 
                            spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
                          }
                source = "abfss://" + analysisID + "@" + storageAccountName + ".dfs.core.windows.net/" + folder
                dbutils.fs.mount(
                                  source = source,
                                  mount_point = gl_MountPoint,
                                  extra_configs = configs)
        except Exception as err:
            raise

    @staticmethod
    def gen_mountContainerToWorkspace_perform(analysisID, storageAccountName, gl_MountPoint,folder=''):
        try:
            global gl_ExecutionMode
            if(gen_mountADLSContainer.__isCredentialPassThroughEnabled() == False):
                print('dbfs mounting through SPN...')
                gl_ExecutionMode = EXECUTION_MODE.STANDARD
                gen_mountADLSContainer.__mountADLSUsingServicePrincipal(analysisID, storageAccountName, gl_MountPoint)
            else:
                print('dbfs mounting through AAD Credential passthrough..')
                gl_ExecutionMode = EXECUTION_MODE.DEBUGGING
                gen_mountADLSContainer.__mountADLSUsingADLSCredentailPassThrough(analysisID, storageAccountName, gl_MountPoint, folder)
        except Exception as err:
            raise
