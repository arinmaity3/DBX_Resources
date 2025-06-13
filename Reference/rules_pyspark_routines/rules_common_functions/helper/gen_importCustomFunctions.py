# Databricks notebook source
import requests
import base64
import os

class gen_customFunctions():
  @staticmethod
  def customFunctions_load(callerNoteBook,analysisID):
    try:
      orchestartion = callerNoteBook[callerNoteBook.rindex('/')+1:]      
      if(orchestartion == 'orchestration_erp'):
        customFunctionPath = "/custom_functions/" + analysisID + "/erp"
      elif(orchestartion == 'orchestration_generic_layer_one'):
        customFunctionPath = "/custom_functions/" + analysisID + "/layer_one"
      elif(orchestartion == 'orchestration_generic_layer_two'):
        customFunctionPath = "/custom_functions/" + analysisID + "/layer_two"
      elif(orchestartion == 'orchestration_generic_layer_zero'):
        customFunctionPath = "/custom_functions/" + analysisID + "/layer_zero"        
              
      api_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
      host_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
      
      # fetch all custom function names
      response = requests.get(f"{api_url}/api/2.0/workspace/list",  
							  json = {"format": "SOURCE", "path": customFunctionPath},
                              headers={"Authorization": f"Bearer {host_token}"}
							  ).json()
            
      if 'error_code' in response.keys():
        print('No custom funcitons defined for this analysis...')  
        return
      lstOfCustomFunctions = list()
      [lstOfCustomFunctions.append(l['path']) for item in response.values() for l in item]
            
      if(len(lstOfCustomFunctions) >0):
        for f in lstOfCustomFunctions:
          # fetch notebook
          response = requests.get(f"{api_url}/api/2.0/workspace/export",
                                  json = {"format": "SOURCE", "path": f},
                                  headers={"Authorization": f"Bearer {host_token}"}
                                 ).json()
          
           # decode base64 encoded content
          data = base64.b64decode(response["content"].encode("ascii"))
          script = data.decode("utf-8")          
          exec(script,globals())
    except Exception as err:
      raise err

#gen_customFucntions_load(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
 #                        'd2a352fc-4a01-4720-bed8-43c496324e48')
