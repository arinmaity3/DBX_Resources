# Databricks notebook source
import sys
import traceback
import warnings
import chardet   
#import codecs
import pathlib
from pathlib import Path
from pyspark.sql import Row
import io
import os
from os.path import exists
from chardet.universaldetector   import UniversalDetector

def _IsUTF8Bytes(data):
  charByteCounter = 1
  for i in range(len(data)):
    curByte = data[i]
    if (charByteCounter == 1):
      if (int(curByte, base=16) >= 0x80):
        while (((int(curByte, base=16) << 1) & 0x80) != 0):
           charByteCounter += 1
           curByte = hex(int(curByte, base=16) << 1)
        if (charByteCounter == 1 or charByteCounter > 6):
           return False
    else:
      if ((int(curByte, base=16) & 0xC0) != 0x80):
         return False
      charByteCounter -= 1

    if (charByteCounter > 1):
       warnings.warn("Warning: Error byte format")
       ###return False
  return True    

def _fileEncoding_validate_V0(fileFullPath,fileName, fileID, fileType):
  try:
    global gl_fileEncoding
    objGenHelper = gen_genericHelper()
    #executionStatusID = str(LOG_EXECUTION_STATUS.STARTED.name)
    executionStatus = ""
    fileEncoding=""
    # Encoding detection logic
    detectedEncoding = 'Encoding is yet to be detected'
    lstOfValidationStatusInfo = list()
    f = open('/dbfs' + fileFullPath, "rb")
    # the first 5000 elements of a file in hex format
    ls_bytes = f.read(5000)
    get_hex = ls_bytes.hex()

    # maintain all hex-symbols in one list
    ls_hex_values = []
    for i in range(len(get_hex) // 2):
        ls_hex_values.append('0x' + get_hex[i * 2:i * 2 + 2])

    if (int(ls_hex_values[0], base=16) == 0x00 and
            int(ls_hex_values[1], base=16) == 0x00 and
            int(ls_hex_values[2], base=16) == 0xFE and
            int(ls_hex_values[3], base=16) == 0xFF):  ###### Heuristic 1
        detectedEncoding = 'utf-32BE'
        #gl_fileEncoding[fileID] = [fileType,detectedEncoding]
        fileEncoding=detectedEncoding
        print('Heuristic 1')
        return lstOfValidationStatusInfo

    elif (int(ls_hex_values[0], base=16) == 0xFF and
          int(ls_hex_values[1], base=16) == 0xFE and
          int(ls_hex_values[2], base=16) == 0x00 and
          int(ls_hex_values[3], base=16) == 0x00):  #### Heuristic 2
        detectedEncoding = 'UTF-32 little-endian'
        #gl_fileEncoding[fileID] = [fileType,detectedEncoding]
        fileEncoding=detectedEncoding
        fileEncoding=""
        print('Heuristic 2')
        return lstOfValidationStatusInfo

    elif (int(ls_hex_values[0], base=16) == 0xFF and
          int(ls_hex_values[1], base=16) == 0xFE):  #### Heuristic 3
        detectedEncoding = 'UTF-16'
        #gl_fileEncoding[fileID] = [fileType,detectedEncoding]
        fileEncoding=detectedEncoding
        print('Heuristic 3')
        return lstOfValidationStatusInfo

    elif (int(ls_hex_values[0], base=16) == 0xEF and
          int(ls_hex_values[1], base=16) == 0xBB and
          int(ls_hex_values[2], base=16) == 0xBF):  #### Heuristic 4
        detectedEncoding = 'UTF8'
        #gl_fileEncoding[fileID] = [fileType,detectedEncoding]
        fileEncoding=detectedEncoding
        print('Heuristic 4')
        return lstOfValidationStatusInfo

    elif (int(ls_hex_values[0], base=16) == 0x2B and
          int(ls_hex_values[1], base=16) == 0x2F and
          int(ls_hex_values[2], base=16) == 0x76):  #### Heuristic 5
        detectedEncoding = 'UTF7'
        #gl_fileEncoding[fileID] = [fileType,detectedEncoding]
        fileEncoding=detectedEncoding
        print('Heuristic 5')
        return lstOfValidationStatusInfo
    else:
        detectedEncoding = 'Encoding is yet to be detected'
        #gl_fileEncoding[fileID] = [fileType,'UTF8']
        fileEncoding='UTF8'

    i = 1
    utf8 = False
    while (i < (len(ls_hex_values) - 4)):
        if (int(ls_hex_values[i],
                base=16) <= 0x7F):  # (explanation from c# script) If all characters are below 0x80, then it is valid UTF8, but UTF8 is not 'required' (and therefore the text is more desirable to be treated as the default codepage of the computer). Hence, there's no "utf8 = true;" code unlike the next three checks.
            i += 1
            continue
        if (int(ls_hex_values[i], base=16) >= 0xE0  ####  Heuristic 6
                and int(ls_hex_values[i], base=16) <= 0xF0
                and int(ls_hex_values[i + 1], base=16) >= 0x80
                and int(ls_hex_values[i + 1], base=16) < 0xC0
                and int(ls_hex_values[i + 2], base=16) >= 0x80
                and int(ls_hex_values[i + 2], base=16) < 0xC0):
            i += 2
            utf8 = True
            print('Heuristic 6')
            continue
        if (int(ls_hex_values[i], base=16) >= 0xE0  ####   Heuristic 7
                and int(ls_hex_values[i], base=16) <= 0xF0
                and int(ls_hex_values[i + 1], base=16) >= 0x80
                and int(ls_hex_values[i + 1], base=16) < 0xC0
                and int(ls_hex_values[i + 2], base=16) >= 0x80
                and int(ls_hex_values[i + 2], base=16) < 0xC0):
            i += 3
            utf8 = True
            print('Heuristic 7')
            continue
        if (int(ls_hex_values[i], base=16) >= 0xF0  ####   Heuristic 8
                and int(ls_hex_values[i], base=16) <= 0xF4
                and int(ls_hex_values[i + 1], base=16) >= 0x80
                and int(ls_hex_values[i + 1], base=16) < 0xC0
                and int(ls_hex_values[i + 2], base=16) >= 0x80
                and int(ls_hex_values[i + 2], base=16) < 0xC0
                and int(ls_hex_values[i + 3], base=16) >= 0x80
                and int(ls_hex_values[i + 3], base=16) < 0xC0):
            i += 4
            utf8 = True 
            print('Heuristic 8')
            continue
        utf8 = False
        break

    if (utf8 == True):
        detectedEncoding = 'UTF8'
        #gl_fileEncoding[fileID] = [fileType,detectedEncoding]
        fileEncoding=detectedEncoding
        return lstOfValidationStatusInfo

    elif (_IsUTF8Bytes(ls_hex_values)):  ####   Heuristic 9
        detectedEncoding = 'UTF8'
        #gl_fileEncoding[fileID] = [fileType,detectedEncoding]
        fileEncoding=detectedEncoding
        print('Heuristic 9')
        return lstOfValidationStatusInfo

    # The next check is a heuristic attempt to detect UTF-16 without a BOM.  ### Heuristic 10
    # We simply look for zeroes in odd or even byte places, and if a certain
    # threshold is reached, the code is 'probably' UF-16.
    threshold = 0.1  # proportion of chars step 2 which must be zeroed to be diagnosed as utf-16. 0.1 = 10%
    count = 0
    for n in range(0, len(ls_hex_values), 2):
        if (int(ls_hex_values[n], base=16) == 0x00):
            count += 1
    if ((count / len(ls_hex_values)) > threshold):
        detectedEncoding = 'BigEndianUnicode'
        #gl_fileEncoding[fileID] = [fileType,detectedEncoding]
        fileEncoding=detectedEncoding
        print('Heuristic 10')
        return lstOfValidationStatusInfo

    count = 0  ### Heuristic 11
    for n in range(1, len(ls_hex_values), 2):
        if (int(ls_hex_values[n], base=16) == 0x00):
           count += 1
    if ((count / len(ls_hex_values)) > threshold):
        detectedEncoding = 'Unicode'
        #gl_fileEncoding[fileID] = [fileType,detectedEncoding]
        fileEncoding=detectedEncoding
        print('Heuristic 11')
        return lstOfValidationStatusInfo

    # Finally, a long shot - let's see if we can find "charset=xyz" or
    # "encoding=xyz" to identify the encoding:
    for n in range(0, len(ls_hex_values), 1):  ### Heuristic 12
        if (
                (
                        (ls_hex_values[n + 0] == 'c' or ls_hex_values[n + 0] == 'C') and
                        (ls_hex_values[n + 1] == 'h' or ls_hex_values[n + 1] == 'H') and
                        (ls_hex_values[n + 2] == 'a' or ls_hex_values[n + 2] == 'A') and
                        (ls_hex_values[n + 3] == 'r' or ls_hex_values[n + 3] == 'R') and
                        (ls_hex_values[n + 4] == 's' or ls_hex_values[n + 4] == 'S') and
                        (ls_hex_values[n + 5] == 'e' or ls_hex_values[n + 5] == 'E') and
                        (ls_hex_values[n + 6] == 't' or ls_hex_values[n + 6] == 'T') and
                        (ls_hex_values[n + 7] == '=')
                )
                or
                (
                        (ls_hex_values[n + 0] == 'e' or ls_hex_values[n + 0] == 'E') and
                        (ls_hex_values[n + 1] == 'n' or ls_hex_values[n + 1] == 'N') and
                        (ls_hex_values[n + 2] == 'c' or ls_hex_values[n + 2] == 'C') and
                        (ls_hex_values[n + 3] == 'o' or ls_hex_values[n + 3] == 'O') and
                        (ls_hex_values[n + 4] == 'd' or ls_hex_values[n + 4] == 'D') and
                        (ls_hex_values[n + 5] == 'i' or ls_hex_values[n + 5] == 'I') and
                        (ls_hex_values[n + 6] == 'n' or ls_hex_values[n + 6] == 'N') and
                        (ls_hex_values[n + 7] == 'g' or ls_hex_values[n + 7] == 'G') and
                        (ls_hex_values[n + 8] == '=')
                )):
            if (ls_hex_values[n + 0] == 'c' or ls_hex_values[n + 0] == 'C'):
                n += 8
            else:
                n += 9
            if (ls_hex_values[n] == '"' or ls_hex_values[n] == '\''):
                n += 1
            oldn = n
            while (n < len(ls_hex_values) and
                   (ls_hex_values[n] == '_' or
                    ls_hex_values[n] == '-' or
                    (ls_hex_values[n] >= '0' and ls_hex_values[n] <= '9') or
                    (ls_hex_values[n] >= 'a' and ls_hex_values[n] <= 'z') or
                    (ls_hex_values[n] >= 'A' and ls_hex_values[n] <= 'Z')
                   )
            ):
                n += 1
            nb = ls_hex_values[oldn:(oldn + n - oldn)]
            try:
                internalEnc = ''
                for i in range(0, len(nb)):
                    bytes_object = bytes.fromhex(nb[i][2:])
                    internalEnc += bytes_object.decode("ASCII")
                    text = bytes.fromhex(get_hex[0]).decode("ASCII")
                    detectedEncoding = internalEnc
                    print('Heuristic 12')
            except:
                break  # If C# doesn't recognize the name of the encoding, break.

    #gl_fileEncoding[fileID] = [fileType,"ASCII"]
    fileEncoding="ASCII"
  except Exception as e:
    executionStatus =  objGenHelper.gen_exceptionDetails_log()
    executionStatusID = LOG_EXECUTION_STATUS.FAILED     
    gen_usf_workSpace_clean(executionStatus)
    #gl_fileEncoding[fileID] = [fileType,'UTF8']
    fileEncoding="UTF8"

  finally:
    
    if detectedEncoding=='Encoding is yet to be detected':
      #gl_fileEncoding[fileID] = [fileType,'UTF8']
      fileEncoding="UTF8"
      executionStatus =  "For" + fileName + ": " + detectedEncoding
      executionStatusID = LOG_EXECUTION_STATUS.FAILED
    else:
      executionStatus= "The encoding of " + fileName + " is " + detectedEncoding
      executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
    #lstOfValidationResult = executionLog.add(executionStatusID,logID,executionStatus)
    #gl_lstOfImportValidationSummary.append(lstOfValidationResult)
    #print(executionStatus)
    return [executionStatusID,executionStatus,fileEncoding]



def _fileEncoding_validate(fileFullPath,fileName, fileID, fileType):
  try:
    listUTF8 = ['UTF-8', 'UTF8', 'UTF-8-SIG','UTF-8-BOM']
    executionStatus = ""
    fileEncoding=""
    # Encoding detection logic
    detectedEncoding = 'Encoding is yet to be detected'
    file="/dbfs"+ fileFullPath
    
    BLOCKSIZE = 1048576 # or some other, desired size in bytes
    #source = 'cp ' + '"/dbfs' + fileFullPath + '"'
    try:
      #os.system(source + ' /tmp')
      #targetFile = "/tmp/" + fileName
      detector = UniversalDetector()
      with io.open(file = file, mode="rb") as sourceFile:
        while True:
          contents = sourceFile.read(BLOCKSIZE)
          detector.feed(contents)
          if not contents: break
          if detector.done: break
      detector.close()
      detectedEncoding = detector.result['encoding']
    except Exception as e:
      if 'java.io.FileNotFoundException' in str(e):
        pass
  finally:
    try:
      newFileName = '"'+ fileName + '"'
      #if os.path.exists('/tmp/' + newFileName):
      #  os.system('rm /tmp/' + newFileName)
    except Exception as e:
      if 'java.io.FileNotFoundException' in str(e):
        pass       
    if detectedEncoding=='Encoding is yet to be detected':
      fileEncoding="UTF8"
      executionStatus = "For" + fileName + ": " + detectedEncoding
      executionStatusID = LOG_EXECUTION_STATUS.FAILED
    else:
      if str(detectedEncoding).upper() in listUTF8 :
        fileEncoding="UTF8"
      else:
        fileEncoding=detectedEncoding
      executionStatus= "The encoding of " + fileName + " is " + fileEncoding
      executionStatusID = LOG_EXECUTION_STATUS.SUCCESS
  return [executionStatusID,executionStatus,fileEncoding]  

def app_fileEncoding_validate( fileName, fileID, fileType, executionID,filePath):
  try:
    global gl_fileEncoding
    global gl_lstOfImportValidationSummary
    
    logID = executionLog.init(PROCESS_ID.IMPORT_VALIDATION,fileID,fileName,fileType,VALIDATION_ID.FILE_ENCODING,executionID = executionID)    
    objGenHelper = gen_genericHelper()
    
    executionStatusID = ""
    executionStatus = "" 
        
    #lstOfStatus=_fileEncoding_validate_V0(filePath,fileName,fileID,fileType)
    lstOfStatus=_fileEncoding_validate(filePath,fileName,fileID,fileType)
    executionStatus  = lstOfStatus[1]
    detectedEncoding = lstOfStatus[2]
    
    if(lstOfStatus[0] == LOG_EXECUTION_STATUS.SUCCESS):            
      executionLog.add(LOG_EXECUTION_STATUS.SUCCESS,logID,lstOfStatus[1])  
      return [LOG_EXECUTION_STATUS.SUCCESS,lstOfStatus[1],detectedEncoding]
    else:            
      executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,lstOfStatus[1])  
      return [LOG_EXECUTION_STATUS.FAILED,lstOfStatus[1],detectedEncoding]
  except Exception as err:        
    executionStatus = objGenHelper.gen_exceptionDetails_log()       
    executionLog.add(LOG_EXECUTION_STATUS.FAILED,logID,executionStatus)
    detectedEncoding="UTF8"
    return [LOG_EXECUTION_STATUS.FAILED,executionStatus,detectedEncoding] 
  
  


 
  
