import sys
import os
import time
import urllib
import traceback
import pandas as pd
from mysql.connector import Error
from sqlalchemy import create_engine
"""
This file is untracked in Git
"""

ProfileID = int(sys.argv[1])

server_python_exe  = r'"C:\Users\Public\Public Scripts\DST AMS_CLM Azure Repo\venv\Scripts\python.exe"'
local_python_exe = r'C:\Users\581086\Scripts\AMS_CLM\venv\Scripts\python.exe'

python_exe = local_python_exe
#######################################
LocalScriptDir = r"C:/Users/581086/Scripts/AMS_CLM/AMS_"
ServerScriptDir = r'"C:\Users\Public\Public Scripts\AMS_CLM\AMS_"'

ScriptDir = LocalScriptDir
##############################################
server_output = 'C:\\Users\\Public\\Public Scripts\\AMS_CLM\\AMS_Trigger\\output'
local_output = 'C:\\Users\\581086\\Scripts\\AMS_CLM\\AMS_Trigger\\output'

output = local_output

###################### DB CONNECTION ######################################

class SQLServerUtilities(object):
    """
    Clss that conn to AMS DB
    """
    
    def __init__(self, Environment):
        scriptDir = os.path.dirname(os.path.abspath(__file__))

        # application_path = os.path.dirname(os.path.abspath(__file__))
        
        if getattr(sys, 'frozen', False):
            # Get Path of where Executable is running
            # If the application is run as a bundle/exe, the PyInstaller bootloader
            # extends the sys module by a flag frozen=True
            scriptDir = os.path.dirname(sys.executable)
        else:
            # Get Path of where Script is running
            scriptDir = os.path.dirname(os.path.abspath(__file__))
        Params = eval(open(scriptDir + '\\dbEnv.txt', 'r').read())
        
        self.SQLServer = Params[Environment]['SQLServer']
        # print(self.SQLServer)
        self.SQLUser = Params[Environment]['SQLUser']
        self.SQLPassword = Params[Environment]['SQLPassword']
    
    def AMSDBconnect(self):
        """dsfsd
        """
        Connection_string = (
                "Driver={};"
                "Server={};"
                "UID={};"
                "PWD={};"
                "Database=AMSDB;"
        ).format('ODBC Driver 17 for SQL Server', self.SQLServer, self.SQLUser, self.SQLPassword)
        
        quoted = urllib.parse.quote_plus(Connection_string)  # type: ignore
        engine = create_engine(
                f"mssql+pyodbc:///?odbc_connect={quoted}", fast_executemany=True
                )
        # with engine.connect() as cnn:
        con = engine.connect()
        return con

try:
    Environment = sys.argv[2]
    con = SQLServerUtilities(Environment).AMSDBconnect()


except Exception as error:
    print('---')
    print('An exception occurred: {}'.format(error))
    # traceBack = str(traceback.format_exc()).replace('\n', ' ').replace('\r', '')
    print('---')
    print(traceback.format_exc())


## DocuSignAPI ##
class DemoEnvironment(object):
    clm_base_url = "https://apiuatna11.springcm.com"
    esign_base_url = "https://demo.docusign.net/restapi"
    ds_account_id = "xxx-xxx-xxx-xx-xxxxx"
    endpoint_instance = "uatna11"
    iss = "xxxxxxx-"
    sub = "xxxxxxxx"
    aud = "account-d.docusign.com"
    scope = tuple(['signature', 'impersonation', 'spring_read', 'spring_write'])
    private_key = "-----BEGIN RSA PRIVATE KEY-----\"

class ProductionEnvironment(object):
    clm_base_url = "https://apina11.springcm.com"
    esign_base_url = "https://na4.docusign.net/restapi"
    ds_account_id = "xxx-xxx-xxx-xxx-xxxxxx"
    endpoint_instance = "apina11"
    iss = "xxxxxx"
    sub = "xxxxxx"
    aud = "account.docusign.com"
    scope = tuple(['signature', 'impersonation', 'spring_read', 'spring_write'])
    private_key = "-----BEGIN RSA PRIVATE KEY-----"


if __name__ == "__main__":
    print('AMS Configration')

