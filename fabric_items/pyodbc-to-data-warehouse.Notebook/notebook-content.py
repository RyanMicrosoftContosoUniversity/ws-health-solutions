# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

"""
This module will write log data to sql table objects of expected structure
"""
import pyodbc
import datetime
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import notebookutils.mssparkutils


class FabricPipelinesDataWarehouseClient:
    def __init__(self, server_name:str, database_name:str, client_id:str, client_secret:str):
        self.server_name = server_name
        self.database_name = database_name
        self.client_id = client_id
        self.client_secret = client_secret
        self.connection = self._connect_to_database()


    def _connect_to_database(self):
        """
        Connect to database via ActiveDirectoryServicePrincipal
        """
        
        constr = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={self.server_name};"
        f"Database={self.database_name};"
        f"UID={self.client_id};"
        f"PWD={self.client_secret};"
        f"Authentication=ActiveDirectoryServicePrincipal;"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
        )

        conn = pyodbc.connect(constr)
        return conn

        
    def close_connection(self):
        self.connection.close()

    def execute_sp(self, command:str)->str:
        """
        Execute a stored proc and return the results
        Note this will only ever return the top row
        """
        conn = self._connect_to_database()
        result = conn.execute(command).fetchall()
        conn.close()

        return result[0][0]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SPN/ app registration authentication --> ActiveDirectoryServicePrincipal


server_name = 'ftykynmhjpteverb75ok7vbqwq-vg4kpqas5nnetoddfs2yhyyrkq.datawarehouse.fabric.microsoft.com'
database_name = 'test_wh'
client_id = '15a84224-c1e2-45e0-a2f5-fc8f5206f81d'
kv_url = 'https://kvfabricprodeus2rh.vault.azure.net/'
secret_name = 'spn-secret'


client_secret = mssparkutils.credentials.getSecret(kv_url, secret_name)

client = FabricPipelinesDataWarehouseClient(server_name, database_name, client_id, client_secret)

result = client.execute_sp('EXEC test_InsertRecord @id=6, @datetime_value="2025-04-18 12:00:00"')

print(result)

notebookutils.notebook.exit(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
