# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import pyodbc

class FabricPipelinesDataWarehouseClient:
    def __init__(self, server_name:str, client_id:str, client_secret:str, database_name:str=None, kind:str='sp'):
        self.server_name = server_name
        self.database_name = database_name
        self.client_id = client_id
        self.client_secret = client_secret
        self.connection = self._connect_to_database()


    def _connect_to_database(self):
        """
        Connect to database via ActiveDirectoryServicePrincipal
        """
        
        if database_name is None and kind=='sp':
            constr = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={self.server_name};"
            f"UID={self.client_id};"
            f"PWD={self.client_secret};"
            f"Authentication=ActiveDirectoryServicePrincipal;"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
            )
            
        else:
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
    
    def execute(self, command:str)->str:
        """
        Execute a SQL Command
        """
        conn = self._connect_to_database()
        result = conn.execute(command).fetchall()

        return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# connection for Fabric Data Warehouse

server_name = 'ftykynmhjpteverb75ok7vbqwq-vg4kpqas5nnetoddfs2yhyyrkq.datawarehouse.fabric.microsoft.com'
database_name = 'test_wh'
client_id = '15a84224-c1e2-45e0-a2f5-fc8f5206f81d'
kv_url = 'https://kvfabricprodeus2rh.vault.azure.net/'
secret_name = 'spn-secret'


client_secret = mssparkutils.credentials.getSecret(kv_url, secret_name)

dw_client = FabricPipelinesDataWarehouseClient(server_name, client_id, client_secret, database_name,)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# read from Fabric Data Warehouse

dw_result = dw_client.execute('SELECT * FROM test_table')

print(dw_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Connection for Azure SQL Server Public
# Driver={ODBC Driver 18 for SQL Server};Server=tcp:fabric-metadata-server.database.windows.net,1433;Database=newadforchestrationdb;Uid=rharrington;Pwd={your_password_here};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;

server_name = 'fabric-metadata-server.database.windows.net'
database_name = 'newadforchestrationdb'
client_id = '15a84224-c1e2-45e0-a2f5-fc8f5206f81d'
kv_url = 'https://kvfabricprodeus2rh.vault.azure.net/'
secret_name = 'spn-secret'


client_secret = mssparkutils.credentials.getSecret(kv_url, secret_name)

sql_client = FabricPipelinesDataWarehouseClient(server_name, client_id, client_secret, database_name,)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read from Azure SQL Server Public

sql_result = sql_client.execute('SELECT * FROM dbo.apiOrchestration')

print(sql_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Connection for SQL MI Private

server_name = 'sql-mi-private.e398fc3b6400.database.windows.net'
database_name = 'test'
client_id = '15a84224-c1e2-45e0-a2f5-fc8f5206f81d'
kv_url = 'https://kvfabricprodeus2rh.vault.azure.net/'
secret_name = 'spn-secret'


client_secret = mssparkutils.credentials.getSecret(kv_url, secret_name)

sql_mi_client = FabricPipelinesDataWarehouseClient(server_name, client_id, client_secret, database_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read from Azure SQL Server Public

sql_result = sql_mi_client.execute('SELECT * FROM test_table')

print(sql_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Connection for Azure SQL Private

server_name = 'private-fabric-sql-server.database.windows.net'
database_name = 'private-fabric-sql-db'
client_id = '15a84224-c1e2-45e0-a2f5-fc8f5206f81d'
kv_url = 'https://kvfabricprodeus2rh.vault.azure.net/'
secret_name = 'spn-secret'


client_secret = mssparkutils.credentials.getSecret(kv_url, secret_name)

sql_private_client = FabricPipelinesDataWarehouseClient(server_name, client_id, client_secret, database_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read from Azure SQL Server Private

sql_result = sql_private_client.execute('SELECT * FROM SalesLT.Customer')

print(sql_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
