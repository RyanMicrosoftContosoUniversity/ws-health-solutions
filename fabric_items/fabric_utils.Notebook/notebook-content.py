# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************


# CELL ********************

from urllib.parse import urlencode
import requests
import notebookutils
from notebookutils import mssparkutils
import time
import sys
import json
from datetime import datetime, timezone
import logging
import os
import pyodbc
import re
from pyspark.sql.functions import current_timestamp, lit, concat_ws, sha2, struct, col, when, to_timestamp
from notebookutils import mssparkutils
from datetime import datetime, timezone
import os
import shutil
from notebookutils.mssparkutils.fs import ls
from pyspark.sql import DataFrame, Row
from enum import Enum
from pyspark.errors import SparkUpgradeException

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class Layer(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    PLATINUM = "platinum"
    GOLD = "gold"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class Fabric_Utils:
    @staticmethod
    def api_request_with_stats(base_url:str, api:str, url_args:str)-> tuple[float, dict, int, datetime, datetime, str]:

        """
        Make API Request and time duration and counts for stats

        Parameters:
        base_url (str): The base URL for the API request
        api (str): The API endpoint
        url_args (str): The URL arguments

        Returns:
        tuple: A tuple containing the elapsed time for the request, 
        the JSON response as a dictionary,
        the size of the file in bytes,
        request start time in UTC
        request stop time in UTC
        """
        start_time = time.time()
        
        print(f'Making request to API endpoint: {base_url}{api}?{url_args} ')

        response = requests.get(f'{base_url}{api}?{url_args}', 
        headers=headers)

        end_time = time.time()
        elapsed_time = end_time - start_time

        size = sys.getsizeof(json.dumps(response.json()))

        start_time_dt = datetime.fromtimestamp(start_time, tz=timezone.utc)
        end_time_dt = datetime.fromtimestamp(end_time, tz=timezone.utc)

        # return url for logging
        url = f'{base_url}{api}?{url_args}'

        return elapsed_time, response.json(), size, start_time_dt, end_time_dt, url

    @staticmethod
    def _create_api_folder(api:str)->None:
        """
        This will check if a folder for the api exists
        if it doesn't exist, it will create one

        Parameters:
        api:(str): The api that is being leveraged

        Returns: None
        """
        if notebookutils.fs.exists(f'Files/Ingest/{api}') == True:
            print(f'The file path: Files/Ingest/{api} already exists')
            logger.info(f'The file path: Files/Ingest/{api} already exists')
        else:
            notebookutils.fs.mkdirs(f'Files/Ingest/{api}')
            print(f'File created in Files/Ingest/{api}')
            logger.info(f'File created in Files/Ingest{api}')

    @staticmethod
    def write_file(data: dict, api: str, symbol: str) -> str:
        """
        This will write the files to the attached default lakehouse

        Parameters:
        data (dict): The JSON response from the API endpoint
        api (str): The API name
        symbol (str): The symbol for the data

        Returns:
        str: String for the file name
        """
        json_str = json.dumps(data)

        # Check API folder existence and create if it doesn't exist
        directory_path = f'/lakehouse/default/Files/Ingest/{api}'
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            logger.info(f'Created directory: {directory_path}')

        # Create file name
        file_name = _create_file_name(api, symbol)

        file_path = os.path.join(directory_path, file_name)
        with open(file_path, 'w') as fw:
            fw.write(json_str)
            logger.info(f'{file_path} has been written')
            print(f'{file_path} has been written')

        return file_path

    @staticmethod
    def _create_file_name(api:str, symbol:str)->str:
        """
        Based on the API used, create the file name to be used
        """
        file_name_map = {
            "stock/insider-transactions": symbol,
            "stock/financials-reported": symbol,
            "stock/insider-sentiment": symbol
        }

        # get file name pattern
        base_file_name = file_name_map.get(api)
        print(f'Base File Path: {base_file_name}')

        now_utc = datetime.utcnow()
        formatted_date = now_utc.strftime('%m-%d-%Y')

        file_name = f'{base_file_name}-{formatted_date}.json'

        return file_name

    @staticmethod
    def get_workspace_id(file_path:str):
        """
        Given a file path, this will grab the workspace id which is needed to programatically attach the lakehouse
        """
        pattern = r'abfss://([^@]+)@'
        match = re.search(pattern, file_path)

        return match.group(1) if match else None

    @staticmethod
    def get_lakehouse_id(file_path:str):
        """
        Given a file path, this will get the lakehouse id which is needed to programatically attach the lakehouse
        """
        string_list = file_path.split('/')

        return string_list[3]


    @staticmethod
    def connect_sql_database(server_name:str, database_name:str, username:str, password:str):
        """
        Make a pyodbc connection to azure sql database and return a client object for other functions to leverage

        parameters
        server_name:str: The name of the SQL Server to connect to
        database_name:str: The name of the database to connect to
        username:str: The username to be used as a client for the connection
        password:str: The password for the username used in the connection

        returns:
            conn
        """
        driver = '{ODBC Driver 18 for SQL Server}'

        conn_str = f'DRIVER={driver};SERVER={server_name};DATABASE={database_name};UID={username};PWD={password}'
        conn = pyodbc.connect(conn_str)

        # Create a cursor object
        cursor = conn.cursor()

        return conn

    @staticmethod
    def insert_schema_registry(cxn, tableName:str, layer:str, fullTablePath:str, namespace:str, schemaDefinition:str, tableLakehouse:str, tableSource:str, tableLakehouseID:str, tableWorkspaceID:str, createdByPipelineName:str, createdByPipelineRunID:str):
        """
        Insert into the schema registry

        parameters
        name:str: The name of the schema
        layer:str: Either bronze, silver, or gold
        fullTablePath:str: The full table path of the pyspark stored table.  Should be an abfss path
        namespace:str: The namespace associated with the schema
        schemaDefinition:str: The definition of the schema
        tableLakehouse:str: The name of the lakehouse the table is stored
        tableSource:str: The source of the table
        tableLakehouseID:str: The GUID of the lakehouse
        tableWorkspaceID:str: The GUID of the workspace the table is stored
        """

        cursor = cxn.cursor()
        lastModifieddttm = datetime.now(timezone.utc)

        insert_query = """
        INSERT INTO schemaRegistry (tableName, layer, fullTablePath, namespace, schemaDefinition, tableLakehouse, tableSource, tableLakehouseID, tableWorkspaceID, _createdByPipelineName, _createdByPipelineRunID, _lastModifieddttm)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # prepare schema
        schemaDefinition = _prepare_schema_for_insert(schemaDefinition)

        # Define the values to insert
        values = (tableName, layer, fullTablePath, namespace, schemaDefinition, tableLakehouse, tableSource, tableLakehouseID, tableWorkspaceID, createdByPipelineName, createdByPipelineRunID, lastModifieddttm)

        # Execute the INSERT statement
        cursor.execute(insert_query, values)

        # Commit the transaction
        cxn.commit()

        # Close the cursor and connection
        cursor.close()
        cxn.close()

    @staticmethod
    def check_schema_exists(cxn, check_schema:str):
        """
        Check if the schema exists

        parameters
        cxn: connection to the sql database
        check_schema: The schema to validate against from pyspark.schema
        """
        str_schema_check = _prepare_schema_for_insert(check_schema)

        cursor = cxn.cursor()

        select_query = f"""
        SELECT * FROM schemaRegistry WHERE schemaDefinition = '{str_schema_check}'
        """

        print(f'Running Query: {select_query}')

        # Execute the INSERT statement
        results = cursor.execute(select_query)
        resultsall_results = results.fetchall()

        # check the length of the results
        if len(resultsall_results) == 0:
            return False
        else:
            return True


    @staticmethod
    def get_existing_schema_results(cxn):
        """
        Get the results of the schema if the schema exists
        """
        cursor = cxn.cursor()

        select_query = """
        SELECT * FROM schemaRegistry
        """


        # Execute the INSERT statement
        results = cursor.execute(select_query)

        resultsall_results = results.fetchall()
        first_row_results = resultsall_results[0]

        # get the values of the row
        id = first_row_results[0]
        name = first_row_results[1]
        layer = first_row_results[2]
        namespace = first_row_results[3]
        schemaDefinition = first_row_results[4]

        return id, name, layer, namespace, schemaDefinition
        
    @staticmethod
    def _prepare_schema_for_insert(schema):
        """
        Since SQL Server can't handle ' characters, all ' will be replaced with " before inserting
        Note that this needs to be handled when the schemas are being compared by other modules in the schema registry app
        """
        # convert schema to str
        schema_str = str(schema)

        # prepare for insert
        schema_str = schema_str.replace("'", '"')

        return schema_str

    @staticmethod
    def convert_schema_to_pyspark(strSchema:str):
        """
        This is used when getting the resulting schemaDefintion column from the schemaRegistry SQL Server Table
        This will convert the schemaDefinition back to how it is created in pyspark for comparison purposes
        """
        schema_str = strSchema.replace('"', "'")

        return schema_str

    @staticmethod
    def find_all_directories_with_files(start_dir: str) -> list:
        """
        Traverse a directory structure and return the paths of directories containing files.

        :param start_dir: The root directory to start the search
        
        :return: A list of directory paths containing files
        """
        directories_with_files = set()

        def traverse_directory(directory):
            items = ls(directory)
            contains_file = False
            for item in items:
                if item.isDir:
                    traverse_directory(item.path)
                else:
                    contains_file = True
            if contains_file:
                directories_with_files.add(directory)

        traverse_directory(start_dir)
        return list(directories_with_files)

    @staticmethod
    def load_data(file_path, file_format, options=None):
        """
        Load data into a PySpark DataFrame based on the file format

        :param file_path: Path to the input file
        :param file_format: Format of the input file
        :param options: Dictionary of options for the file format 

        :return: PySpark DataFrame
        """
        if options is None:
            options = {}

        # Dictionary mapping file formats to their corresponding read methods
        format_to_method = {
            'json': spark.read.options(**options).json,
            'csv': spark.read.options(**options).csv
        }

        # Get the read method based on the file format
        read_method = format_to_method.get(file_format.lower())

        if read_method is None:
            raise ValueError(f'Unsupported file format: {file_format}')

        # Read the data using the appropriate method
        df = read_method(file_path)
        
        return df

    @staticmethod
    def add_hash_column(df: DataFrame, hash_col_name: str = "hash") -> DataFrame:
        # Create a struct of all columns
        struct_cols = struct(*df.columns)
        
        # Apply SHA-256 hashing function to the struct
        hashed_col = sha2(struct_cols.cast("string"), 256)
        
        # Add the new hash column to the DataFrame
        df_with_hash = df.withColumn(hash_col_name, hashed_col)
        
        return df_with_hash

    staticmethod
    def write_bronze_tables(df: DataFrame, full_target_path: str, ingest_path: str, processed_path: str, failed_path: str):
        """
        Incorporate _mv_processed_files(full_file_name, ingest_path, processed_path, failed_path, handling) into this method

        Need to add a means to prevent recursion overflow for SparkUpgradeException being continuously called

        """
        try:
            # Write DataFrame to Delta format
            df.write.format('delta').mode('append').option("mergeSchema", "true").save(full_target_path)
            
            # Move files from Ingest to Processed
            files = mssparkutils.fs.ls(ingest_path)

            # iterate over the files and construct the full file path
            for file_info in files:
                file_name = file_info.name
                print(f'{file_name}\n\n')
                full_file_name = f'{ingest_path}/{file_name}'
                

                # move the file to processed
                _mv_processed_files(full_file_name, ingest_path, processed_path, failed_path, handling='processed')
                
        except SparkUpgradeException:
            # send to _handle_ancient_dates method
            _handle_ancient_dates(df, full_target_path, ingest_path, processed_path, failed_path)

            ### determine how to handle logging        
        
        except Exception as e:
            print(f"An error occurred: {e}")
            
            for file_info in files:
                file_name = file_info.name
                full_file_name = f'{ingest_path}/{file_name}'

                # move the file to processed
                _mv_processed_files(full_file_name, ingest_path, processed_path, failed_path, handling='failed')

    @staticmethod
    def _mv_processed_files(full_file_name:str, ingest_path:str, processed_path:str, failed_path:str, handling:str):
        """
        After the write_tables method has been run, it needs to move the processed files to either processed or failed

        Inputs:
            full_file_name: The full name of the source file, such as from the ingest folder
            ingest_path: The path of the ingest location
            processed_path: The path to land processed files
            file_name: The name of the source file, with timestamp and file extension
            handling: either ['processed' or 'failed']

        Output:

            Example:
                full_file_name = 'abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/b029b982-e97a-4007-8223-10acbeb425d4/Files/Ingest/stock/financials-reported/file.json'
                ingest_path = 'abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/b029b982-e97a-4007-8223-10acbeb425d4/Files/Ingest/stock/insider-sentiment'
                processed_path = 'abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/b029b982-e97a-4007-8223-10acbeb425d4/Files/Processed/stock/insider-transactions'
                failed_path = 'abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/b029b982-e97a-4007-8223-10acbeb425d4/Files/Failed/stock/financials-reported'

                full_file_name --> full_failed_path || full_processed_path
        """
        # get file
        my_file = split_path_return_last(full_file_name)

        if handling == 'processed':
            # build full target path
            full_target_path = f'{processed_path}/{my_file}'
            
            # info messages for move processed
            print(f'INFO: Process Success: Moving file: {full_file_name} from {ingest_path} to {full_target_path}')

        elif handling == 'failed':
            # build full target path
            full_target_path = f'{failed_path}/{my_file}'

            # info messages for move failed
            print(f'ERROR: Process Failed: Moving file: {full_file_name} from {ingest_path} to {full_target_path}')
        
        # move files
        mssparkutils.fs.mv(full_file_name, f'{processed_path}/{my_file}')


    @staticmethod
    def split_path_return_last(file_path:str)->str:
        """
        This method will take a full file path with a file as an argument and return the name of the last folder in the path
        This is used by a mapping table to lookup what the table name should be for a folder

        Example
        Input: abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/b029b982-e97a-4007-8223-10acbeb425d4/Files/Ingest/stock/insider-transactions/
        Output: insider-transactions

        Input: abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/b029b982-e97a-4007-8223-10acbeb425d4/Files/Ingest/stock/insider-sentiment/TSLA-01-29-2025.json
        Output: TSLA-01-29-2025.json

        """
        # split the file paths into a list
        my_file_list = file_path.split('/')

        # return -2 from list
        target_folder = my_file_list[-1]

        return target_folder

    @staticmethod
    def lookup_folder_source_target_mapping(folder_name:str, layer:Layer = Layer.BRONZE)->(str, str, str, str, str):
        """
        Given a folder_name this will look up the table mapping in the 

        lakehouse_path: abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/1ac4412c-84bb-4fe9-8629-68d6faf28eaf/Tables/folder_table_mapping

        Input:
        financials_reported

        Return:
        table_name, source_file_path, full_target_path, processed_file_path, failed_file_path
        abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/b029b982-e97a-4007-8223-10acbeb425d4/Files/Ingest/stock/financials-reported , abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/b029b982-e97a-4007-8223-10acbeb425d4/Tables/financials_reported

        """
        metadata_lh_df = spark.read.format("delta").load('abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/1ac4412c-84bb-4fe9-8629-68d6faf28eaf/Tables/folder_source_target_mapping')
        
        print(f'Layer Value: {layer.value}')
        result = metadata_lh_df.filter((col('folder') == folder_name) & (col('layer') == layer.value)).select('table', 'source_file_path', 'full_target_path', 'processed_file_path', 'failed_file_path').first()

        table_name = result['table']
        source_file_path = result['source_file_path']
        full_target_path = result['full_target_path']
        processed_file_path = result['processed_file_path']
        failed_file_path = result['failed_file_path']

        print(f"Table: {table_name}")
        print(f"Source File Path: {source_file_path}")
        print(f"Full Target Path: {full_target_path}")
        print(f"Processed File Path: {processed_file_path}")
        print(f"Failed File Path: {failed_file_path}")

        return table_name, source_file_path, full_target_path, processed_file_path, failed_file_path

    @staticmethod
    def mount_lakehouse(source_path:str, mount_path:str):
        """
        Mount a storage path to access as if storage is local
        """
        try:
            mssparkutils.fs.mount(source=source_path, mountPoint=mount_path)
            print("Mount successful")
        except Exception as e:
            print(f"Mount failed: {e}")


    @staticmethod
    def _handle_ancient_dates_sentinel(df: DataFrame, full_target_path:str, date_col:str):
        """
        This is a method that will only be invoked from the write_bronze_tables method when the SparkUpgradeException is encountered

        GAP: Going to need to know or find the datetime column where the issue is occuring

        This will replace the bad date records with None and return the Dataframe
        """
        df_clean = df.withColumn(
            date_col,
            when(col(date_col) < lit('1900-01-01'), lit('1900-MM-DD ZZ:ZZ:ZZ'))
            .otherwise(col(date_col))
        )

        return df_clean

    @staticmethod
    def _handle_ancient_dates_sentinel(df: DataFrame, full_target_path:str, date_col:str):
        """
        This is a method that will only be invoked from the write_bronze_tables method when the SparkUpgradeException is encountered

        GAP: Going to need to know or find the datetime column where the issue is occuring

        This will replace the bad date records with None and return the Dataframe
        """
        df_clean = df.withColumn(
            date_col,
            when(col(date_col) < lit('1900-01-01'), lit('1900-01-01 00:00:00'))
            .otherwise(col(date_col))
        )

        return df_clean
    
# Options for lit
# None
# sentinel datetime --> 1900-01-01 00:00:00
# shift date --> adjusts the year to 1900, but preserves the month and day and time --> adjust to this pattern
# move invalids to new table
# drop them

# patients per hour processed --> time basis (lose the year) --> all defaults --> 00:00:00 --> lot of patient traffic at midnight


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
