from datetime import datetime, timezone
import pyodbc

class SQLUtils:
    @staticmethod
    def connect_sql_database(server_name: str, database_name: str, username: str, password: str):
        driver = '{ODBC Driver 18 for SQL Server}'
        conn_str = f'DRIVER={driver};SERVER={server_name};DATABASE={database_name};UID={username};PWD={password}'
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        return conn

    @staticmethod
    def insert_schema_registry(cxn, tableName: str, layer: str, fullTablePath: str, namespace: str, schemaDefinition: str, tableLakehouse: str, tableSource: str, tableLakehouseID: str, tableWorkspaceID: str, createdByPipelineName: str, createdByPipelineRunID: str):
        cursor = cxn.cursor()
        lastModifieddttm = datetime.now(timezone.utc)
        insert_query = """
        INSERT INTO schemaRegistry (tableName, layer, fullTablePath, namespace, schemaDefinition, tableLakehouse, tableSource, tableLakehouseID, tableWorkspaceID, _createdByPipelineName, _createdByPipelineRunID, _lastModifieddttm)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        schemaDefinition = _prepare_schema_for_insert(schemaDefinition)
        values = (tableName, layer, fullTablePath, namespace, schemaDefinition, tableLakehouse, tableSource, tableLakehouseID, tableWorkspaceID, createdByPipelineName, createdByPipelineRunID, lastModifieddttm)
        cursor.execute(insert_query, values)
        cxn.commit()
        cursor.close()
        cxn.close()

    @staticmethod
    def check_schema_exists(cxn, check_schema: str):
        str_schema_check = _prepare_schema_for_insert(check_schema)
        cursor = cxn.cursor()
        select_query = f"""
        SELECT * FROM schemaRegistry WHERE schemaDefinition = '{str_schema_check}'
        """
        print(f'Running Query: {select_query}')
        results = cursor.execute(select_query)
        resultsall_results = results.fetchall()
        if len(resultsall_results) == 0:
            return False
        else:
            return True

    @staticmethod
    def get_existing_schema_results(cxn):
        cursor = cxn.cursor()
        select_query = """
        SELECT * FROM schemaRegistry
        """
        results = cursor.execute(select_query)
        resultsall_results = results.fetchall()
        first_row_results = resultsall_results[0]
        id = first_row_results[0]
        name = first_row_results[1]
        layer = first_row_results[2]
        namespace = first_row_results[3]
        schemaDefinition = first_row_results[4]
        return id, name, layer, namespace, schemaDefinition

    @staticmethod
    def _prepare_schema_for_insert(schema):
        schema_str = str(schema)
        schema_str = schema_str.replace("'", '"')
        return schema_str

    @staticmethod
    def convert_schema_to_pyspark(strSchema: str):
        schema_str = strSchema.replace('"', "'")
        return schema_str

