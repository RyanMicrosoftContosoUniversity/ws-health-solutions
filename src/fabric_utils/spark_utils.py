from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import current_timestamp, lit, concat_ws, sha2, struct, col, when, to_timestamp
from pyspark.errors import SparkUpgradeException
from notebookutils import mssparkutils
from fabric_utils.file_utils import FileUtils
from fabric_utils.layer import Layer
from datetime import datetime

class SparkUtils:
    @staticmethod
    def load_data(file_path, file_format, options=None):
        if options is None:
            options = {}
        format_to_method = {
            'json': spark.read.options(**options).json,
            'csv': spark.read.options(**options).csv,
        }
        read_method = format_to_method.get(file_format.lower())
        if read_method is None:
            raise ValueError(f'Unsupported file format: {file_format}')
        df = read_method(file_path)
        return df

    @staticmethod
    def add_hash_column(df: DataFrame, hash_col_name: str = "hash") -> DataFrame:
        struct_cols = struct(*df.columns)
        hashed_col = sha2(struct_cols.cast("string"), 256)
        df_with_hash = df.withColumn(hash_col_name, hashed_col)
        return df_with_hash

    @staticmethod
    def write_bronze_tables(df: DataFrame, full_target_path: str, ingest_path: str, processed_path: str, failed_path: str):
        try:
            df.write.format('delta').mode('append').option("mergeSchema", "true").save(full_target_path)
            files = mssparkutils.fs.ls(ingest_path)
            for file_info in files:
                file_name = file_info.name
                print(f'{file_name}\n\n')
                full_file_name = f'{ingest_path}/{file_name}'
                SparkUtils._mv_processed_files(full_file_name, ingest_path, processed_path, failed_path, handling='processed')
        except SparkUpgradeException:
            SparkUtils._handle_ancient_dates_sentinel(df, full_target_path, '')
        except Exception as e:
            print(f"An error occurred: {e}")
            for file_info in files:
                file_name = file_info.name
                full_file_name = f'{ingest_path}/{file_name}'
                SparkUtils._mv_processed_files(full_file_name, ingest_path, processed_path, failed_path, handling='failed')

    @staticmethod
    def _mv_processed_files(full_file_name: str, ingest_path: str, processed_path: str, failed_path: str, handling: str):
        my_file = FileUtils.split_path_return_last(full_file_name)
        if handling == 'processed':
            full_target_path = f'{processed_path}/{my_file}'
            print(f'INFO: Process Success: Moving file: {full_file_name} from {ingest_path} to {full_target_path}')
        elif handling == 'failed':
            full_target_path = f'{failed_path}/{my_file}'
            print(f'ERROR: Process Failed: Moving file: {full_file_name} from {ingest_path} to {full_target_path}')
        mssparkutils.fs.mv(full_file_name, f'{processed_path}/{my_file}')

    @staticmethod
    def lookup_folder_source_target_mapping(folder_name: str, layer: Layer = Layer.BRONZE):
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
    def _handle_ancient_dates_sentinel(df: DataFrame, full_target_path: str, date_col: str):
        df_clean = df.withColumn(
            date_col,
            when(col(date_col) < lit('1900-01-01'), lit('1900-01-01 00:00:00')).otherwise(col(date_col))
        )
        return df_clean
