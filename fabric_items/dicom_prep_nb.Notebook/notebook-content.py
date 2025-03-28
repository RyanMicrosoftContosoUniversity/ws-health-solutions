# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b029b982-e97a-4007-8223-10acbeb425d4",
# META       "default_lakehouse_name": "lh_api_ingestion",
# META       "default_lakehouse_workspace_id": "c0a7b8a9-eb12-495a-b863-2cb583e31154",
# META       "known_lakehouses": [
# META         {
# META           "id": "b029b982-e97a-4007-8223-10acbeb425d4"
# META         },
# META         {
# META           "id": "31147e4c-564b-41de-9820-1c3ea7f2c4e0"
# META         },
# META         {
# META           "id": "77644675-ed05-425e-bfbc-47d56edb343c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # DICOM Pre-Processing Notebook

# MARKDOWN ********************

# 


# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql import DataFrame
import logging
from pyspark.sql.functions import monotonically_increasing_id, current_timestamp
from pyspark.sql import Row


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_dir = 'abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/77644675-ed05-425e-bfbc-47d56edb343c/Files/External/Imaging/DICOM/AHDS-DICOM/input'
target_dir = 'abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/77644675-ed05-425e-bfbc-47d56edb343c/Files/Ingest/Imaging/DICOM/DICOM-HDS'
dicom_ingestion_table_path = 'abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/1ac4412c-84bb-4fe9-8629-68d6faf28eaf/Tables/dicom_ingest_hist'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# lsit all files in the firectory
objects = mssparkutils.fs.ls(source_dir)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



class DICOM_Utils:

    @staticmethod
    def get_folder_from_path(path:str)->str:
        """
        Given a file path such as abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/77644675-ed05-425e-bfbc-47d56edb343c/Files/External/Imaging/DICOM/AHDS-DICOM/input/20250328 return the folder
        
        path:str

        returns:
            folder_str
        """
        path_list = path.split('/')
        folder = path_list[-1]

        logger.info(f'Returning folder: {folder} from path: {path}')

        return folder

    @staticmethod
    def create_df_for_dicom_ingestion_table(path:str):
        """
        Create a dataframe that references the dicom_ingestion table that keeps track of processed dicom files from storage account

        path:str: abfss path to dicom_ingestion table (only works for lakehouse.  Expected to be in parquet or delta format)

        returns:
            df:DataFrame
        """
        df = spark.read.parquet(path)
        
        logger.info(f'DataFrame created referencing dicom_ingestion table')

        return df

    @staticmethod
    def check_folder_in_dicom_ingestion_table(folder:str, df:DataFrame)->bool:
        """
        Given a folder such as 20250328 check to see if the folder exists in the dicom_ingestion table that tracks processed files

        folder:str
        """
        record_exists = df.filter(df.source_folder == folder).count() > 0

        return record_exists

    @staticmethod
    def get_dicom_files_to_process_from_folder(dicom_folder:str)->list:
        """
        Given a dicom file to be processed, return a list of all the files inside
        """
        files = mssparkutils.fs.ls(dicom_folder)

        return files

    @staticmethod
    def insert_log_record_dicom_ingestion(df:DataFrame, table_path:str, source_dir:str, target_dir:str, source_folder:str, total_files_processed:int):
        """
        Insert a record to the dicom_ingestion_df for source_dir, target_dir, source_folder, total_files_processed, and processed_dttm

        df:DataFrame The existing dataframe for the dicom_ingestion table
        table_path:str The path to the dicom_ingestion table
        source_dir:str The source directory where the file was processed
        target_dir:str The target directory where the file was written
        source_folder:str The name of the folder processed
        total_files_processed:int The total number of files processed in the source_folder
        

        return:
            None
        """
        ### be sure to add in id and processed_dttm via this method as they will not be input

        new_record = Row(source_dir=source_dir, target_dir=target_dir, source_folder=source_folder, total_files_processed=total_files_processed)

        # Create a DataFrame with the new record
        new_df = spark.createDataFrame([new_record])

        # Add id and processed_dttm columns to the new DataFrame
        new_df = new_df.withColumn("id", monotonically_increasing_id())
        new_df = new_df.withColumn("processed_dttm", current_timestamp())

        # Reorder columns to ensure id is the first column
        new_df = new_df.select("id", "source_dir", "target_dir", "source_folder", "total_files_processed", "processed_dttm")


        # Append the new record to the original DataFrame
        df = df.union(new_df)

        print(df.schema)

        # save over table
        df.write.mode('overwrite').format('delta').save(table_path)
        logger.info('Record written to dicom_ingestion table')   




    @staticmethod
    def process_dicom_files(files_list:list, ingest_path:str)->int:
        """
        Given a file list, process the files to be dicom files and move them to the ingest folder of the target lakehouse

        files_list:list List of files to process
        ingest_path:str Path to the Ingest location, such as abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/77644675-ed05-425e-bfbc-47d56edb343c/Files/Ingest/Imaging/DICOM/DICOM-HDS

        returns:
            total_files_processed:int
        """
        total_files_processed = 0

        for file in files_list:
            # make sure the item is a file not a subdirectory
            if not file.isDir:
                old_path = file.path
                file_name = file.name
                
                # check if file name is DICOMDIR
                if file_name in('DICOMDIR', 'dicomdir'):
                    logger.info(f'File name: {file_name} at {old_path} is in exclude list and will be skipped')
                    continue

                # only append .dcm if file_name does not already include a .dcm
                if '.' not in file_name:
                    logger.info(f'No . found in {file_name} so will have .dcm added to file')

                    new_name = file_name + '.dcm'
                    new_path = target_dir.rstrip('/') + '/' + new_name

                    # don't change the existing file and copy the file to the ingest location
                    try:
                        mssparkutils.fs.cp(old_path, new_path)
                        logger.info(f'Copied {old_path} to {new_path}')

                        total_files_processed = total_files_processed + 1
                    except Exception as e:
                        logger.error(f'Exception when processing {file_name} at path: {old_path} with Exception: {e}')
                        
                else:
                    logger.info(f'skipping {old_path}; file already has an extension')
            
        return total_files_processed

   
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Main

# CELL ********************

# look for directories
dicom_ingestion_df = DICOM_Utils.create_df_for_dicom_ingestion_table(dicom_ingestion_table_path)
ingest_path = target_dir

for object in objects:
    # check if that object is a directory
    if object.isDir:
        print(f'Directory found as {object.path}')

        ### check if that directory has been processed already from the DB
        # get the last value from the file_path
        folder = DICOM_Utils.get_folder_from_path(object.path)

        if DICOM_Utils.check_folder_in_dicom_ingestion_table(folder, dicom_ingestion_df) is True:
            # folder has already been processed
            continue
        else:
            # process the folder
            files_list = DICOM_Utils.get_dicom_files_to_process_from_folder(object.path)

            # process files
            total_files_processed = DICOM_Utils.process_dicom_files(files_list, ingest_path)
            final_df = DICOM_Utils.insert_log_record_dicom_ingestion(dicom_ingestion_df, dicom_ingestion_table_path, source_dir, target_dir, folder, total_files_processed)
        

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

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
