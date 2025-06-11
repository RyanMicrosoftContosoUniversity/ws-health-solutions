import os
import re
from datetime import datetime
import notebookutils
from notebookutils import mssparkutils
from notebookutils.mssparkutils.fs import ls

class FileUtils:
    @staticmethod
    def get_workspace_id(file_path: str):
        pattern = r"abfss://([^@]+)@"
        match = re.search(pattern, file_path)
        return match.group(1) if match else None

    @staticmethod
    def get_lakehouse_id(file_path: str):
        string_list = file_path.split("/")
        return string_list[3]

    @staticmethod
    def find_all_directories_with_files(start_dir: str) -> list:
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
    def split_path_return_last(file_path: str) -> str:
        my_file_list = file_path.split("/")
        target_folder = my_file_list[-1]
        return target_folder

    @staticmethod
    def mount_lakehouse(source_path: str, mount_path: str):
        try:
            mssparkutils.fs.mount(source=source_path, mountPoint=mount_path)
            print("Mount successful")
        except Exception as e:
            print(f"Mount failed: {e}")
