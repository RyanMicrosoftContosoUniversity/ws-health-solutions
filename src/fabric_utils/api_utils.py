import time
import sys
import json
from datetime import datetime, timezone
import os
import requests
import notebookutils
from notebookutils import mssparkutils
from .logging_config import get_logger

logger = get_logger(__name__)

class APIUtils:
    @staticmethod
    def api_request_with_stats(base_url: str, api: str, url_args: str):
        """Make an API request and return timing statistics."""
        start_time = time.time()
        print(f"Making request to API endpoint: {base_url}{api}?{url_args} ")
        response = requests.get(f"{base_url}{api}?{url_args}", headers=headers)
        end_time = time.time()
        elapsed_time = end_time - start_time
        size = sys.getsizeof(json.dumps(response.json()))
        start_time_dt = datetime.fromtimestamp(start_time, tz=timezone.utc)
        end_time_dt = datetime.fromtimestamp(end_time, tz=timezone.utc)
        url = f"{base_url}{api}?{url_args}"
        return elapsed_time, response.json(), size, start_time_dt, end_time_dt, url

    @staticmethod
    def _create_api_folder(api: str) -> None:
        if notebookutils.fs.exists(f"Files/Ingest/{api}"):
            print(f"The file path: Files/Ingest/{api} already exists")
            logger.info(f"The file path: Files/Ingest/{api} already exists")
        else:
            notebookutils.fs.mkdirs(f"Files/Ingest/{api}")
            print(f"File created in Files/Ingest/{api}")
            logger.info(f"File created in Files/Ingest{api}")

    @staticmethod
    def write_file(data: dict, api: str, symbol: str) -> str:
        json_str = json.dumps(data)
        directory_path = f"/lakehouse/default/Files/Ingest/{api}"
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            logger.info(f"Created directory: {directory_path}")
        file_name = APIUtils._create_file_name(api, symbol)
        file_path = os.path.join(directory_path, file_name)
        with open(file_path, "w") as fw:
            fw.write(json_str)
            logger.info(f"{file_path} has been written")
            print(f"{file_path} has been written")
        return file_path

    @staticmethod
    def _create_file_name(api: str, symbol: str) -> str:
        file_name_map = {
            "stock/insider-transactions": symbol,
            "stock/financials-reported": symbol,
            "stock/insider-sentiment": symbol,
        }
        base_file_name = file_name_map.get(api)
        print(f"Base File Path: {base_file_name}")
        now_utc = datetime.utcnow()
        formatted_date = now_utc.strftime("%m-%d-%Y")
        file_name = f"{base_file_name}-{formatted_date}.json"
        return file_name
