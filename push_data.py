import os
import sys
import json
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import certifi
import pandas as pd
import numpy as np
import pymongo

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

# MongoDB CA certificate
ca = certifi.where()

# Load MongoDB URL from .env
MONGO_DB_URL = os.getenv("MONGO_DB_URL")


class NetworkDataExtract:
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def cv_to_json_converter(self, file_path):
        try:
            file_path = Path(file_path)  # Ensure it's a Path object

            # Check if file exists
            if not file_path.exists():
                raise FileNotFoundError(f"CSV file not found at: {file_path}")

            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)

            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def insert_data_mongodb(self, records, database, collection):
        try:
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            db = self.mongo_client[database]
            col = db[collection]

            col.insert_many(records)
            return len(records)
        except Exception as e:
            raise NetworkSecurityException(e, sys)


if __name__ == '__main__':
    try:
        # Build CSV path relative to this file
        FILE_PATH = "Network_Data\phisingData.csv"
        DATABASE = "Tanmay"
        COLLECTION = "NetworkData"

        networkobj = NetworkDataExtract()

        # Convert CSV to JSON records
        records = networkobj.cv_to_json_converter(file_path=FILE_PATH)
        print(records)

        # Insert into MongoDB
        no_of_records = networkobj.insert_data_mongodb(records, DATABASE, COLLECTION)
        print(f"Inserted {no_of_records} records into MongoDB.")

    except Exception as e:
        raise NetworkSecurityException(e, sys)
