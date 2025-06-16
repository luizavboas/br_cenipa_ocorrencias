import os
import re
from datetime import datetime
import json
import logging
import requests
import pandas as pd
from pathlib import Path
from constants import *

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
file_handler = logging.FileHandler(os.path.join(ROOT_DIR, 'tmp',f"logging{datetime.now().strftime('%Y-%m-%d-%H%m')}.txt"))
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)


## Methods
def download_table_to_csv(table_name, table_url, table_path=INPUT_DIR_PATH):
    """
        Downloads a table from the CENIPA dataset and saves it as a CSV file. 
    """
    try:
        response = requests.get(table_url)
        response.raise_for_status()

        if os.path.exists(table_path) is False:
            os.makedirs(table_path)

        file_path = os.path.join(table_path, f"{table_name}.csv")
        with open(file_path, "wb") as f:
            f.write(response.content)
        logger.info(f"Downloaded {table_name} to {file_path}")
    except Exception as e:
        raise
    
def correct_csv_encoding():
    """
        Corrects the encoding of CSV files in the input directory from 'latin1' to 'utf-8'.
    """
    logger.info("Correcting CSV file encodings from 'latin1' to 'utf-8'...")
    for  file_name in os.listdir(INPUT_DIR_PATH):
        if file_name.endswith(".csv"):
            file_path = os.path.join(INPUT_DIR_PATH, file_name)
            pd.read_csv(file_path, sep=";", encoding="latin1")\
            .to_csv(file_path, sep=";", encoding="utf-8", index=False)


def show_uniques(df, columns):
    """
        Displays unique values for specified columns in the dataset.
    """
    logger.info('\n')
    logger.info("Showing unique values for specified columns...")
    for col in columns:
        if not col.startswith('id_'):
            unique_values = df[col].unique()
            logger.info(f"Unique values in {col}: {unique_values}")
    logger.info('\n')

def transform_lat_long(value:str):
    extraction = re.findall(r'-?[\d\.]+', value)
    if extraction:
        value_match = re.match(r'(^-?\d+)([\.\d]+)', extraction[0])
        if value_match:
            new_value = f"""{
                value_match.groups()[0][:-1]
                }.{
                    value_match.groups()[0][-1]
                    }{
                        str(value_match.groups()[1]).replace('.','')
                        }"""
        else:
            new_value = extraction[0]
        return new_value
    else:
        return value