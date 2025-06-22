# -*- coding: utf-8 -*-
"""
General utility functions for the br_cenipa project.

This module provides helper functions for webscraping, file handling, data formatting,
consistency checks, and uploading data to Google Cloud Storage in chunks.
"""

import os
import io
import re
import logging
import math
from google.cloud import storage
from loguru import logger
import pandas as pd
from typing import List

# Wehscraping option libs
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# API option libs
import json
import requests

# Internals
from src.constants import constants

logging.basicConfig(level=logging.INFO)

def set_driver():
    """
    Initializes and returns a Selenium Chrome WebDriver in headless mode with custom download preferences.
    
    Returns:
        webdriver.Chrome: Configured Chrome WebDriver instance.
    """
    options = Options()
    options.add_argument("--headless") 
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    prefs = {"download.default_directory" : constants.INPUT_DIR_PATH.value}
    options.add_experimental_option("prefs",prefs)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def download_table_to_csv(table_name, table_url, table_path=constants.INPUT_DIR_PATH.value):
    """
    Downloads a table from a given URL and saves it as a CSV file in the specified path.

    Args:
        table_name (str): Name of the table (used as file name).
        table_url (str): URL to download the table from.
        table_path (str, optional): Directory to save the CSV file. Defaults to constants.INPUT_DIR_PATH.value.
    """
    try:
        response = requests.get(table_url)
        response.raise_for_status()

        if os.path.exists(table_path) is False:
            os.makedirs(table_path)

        file_path = os.path.join(table_path, f"{table_name}.csv")
        with open(file_path, "wb") as f:
            f.write(response.content)
        logging.info(f"Downloaded {table_name} to {file_path}")
        print(f"Downloaded {table_name} to {file_path}")
    except Exception as e:
        raise
    
def correct_csv_encoding():
    """
    Converts the encoding of all CSV files in the input directory from 'latin1' to 'utf-8'.
    Overwrites the original files.
    """
    logging.info("Correcting CSV file encodings from 'latin1' to 'utf-8'...")
    print("Correcting CSV file encodings from 'latin1' to 'utf-8'...")
    for  file_name in os.listdir(constants.INPUT_DIR_PATH.value):
        if file_name.endswith(".csv"):
            file_path = os.path.join(constants.INPUT_DIR_PATH.value, file_name)
            pd.read_csv(file_path, sep=";", encoding="latin1")\
            .to_csv(file_path, sep=";", encoding="utf-8", index=False)

def show_uniques(df, columns):
    """
    Displays and logs unique values for the specified columns in the given DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to analyze.
        columns (list): List of column names to show unique values for.
    """
    logging.info('-----------------------------------------------------------------------------')
    logging.info("Showing unique values for specified columns...")
    print('-----------------------------------------------------------------------------')
    print("Showing unique values for specified columns...")
    for col in columns:
        if not col.startswith('id_'):
            unique_values = df[col].unique()
            logging.info(f"Unique values in {col}: {unique_values}")
            print(f"Unique values in {col}: {unique_values}")

def check_inconsistences(dataframe:pd.DataFrame, primary_key:bool=False):
    """
    Checks for inconsistencies in the DataFrame, such as duplicate rows, missing values,
    and non-unique primary keys. Logs and prints warnings if inconsistencies are found.

    Args:
        dataframe (pd.DataFrame): The DataFrame to check.
        primary_key (bool, optional): If True, checks uniqueness of 'id_ocorrencia'. Defaults to False.
    """
    if primary_key:
        if not dataframe['id_ocorrencia'].is_unique:
            logging.warning("The 'id_ocorrencia' column should have unique values.")
            print("The 'id_ocorrencia' column should have unique values.")
            logging.warning(dataframe.loc[dataframe['id_ocorrencia'].duplicated(),['id_ocorrencia']])
            print(dataframe.loc[dataframe['id_ocorrencia'].duplicated(),['id_ocorrencia']])
    if not dataframe[dataframe.duplicated()].empty:
        logging.warning("The dataframe has duplicated rows:")
        print("The dataframe has duplicated rows:")
        logging.warning(dataframe[dataframe.duplicated()])
        print(dataframe[dataframe.duplicated()])
    for col in dataframe.columns:
        if dataframe[col].isnull().any():
            logging.warning(f"Column '{col}' has missing values.")
            print(f"Column '{col}' has missing values.")
        if col.startswith('id'):
            if dataframe[col].is_unique:
                pass
            else:
                logging.warning(f"The {col} has duplicated values. Shouldn't they be unique?")
    if dataframe.duplicated().any():
        logging.warning("There are duplicate rows in the DataFrame.")
        print("There are duplicate rows in the DataFrame.")
        logging.info(dataframe[dataframe.duplicated()])
        print(dataframe[dataframe.duplicated()])

def format_string(dataframe:pd.DataFrame, string_columns:List[str]):
    """
    Formats string columns: strips whitespace, removes unwanted characters, 
    applies case formatting, and fills NaNs with empty strings.

    Args:
        dataframe (pd.DataFrame): The DataFrame to format.
        string_columns (List[str]): List of column names to format as strings.
    """
    try:
        for col in string_columns:
            try:
                dataframe[col] = dataframe[col]\
                    .astype(str)\
                    .str.strip()\
                    .str.replace(r'\*|nan|Nan', '', regex=True)\
                    .str.replace(r'\s+', ' ', regex=True)
            
                if not col.startswith('id'):
                    dataframe[col] = dataframe[col].str.lower()
            
                if col.startswith('nome'):
                    dataframe[col] = dataframe[col].str.title()
                    dataframe[col] = dataframe[col].str.replace(
                    r'\b(De|Da|Do|Das|Dos|E|D\')\b', 
                    lambda x: x.group(0).lower(), 
                    regex=True)
            
                if col.startswith('sigla'):
                    dataframe[col] = dataframe[col].str.upper()

                dataframe[col] = dataframe[col].fillna('')
            
            except Exception as e:
                logging.error(f"Unable to cast column {col} to string type due to: {e}")
                print(f"Unable to cast column {col} to string type due to: {e}")
    except Exception as e:
        logging.error(f"Unable to cast columns to string type due to: {e}\nStopped at {col} column.")
        print(f"Unable to cast columns to string type due to: {e}\nStopped at {col} column.")

def transform_lat_long(value:str):
    """
    Transforms a latitude or longitude string to a standardized numeric format.

    Args:
        value (str): The latitude or longitude value as a string.

    Returns:
        str: The transformed value.
    """
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

def format_floats(dataframe:pd.DataFrame, float_columns:List[str]):
    """
    Formats float columns: strips whitespace, replaces NaNs, normalizes decimal separators,
    and casts columns to float type.

    Args:
        dataframe (pd.DataFrame): The DataFrame to format.
        float_columns (List[str]): List of column names to format as floats.
    """
    try:
        for col in float_columns:
            dataframe[col] = dataframe[col]\
                .astype(str)\
                .str.strip() 
              
            dataframe[col] = dataframe[col].str.replace(r'NaN|nan', '0', regex=True) 
            dataframe[col] = dataframe[col].str.replace(r'\s+', '', regex=True) 
        
            dataframe[col] = dataframe[col].str.replace(',','.')
        try:
            dataframe[col] = dataframe[col].astype(float)
        except Exception as e:
            logging.error(f"Unable to cast column {col} to float type due to: {e}")
            print(f"Unable to cast column {col} to float type due to: {e}")
    except Exception as e:
        logging.error(f"Unable to cast columns to float type due to: {e}\nStopped at {col} column.")
        print(f"Unable to cast columns to float type due to: {e}\nStopped at {col} column.")

def format_date(dataframe:pd.DataFrame, date_columns:List[str]):
    """
    Converts date columns to a standardized 'YYYY-MM-DD' string format.

    Args:
        dataframe (pd.DataFrame): The DataFrame to format.
        date_columns (List[str]): List of column names to format as dates.
    """
    try:
        for col in date_columns:
            try:
                dataframe[col] = dataframe[col]\
                    .astype(str)\
                    .str.strip()\
                    .fillna('')
                formats = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"]
                i=0
                while i<len(formats):
                    try:
                        dataframe[col] = pd.to_datetime(dataframe[col],
                                                        format = formats[i])
                        break
                    except Exception as e:
                        i+=1

                dataframe[col] = pd.to_datetime(dataframe[col], errors='coerce')
                dataframe[col] = dataframe[col].dt.strftime('%Y-%m-%d')
            except Exception as e:
                logging.error(f"Unable to cast column {col} to date type due to: {e}")
                print(f"Unable to cast column {col} to date type due to: {e}")
    except Exception as e:
        logging.error(f"Unable to cast columns to date type due to: {e}\nStopped at {col} column.")
        print(f"Unable to cast columns to date type due to: {e}\nStopped at {col} column.")

def format_time(dataframe:pd.DataFrame, timestamp_columns:List[str]):
    """
    Converts timestamp columns to a standardized 'HH:MM:SS' string format.

    Args:
        dataframe (pd.DataFrame): The DataFrame to format.
        timestamp_columns (List[str]): List of column names to format as timestamps.
    """
    try:
        for col in timestamp_columns:
            try:
                dataframe[col] = dataframe[col]\
                    .astype(str)\
                    .str.strip()\
                    .fillna('')
                
                dataframe[col] = pd.to_datetime(dataframe[col],
                                                format="%H:%M:%S",
                                                errors='coerce')                                                    
                dataframe[col] = dataframe[col].dt.strftime('%H:%M:%S')
            except Exception as e:
                logging.error(f"Unable to cast column {col} to timestamp type due to: {e}")
    except Exception as e:
        logging.error(f"Unable to cast columns to timestamp type due to: {e}\nStopped at {col} column.")
        print(f"Unable to cast columns to timestamp type due to: {e}\nStopped at {col} column.")

def format_bools(dataframe:pd.DataFrame, bool_columns:List[str]):
    """
    Converts boolean columns to Python bool type, mapping 'sim' to True and 'não' to False.

    Args:
        dataframe (pd.DataFrame): The DataFrame to format.
        bool_columns (List[str]): List of column names to format as booleans.
    """
    try:
        for col in bool_columns:
            try:
                dataframe[col] = dataframe[col]\
                    .astype(str)\
                    .str.strip()\
                    .str.lower()\
                    .fillna('')
                dataframe.loc[dataframe[col]=='sim',[col]] = 'True'     
                dataframe.loc[dataframe[col]=='não',[col]] = 'False'
                dataframe[col] = dataframe[col].astype(bool)
            except Exception as e:
                logging.error(f"Unable to cast column {col} to bool type due to: {e}")
                print(f"Unable to cast column {col} to bool type due to: {e}")
    except Exception as e:
        logging.error(f"Unable to cast columns to bool type due to: {e}\nStopped at {col} column.")
        print(f"Unable to cast columns to bool type due to: {e}\nStopped at {col} column.")

def upload_dataframe_chunks_to_gcs(
    dataframe: pd.DataFrame,
    blob_prefix: str,
    bucket_name: str=os.getenv("GCP_BUCKET", "br_cenipa"),
    chunk_size: int = 100_000):
    """
    Splits a DataFrame into chunks and uploads each chunk as a Parquet file to Google Cloud Storage.

    Args:
        dataframe (pd.DataFrame): The DataFrame to upload.
        blob_prefix (str): Prefix for the blob (file) name in GCS.
        bucket_name (str, optional): Name of the GCS bucket. Defaults to value from environment variable 'GCP_BUCKET'.
        chunk_size (int, optional): Number of rows per chunk/file. Defaults to 100,000.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    num_chunks = math.ceil(len(dataframe) / chunk_size)

    for i in range(num_chunks):
        start = i * chunk_size
        end = min((i + 1) * chunk_size, len(dataframe))
        chunk = dataframe.iloc[start:end]
        buffer = io.BytesIO()
        chunk.to_parquet(buffer, index=False)
        buffer.seek(0)
        blob_name = f"{blob_prefix}_part{i+1}.parquet"
        blob = bucket.blob(blob_name)
        blob.upload_from_file(buffer, content_type='application/octet-stream')
        logging.info(f"Chunk {i+1}/{num_chunks} enviado para gs://{bucket_name}/{blob_name}")