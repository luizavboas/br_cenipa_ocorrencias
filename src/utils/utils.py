# -*- coding: utf-8 -*-
"""
General purpose functions for the br_cenipa project
"""

import os
import re
import logging
import dotenv
from loguru import logger
import pandas as pd
from typing import List
from pathlib import Path
from datetime import datetime

# Wehscraping option libs
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# API option libs
import json
import requests

# Internals
from src.constants import constants

# file_handler = logging.FileHandler(os.path.join(constants.ROOT_DIR.value, 'tmp',f"logging{datetime.now().strftime('%Y-%m-%d-%H%M')}.txt"))

# logger.add(file_handler,   
#            level='INFO')

logging.basicConfig(level=logging.INFO)

def set_driver():
    """
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
        logging.info(f"Downloaded {table_name} to {file_path}")
        print(f"Downloaded {table_name} to {file_path}")
    except Exception as e:
        raise
    
def correct_csv_encoding():
    """
        Corrects the encoding of CSV files in the input directory from 'latin1' to 'utf-8'.
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
        Displays unique values for specified columns in the dataset.
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

## Inconsistencies
def check_inconsistences(dataframe:pd.DataFrame, primary_key:bool=False):
    # Check for unique values in the 'id_ocorrencia' column
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
    # Check for missing values in the columns
    for col in dataframe.columns:
        if dataframe[col].isnull().any():
            logging.warning(f"Column '{col}' has missing values.")
            print(f"Column '{col}' has missing values.")
        if col.startswith('id'):
            # Check for unique values in the 'id_relatorio' column
            if dataframe[col].is_unique:
                pass
            else:
                logging.warning(f"The {col} has duplicated values. Shouldn't they be unique?")
    # Check for duplicate rows
    if dataframe.duplicated().any():
        logging.warning("There are duplicate rows in the DataFrame.")
        print("There are duplicate rows in the DataFrame.")
        logging.info(dataframe[dataframe.duplicated()])
        print(dataframe[dataframe.duplicated()])

## Formatting String Columns        
# Convert string columns to lowercase with first letter of each word capitalized (except connectors) 
def format_string(dataframe:pd.DataFrame, string_columns:List[str]):
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

## Formatting float columns
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

def format_floats(dataframe:pd.DataFrame, float_columns:List[str]):
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

## Formatting Date Columns
# Convert date columns to datetime format
def format_date(dataframe:pd.DataFrame, date_columns:List[str]):
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

## Formatting Timestamp Columns
def format_time(dataframe:pd.DataFrame, timestamp_columns:List[str]):
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

## Formatting Boolean Columns
# Convert boolean columns to boolean type
def format_bools(dataframe:pd.DataFrame, bool_columns:List[str]):
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