
# -*- coding: utf-8 -*-
import os
import json
from datetime import datetime
import logging
import pandas as pd
from pathlib import Path

from constants import *
from utils import *


logger = logging.getLogger(__name__)

## Ocorrencias Table
df_ocorrencias = pd.read_csv(
    os.path.join(INPUT_DIR_PATH, "ocorrencia.csv"),
    sep=";",
    encoding="utf-8"
)

logger.info(f"Checking code columns for inconsistencies...")
columns_code = [
        'codigo_ocorrencia', 
        'codigo_ocorrencia1',
        'codigo_ocorrencia2',
        'codigo_ocorrencia3',
        'codigo_ocorrencia4']

logger.info(f"Any row with one or more nulls: {df_ocorrencias[df_ocorrencias[columns_code].isnull().any(axis=1)]}")
logger.info(f"Any null row: {df_ocorrencias[df_ocorrencias[columns_code].isnull().all(axis=1)]}")

df_ocorrencias[(df_ocorrencias['codigo_ocorrencia'] == df_ocorrencias['codigo_ocorrencia1'])&\
               (df_ocorrencias['codigo_ocorrencia'] == df_ocorrencias['codigo_ocorrencia2'])&\
               (df_ocorrencias['codigo_ocorrencia'] == df_ocorrencias['codigo_ocorrencia3'])&\
               (df_ocorrencias['codigo_ocorrencia'] == df_ocorrencias['codigo_ocorrencia4'])]
columns_code.remove('codigo_ocorrencia')

# Remove columns with codes that are not unique
df_ocorrencias_modif = df_ocorrencias.drop(columns=columns_code)\
    .rename(columns=RENAME_MAPPING).copy()

## Inconsistencies
# Check for missing values in the columns
for col in df_ocorrencias_modif.columns:
    if df_ocorrencias_modif[col].isnull().any():
        logger.warning(f"Column '{col}' has missing values.")
# Check for unique values in the 'id_ocorrencia' column
assert df_ocorrencias_modif['id_ocorrencia'].is_unique, "The 'id_ocorrencia' column should have unique values."
# Check for unique values in the 'id_relatorio' column
if df_ocorrencias_modif['id_relatorio'].is_unique:
    pass
else:
    logger.warning("The 'id_relatorio' has duplicate values.")

# Check for duplicate rows
if df_ocorrencias_modif.duplicated().any():
    logger.warning("There are duplicate rows in the DataFrame.")
    logger.info(df_ocorrencias_modif[df_ocorrencias_modif.duplicated()])


## Formatting float columns
for col in FLOAT_COLUMNS:
    
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].astype(str)
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].str.strip()   
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].apply(transform_lat_long)
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].str.replace(r'\s+|°', '', regex=True) 
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].str.replace(r'\*|nan', '0', regex=True)
    
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].str.replace(',','.')
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].astype(float)


## Formatting String Columns
# Unique values for each column
        
# Convert string columns to lowercase with first letter of each word capitalized (except connectors) 
for col in STRING_COLUMNS:
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].astype(str)
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].str.strip()
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].str.replace(r'\*|nan', '', regex=True)
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].str.replace(r'\s+', ' ', regex=True)
    
    if not col.startswith('id'):
        df_ocorrencias_modif[col] = df_ocorrencias_modif[col].str.lower()
    
    if col.startswith('nome'):
        df_ocorrencias_modif[col] = df_ocorrencias_modif[col].str.title()
        df_ocorrencias_modif[col] = df_ocorrencias_modif[col].str.replace(
            r'\b(De|Da|Do|Das|Dos|E)\b', 
            lambda x: x.group(0).lower(), 
            regex=True)
    
    if col.startswith('sigla'):
        df_ocorrencias_modif[col] = df_ocorrencias_modif[col].str.upper()

    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].fillna('')

show_uniques(df_ocorrencias_modif, STRING_COLUMNS)

## Formatting Date Columns
# Convert date columns to datetime format
for col in DATE_COLUMNS:
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].astype(str)
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].str.strip()
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].fillna('')

    df_ocorrencias_modif[col] = pd.to_datetime(df_ocorrencias_modif[col], errors='coerce')
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].dt.strftime('%Y-%m-%d')

## Formatting Timestamp Columns
for col in TIMESTAMP_COLUMNS:
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].astype(str)
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].str.strip()
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].fillna('')

    df_ocorrencias_modif[col] = pd.to_datetime(df_ocorrencias_modif[col], errors='coerce')
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].dt.strftime('%H:%M:%S')

## Formatting Boolean Columns
show_uniques(df_ocorrencias_modif, BOOL_COLUMNS)
# Convert boolean columns to boolean type
for col in BOOL_COLUMNS:
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].astype(str)
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].str.strip()
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].fillna('')

    df_ocorrencias_modif[col] = df_ocorrencias_modif[col].str.lower()
    df_ocorrencias_modif.loc[df_ocorrencias_modif[col]=='sim',[col]] = 'True'     
    df_ocorrencias_modif.loc[df_ocorrencias_modif[col]=='não',[col]] = 'False'
    df_ocorrencias_modif[col] = df_ocorrencias_modif.astype(bool)

show_uniques(df_ocorrencias_modif, BOOL_COLUMNS)



