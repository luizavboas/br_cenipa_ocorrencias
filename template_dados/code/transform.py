
# -*- coding: utf-8 -*-
import os
import logging
import pandas as pd

from constants import *
from utils import *

## Fact table
df_ocorrencias = pd.read_csv(
    os.path.join(INPUT_DIR_PATH, "ocorrencia.csv"),
    sep=";",
    encoding="utf-8"
)

logger.info(f"Checking fact table code columns for inconsistencies...")
columns_code = [
        'codigo_ocorrencia', 
        'codigo_ocorrencia1',
        'codigo_ocorrencia2',
        'codigo_ocorrencia3',
        'codigo_ocorrencia4']
df_null = pd.DataFrame([])
df_null = df_ocorrencias[df_ocorrencias[columns_code].isnull().any(axis=1)]
if not df_null.empty:
    logger.info(f"Any row with one or more nulls: {df_null}")
    df_null = pd.DataFrame([])
df_null = df_ocorrencias[df_ocorrencias[columns_code].isnull().all(axis=1)]
if not df_null.empty:
    logger.info(f"Any null row: {df_null}")

df_ocorrencias[(df_ocorrencias['codigo_ocorrencia'] == df_ocorrencias['codigo_ocorrencia1'])&\
               (df_ocorrencias['codigo_ocorrencia'] == df_ocorrencias['codigo_ocorrencia2'])&\
               (df_ocorrencias['codigo_ocorrencia'] == df_ocorrencias['codigo_ocorrencia3'])&\
               (df_ocorrencias['codigo_ocorrencia'] == df_ocorrencias['codigo_ocorrencia4'])]
columns_code.remove('codigo_ocorrencia')

# Remove columns with codes that are not unique
df_ocorrencias_modif = df_ocorrencias.drop(columns=columns_code)\
    .rename(columns=RENAME_MAPPING).copy()

check_inconsistences(df_ocorrencias_modif)

# Type casting
for col in FLOAT_COLUMNS:
    df_ocorrencias_modif[col] = df_ocorrencias_modif[col]\
        .astype(str)\
        .str.replace(r'\*+', '0', regex=True)\
        .replace(r'°', '', regex=True)\
        .apply(transform_lat_long)
format_floats(df_ocorrencias_modif,
              FLOAT_COLUMNS)
format_string(df_ocorrencias_modif, STRING_COLUMNS)
format_date(df_ocorrencias_modif, DATE_COLUMNS)
format_time(df_ocorrencias_modif, TIMESTAMP_COLUMNS)
show_uniques(df_ocorrencias_modif, BOOL_COLUMNS)
format_bools(df_ocorrencias_modif, BOOL_COLUMNS)
show_uniques(df_ocorrencias_modif, BOOL_COLUMNS)

logger.info("Checking consistency after transformations...")
check_inconsistences(df_ocorrencias_modif)

## Dimension tables
logger.info("Reading dimension tables...")
try:
    df_tipos = pd.read_csv(
        os.path.join(INPUT_DIR_PATH, "ocorrencia_tipo.csv"),
        sep=";",
        encoding="utf-8")

    df_aeronave = pd.read_csv(
        os.path.join(INPUT_DIR_PATH, "aeronave.csv"),
        sep=";",
        encoding="utf-8")

    df_fator = pd.read_csv(
        os.path.join(INPUT_DIR_PATH, "fator_contribuinte.csv"),
        sep=";",
        encoding="utf-8")

    df_recomendacao = pd.read_csv(
        os.path.join(INPUT_DIR_PATH, "recomendacao.csv"),
        sep=";",
        encoding="utf-8")
except Exception as e:
    logger.error(f"Error during dimension tables reading: {e}")

# Renaming columns with mappings for each dataframe
logger.info("Renaming dimension tables...")
try:
    df_tipos_modif = df_tipos.rename(columns=TIPO_RENAME_MAPPING).copy()
    df_aeronave_modif = df_aeronave.rename(columns=AERONAVE_RENAME_MAPPING).copy()
    df_fator_modif = df_fator.rename(columns=FATOR_RENAME_MAPPING).copy()
    df_recomendacao_modif = df_recomendacao.rename(columns=RECOMENDACAO_RENAME_MAPPING).copy()
except Exception as e:
    logger.error(f"Error during dimension tables renaming: {e}")

if df_tipos_modif is not None:
    try:
        check_inconsistences(df_tipos_modif)
    except Exception as e:
        logger.error(f"Error during 'tipo' table checking: {e}")
    try:
        # Type casting
        TIPO_STRING_COLUMNS = list(df_tipos_modif.columns.values)
        TIPO_STRING_COLUMNS.remove('id_ocorrencia')
        format_string(df_tipos_modif, TIPO_STRING_COLUMNS)
        logger.info("Checking consistency after transformations...")
        check_inconsistences(df_tipos_modif)
    except Exception as e:
        logger.error(f"Error during 'tipo' table type casting: {e}")

if df_aeronave_modif is not None:
    try:
        check_inconsistences(df_aeronave_modif)
    except Exception as e:
        logger.error(f"Error during 'aeronave' table checking: {e}")
    try:
        format_string(df_aeronave_modif, AERONAVE_STR_COLUMNS)
        format_floats(df_aeronave_modif, AERONAVE_INT_COLUMNS)
        logger.info("Checking consistency after transformations...")
        check_inconsistences(df_aeronave_modif)
    except Exception as e:
        logger.error(f"Error during 'aeronave' table type casting: {e}")

if df_fator_modif is not None:
    try:
        check_inconsistences(df_fator_modif)
    except Exception as e:
        logger.error(f"Error during 'fator contribuinte' table checking: {e}")
    try:
        FATOR_STRING_COLUMNS = list(df_fator_modif.columns.values)
        FATOR_STRING_COLUMNS.remove('id_ocorrencia')
        format_string(df_fator_modif, FATOR_STRING_COLUMNS)
        logger.info("Checking consistency after transformations...")
        check_inconsistences(df_fator_modif)
    except Exception as e:
        logger.error(f"Error during 'fator contribuinte' table type casting: {e}")

if df_recomendacao_modif is not None:
    try:
        check_inconsistences(df_recomendacao_modif)
    except Exception as e:
        logger.error(f"Error during 'recomendacao' table checking: {e}")
    try:
        format_string(df_recomendacao_modif, RECOMENDACAO_STR_COLUMNS)
        format_date(df_recomendacao_modif, RECOMENDACAO_DATE_COLUMNS)
        logger.info("Checking consistency after transformations...")
        check_inconsistences(df_recomendacao_modif)
    except Exception as e:
        logger.error(f"Error during 'recomendacao' table type casting: {e}")