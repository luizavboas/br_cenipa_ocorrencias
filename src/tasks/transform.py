# -*- coding: utf-8 -*-
"""
Transformation tasks for the br_cenipa project.

This module defines Prefect tasks for data cleaning, type casting, consistency checks,
and uploading processed data to Google Cloud Storage.
"""

import os
import logging
import pandas as pd
from prefect import task

from src.constants import *
from src.utils.utils import *

# Fact table
@task(log_prints=True)
def load_fact_table() -> pd.DataFrame:
    """
    Loads the fact table from the input directory.

    Returns:
        pd.DataFrame: The loaded fact table as a pandas DataFrame.
    """
    df_fact_table = pd.read_csv(
        os.path.join(constants.INPUT_DIR_PATH.value, "ocorrencia.csv"),
        sep=";",
        encoding="utf-8"
    )
    return df_fact_table

@task(log_prints=True)
def check_fact_table(df_fact_table: pd.DataFrame) -> pd.DataFrame:
    """
    Checks the fact table for nulls and code inconsistencies, removes non-unique code columns,
    and applies renaming.

    Args:
        df_fact_table (pd.DataFrame): The fact table DataFrame.

    Returns:
        pd.DataFrame: The modified fact table DataFrame.
    """
    logging.info(f"Checking fact table code columns for inconsistencies...")
    columns_code = [
        'codigo_ocorrencia', 
        'codigo_ocorrencia1',
        'codigo_ocorrencia2',
        'codigo_ocorrencia3',
        'codigo_ocorrencia4']
    df_null = pd.DataFrame([])
    df_null = df_fact_table[df_fact_table[columns_code].isnull().any(axis=1)]
    if not df_null.empty:
        logging.info(f"Any row with one or more nulls: {df_null}")
        print(f"Any row with one or more nulls: {df_null}")
        df_null = pd.DataFrame([])
    df_null = df_fact_table[df_fact_table[columns_code].isnull().all(axis=1)]
    if not df_null.empty:
        logging.info(f"Any null row: {df_null}")
        print(f"Any null row: {df_null}")

    df_fact_table[(df_fact_table['codigo_ocorrencia'] == df_fact_table['codigo_ocorrencia1'])&
                  (df_fact_table['codigo_ocorrencia'] == df_fact_table['codigo_ocorrencia2'])&
                  (df_fact_table['codigo_ocorrencia'] == df_fact_table['codigo_ocorrencia3'])&
                  (df_fact_table['codigo_ocorrencia'] == df_fact_table['codigo_ocorrencia4'])]
    columns_code.remove('codigo_ocorrencia')

    # Remove columns with codes that are not unique
    df_fact_table_modif = df_fact_table.drop(columns=columns_code)\
        .rename(columns=constants.RENAME_MAPPING.value).copy()

    check_inconsistences(df_fact_table_modif)
    return df_fact_table_modif

@task(log_prints=True)
def type_cast_fact_table(df_fact_table_modif: pd.DataFrame):
    """
    Applies type casting and formatting to the fact table, including float, string, date, time, and boolean columns.
    Saves the processed table as a CSV file in the output directory.

    Args:
        df_fact_table_modif (pd.DataFrame): The modified fact table DataFrame.
    """
    df_fact_cast = df_fact_table_modif.copy()
    for col in constants.FLOAT_COLUMNS.value:
        df_fact_cast[col] = df_fact_cast[col]\
            .astype(str)\
            .str.replace(r'\*+', '0', regex=True)\
            .replace(r'°', '', regex=True)\
            .apply(transform_lat_long)
    format_floats(df_fact_cast, constants.FLOAT_COLUMNS.value)
    format_string(df_fact_cast, constants.STRING_COLUMNS.value)
    format_date(df_fact_cast, constants.DATE_COLUMNS.value)
    format_time(df_fact_cast, constants.TIMESTAMP_COLUMNS.value)
    show_uniques(df_fact_cast, constants.BOOL_COLUMNS.value)
    format_bools(df_fact_cast, constants.BOOL_COLUMNS.value)
    show_uniques(df_fact_cast, constants.BOOL_COLUMNS.value)

    logging.info("Checking consistency after transformations...")
    check_inconsistences(df_fact_cast)
    df_fact_cast.to_csv(os.path.join(constants.OUTPUT_DIR_PATH.value, "br_cenipa_ocorrencia.csv"), index=False)

@task(log_prints=True)
def load_dim_tables() -> List[pd.DataFrame]:
    """
    Loads all dimension tables from the input directory.

    Returns:
        List[pd.DataFrame]: List containing all loaded dimension tables as pandas DataFrames.
    """
    logging.info("Reading dimension tables...")
    print("Reading dimension tables...")
    try:
        df_tipos = pd.read_csv(
            os.path.join(constants.INPUT_DIR_PATH.value, "ocorrencia_tipo.csv"),
            sep=";",
            encoding="utf-8"
        )
        df_aeronave = pd.read_csv(
            os.path.join(constants.INPUT_DIR_PATH.value, "aeronave.csv"),
            sep=";",
            encoding="utf-8"
        )
        df_fator = pd.read_csv(
            os.path.join(constants.INPUT_DIR_PATH.value, "fator_contribuinte.csv"),
            sep=";",
            encoding="utf-8"
        )
        df_recomendacao = pd.read_csv(
            os.path.join(constants.INPUT_DIR_PATH.value, "recomendacao.csv"),
            sep=";",
            encoding="utf-8"
        )
    except Exception as e:
        logging.error(f"Error during dimension tables reading: {e}")
        print(f"Error during dimension tables reading: {e}")
    dim_tables = [df_tipos, df_aeronave, df_fator, df_recomendacao]
    return dim_tables

@task(log_prints=True)
def renaming_dim_tables(dim_tables: List[pd.DataFrame]) -> List[pd.DataFrame]:
    """
    Renames columns in all dimension tables using the mappings defined in constants.

    Args:
        dim_tables (List[pd.DataFrame]): List of dimension tables.

    Returns:
        List[pd.DataFrame]: List of renamed dimension tables.
    """
    logging.info("Renaming dimension tables...")
    print("Renaming dimension tables...")
    try:
        df_tipos_modif = dim_tables[0].rename(columns=constants.TIPO_RENAME_MAPPING.value).copy()
        df_aeronave_modif = dim_tables[1].rename(columns=constants.AERONAVE_RENAME_MAPPING.value).copy()
        df_fator_modif = dim_tables[2].rename(columns=constants.FATOR_RENAME_MAPPING.value).copy()
        df_recomendacao_modif = dim_tables[3].rename(columns=constants.RECOMENDACAO_RENAME_MAPPING.value).copy()
        del dim_tables
        return [df_tipos_modif, df_aeronave_modif, df_fator_modif, df_recomendacao_modif]
    except Exception as e:
        logging.error(f"Error during dimension tables renaming: {e}")
        print(f"Error during dimension tables renaming: {e}")

@task(log_prints=True)
def type_cast_tipo_table(df_tipo_modif: pd.DataFrame):
    """
    Applies type casting and formatting to the 'tipo' dimension table and saves it as a CSV file.

    Args:
        df_tipo_modif (pd.DataFrame): The modified 'tipo' dimension table DataFrame.
    """
    if df_tipo_modif is not None:
        try:
            df_tipo_cast = df_tipo_modif.copy()
            TIPO_STRING_COLUMNS = list(df_tipo_cast.columns.values)
            TIPO_STRING_COLUMNS.remove('id_ocorrencia')
            format_string(df_tipo_cast, TIPO_STRING_COLUMNS)
            logging.info("Checking consistency after transformations...")
            print("Checking consistency after transformations...")
            check_inconsistences(df_tipo_cast)
            del df_tipo_modif
            df_tipo_cast.to_csv(os.path.join(constants.OUTPUT_DIR_PATH.value, "br_cenipa_tipo_ocorrencia.csv"), index=False)
        except Exception as e:
            logging.error(f"Error during 'tipo' table type casting: {e}")
            print(f"Error during 'tipo' table type casting: {e}")

@task(log_prints=True)
def type_cast_aeronave_table(df_aeronave_modif: pd.DataFrame):
    """
    Applies type casting and formatting to the 'aeronave' dimension table and saves it as a CSV file.

    Args:
        df_aeronave_modif (pd.DataFrame): The modified 'aeronave' dimension table DataFrame.
    """
    if df_aeronave_modif is not None:
        try:
            df_aeronave_cast = df_aeronave_modif.copy()
            format_string(df_aeronave_cast, constants.AERONAVE_STR_COLUMNS.value)
            format_floats(df_aeronave_cast, constants.AERONAVE_INT_COLUMNS.value)
            logging.info("Checking consistency after transformations...")
            print("Checking consistency after transformations...")
            check_inconsistences(df_aeronave_cast)
            del df_aeronave_modif
            df_aeronave_cast.to_csv(os.path.join(constants.OUTPUT_DIR_PATH.value, "br_cenipa_aeronave.csv"), index=False)
        except Exception as e:
            logging.error(f"Error during 'aeronave' table type casting: {e}")
            print(f"Error during 'aeronave' table type casting: {e}")

def type_cast_fator_table(df_fator_modif: pd.DataFrame):
    """
    Applies type casting and formatting to the 'fator contribuinte' dimension table and saves it as a CSV file.

    Args:
        df_fator_modif (pd.DataFrame): The modified 'fator contribuinte' dimension table DataFrame.
    """
    if df_fator_modif is not None:
        try:
            df_fator_cast = df_fator_modif.copy()
            FATOR_STRING_COLUMNS = list(df_fator_cast.columns.values)
            FATOR_STRING_COLUMNS.remove('id_ocorrencia')
            format_string(df_fator_cast, FATOR_STRING_COLUMNS)
            logging.info("Checking consistency after transformations...")
            print("Checking consistency after transformations...")
            check_inconsistences(df_fator_cast)
            del df_fator_modif
            df_fator_cast.to_csv(os.path.join(constants.OUTPUT_DIR_PATH.value, "br_cenipa_fator_contribuinte.csv"), index=False)
        except Exception as e:
            logging.error(f"Error during 'fator contribuinte' table type casting: {e}")
            print(f"Error during 'fator contribuinte' table type casting: {e}")

@task(log_prints=True)
def type_cast_recom_table(df_recomendacao_modif: pd.DataFrame):
    """
    Applies type casting and formatting to the 'recomendacao' dimension table and saves it as a CSV file.

    Args:
        df_recomendacao_modif (pd.DataFrame): The modified 'recomendacao' dimension table DataFrame.
    """
    if df_recomendacao_modif is not None:
        try:
            df_recomendacao_cast = df_recomendacao_modif.copy()
            format_string(df_recomendacao_cast, constants.RECOMENDACAO_STR_COLUMNS.value)
            format_date(df_recomendacao_cast, constants.RECOMENDACAO_DATE_COLUMNS.value)
            logging.info("Checking consistency after transformations...")
            print("Checking consistency after transformations...")
            check_inconsistences(df_recomendacao_cast)
            del df_recomendacao_modif
            df_recomendacao_cast.to_csv(os.path.join(constants.OUTPUT_DIR_PATH.value, "br_cenipa_recomendacao.csv"), index=False)
        except Exception as e:
            logging.error(f"Error during 'recomendacao' table type casting: {e}")
            print(f"Error during 'recomendacao' table type casting: {e}")        

@task
def upload_output():
    """
    Uploads all CSV files from the output directory to Google Cloud Storage in chunks.
    Each CSV is split into multiple Parquet files if necessary.

    Returns:
        None
    """
    try:
        folder = constants.OUTPUT_DIR_PATH.value
        files_names = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
        logging.info(f"Found {len(files_names)} files in output folder: {folder}")

        for file_name in files_names:
            try:
                if file_name.endswith(".csv"):
                    file_path = os.path.join(folder, file_name)
                    logging.info(f"Uploading {file_name} to GCS in chunks...")
                    upload_dataframe_chunks_to_gcs(
                        pd.read_csv(file_path),
                        f"output/{file_name.replace('.csv','')}",
                        os.getenv("GCP_BUCKET", "br_cenipa")
                    )
                    logging.info(f"File {file_name} uploaded successfully.")
            except Exception as e:
                logging.error(f"Error uploading {file_name} to GCS: {e}")
    except Exception as e:
        logging.error(f"Error listing files for upload: {e}")