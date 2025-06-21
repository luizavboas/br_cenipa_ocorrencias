# -*- coding: utf-8 -*-
"""
Tasks for br_cenipa
"""
import os
import logging
import pandas as pd
from prefect import task

from constants import *
from utils import *

print("Hello from transform_tasks.py")
# Fact table
@task
def load_fact_table()->pd.DataFrame:
    df_fact_table = pd.read_csv(
    os.path.join(constants.INPUT_DIR_PATH.value, "ocorrencia.csv"),
    sep=";",
    encoding="utf-8")
    
    return df_fact_table

@task
def check_fact_table(df_fact_table:pd.DataFrame)->pd.DataFrame:
    logging.info(f"Checking fact table code columns for inconsistencies...")
    print(f"Checking fact table code columns for inconsistencies...")
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

    df_fact_table[(df_fact_table['codigo_ocorrencia'] == df_fact_table['codigo_ocorrencia1'])&\
               (df_fact_table['codigo_ocorrencia'] == df_fact_table['codigo_ocorrencia2'])&\
               (df_fact_table['codigo_ocorrencia'] == df_fact_table['codigo_ocorrencia3'])&\
               (df_fact_table['codigo_ocorrencia'] == df_fact_table['codigo_ocorrencia4'])]
    columns_code.remove('codigo_ocorrencia')

    # Remove columns with codes that are not unique
    df_fact_table_modif = df_fact_table.drop(columns=columns_code)\
        .rename(columns=constants.RENAME_MAPPING.value).copy()

    check_inconsistences(df_fact_table_modif)
    return df_fact_table_modif

# Type casting
@task
def type_cast_fact_table(df_fact_table_modif:pd.DataFrame):
    df_fact_cast = df_fact_table_modif.copy()
    for col in constants.FLOAT_COLUMNS.value:
        df_fact_cast[col] = df_fact_cast[col]\
        .astype(str)\
        .str.replace(r'\*+', '0', regex=True)\
        .replace(r'°', '', regex=True)\
        .apply(transform_lat_long)
    format_floats(df_fact_cast,
              constants.FLOAT_COLUMNS.value)
    format_string(df_fact_cast, constants.STRING_COLUMNS.value)
    format_date(df_fact_cast, constants.DATE_COLUMNS.value)
    format_time(df_fact_cast, constants.TIMESTAMP_COLUMNS.value)
    show_uniques(df_fact_cast, constants.BOOL_COLUMNS.value)
    format_bools(df_fact_cast, constants.BOOL_COLUMNS.value)
    show_uniques(df_fact_cast, constants.BOOL_COLUMNS.value)

    logging.info("Checking consistency after transformations...")
    check_inconsistences(df_fact_cast)
    df_fact_cast.to_csv(os.path.join(constants.OUTPUT_DIR_PATH.value,"br_cenipa_ocorrencia.csv"), index=False)


## Dimension tables
@task
def load_dim_tables():
    logging.info("Reading dimension tables...")
    print("Reading dimension tables...")
    try:
        df_tipos = pd.read_csv(
        os.path.join(constants.INPUT_DIR_PATH.value, "ocorrencia_tipo.csv"),
        sep=";",
        encoding="utf-8")

        df_aeronave = pd.read_csv(
        os.path.join(constants.INPUT_DIR_PATH.value, "aeronave.csv"),
        sep=";",
        encoding="utf-8")

        df_fator = pd.read_csv(
        os.path.join(constants.INPUT_DIR_PATH.value, "fator_contribuinte.csv"),
        sep=";",
        encoding="utf-8")

        df_recomendacao = pd.read_csv(
        os.path.join(constants.INPUT_DIR_PATH.value, "recomendacao.csv"),
        sep=";",
        encoding="utf-8")
    except Exception as e:
        logging.error(f"Error during dimension tables reading: {e}")
        print(f"Error during dimension tables reading: {e}")
    dim_tables = [df_tipos,df_aeronave,df_fator,df_recomendacao]
    return dim_tables


# Renaming columns with mappings for each dataframe
@task
def renaming_dim_tables(dim_tables:List[pd.DataFrame])->List[pd.DataFrame]:
    logging.info("Renaming dimension tables...")
    print("Renaming dimension tables...")
    try:
        df_tipos_modif = dim_tables[0]\
            .rename(columns=constants.TIPO_RENAME_MAPPING.value).copy()
        df_aeronave_modif = dim_tables[1]\
            .rename(columns=constants.AERONAVE_RENAME_MAPPING.value).copy()
        df_fator_modif = dim_tables[2]\
            .rename(columns=constants.FATOR_RENAME_MAPPING.value).copy()
        df_recomendacao_modif = dim_tables[3]\
            .rename(columns=constants.RECOMENDACAO_RENAME_MAPPING.value).copy()
        del dim_tables
        return [df_tipos_modif,df_aeronave_modif,df_fator_modif,df_recomendacao_modif]
    except Exception as e:
        logging.error(f"Error during dimension tables renaming: {e}")
        print(f"Error during dimension tables renaming: {e}")

@task
def type_cast_tipo_table(df_tipo_modif:pd.DataFrame):
    if df_tipo_modif is not None:
        try:
            check_inconsistences(df_tipo_modif)
            try:
                # Type casting
                df_tipo_cast = df_tipo_modif.copy()
                TIPO_STRING_COLUMNS = list(df_tipo_cast.columns.values)
                TIPO_STRING_COLUMNS.remove('id_ocorrencia')
                format_string(df_tipo_cast, TIPO_STRING_COLUMNS)
                logging.info("Checking consistency after transformations...")
                print("Checking consistency after transformations...")
                check_inconsistences(df_tipo_cast)
                del df_tipo_modif
                df_tipo_cast.to_csv(os.path.join(constants.OUTPUT_DIR_PATH.value,"br_cenipa_tipo_ocorrencia.csv"), index=False)
            except Exception as e:
                logging.error(f"Error during 'tipo' table type casting: {e}")
                print(f"Error during 'tipo' table type casting: {e}")
        except Exception as e:
            logging.error(f"Error during 'tipo' table checking: {e}")
            print(f"Error during 'tipo' table checking: {e}")
        
@task
def type_cast_aeronave_table(df_aeronave_modif:pd.DataFrame):
    if df_aeronave_modif is not None:
        try:
            check_inconsistences(df_aeronave_modif)
            try:
                df_aeronave_cast = df_aeronave_modif.copy()
                format_string(df_aeronave_cast, constants.AERONAVE_STR_COLUMNS.value)
                format_floats(df_aeronave_cast, constants.AERONAVE_INT_COLUMNS.value)
                logging.info("Checking consistency after transformations...")
                print("Checking consistency after transformations...")
                check_inconsistences(df_aeronave_cast)
                del df_aeronave_modif
                df_aeronave_cast.to_csv(os.path.join(constants.OUTPUT_DIR_PATH.value,"br_cenipa_aeronave.csv"), index=False)
            except Exception as e:
                logging.error(f"Error during 'aeronave' table type casting: {e}")
                print(f"Error during 'aeronave' table type casting: {e}")
        except Exception as e:
            logging.error(f"Error during 'aeronave' table checking: {e}")
            print(f"Error during 'aeronave' table checking: {e}")

def type_cast_fator_table(df_fator_modif:pd.DataFrame):
    if df_fator_modif is not None:
        try:
            check_inconsistences(df_fator_modif)
            try:
                df_fator_cast = df_fator_modif.copy()
                FATOR_STRING_COLUMNS = list(df_fator_cast.columns.values)
                FATOR_STRING_COLUMNS.remove('id_ocorrencia')
                format_string(df_fator_cast, FATOR_STRING_COLUMNS)
                logging.info("Checking consistency after transformations...")
                print("Checking consistency after transformations...")
                check_inconsistences(df_fator_cast)
                del df_fator_modif
                df_fator_cast.to_csv(os.path.join(constants.OUTPUT_DIR_PATH.value,"br_cenipa_fator_contribuinte.csv"), index=False)
            except Exception as e:
                logging.error(f"Error during 'fator contribuinte' table type casting: {e}")
                print(f"Error during 'fator contribuinte' table type casting: {e}")
        except Exception as e:
            logging.error(f"Error during 'fator contribuinte' table checking: {e}")
            print((f"Error during 'fator contribuinte' table checking: {e}"))


@task
def type_cast_recom_table(df_recomendacao_modif:pd.DataFrame):
    if df_recomendacao_modif is not None:
        try:
            check_inconsistences(df_recomendacao_modif)
            try:
                df_recomendacao_cast = df_recomendacao_modif.copy()
                format_string(df_recomendacao_cast, constants.RECOMENDACAO_STR_COLUMNS.value)
                format_date(df_recomendacao_cast, constants.RECOMENDACAO_DATE_COLUMNS.value)
                logging.info("Checking consistency after transformations...")
                print("Checking consistency after transformations...")
                check_inconsistences(df_recomendacao_cast)
                del df_recomendacao_modif
                df_recomendacao_cast.to_csv(os.path.join(constants.OUTPUT_DIR_PATH.value,"br_cenipa_recomendacao.csv"), index=False)
            except Exception as e:
                logging.error(f"Error during 'recomendacao' table type casting: {e}")
                print(f"Error during 'recomendacao' table type casting: {e}")
        except Exception as e:
            logging.error(f"Error during 'recomendacao' table checking: {e}")
            print(f"Error during 'recomendacao' table checking: {e}")
        