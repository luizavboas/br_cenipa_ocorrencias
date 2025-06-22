# -*- coding: utf-8 -*-
"""
Flows for br_cenipa
"""
import os
import logging
from prefect import flow, task
from src.constants import constants
from src.tasks.extract import *
from src.tasks.transform import *

@flow
def br_cenipa_extract_flow():
    print(f"Extractioin mode: {constants.EXTRACTION_MODE.value}")
    if constants.EXTRACTION_MODE.value == 'API':
        get_cenipa_metadata()
        get_cenipa_data()
    elif constants.EXTRACTION_MODE.value == 'SCRAPE':
        scrape_data()
    else:
        print("No extraction mode was chosen. Assuming API mode")
        get_cenipa_metadata()
        get_cenipa_data()

@flow
def br_cenipa_transform_flow():
    fact_table = load_fact_table()
    fact_table_checked = check_fact_table(fact_table)
    type_cast_fact_table(fact_table_checked)
    dim_tables = load_dim_tables()
    renamed_dim_tables = renaming_dim_tables(dim_tables)
    type_cast_tipo_table(renamed_dim_tables[0])
    type_cast_aeronave_table(renamed_dim_tables[1])
    type_cast_fator_table(renamed_dim_tables[2])
    type_cast_recom_table(renamed_dim_tables[3])


print("Starting process...")
br_cenipa_extract_flow()
br_cenipa_transform_flow()
