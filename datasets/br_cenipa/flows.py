# -*- coding: utf-8 -*-
"""
Flows for br_cenipa
"""
import dotenv
from prefect import flow, task
from constants import constants
from extract_tasks import *
from transform_tasks import *

dotenv.load_dotenv(constants.ROOT_DIR.value)

@flow
def br_cenipa_extract_flow():
    if EXTRACT_OPTION == 'API':
        get_cenipa_metadata()
        get_cenipa_data()
    elif EXTRACT_OPTION == 'SCRAPE':
        scrape_data()

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
