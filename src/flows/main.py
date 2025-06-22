# -*- coding: utf-8 -*-
"""
Prefect flows for the br_cenipa project.

This module defines the main Prefect flows for data extraction and transformation,
including orchestration logic and conditional execution based on environment variables.
"""

import os
import logging
from prefect import flow, task
from src.constants import constants
from src.tasks.extract import *
from src.tasks.transform import *

@flow
def br_cenipa_extract_flow():
    """
    Prefect flow for extracting CENIPA data.

    Chooses the extraction method (API or web scraping) based on the EXTRACTION_MODE constant.
    If no mode is set, defaults to API extraction.

    Returns:
        None
    """
    print(f"Extraction mode: {constants.EXTRACTION_MODE.value}")
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
    """
    Prefect flow for transforming CENIPA data.

    Loads, checks, and type casts the fact and dimension tables, then uploads the processed
    output to Google Cloud Storage if credentials and bucket are set.

    Returns:
        None
    """
    fact_table = load_fact_table()
    fact_table_checked = check_fact_table(fact_table)
    type_cast_fact_table(fact_table_checked)
    dim_tables = load_dim_tables()
    renamed_dim_tables = renaming_dim_tables(dim_tables)
    type_cast_tipo_table(renamed_dim_tables[0])
    type_cast_aeronave_table(renamed_dim_tables[1])
    type_cast_fator_table(renamed_dim_tables[2])
    type_cast_recom_table(renamed_dim_tables[3])

    GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    GCP_BUCKET = os.getenv("GCP_BUCKET","br_cenipa")
    
    if all([var != "" and var is not None for var in [GOOGLE_APPLICATION_CREDENTIALS, GCP_BUCKET]]):
        upload_output()

print("Starting process...")
br_cenipa_extract_flow()
br_cenipa_transform_flow()
