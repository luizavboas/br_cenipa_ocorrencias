# -*- coding: utf-8 -*-
"""
Tasks para dataset do CENIPA (Centro de Investigação e Prevenção de Acidentes Aeronáuticos).
"""
import os
import json
import time
import logging
import requests
import pandas as pd
from prefect import task
from prefect.logging import get_run_logger
from pathlib import Path

from src.constants import constants
from src.utils.utils import *

@task
def scrape_data():
    logging.info("Setting driver up...")
    driver = set_driver()
    logging.info("Fetching url...")
    response = driver.get(f"{constants.DADOS_GOV_URL.value}/{constants.DADOS_GOV_DATASET_NAME.value}")

    logging.info(f"Current URL:{driver.current_url}")
    dropdown_items = driver.find_elements(by=By.XPATH,
                    value="//*[@id='btnCollapse']")

    time.sleep(1)
    logging.info("Dropdown Items")
    for item in dropdown_items:
        span = item.find_element(by=By.TAG_NAME,
                                 value="span")
        if span:
            logging.info(span.text)
            if span.text == "Recursos":
                item.click()
                time.sleep(5)
                resources = driver.find_elements(by=By.XPATH,
                                value=constants.RESOURCES_XPATH.value)
                logging.info("Found 'Recursos'!")
                break
            else:
                resources = None
        else:
            resources = None
    logging.info("Going through resources...")
    if resources is not None:
        buttons = [
        element for element in 
        driver.find_elements(by=By.XPATH, value=constants.BUTTONS_XPATH.value)
        if element.text == "Acessar o recurso"]
        for i, resource in enumerate(resources):
                titulo = resource.find_element(By.TAG_NAME,'h4')
                button = buttons[i]
                if button and button.text == "Acessar o recurso": 
                    button.click()
                    time.sleep(1)
                button = None
    else:
        logging.warning("Nenhum recurso encontrado")

    driver.close()

@task
def get_cenipa_metadata():
    print("Getting metadata from API...")
    HEADERS = {
        "accept":"application/json",
        "chave-api-dados-abertos":f"{constants.API_KEY.value}"
    }

    response = requests.get(
        headers = HEADERS,
        url = f"{constants.API_URL.value}/conjuntos-dados/{constants.API_DATASET_ID.value}")

    with open(os.path.join(constants.INPUT_DIR_PATH.value,"cenipa_metadata.json"), "w") as f:
        json.dump(response.json(), f, indent=4)
        metadata = response.json()
    return metadata

@task           
def get_cenipa_data():
    with open(os.path.join(constants.INPUT_DIR_PATH.value,"cenipa_metadata.json"), "r") as metadata_file:
        metadata = json.load(metadata_file)
        for resource in metadata["recursos"]:
            table_id = resource["id"]
            table_title = resource["titulo"]
            table_url = resource["link"]
            table_name = table_url.split("/")[-1].replace(".csv", "")
            table_format = resource["formato"]
        
            if table_format == "CSV":
                try:
                    logging.info(f"Downloading table: {table_title} (ID: {table_id})")
                    print(f"Downloading table: {table_title} (ID: {table_id})")
                    download_table_to_csv(table_name, table_url)
                except Exception as e:
                    logging.error(f"Failed to download {table_title}: {e}")
                    print(f"Failed to download {table_title}: {e}")

        correct_csv_encoding()
