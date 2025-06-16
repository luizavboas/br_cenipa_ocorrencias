# -*- coding: utf-8 -*-
"""
Tasks para dataset do CENIPA (Centro de Investigação e Prevenção de Acidentes Aeronáuticos).
# https://www.gov.br/cenipa/pt-br
"""

import os
import json
import logging
import requests
import pandas as pd
from pathlib import Path

# Constants
this_file_path = Path(os.path.abspath(os.path.dirname(__file__)))
parent_dir = this_file_path.parent
INPUT_DIR_PATH = os.path.join(parent_dir.parent, "input")
DATASET_URL = "https://dados.gov.br/dados/conjuntos-dados/ocorrencias-aeronauticas-da-aviacao-civil-brasileira"
DATASET_ID = "623d13d9-3465-4be0-82e7-c13b78b08282"
API_URL = "https://dados.gov.br/dados/api/publico"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJkNDRMMXkzcGxRTlFIX1pvU2VTb28yU0t3TjFzUkNBTS13LTRYZ2Ywc0d3dFVveW1ZNzJMQXNUakZtNlhFbWZhMm8taWozYWJicGlxMnN3eiIsImlhdCI6MTc0OTkzNzUwOH0.__f5sSiipPmb_gwhGIA06UgCmYbR7UWZ2Li8LH-KI_E"

session = requests.Session()

HEADERS = {
    "accept":"application/json",
    "chave-api-dados-abertos":f"{API_KEY}"
}


def download_table_to_csv(table_name, table_url, table_path=INPUT_DIR_PATH):
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
    except Exception as e:
        raise
    
def correct_csv_encoding():
    """
        Corrects the encoding of CSV files in the input directory from 'latin1' to 'utf-8'.
    """
    logging.info("Correcting CSV file encodings from 'latin1' to 'utf-8'...")
    for  file_name in os.listdir(INPUT_DIR_PATH):
        if file_name.endswith(".csv"):
            file_path = os.path.join(INPUT_DIR_PATH, file_name)
            pd.read_csv(file_path, sep=";", encoding="latin1")\
            .to_csv(file_path, sep=";", encoding="utf-8", index=False)

session = requests.Session()

HEADERS = {
    "accept":"application/json",
    "chave-api-dados-abertos":f"{API_KEY}"
}

response = requests.get(
    headers = HEADERS,
    url = f"{API_URL}/conjuntos-dados/{DATASET_ID}")

with open(os.path.join(INPUT_DIR_PATH,"cenipa_metadata.json"), "w") as f:
    json.dump(response.json(), f, indent=4)

metadata = response.json()
            
for resource in metadata["recursos"]:
    table_id = resource["id"]
    table_title = resource["titulo"]
    table_url = resource["link"]
    table_name = table_url.split("/")[-1].replace(".csv", "")
    table_format = resource["formato"]
    
    if table_format == "CSV":
        try:
            logging.info(f"Downloading table: {table_title} (ID: {table_id})")
            download_table_to_csv(table_name, table_url)
        except Exception as e:
            logging.error(f"Failed to download {table_title}: {e}")

correct_csv_encoding()