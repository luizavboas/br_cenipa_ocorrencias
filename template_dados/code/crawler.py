# -*- coding: utf-8 -*-
"""
Tasks para dataset do CENIPA (Centro de Investigação e Prevenção de Acidentes Aeronáuticos).
# https://www.gov.br/cenipa/pt-br
# API: https://dados.gov.br/swagger-ui/index.html
"""
import os
import json
import logging
import requests
import pandas as pd
from pathlib import Path

from constants import *
from utils import *


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
