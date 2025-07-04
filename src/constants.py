# -*- coding: utf-8 -*-
"""
Constant values and configuration for the br_cenipa project.

This module defines all constant values, mappings, and environment variables
used throughout the br_cenipa data pipeline.
"""

import os
from enum import Enum
from pathlib import Path

class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_cenipa project
    """
    ROOT_DIR = os.getenv("ROOT_DIR",".")
    INPUT_DIR_PATH = os.getenv("INPUT_DIR_PATH","input")
    OUTPUT_DIR_PATH = os.getenv("OUTPUT_DIR_PATH", "output")
    EXECUTION_MODE = os.getenv("EXECUTION_MODE","local")
    EXTRACTION_MODE = os.getenv("EXTRACTION_MODE", "API")

    # Constants for API option
    API_DATASET_ID = "623d13d9-3465-4be0-82e7-c13b78b08282"
    API_URL = "https://dados.gov.br/dados/api/publico"
    API_KEY = os.getenv("API_KEY","")
    
    # Constants for webscraping option
    DADOS_GOV_DATASET_NAME = "ocorrencias-aeronauticas-da-aviacao-civil-brasileira"
    DADOS_GOV_URL = "https://dados.gov.br/dados/conjuntos-dados"
    RESOURCES_XPATH = "//*[@id='collapse-recursos']/div[contains(@class, 'row flex mb-5')]/div[contains(@class, 'col-10')]"
    BUTTONS_XPATH = "//*[@id='collapse-recursos']/div[contains(@class, 'row flex mb-5')]/div[contains(@class, 'col-10')]//button[@id='btnDownloadUrl']"
        
    RENAME_MAPPING = {
            'codigo_ocorrencia': 'id_ocorrencia',                               # NULLABLE FALSE
            'ocorrencia_classificacao':'classificacao_ocorrencia',              
            'ocorrencia_latitude':'latitude_ocorrencia',                        
            'ocorrencia_longitude': 'longitude_ocorrencia',                     
            'ocorrencia_cidade':'nome_municipio',                               
            'ocorrencia_uf':'sigla_uf',                                         
            'ocorrencia_pais':'nome_pais',
            'ocorrencia_aerodromo':'sigla_aerodromo',
            'ocorrencia_dia':'data_ocorrencia', 
            'ocorrencia_hora':'hora_ocorrencia',
            'investigacao_aeronave_liberada':'indicador_investigacao_liberada',
            'investigacao_status':'status_investigacao',
            'divulgacao_relatorio_numero':'id_relatorio',
            'divulgacao_relatorio_publicado':'indicador_relatorio_publicado', 
            'divulgacao_dia_publicacao':'data_publicacao_relatorio',
            'total_recomendacoes':'quantidade_recomendacoes', 
            'total_aeronaves_envolvidas':'quantidade_aeronaves_envolvidas',
            'ocorrencia_saida_pista':'indicador_saida_pista'
        }

    STRING_COLUMNS = [ 
        'classificacao_ocorrencia', 
        'nome_municipio', 
        'sigla_uf', 
        'nome_pais',
        'sigla_aerodromo', 
        'status_investigacao', 
        'id_relatorio'
    ]

    DATE_COLUMNS = [
        'data_ocorrencia', 
        'data_publicacao_relatorio']

    TIMESTAMP_COLUMNS = [
        'hora_ocorrencia'
    ]

    BOOL_COLUMNS = [ 
        'indicador_investigacao_liberada', 
        'indicador_relatorio_publicado',
        'indicador_saida_pista'
    ]

    FLOAT_COLUMNS = [
        'latitude_ocorrencia',
        'longitude_ocorrencia'
    ]

    INT_COLUMNS = [
        'quantidade_recomendacoes',
        'quantidade_aeronaves_envolvidas'
    ]

    ## Aeronave
    AERONAVE_RENAME_MAPPING = {
        "codigo_ocorrencia2":"id_ocorrencia",
        "aeronave_matricula":"matricula_aeronave",
        "aeronave_operador_categoria":"categoria_operador",
        "aeronave_tipo_veiculo":"tipo_veiculo",
        "aeronave_fabricante":"nome_fabricante",
        "aeronave_modelo":"nome_modelo",
        "aeronave_tipo_icao":"tipo_icao",
        "aeronave_motor_tipo":"tipo_motor",
        "aeronave_motor_quantidade":"quantidade_motores",
        "aeronave_pmd":"pmd_aeronave",
        "aeronave_pmd_categoria":"categoria_pmd",
        "aeronave_assentos":"quantidade_assentos",
        "aeronave_ano_fabricacao":"ano_fabricacao",
        "aeronave_pais_fabricante":"nome_pais_fabricante",
        "aeronave_pais_registro":"nome_pais_registro",
        "aeronave_registro_categoria":"categoria_registro",
        "aeronave_registro_segmento":"segmento_registro",
        "aeronave_voo_origem":"nome_voo_origem",
        "aeronave_voo_destino":"nome_voo_destino",
        "aeronave_fase_operacao":"fase_operacao",
        "aeronave_tipo_operacao":"tipo_operacao",
        "aeronave_nivel_dano":"nivel_dano",
        "aeronave_fatalidades_total":"quantidade_fatalidades"
    }

    AERONAVE_INT_COLUMNS = [
        "pmd_aeronave",
        "categoria_pmd",
        "quantidade_assentos",
        "ano_fabricacao",
        "quantidade_fatalidades"
    ]

    AERONAVE_STR_COLUMNS = [
        "matricula_aeronave",
        "categoria_operador",
        "tipo_veiculo",
        "nome_fabricante",
        "nome_modelo",
        "tipo_icao",
        "tipo_motor",
        "quantidade_motores",
        "nome_pais_fabricante",
        "nome_pais_registro",
        "categoria_registro",
        "segmento_registro",
        "nome_voo_origem",
        "nome_voo_destino",
        "fase_operacao",
        "tipo_operacao",
        "nivel_dano"
    ]

    ## Ocorrencia Tipo
    TIPO_RENAME_MAPPING = {
        "codigo_ocorrencia1":"id_ocorrencia",
        "ocorrencia_tipo":"tipo_ocorrencia",
        "ocorrencia_tipo_categoria":"categoria_ocorrencia",
        "taxonomia_tipo_icao":"taxonomia_icao"
    }

    ## Fator contribuinte
    FATOR_RENAME_MAPPING = {
        "codigo_ocorrencia3":"id_ocorrencia",
        "fator_nome":"nome_fator",
        "fator_aspecto":"aspecto_fator",
        "fator_condicionante":"condicionante_fator",
        "fator_area":"area_fator"
    }

    ## Recomendacao
    RECOMENDACAO_RENAME_MAPPING = {
        "codigo_ocorrencia4":"id_ocorrencia",
        "recomendacao_numero":"id_recomendacao",
        "recomendacao_dia_assinatura":"data_assinatura",
        "recomendacao_dia_encaminhamento":"data_encaminhamento",
        "recomendacao_dia_feedback":"data_feedback",
        "recomendacao_conteudo":"descricao",
        "recomendacao_status":"status",
        "recomendacao_destinatario_sigla":"sigla_destinatario",
        "recomendacao_destinatario":"nome_destinatario"
    }

    RECOMENDACAO_STR_COLUMNS = [
        "id_recomendacao",
        "descricao",
        "status",
        "sigla_destinatario",
        "nome_destinatario"
    ]

    RECOMENDACAO_DATE_COLUMNS = [
        "data_assinatura",
        "data_encaminhamento",
        "data_feedback"
    ]
