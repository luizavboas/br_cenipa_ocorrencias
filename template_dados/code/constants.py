import os
from pathlib import Path

## Constants
ROOT_DIR = Path(os.path.dirname(__file__)).parent
INPUT_DIR_PATH = os.path.join(Path(os.path.dirname(__file__)).parent,"input")

DATASET_ID = "623d13d9-3465-4be0-82e7-c13b78b08282"
API_URL = "https://dados.gov.br/dados/api/publico"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJkNDRMMXkzcGxRTlFIX1pvU2VTb28yU0t3TjFzUkNBTS13LTRYZ2Ywc0d3dFVveW1ZNzJMQXNUakZtNlhFbWZhMm8taWozYWJicGlxMnN3eiIsImlhdCI6MTc0OTkzNzUwOH0.__f5sSiipPmb_gwhGIA06UgCmYbR7UWZ2Li8LH-KI_E"

RENAME_MAPPING = {
        'codigo_ocorrencia': 'id_ocorrencia',
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
        'investigacao_status':'satus_investigacao',
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
    'satus_investigacao', 
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
    'id_ocorrencia',
    'quantidade_recomendacoes',
    'quantidade_aeronaves_envolvidas'
]

## Aeronave
AERONAVE_RENAME_MAPPING = {
    "codigo_ocorrencia2":"id_ocorrencia",
    "aeronave_matricula":"id_aeronave",
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
    "aeronave_voo_origem":"voo_origem",
    "aeronave_voo_destino":"voo_destino",
    "aeronave_fase_operacao":"fase_operacao",
    "aeronave_tipo_operacao":"tipo_operacao",
    "aeronave_nivel_dano":"nivel_dano",
    "aeronave_fatalidades_total":"quantidade_fatalidades"
}

AERONAVE_INT_COLUMNS = [
    "id_ocorrencia",
    "pmd_aeronave",
    "categoria_pmd",
    "quantidade_assentos",
    "ano_fabricacao",
    "quantidade_fatalidades"
]

AERONAVE_STR_COLUMNS = [
    "id_aeronave",
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
    "voo_origem",
    "voo_destino",
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