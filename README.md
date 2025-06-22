# Dados CENIPA de Ocorrências Aeronáuticas

Este repositório contém um pipeline para extração, transformação e tratamento de dados de ocorrências aeronáuticas do CENIPA, utilizando Prefect para orquestração.

## Sumário

- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Configuração](#configuração)
- [Execução](#execução)
- [Estrutura de Pastas](#estrutura-de-pastas)
- [Contribuição](#contribuição)
- [Licença](#licença)

## Pré-requisitos

- Python 3.10+
- [Poetry](https://python-poetry.org/) ou `pip`
- [Prefect](https://docs.prefect.io/)
- [Selenium](https://selenium-python.readthedocs.io/)
- [ChromeDriver](https://chromedriver.chromium.org/) ou equivalente

## Instalação

```bash
git clone https://github.com/seu-usuario/br_cenipa.git
cd br_cenipa
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# ou, se usar poetry
poetry install
```

## Configuração

Crie um arquivo `.env` na raiz:

```
EXECUTION_MODE=local
EXTRACTION_MODE=API
API_KEY=sua_chave_api
```

## Execução

### Rodando o pipeline

```sh
# Ative o ambiente virtual
source .venv/bin/activate

# Execute o pipeline
python -m src.flows.main
```

Ou usando Prefect:

```sh
prefect deployment run src.flows.main
```

### Usando Docker

```sh
docker build -t br_cenipa .
docker run --env-file .env br_cenipa
```

O script aceita os modos: `local`, `server` ou `cloud` (ajuste a variável `EXECUTION_MODE`).


## Estrutura do Projeto

```
br_cenipa/
│
├── input/                      # Dados brutos
├── output/                     # Dados tratados
├── src/                        # Código-fonte principal do projeto
│   ├── __init__.py
│   ├── constants.py
│   ├── schedules.py
│   ├── flows/
│   │   ├── __init__.py
│   │   └── main.py
│   ├── tasks/
│   │   ├── __init__.py
│   │   ├── extract.py
│   │   └── transform.py
│   └── utils/
│       ├── __init__.py
│       └── utils.py
├── .env
├── .gitignore
├── config.py
├── Dockerfile
├── README.md
├── requirements.txt            # (ou poetry.lock/pyproject.toml)
└── startup.sh
```

## Licença

[MIT](LICENSE)