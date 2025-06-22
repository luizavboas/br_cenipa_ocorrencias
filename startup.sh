#!/bin/bash
set -e

# Ativa o ambiente virtual (Linux)
source .venv/bin/activate

# Carrega variáveis do .env
export $(grep -v '^#' .env | xargs)

python3 config.py

echo "Inicializando ambiente Prefect: $EXECUTION_MODE"

if [ "$EXECUTION_MODE" = "local" ]; then
    echo "Rodando flow sem orquestração (standalone)..."
    export PREFECT_API_URL=""
    python3 -m src.flows.main

elif [ "$EXECUTION_MODE" = "server" ]; then
    echo "Iniciando Prefect Server..."
    export PREFECT_API_URL=http://127.0.0.1:4200/api
    nohup prefect server start &
    sleep 5
    echo "Rodando flow com Prefect Server local..."
    python3 -m src.flows.main

elif [ "$EXECUTION_MODE" = "cloud" ]; then
    echo "Rodando com Prefect Cloud..."
    python3 -m src.flows.main

else
    echo "Modo de execução inválido: $EXECUTION_MODE"
    exit 1
fi