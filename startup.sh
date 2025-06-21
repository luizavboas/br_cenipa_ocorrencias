#!/bin/bash
source .env/bin/activate

echo "Inicializando ambiente Prefect: $EXECUTION_MODE"

if [ "$EXECUTION_MODE" == "local" ]; then
    echo "Rodando flow sem orquestração (standalone)..."
    export PREFECT_API_URL=""
    python3 datasets/br_cenipa/flows.py

elif [ "$EXECUTION_MODE" == "server" ]; then
    echo "Iniciando Prefect Server..."
    export PREFECT_API_URL=http://127.0.0.1:4200/api
    nohup prefect server start &
    sleep 5
    echo "Rodando flow com Prefect Server local..."
    python3 datasets/br_cenipa/flows.py

elif [ "$EXECUTION_MODE" == "cloud" ]; then
    echo "Rodando com Prefect Cloud..."
    python3 datasets/br_cenipa/flows.py
    
else
    echo "Modo de execução inválido: $EXECUTION_MODE"
    exit 1
fi