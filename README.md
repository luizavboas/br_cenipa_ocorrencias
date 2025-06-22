# br_cenipa – Pipeline de Dados de Ocorrências Aeronáuticas do CENIPA

Este repositório contém um pipeline para extração, transformação e carga (ETL) de dados de ocorrências aeronáuticas do CENIPA, utilizando Prefect para orquestração. O pipeline suporta modos de extração via API ou webscraping e pode ser executado tanto em modo local quanto em modo servidor.

---

## Índice

- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Configuração](#configuração)
- [Execução](#execução)
  - [Modos de Extração](#modos-de-extração)
  - [Modos de Execução](#modos-de-execução)
- [Credenciais GCP para Upload](#credenciais-gcp-para-upload)
- [Uso com Docker & Containers](#uso-com-docker--containers)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Contribuindo](#contribuindo)
- [Licença](#licença)

---

## Pré-requisitos

- Python 3.10+
- [Poetry](https://python-poetry.org/) ou `pip`
- [Prefect](https://docs.prefect.io/)
- [Selenium](https://selenium-python.readthedocs.io/)
- [Google Cloud SDK](https://cloud.google.com/sdk) (para upload no GCP)
- Docker (para execução em container)
- Chrome/ChromeDriver (para modo webscraping)

---

## Instalação

```bash
git clone https://github.com/seu-usuario/br_cenipa.git
cd br_cenipa
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# ou, se preferir poetry
poetry install
```

---

## Configuração

Crie um arquivo `.env` na raiz do projeto (veja `.env.example`):

```env
EXECUTION_MODE=local         # Opções: local, server
EXTRACTION_MODE=API          # Opções: API, SCRAPE
API_KEY=sua_api_key_aqui
GCP_BUCKET=seu_bucket_gcp    # Necessário para upload
GOOGLE_APPLICATION_CREDENTIALS=/caminho/para/seu/service-account.json
```

**Não faça commit do seu `.env` ou credenciais no versionamento.**  
O `.env` já está incluído no `.gitignore` e `.dockerignore`.

---

## Execução

### Modos de Extração

- **API**: Baixa os dados diretamente da API pública do CENIPA.
- **SCRAPE**: Usa Selenium para baixar os dados via webscraping do site do CENIPA.

Defina `EXTRACTION_MODE` no seu `.env` conforme desejado.

### Modos de Execução

- **local**: Executa o pipeline sem orquestração Prefect.
- **server**: Executa o pipeline usando um servidor Prefect (requer um servidor Prefect rodando, veja abaixo).

Defina `EXECUTION_MODE` no seu `.env` conforme desejado.

### Executando Localmente

```bash
source .venv/bin/activate
python -m src.flows.main
```

Ou, usando Prefect:

```bash
prefect deployment run src.flows.main
```

---

## Credenciais GCP para Upload

Para habilitar o upload para um bucket Google Cloud Storage:

1. Crie uma service account no GCP com permissão `Storage Object Admin`.
2. Baixe a chave da service account como um arquivo JSON.
3. Defina o caminho para esse arquivo no seu `.env`:

   ```env
   GOOGLE_APPLICATION_CREDENTIALS=/caminho/absoluto/para/seu/service-account.json
   GCP_BUCKET=nome-do-seu-bucket
   ```

4. O pipeline fará upload automático dos arquivos processados para o GCS se essas variáveis estiverem configuradas.

---

## Uso com Docker & Containers

### Build da imagem

```bash
docker build -t br_cenipa .
```

### Executando o container

```bash
docker run --env-file .env br_cenipa
```

- O arquivo `.env` **não** é incluído na imagem (veja `.dockerignore`).
- O container usará as variáveis de ambiente do `.env` em tempo de execução.

### Usando Prefect Server com Docker Compose

Se quiser usar o Prefect Server (`EXECUTION_MODE=server`), você deve rodar o servidor Prefect em um container ou serviço separado.  
**Exemplo de `docker-compose.yml`:**

```yaml
version: "3.8"
services:
  prefect-server:
    image: prefecthq/prefect:2-latest
    command: prefect server start
    ports:
      - "4200:4200"
  br_cenipa:
    build: .
    env_file: .env
    depends_on:
      - prefect-server
```

- Ajuste o `PREFECT_API_URL` no seu `.env` se necessário (ex: `http://prefect-server:4200/api`).
- O pipeline aguardará o Prefect Server estar disponível antes de rodar em modo servidor.

---

## Estrutura do Projeto

```
br_cenipa/
│
├── input/                      # Dados brutos
├── output/                     # Dados processados
├── src/                        # Código fonte principal
│   ├── __init__.py
│   ├── constants.py
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
├── .dockerignore
├── config.py
├── Dockerfile
├── README.md
├── requirements.txt
├── poetry.lock / pyproject.toml
└── startup.sh
```