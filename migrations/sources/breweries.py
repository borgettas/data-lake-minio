# airflow/scripts/extract_data.py

import json
import logging
import requests
import pandas as pd
from datetime import datetime
from io import BytesIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Configuração
logging.basicConfig(level=logging.INFO)
MINIO_CONN_ID = "s3_default" # Nome da conexao S3 definida no docker-compose.yml
BUCKET_NAME = "bronze-layer"
RAW_DATA_URL = "https://api.openbrewerydb.org/v1/breweries"


def extract_and_upload_to_bronze():
    """
    Extrai dados brutos de uma API externa e faz o upload
    para a Bronze Layer (MinIO).
    """
    logging.info("Iniciando a extração de dados brutos.")
    
    try:
        # 1. Extração dos Dados da Fonte (API)
        response = requests.get(RAW_DATA_URL)
        response.raise_for_status() # Lanca erro para status 4xx/5xx
        raw_data = response.json()
        
        logging.info(f"Dados extraídos com sucesso. Total de {len(raw_data)} registros.")

        # 2. Converte os dados para o formato que o S3Hook aceita (JSON em um buffer)
        file_content = json.dumps(raw_data, indent=2)
        
        # 3. Define o caminho no MinIO (Chave)
        # O dado é salvo com carimbo de data/hora para garantir idempotência e rastreabilidade
        current_date_str = datetime.now().strftime("%Y/%m/%d")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        s3_key = f"raw_data/beer_data/{current_date_str}/raw_beer_data_{timestamp}.json"

        # 4. Upload para o MinIO (Bronze Layer)
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        
        # Usamos o 'replace=True' para garantir que o arquivo seja sobrescrito se a chave for a mesma
        s3_hook.load_string(
            string_data=file_content,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True,
            encoding='utf-8'
        )

        logging.info(f"Dados brutos enviados com sucesso para s3://{BUCKET_NAME}/{s3_key}")
        
        # Retorna a chave do S3 para ser usada pelo proximo passo (Silver)
        return s3_key

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro na extração da API: {e}")
        raise
    except Exception as e:
        logging.error(f"Erro no upload para o MinIO: {e}")
        raise


if __name__ == "__main__":
    extract_and_upload_to_bronze()