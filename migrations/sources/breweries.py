import requests
import json
import logging
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError
from io import BytesIO
from typing import Dict, Any, List
from datetime import datetime
import os

# Configurar logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

def bronze_layer_ingestion(
    execution_date: str,
    raw_data_url: str,
    bucket_name: str = "bronze-layer",
    endpoint_url: str = None
) -> str:
    """
    Extrai dados brutos da API para a Bronze Layer (MinIO), usando a data de execução
    para particionamento.

    Args:
        execution_date (str): Data de execução para particionamento (ex.: '2025-10-06').
        raw_data_url (str): URL da API para extrair dados.
        bucket_name (str): Nome do bucket no MinIO (default: 'bronze-layer').
        endpoint_url (str): URL do endpoint MinIO (default: None, tenta variável de ambiente).

    Returns:
        str: A chave S3 (caminho) onde o arquivo foi salvo.

    Raises:
        Exception: Se houver erro na extração ou upload.
    """
    # Determinar o endpoint (prioridade: parâmetro > env var > default Docker)
    if endpoint_url is None:
        endpoint_url = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")  # Use 'minio:9000' para Docker
    log.debug(f"Iniciando ingestão para bucket: {bucket_name}, endpoint: {endpoint_url}")

    try:
        # 1. Testar conectividade com o MinIO
        log.debug("Testando conectividade com o endpoint MinIO...")
        response = requests.get(f"{endpoint_url}/minio/health/live")
        if response.status_code != 200:
            raise Exception(f"MinIO não está acessível em {endpoint_url}. Status: {response.status_code}")
        log.debug("MinIO está acessível!")

        # 2. Extrair dados da API
        log.debug(f"Extraindo dados de {raw_data_url}")
        response = requests.get(raw_data_url)
        response.raise_for_status()
        raw_data = response.json()
        log.info(f"Sucesso na extração. Encontrados '{len(raw_data)}' registros.")

        # 3. Converte para bytes
        file_bytes = json.dumps(raw_data, indent=2).encode('utf-8')
        data_stream = BytesIO(file_bytes)

        # 4. Define o caminho no MinIO
        s3_key = f"breweries/dt={execution_date}/breweries.json"
        log.debug(f"Chave S3: {s3_key}")

        # 5. Configura o cliente boto3 para MinIO
        log.debug("Configurando cliente boto3...")
        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "minioadmin"),
            aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
        )

        # 6. Faz o upload para o MinIO
        log.debug(f"Fazendo upload para s3://{bucket_name}/{s3_key}")
        s3_client.upload_fileobj(
            Fileobj=data_stream,
            Bucket=bucket_name,
            Key=s3_key,
            ExtraArgs={"ContentType": "application/json"}
        )
        log.info(f"Dados brutos enviados com sucesso para s3://{bucket_name}/{s3_key}")

        # 7. Verifica se o arquivo é acessível publicamente
        public_url = f"{endpoint_url}/{bucket_name}/{s3_key}"
        log.debug(f"Testando acesso público em: {public_url}")
        response = requests.get(public_url)
        if response.status_code == 200:
            log.info(f"Arquivo acessível publicamente em: {public_url}")
        else:
            log.warning(f"Falha ao acessar o arquivo publicamente: {response.status_code}")

        return s3_key

    except requests.exceptions.RequestException as e:
        log.error(f"Erro ao extrair dados da API: {e}")
        raise
    except EndpointConnectionError as e:
        log.error(f"Erro ao conectar ao endpoint MinIO {endpoint_url}: {e}")
        raise
    except ClientError as e:
        log.error(f"Erro ao fazer upload para o MinIO: {e}")
        raise
    except Exception as e:
        log.error(f"Erro no processo de ingestão: {e}")
        raise