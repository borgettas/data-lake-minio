# Usa a imagem base do Airflow
FROM apache/airflow:2.8.4

# Instala as dependências do Data Pipeline
USER root

# Instala as dependências do sistema necessárias
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Instala as bibliotecas Python necessárias
USER airflow
RUN pip install --no-cache-dir \
    # Bibliotecas de Data Science e S3 (MinIO)
    "polars[parquet]" \
    "requests" \
    "boto3" \
    "pandas" \
    # Bibliotecas de Testes e Qualidade de Dados
    "pytest" \
    "pandera"