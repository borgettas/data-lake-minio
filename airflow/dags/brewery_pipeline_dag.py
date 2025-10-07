from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.python import PythonOperator

from migrations.sources.breweries import bronze_layer_ingestion 

# --- VARIÁVEIS DE CONFIGURAÇÃO DO PIPELINE ---
RAW_DATA_URL = "https://api.openbrewerydb.org/v1/breweries" 
MINIO_CONN_ID = "minio_s3" 
BUCKET_NAME = "bronze-layer" 
# ---------------------------------------------

default_args = {
    'owner': 'bees',
    'start_date': datetime(2025, 1, 1),
    'retries': 3, 
}

@dag(
    dag_id='breweries_ingestion',
    default_args=default_args,
    schedule=None, 
    catchup=False,
    tags=['brewery',]
)
def brewery_pipeline():
    
    # Task 1: Camada BRONZE (Extração e Carregamento)
    task_bronze = PythonOperator(
        task_id='bronze_layer_ingestion',
        python_callable=bronze_layer_ingestion,
        op_kwargs={
            # Macro do Airflow: data de execução (YYYY-MM-DD)
            'execution_date': '{{ ds }}', 
            'raw_data_url': RAW_DATA_URL,
            'bucket_name': BUCKET_NAME
        },
    )
    
    # Próximas Tasks (Silver, Gold) virão aqui
    # task_bronze >> task_silver >> task_gold


# Inicializa o DAG
pipeline_dag = brewery_pipeline()