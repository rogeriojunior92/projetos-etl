import os
import logging
import tempfile
import requests
from minio import Minio
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

#=============================================================================
# Configuração do MinIO
#=============================================================================
minio_client = Minio(
    #"projetos-etl-minio-1:9001",
    "minio:9000", # Endereço do servidor MinIO
    access_key=Variable.get("access_key_minio"),
    secret_key=Variable.get("secret_key_minio"),
    secure=False,
    region="us-east-1"
)
logging.info(f"Acessando o MinIO com access_key={Variable.get('access_key_minio')} e secret_key={Variable.get('secret_key_minio')}")

#=============================================================================
# Função para baixar o CSV e fazer o upload para o MinIO
#=============================================================================
def download_and_upload_to_minio(base_url, bucket_name, object_name):
    """
    Baixa um arquivo de uma URL e realiza o upload para o MinIO em um bucket específico.

    Args:
        base_url (str): URL do arquivo a ser baixado.
        bucket_name (str): Nome do bucket do MinIO onde o arquivo será armazenado.
        object_name (str): Nome do arquivo (objeto) no bucket do MinIO.

    Exceções:
        Se ocorrer um erro durante o download, a criação do bucket ou o upload do arquivo, ele será registrado.
    """
    try:
        # Baixa o arquivo da URL
        resposta = requests.get(base_url, timeout=60)
        # Verifica se o bucket já existe
        if resposta.status_code == 200:
            if not minio_client.bucket_exists(bucket_name):
                logging.info(f"Bucket {bucket_name} não encontrado, criando...")
                try:
                    minio_client.make_bucket(bucket_name)
                    logging.info(f"Bucket {bucket_name} criado com sucesso.")
                except Exception as e:
                    logging.error(f"Erro ao criar o bucket {bucket_name}: {e}")
                    raise
            else:
                logging.info(f"Bucket {bucket_name} já existe.")

            # Criando arquivo temporário para armazenar o arquivo baixado
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(resposta.content)
                temp_file_path = temp_file.name
                logging.info(f"Arquivo temporário criado com tamanho: {len(resposta.content)} bytes")
                print(f"Arquivo temporário salvo em: {temp_file_path}")

                try:
                    # Tenta fazer o upload para o MinIO
                    minio_client.fput_object(bucket_name, object_name, temp_file_path)
                    logging.info(f"Arquivo {object_name} enviado com sucesso para o bucket {bucket_name}.")
                except Exception as upload_exception:
                    logging.error(f"Erro ao fazer o upload do arquivo para o MinIO: {upload_exception}")
                    raise
                finally:
                    # Remover o arquivo temporário após o upload
                    os.remove(temp_file_path)

        else:
            logging.error(f"Falha ao baixar o arquivo, status code: {resposta.status_code}")
    except requests.exceptions.Timeout:
        logging.error("Erro: O tempo de conexão com o Google Drive expirou.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro na requisição HTTP: {e}")
    except Exception as e:
        logging.error(f"Erro ao fazer o download ou upload: {e}")

#=============================================================================
# Default Arguments
#=============================================================================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 13),
    'retries': 1,
}

#=============================================================================
# DAG Settings
#=============================================================================
with DAG(
    dag_id="dag_extract_people_raw_data",
    description="DAG Extract Financial Sample",
    default_args=default_args,
    schedule_interval=None,
    tags=["DAG", "RAW", "EXTRACT", "FINANCIAL", "DATA"]
) as dag:
    """
    Definindo a estrutura da DAG no Apache Airflow para extrair e carregar dados.
    A DAG consiste em 3 etapas principais: início, download/upload e finalização.

    Argumentos:
        dag_id (str): O identificador único da DAG.
        description (str): A descrição do objetivo da DAG.
        default_args (dict): Argumentos padrão, como o dono e data de início.
        schedule_interval (str): Intervalo de agendamento da DAG (None significa execução manual).
        tags (list): Tags que categorizam a DAG.
    """

    # INICIO DA DAG
    task_start = DummyOperator(
        task_id="task_start",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Definindo a tarefa de baixar e fazer upload do CSV
    task_download_and_upload_to_minio = PythonOperator(
        task_id='download_and_upload_to_minio',
        python_callable=download_and_upload_to_minio,
        op_args=["https://go.microsoft.com/fwlink/?LinkID=521962", "raw", "financial_sample.xlsx"],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Finalizando a DAG
    task_finish = DummyOperator(
        task_id="task_finish",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Definição do fluxo da DAG
    task_start >> task_download_and_upload_to_minio >> task_finish
