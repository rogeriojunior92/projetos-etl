import io
import os
import logging
import tempfile
import requests
import pandas as pd
from minio import Minio
from datetime import datetime
from dotenv import load_dotenv

from airflow import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

#=============================================================================
# Carrega as variáveis do arquivo .env
#=============================================================================
load_dotenv()

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
                    return temp_file_path
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
# Função para criação da tabela
#=============================================================================
def create_table():
    """
    Retorna a instrução SQL para criar a tabela 'raw_financial_sample' no PostgreSQL.

    Esta função cria a definição SQL necessária para criar a tabela no banco de dados 
    PostgreSQL, caso ela não exista. A tabela é utilizada para armazenar dados financeiros 
    de amostras.

    Returns:
        str: A instrução SQL que pode ser executada no PostgreSQL.
    """
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS datalake.raw_financial_sample (
            segment TEXT,
            country TEXT,
            product TEXT,
            discount_band TEXT,
            units_sold FLOAT,
            manufacturing_price FLOAT,
            sale_price FLOAT,
            gross_sales FLOAT,
            discounts FLOAT,
            sales FLOAT,
            cogs TEXT,
            profit FLOAT,
            date DATE,
            month_number INTEGER,
            month_name TEXT,
            year INTEGER
        )
    """
    return create_table_sql

#=============================================================================
# Função para carga dos dados no Postgres
#=============================================================================
def load_csv_to_postgres(bucket_name, object_name):
    """
    Carrega um arquivo CSV do MinIO e insere os dados no banco de dados PostgreSQL.

    Esta função recupera um arquivo CSV armazenado no MinIO, o carrega em um DataFrame do 
    Pandas, e insere os dados na tabela 'raw_financial_sample' no banco de dados PostgreSQL.

    Args:
        bucket_name (str): O nome do bucket do MinIO onde o arquivo está armazenado.
        object_name (str): O nome do objeto (arquivo) dentro do bucket do MinIO.

    Raises:
        Exception: Caso ocorra um erro durante a leitura do arquivo ou na inserção dos dados no PostgreSQL.
    """
    try:
        # Obter o arquivo do MinIO
        data = minio_client.get_object(bucket_name, object_name)
        # Carregar o conteúdo do arquivo para um DataFrame
        file_content = data.read()
        df_financial_sample = pd.read_excel(io.BytesIO(file_content))
        print(df_financial_sample)
        df_financial_sample.columns = df_financial_sample.columns.str.strip()
        
        logging.info(f"Carregando {len(df_financial_sample)} registros para o PostgreSQL.")

        # Conectar ao PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        for _, row in df_financial_sample.iterrows():
            cur.execute("""
                INSERT INTO datalake.raw_financial_sample (
                    segment, country, product, discount_band, units_sold, manufacturing_price, 
                    sale_price, gross_sales, discounts, sales, cogs, profit, 
                    date, month_number, month_name, year) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['Segment'], row['Country'], row['Product'], row['Discount Band'],
                row['Units Sold'], row['Manufacturing Price'], row['Sale Price'], row['Gross Sales'],
                row['Discounts'], row['Sales'], row['COGS'], row['Profit'],
                row['Date'], row['Month Number'], row['Month Name'], row['Year']
            ))

        conn.commit()
        cur.close()
        logging.info(f"Dados do arquivo {object_name} carregados com sucesso no PostgreSQL.")

    except Exception as e:
        logging.error(f"Erro ao carregar dados do CSV para o PostgreSQL: {e}")
        raise

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
    dag_id="dag_extract_raw_financial_sample",
    description="DAG RAW EXTRACT DATA FINANCIAL SAMPLE",
    default_args=default_args,
    schedule_interval=None,
    tags=["DAG", "RAW", "EXTRACT", "FINANCIAL", "SAMPLE", "DATA"]
) as dag:

    # Inicio da DAG
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

    # Tarefa para criar a tabela
    task_create_table = PostgresOperator(
        task_id="task_create_table",
        sql=create_table(),
        postgres_conn_id="postgres_default",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Tarefa para carregar os dados ao banco de dados Postgres
    task_load_data_to_postgres = PythonOperator(
        task_id="task_load_data_to_postgres",
        python_callable=load_csv_to_postgres,
        op_args=["raw", "financial_sample.xlsx"],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Finalizando a DAG
    task_finish = DummyOperator(
        task_id="task_finish",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Definição do fluxo da DAG
    task_start >> task_download_and_upload_to_minio >> task_create_table >> task_load_data_to_postgres >> task_finish