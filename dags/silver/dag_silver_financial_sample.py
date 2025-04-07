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
# Função para ler os dados RAW
#=============================================================================
def read_bronze_data_from_minio(bucket_name, object_name, **kwargs):

    try:
        # Obter o arquivo do MinIO
        data = minio_client.get_object(bucket_name, object_name)
        file_content = data.read()

        # Ler os dados como DataFrame
        df_bronze_financial_sample = pd.read_excel(io.BytesIO(file_content))

        # Serializar o DataFrame como JSON para passar via XCom
        df_json = df_bronze_financial_sample.to_json(orient='split')

        # Passar o DataFrame serializado via XCom
        kwargs["ti"].xcom_push(key="bronze_data", value=df_json)

        return df_bronze_financial_sample

    except Exception as e:
        logging.error(f"Falha na leitura do arquivo: {e}")
        raise

#=============================================================================
# Função para transformar os dados
#=============================================================================
def clean_and_transform_bronze_data(**kwargs):

    try:
        # Obter o DataFrame serializado do XCom
        ti = kwargs['ti']
        df_json = ti.xcom_pull(task_ids='task_read_bronze_data_from_minio', key='bronze_data')
        
        # Deserializar o DataFrame de volta de JSON
        df_bronze_financial_sample = pd.read_json(df_json, orient='split')

        # Remover espaços dos nomes das colunas
        df_bronze_financial_sample.columns = df_bronze_financial_sample.columns.str.strip()

        df_bronze_financial_sample["ingestion_date"] = pd.to_datetime("now")

        # Garantir que as colunas sejam do tipo string antes de usar .str
        df_bronze_financial_sample["Manufacturing Price"] = df_bronze_financial_sample["Manufacturing Price"].astype(str).str.replace('$', '').str.replace(',', '')
        df_bronze_financial_sample["Sale Price"] = df_bronze_financial_sample["Sale Price"].astype(str).str.replace('R$', '').str.replace(',', '').astype(float)
        df_bronze_financial_sample["Gross Sales"] = df_bronze_financial_sample["Gross Sales"].astype(str).str.replace('R$', '').str.replace(',', '').astype(float)
        df_bronze_financial_sample["Discounts"] = df_bronze_financial_sample["Discounts"].astype(str).str.replace('R$', '').str.replace(',', '').astype(float)
        df_bronze_financial_sample["Sales"] = df_bronze_financial_sample["Sales"].astype(str).str.replace('R$', '').str.replace(',', '').astype(float)
        df_bronze_financial_sample["COGS"] = df_bronze_financial_sample["COGS"].astype(str).str.replace('R$', '').str.replace(',', '').astype(float)
        df_bronze_financial_sample["Profit"] = df_bronze_financial_sample["Profit"].astype(str).str.replace('R$', '').str.replace(',', '').astype(float)

        # Renomeando as colunas
        rename_columns = {
            "Segment": "segment",
            "Country": "country",
            "Product": "product",
            "Discount Band": "discount_band",
            "Units Sold": "units_sold",
            "Manufacturing Price": "manufacturing_price",
            "Sale Price": "sale_price",
            "Gross Sales": "gross_sale",
            "Discounts": "discounts",
            "Sales": "sales",
            "COGS": "gocs",
            "Profit": "profit",
            "Date": "date",
            "Month Number": "month_number",
            "Month Name": "month_name",
            "Year": "year",
            "ingestion_date": "ingestion_date"
        }
        df_bronze_financial_sample.rename(columns=rename_columns, inplace=True)

        # Serializar novamente para JSON
        df_json_transformed = df_bronze_financial_sample.to_json(orient='split')

        # Passar os dados transformados para o XCom
        ti.xcom_push(key="transformed_data", value=df_json_transformed)

        # Retornar o DataFrame transformado
        return df_bronze_financial_sample

    except Exception as e:
        logging.error(f"Falha durante a transformação dos dados: {e}")
        raise

#=============================================================================
# Função para salvar dataframe na camada bronze
#=============================================================================
def save_transformed_data_to_silver_layer(**kwargs):
    
    try:
        # Recuperar o DataFrame serializado do XCom
        ti = kwargs["ti"]
        df_json = ti.xcom_pull(task_ids="task_clean_and_transform_bronze_data", key="transformed_data")

        # Deserializar o JSON de volta para um DataFrame
        df_bronze_financial_sample = pd.read_json(df_json, orient="split")

        # Verificar se o DataFrame está vazio
        if df_bronze_financial_sample.empty:
            logging.error("O DataFrame está vazio.")
            raise ValueError("DataFrame vazio")

        # Criar um buffer de memória para salvar o arquivo Parquet
        parquet_buffer = io.BytesIO()

        # Enviar o DataFrame para o buffer Parquet
        df_bronze_financial_sample.to_parquet(parquet_buffer, index=False, engine="pyarrow")

        # Voltar ao início do buffer para ler os dados
        parquet_buffer.seek(0)

        # Verificar o tamanho do buffer
        buffer_size = parquet_buffer.tell()
        logging.info(f"O buffer contém {buffer_size} bytes.")

        # Enviar o arquivo Parquet para o MinIO
        minio_client.put_object("processed", "transformed_financial_sample", parquet_buffer, buffer_size)
        logging.info(f"Arquivo Parquet carregado com sucesso para o bucket 'processed' no MinIO.")

    except Exception as e:
        logging.error(f"Falha ao salvar o arquivo no MinIO: {e}")
        raise

#=============================================================================
# Função para criar tabela
#=============================================================================
def create_silver_table():
    """
    Retorna a instrução SQL para criar a tabela 'processed_financial_sample' no PostgreSQL.

    Esta função cria a definição SQL necessária para criar a tabela no banco de dados 
    PostgreSQL, caso ela não exista. A tabela é utilizada para armazenar dados financeiros 
    de amostras.

    Returns:
        str: A instrução SQL que pode ser executada no PostgreSQL.
    """ 
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS datalake.processed_financial_sample (
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
            year INTEGER,
            ingestion_date DATE
        )
    """
    return create_table_sql

#=============================================================================
# Função para inserir registros
#=============================================================================
# def load_transformed_data_into_postgres(bucket_name, object_name):

#     try:
#         # Obter o arquivo do MinIO
#         data = minio_client.get_object(bucket_name, object_name)
#         # Carregar o conteúdo do arquivo para um DataFrame
#         file_content = data.read()
#         df_financial_sample = pd.read_parquet(io.BytesIO(file_content))
#         print(df_financial_sample)
#         df_financial_sample.columns = df_financial_sample.columns.str.strip()

#         logging.info(f"Carregando {len(df_financial_sample)} registros para o PostgreSQL.")

#         # Conectar ao PostgreSQL
#         postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
#         conn = postgres_hook.get_conn()
#         cur = conn.cursor()

#         for _, row in df_financial_sample.iterrows():
#             cur.execute("""
#                 INSERT INTO datalake.processed_financial_sample (
#                     segment, country, product, discount_band, units_sold, manufacturing_price, 
#                     sale_price, gross_sales, discounts, sales, cogs, profit, 
#                     date, month_number, month_name, year, ingestion_date) 
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#             """, (
#                 row['Segment'], row['Country'], row['Product'], row['Discount Band'],
#                 row['Units Sold'], row['Manufacturing Price'], row['Sale Price'], row['Gross Sales'],
#                 row['Discounts'], row['Sales'], row['COGS'], row['Profit'],
#                 row['Date'], row['Month Number'], row['Month Name'], row['Year'], row["ingestion_date"]
#             ))
        
#         conn.commit()
#         cur.close()
#         logging.info(f"Dados do arquivo {object_name} carregados com sucesso no PostgreSQL.")
    
#     except Exception as e:
#         logging.error(f"Erro ao carregar dados do CSV para o PostgreSQL: {e}")
#         raise

#=============================================================================
# Default Arguments
#=============================================================================
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 21),
    "retries": 1,
}

#=============================================================================
# DAG Settings
#=============================================================================
with DAG(
    dag_id="dag_silver_financial_sample",
    description="DAG SILVER FINANCIAL SAMPLE",
    default_args=default_args,
    schedule_interval=None,
    tags=["DAG", "SILVER", "TRANSFORM", "FINANCIAL", "SAMPLE"]
) as dag:
    
    # Inicio da DAG
    task_start=DummyOperator(
        task_id="task_start",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Tarefa para leitura na camada bronze
    task_read_bronze_data_from_minio = PythonOperator(
        task_id="task_read_bronze_data_from_minio",
        python_callable=read_bronze_data_from_minio,
        op_args=["raw", "financial_sample.xlsx"],
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Tarefa que efetua a transformação dos dados
    task_clean_and_transform_bronze_data = PythonOperator(
        task_id="task_clean_and_transform_bronze_data",
        python_callable=clean_and_transform_bronze_data,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Tarefa para salvar os dados transformado na camada Silver
    task_save_transformed_data_to_silver_layer = PythonOperator(
        task_id="save_transformed_data_to_silver_layer",
        python_callable=save_transformed_data_to_silver_layer,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Tarefa para criar a tabela
    task_create_table = PostgresOperator(
        task_id="task_create_table",
        sql=create_silver_table(),
        postgres_conn_id="postgres_default",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # # Tarefa para carregar os dados ao banco de dados Postgres
    # task_load_data_to_postgres = PostgresOperator(
    #     task_id="task_load_data_to_postgres",
    #     python_callable=load_transformed_data_into_postgres,
    #     op_args=["processed", "transformed_financial_sample"],
    #     trigger_rule=TriggerRule.ALL_SUCCESS
    # )

    # Fim da DAG
    task_finish=DummyOperator(
        task_id="task_finish",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Definição do fluxo da DAG
    task_start >> task_read_bronze_data_from_minio >> task_clean_and_transform_bronze_data >> task_save_transformed_data_to_silver_layer >> task_create_table >> task_finish