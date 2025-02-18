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

"""
Passo a passo
1. Ler os dados da camada raw 
2. Limpeza e transformação dos dados
3. Salvar dataframe transformado na camada bronze
4. Criar a tabela
5. Inserir registros na tabela
"""

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
def read_bronze_data_from_minio(bucket_name, object_name):
    
    try:
        # Obter o arquivo do MinIO
        data = minio_client.get_object(bucket_name, object_name)

        file_content = data.read()
        df_raw_financial_sample = pd.read_excel(io.BytesIO(file_content))
        print(df_raw_financial_sample.dtypes)
        return df_raw_financial_sample

    except Exception as e:
        logging.error(f"Falha na leitura do arquivo: {e}")

#=============================================================================
# Função para transformar os dados
#=============================================================================
def clean_and_transform_bronze_data(df_raw_financial_sample):
    """
    Função para limpar e transformar os dados financeiros brutos.
    Args:
        df_raw_financial_sample (pd.DataFrame): Dados financeiros brutos a serem transformados.
    Returns:
        pd.DataFrame: Dados transformados prontos para serem carregados.
    """
    try:

        # Limpando e transformando os dados
        df_raw_financial_sample["Manufacturing Price"] = df_raw_financial_sample["Manufacturing Price"].astype(str).str.replace('R\$', '').str.replace(',', '').astype(float)
        df_raw_financial_sample["Sale Price"] = df_raw_financial_sample["Sale Price"].astype(str).str.replace('R\$', '').str.replace(',', '').astype(float)
        df_raw_financial_sample["Gross Sales"] = df_raw_financial_sample["Gross Sales"].astype(str).str.replace('R\$', '').str.replace(',', '').astype(float)
        df_raw_financial_sample["Discounts"] = df_raw_financial_sample["Discounts"].astype(str).str.replace('R\$', '').str.replace(',', '').astype(float)
        df_raw_financial_sample["Sales"] = df_raw_financial_sample["Sales"].astype(str).str.replace('R\$', '').str.replace(',', '').astype(float)
        df_raw_financial_sample["COGS"] = df_raw_financial_sample["COGS"].astype(str).str.replace('R\$', '').str.replace(',', '').astype(float)
        df_raw_financial_sample["Profit"] = df_raw_financial_sample["Profit"].astype(str).str.replace('R\$', '').str.replace(',', '').astype(float)
        # df_raw_financial_sample = df_raw_financial_sample.rename(columns=lambda x: x.strip()) # Renomear as colunas removendo espaços extras

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
            "Year": "year"
        }
        df_raw_financial_sample.rename(columns=rename_columns, inplace=True)
        
        logging.info("Transformação dos dados concluída com sucesso.")
        return df_raw_financial_sample

    except Exception as e:
        logging.error(f"Falha durante a transformação dos dados: {e}")
        raise 
        
#=============================================================================
# Função para salvar dataframe na camada bronze
#=============================================================================
def save_transformed_data_to_silver_layer():
    pass


#=============================================================================
# Função para criar tabela
#=============================================================================
def create_silver_table():
    pass

#=============================================================================
# Função para inserir registros
#=============================================================================
def insert_transformed_data_into_postgres():
    pass

#=============================================================================
# Default Arguments
#=============================================================================
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 18),
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
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    task_clean_and_transform_bronze_data = PythonOperator(
        task_id="task_clean_and_transform_bronze_data",
        python_callable=clean_and_transform_bronze_data,
        op_args=["{{ task_instance.xcom_pull(task_ids='task_read_bronze_data_from_minio') }}"],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Fim da DAG
    task_finish=DummyOperator(
        task_id="task_finish",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Definição do fluxo da DAG
    task_start >> task_read_bronze_data_from_minio >> task_clean_and_transform_bronze_data >> task_finish