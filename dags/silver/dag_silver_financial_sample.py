import io
import os
import logging
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
            "Year": "year"
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
        if df_bronze_financial_sample.empty:
            logging.error("O DataFrame está vazio.")
            raise ValueError("DataFrame vazio")
                             
        # Criar um buffer de memória para salvar o arquivo Parquet
        parquet_buffer = io.BytesIO()
        if parquet_buffer.tell() == 0:
            logging.info("O buffer está vazio.")
        else:
            logging.error(f"O buffer contém {parquet_buffer.tell()} bytes.")

        # Enviar o arquivo Parquet para o MinIO
        df_bronze_financial_sample.to_parquet(parquet_buffer, index=False, engine="pyarrow")

        parquet_buffer.seek(0)

        minio_client.put_object("processed", "transformed_financial_sample", parquet_buffer, len(parquet_buffer.getvalue()))
        logging.info(f"Arquivo Parquet carregado com sucesso para o bucket 'processed' no MinIO.")

    except Exception as e:
        logging.error(f"Falha ao salvar o arquivo no MinIO: {e}")
        raise

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
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    task_clean_and_transform_bronze_data = PythonOperator(
        task_id="task_clean_and_transform_bronze_data",
        python_callable=clean_and_transform_bronze_data,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    task_save_transformed_data_to_silver_layer = PythonOperator(
        task_id="save_transformed_data_to_silver_layer",
        python_callable=save_transformed_data_to_silver_layer,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Fim da DAG
    task_finish=DummyOperator(
        task_id="task_finish",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Definição do fluxo da DAG
    task_start >> task_read_bronze_data_from_minio >> task_clean_and_transform_bronze_data >> task_save_transformed_data_to_silver_layer >> task_finish