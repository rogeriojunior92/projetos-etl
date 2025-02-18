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
    acess_key=Variable.get("access_key_minio"),
    secret_key=Variable.get("secret_key_minio"),
    secure=False,
    region="us-east-1"
)
logging.info(f"Acessando o MinIO com access_key={Variable.get('access_key_minio')} e secret_key={Variable.get('secret_key_minio')}")