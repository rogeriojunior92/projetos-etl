import os
import logging
import sqlite3
from dotenv import load_dotenv

#=============================================================================
# Carrega as variáveis do arquivo .env
#=============================================================================
load_dotenv()

#=============================================================================
# Configuração do logging
logging.basicConfig(level=logging.INFO)
#=============================================================================

#=============================================================================
# Função para criar tabela SQLite
#=============================================================================
def criar_tabela(cursor):
    try:
        cursor.execute("DROP TABLE IF EXISTS raw_financial_sample;")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_financial_sample (
                Segment TEXT,
                Country TEXT,
                Product TEXT,
                Discount_Band FLOAT,
                Manufacturing_Price_Sold FLOAT,
                Sale_Price FLOAT,
                Gross_Sales FLOAT,
                Discounts FLOAT,
                Sales FLOAT,
                COGS FLOAT,
                Profit FLOAT,
                Date DATE,
                Month_Number INTEGER,
                Year INTEGER
            )
        """)
        logging.info('Tabela criada com sucesso.')
    except sqlite3.Error as e:
        logging.error(f'Erro ao criar a tabela: {e}')

#=============================================================================
# Função principal que executa todo o processo.
#=============================================================================
def main():
    path_db = os.getenv("PATH_DB")
    if path_db:
        try:
            conn = sqlite3.connect(path_db)
            cursor = conn.cursor()
            criar_tabela(cursor)
            conn.commit()  # Salva as alterações
        except sqlite3.Error as e:
            logging.error(f'Erro ao conectar ao banco de dados: {e}')
        finally:
            if conn:
                conn.close()  # Garante que a conexão seja fechada
    else:
        logging.error('Caminho do banco de dados não encontrado.')

if __name__ == "__main__":
    main()