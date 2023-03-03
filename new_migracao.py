import pandas as pd
import sqlite3
import os
import time
from datetime import datetime
import logging
import sys
from utilitarios.backup_limpeza import backup_simples
from utilitarios.validacoes import existe_dir
from sqlalchemy import create_engine

# gerando log
logging.basicConfig(level=logging.INFO, filename="./logs/migracao.log", encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")
logging.basicConfig(level=logging.ERROR, filename="./logs/migracao.log", encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")
logging.basicConfig(level=logging.DEBUG, filename="./logs/migracao.log")

# Uso do logger
logging.debug('Debug message')
logging.info('Info message')
logging.warning('Warning message')
logging.error('Error message')
logging.critical('Critical message')

# Definindo data atual e gerando o backup
datazip = f'{datetime.now().year}-{datetime.now().month}'

# verificando e gerando o backup dos dados. 
database = r'./database/'



all_files_database = list(filter(lambda x: '.db' in x, os.listdir(database)))

if len(all_files_database) >= 1:
    backup_simples(diretorio_origem=database, nome_zipado=f"backup_database_{datazip}.zip", extensao='.db', diretorio_destino=f"{database}backup/")

agora = datetime.now()
print(f"Início do processo: {agora.date()} às {agora.time()}")
logging.info(f"Início do processo: {agora.date()} às {agora.time()}")

# Define os diretórios
br_diretorio = r'./br_base/'
en_diretorio = r'./en_base/'

# Verifica se existe o diretorios
for diretorio in [database, br_diretorio, en_diretorio]:
    existe_dir(diretorio)

try :
    sqlite3.connect("./database/br_base_cnpj.db")
    sqlite3.connect("./database/en_base_cnpj.db")
    engine = create_engine("sqlite:///database/br_base_cnpj.db", connect_args={'sqlite3_max_variables': 10000})
    engine = create_engine("sqlite:///database/en_base_cnpj.db", connect_args={'sqlite3_max_variables': 10000})

except sqlite3.Error as e:
    print(f"Erro ao conectar com o banco de dados: {e}")
    logging.error(f"Erro ao conectar com o banco de dados: {e}")
    sys.exit(1)

# Cria uma conexão com os SQLites
with sqlite3.connect("./database/br_base_cnpj.db") as br_conn, sqlite3.connect("./database/en_base_cnpj.db") as en_conn:
    # Juntar todos os arquivos CSV em um único dataframe para cada país
    br_dados = pd.concat([pd.read_parquet(os.path.join(br_diretorio, f)) for f in os.listdir(br_diretorio) if f.endswith('.gzip')])
    en_data  = pd.concat([pd.read_parquet(os.path.join(en_diretorio, f)) for f in os.listdir(en_diretorio) if f.endswith('.gzip')])
    br_dados['NOME_FANTASIA'].replace('--empty--','', regex=True, inplace=True)
    en_data['TRADING NAME'].replace('--empty--','', regex=True, inplace=True)

    # Define a chave primária da tabela
    br_primary_key = ['CNPJ']
    en_primary_key = ['EIN (CNPJ)']

    """
    # Salva o dataframe como uma tabela no SQLite e atualiza as linhas existentes
    br_dados.to_sql('estabelecimentos', br_conn, if_exists='append', index=False, method='multi',
                    chunksize=1000, index_label=br_primary_key)
    en_data.to_sql('establishments', en_conn, if_exists='append', index=False, method='multi',
                    chunksize=1000, index_label=en_primary_key)
    """
    
    # Atualiza as colunas "Situação Cadastral" e "Data Situação Cadastral" se as linhas já existem
    for _, row in br_dados.iterrows():
        # Verifica se a linha existe
        result = br_conn.execute(f"SELECT CNPJ, SITUACAO_CADASTRAL, DATA_SITUACAO_CADASTRAL FROM estabelecimentos WHERE CNPJ='{row['CNPJ']}';")    
        if result.fetchone() is not None:
            # linha já existe, fazer update
            if result.SITUACAO_CADASTRAL not in [2, 3, 4]:
                br_conn.execute(f"""UPDATE estabelecimentos 
                SET 
                    SITUACAO_CADASTRAL = '{row['SITUACAO_CADASTRAL']}', 
                    DATA_SITUACAO_CADASTRAL = '{row['DATA_SITUACAO_CADASTRAL']}'
                WHERE CNPJ='{row["CNPJ"]}';""")
        else:
            # linha não existe, fazer insert
            br_conn.execute(f"""
            INSERT INTO estabelecimentos 
            ('CNPJ', 'RAZAO_SOCIAL', 'NOME_FANTASIA', 'RUA', 'NUMERO', 'COMPLEMENTO',
                'BAIRRO', 'CIDADE', 'ESTADO', 'CEP','LATITUDE', 'LONGITUDE', 'TELEFONE1', 'SITE',
                'CNAE_DESCRICAO', 'HORARIO_FUNCIONAMENTO', 'INSTAGRAM', 'FACEBOOK', 
                'OPCOES_DE_SERVICO','SITUACAO_CADASTRAL', 'DATA_SITUACAO_CADASTRAL') 
            VALUES 
            ('{row["CNPJ"]}', '{row["RAZAO_SOCIAL"]}', '{row["NOME_FANTASIA"]}, '{row["RUA"]}',
            '{row["NUMERO"]}','{row["COMPLEMENTO"]}', '{row["COMPLEMENTO"]}', '{row["BAIRRO"]}',
            
            );""")
        # Verificando erros 

        try:

            br_conn.executemany(f"""
                UPDATE estabelecimentos
                SET "SITUACAO_CADASTRAL" = ?, "DATA_SITUACAO_CADASTRAL" = ?
                WHERE CNPJ = ? AND (
                    "SITUACAO_CADASTRAL" <> ? OR
                    "DATA_SITUACAO_CADASTRAL" <> ?
                )
            """, [(row["SITUACAO_CADASTRAL"], row["DATA_SITUACAO_CADASTRAL"], row["CNPJ"],
                row["SITUACAO_CADASTRAL"], row["DATA_SITUACAO_CADASTRAL"]) for _, row in br_dados.iterrows()])
        
        except sqlite3.Error as e:
            print(f"Erro ao executar a query: {e}")
            logging.error(f"Erro ao executar a query: {e}")
            sys.exit(1)

        try:    
            en_conn.executemany(f"""
                    UPDATE establishments
                    SET "REGISTRATION_SITUATION" = ?, "DATE_REGISTRATION_SITUATION" = ?
                    WHERE "EIN (CNPJ)" = ? AND (
                        "REGISTRATION_SITUATION" <> ? OR
                        "DATE_REGISTRATION_SITUATION" <> ?
                    )
                """, [(row["REGISTRATION_SITUATION"], row["DATE_REGISTRATION_SITUATION"], row["EIN (CNPJ)"],
                    row["REGISTRATION_SITUATION"], row["DATE_REGISTRATION_SITUATION"]) for _, row in en_data.iterrows()])
        except sqlite3.Error as e:
            print(f"Erro ao executar a query: {e}")
            logging.error(f"Erro ao executar a query: {e}")
            sys.exit(1)

        # Executa o comando VACUUM para compactar o banco de dados
        br_conn.execute("VACUUM")
        en_conn.execute("VACUUM")

        # Fecha a conexão com o banco de dados
        br_conn.close()
        en_conn.close()

agora = datetime.now()
tempo_execucao = time.process_time()
print(f"Fim do processo: {agora.date()} às {agora.time()}  tempo de execução {tempo_execucao}!")
logging.info(f"Fim do processo: {agora.date()} às {agora.time()}  tempo de execução {tempo_execucao}!")