import pandas as pd
import sqlite3
import os
import time
from datetime import datetime
import logging
from utilitarios.backup_limpeza import backup_simples

# gerando log
logging.basicConfig(level=logging.INFO, filename="./logs/migracao.log", encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")

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
# Cria uma conexão com os SQLites
br_conn = sqlite3.connect("./database/br_base_cnpj.db")
en_conn = sqlite3.connect("./database/en_base_cnpj.db")

# Define os diretórios
br_diretorio = r'./br_base/'
en_diretorio = r'./en_base/'

# Lista todos os arquivos CSV nos diretórios
br_arquivos = [f for f in os.listdir(br_diretorio) if f.endswith('.csv')]
en_arquivos = [f for f in os.listdir(en_diretorio) if f.endswith('.csv')]

# Cria uma conexão com os SQLites
with sqlite3.connect("./database/br_base_cnpj.db") as br_conn:
    # Loop por todos os br_arquivos
    for br_arquivo in br_arquivos:
        # Lê o arquivo para um dataframe
        dados = pd.read_csv(f'{br_diretorio}{br_arquivo}', sep=';', low_memory=False)
        dados['NOME_FANTASIA'].replace('--empty--','', regex=True, inplace=True)
        
        # Define a chave primária da tabela
        primary_key = ['CNPJ']

        # Salva o dataframe como uma tabela no SQLite e atualiza as linhas existentes
        dados.to_sql('estabelecimentos', br_conn, if_exists='replace', index=False, method='multi',
                     chunksize=1000, index_label=primary_key)

        # Atualiza as colunas "Situação Cadastral" e "Data Situacao Cadastral" se as linhas já existem
        br_conn.execute(f"""
            UPDATE estabelecimentos
            SET "SITUACAO_CADASTRAL" = ?, "DATE_REGISTRATION_SITUATION" = ?
            WHERE cnpj = ? AND (
                "SITUACAO_CADASTRAL" <> ? OR
                "DATE_REGISTRATION_SITUATION" <> ?
            )
        """, [(row["SITUACAO_CADASTRAL"], row["DATE_REGISTRATION_SITUATION"], row["cnpj"],
               row["SITUACAO_CADASTRAL"], row["DATE_REGISTRATION_SITUATION"]) for _, row in dados.iterrows()])

    # Executa o comando VACUUM para compactar o banco de dados
    br_conn.execute("VACUUM")

    # Fecha a conexão com o banco de dados
    br_conn.close()
# Cria uma conexão com os SQLites
with sqlite3.connect("./database/en_base_cnpj.db") as en_conn:
    # Loop por todos os br_arquivos
    for br_arquivo in br_arquivos:
        # Lê o arquivo para um dataframe
        data = pd.read_csv(f'{br_diretorio}{br_arquivo}', sep=';', low_memory=False)
        data['TRADING NAME'].replace('--empty--','', regex=True, inplace=True)
        
        # Define a chave primária da tabela
        primary_key = ['EIN (CNPJ)']

        # Salva o dataframe como uma tabela no SQLite e atualiza as linhas existentes
        data.to_sql('establishments', en_conn, if_exists='replace', index=False, method='multi',
                     chunksize=1000, index_label=primary_key)

        # Atualiza as colunas "Situação Cadastral" e "Data Situacao Cadastral" se as linhas já existem
        en_conn.execute(f"""
            UPDATE establishments
            SET "REGISTRATION_SITUATION" = ?, "DATE_REGISTRATION_SITUATION" = ?
            WHERE cnpj = ? AND (
                "REGISTRATION_SITUATION" <> ? OR
                "DATE_REGISTRATION_SITUATION" <> ?
            )
        """, [(row["REGISTRATION_SITUATION"], row["DATE_REGISTRATION_SITUATION"], row["EIN (CNPJ)"],
               row["REGISTRATION_SITUATION"], row["DATE_REGISTRATION_SITUATION"]) for _, row in data.iterrows()])

    # Executa o comando VACUUM para compactar o banco de dados
    en_conn.execute("VACUUM")

    # Fecha a conexão com o banco de dados
    en_conn.close()

agora = datetime.now()
tempo_execucao = time.process_time()
print(f"Fim do processo: {agora.date()} às {agora.time()}  tempo de execução {tempo_execucao}!")
logging.info(f"Fim do processo: {agora.date()} às {agora.time()}  tempo de execução {tempo_execucao}!")