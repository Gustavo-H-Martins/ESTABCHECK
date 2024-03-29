import pandas as pd
import sqlite3
import os
import time
from datetime import datetime
import logging
from utilitarios.backup_limpeza import backup_limpeza_simples

# gerando log
logging.basicConfig(level=logging.INFO, filename="./logs/migracao.log", encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")

# Definindo data atual e gerando o backup
agora = datetime.now()
datazip = agora.strftime("%Y-%m-%d %H_%M_%S")

# verificando e gerando o backup dos dados. 
database = r'./database/'
all_files_database = list(filter(lambda x: '.db' in x, os.listdir(database)))
if len(all_files_database) >= 1:
    backup_limpeza_simples(diretorio_origem=database, nome_zipado=f"backup_database_{datazip}.zip", extensao='.db', diretorio_destino=f"{database}backup/")

# Conecta-se ao banco de dados
br_conn = sqlite3.connect("./database/br_base_cnpj.db")
en_conn = sqlite3.connect("./database/en_base_cnpj.db")

print(f"Início do processo: {agora.date()} às {agora.time()}")
logging.info(f"Início do processo: {agora.date()} às {agora.time()}")

# Define os diretórios
br_diretorio = r'./br_base/'
en_diretorio = r'./en_base/'

# Lista todos os arquivos CSV nos diretórios
br_arquivos = [f for f in os.listdir(br_diretorio) if f.endswith('.gzip')]
en_arquivos = [f for f in os.listdir(en_diretorio) if f.endswith('.gzip')]

# Loop por todos os br_arquivos
for br_arquivo in br_arquivos:
    # Lê o arquivo para um dataframe
    dados = pd.read_parquet(f'{br_diretorio}{br_arquivo}')
    dados['NOME_FANTASIA'].replace('--empty--','', regex=True, inplace=True)

    primary_key = ["CNPJ"]
    contagem = len(dados["CNPJ"])
    descricao_cnae = dados['CNAE_DESCRICAO'].unique()[0]
    # Salva o dataframe como uma tabela no SQLite
    dados.to_sql('estabelecimentos', br_conn, if_exists='append', index=False)
    logging.info(f"Incluídos {contagem} registros do CNAE: {descricao_cnae}")
    print(f"Incluídos {contagem} registros do CNAE: {descricao_cnae}")
# Executa o comando VACUUM para compactar o banco de dados
br_conn.execute("VACUUM")

# Fecha a conexão com o banco de dados
br_conn.close()

# Loop por todos os en_arquivos
for en_arquivo in en_arquivos:
    # Lê o arquivo para um dataframe
    data = pd.read_parquet(f'{en_diretorio}{en_arquivo}')
    data['TRADING NAME'].replace('--empty--','', regex=True, inplace=True)
    
    primary_key = ["EIN (CNPJ)"]
    contagem = len(data["EIN (CNPJ)"])
    descricao_cnae = data['CATEGORY_(CNAE)'].unique()[0]
    # Salva o dataframe como uma tabela no SQLite
    data.to_sql('establishments', en_conn, if_exists='append', index=False, index_label=primary_key)
    logging.info(f"included {contagem} CNAE records: {descricao_cnae}")
    print(f"included {contagem} CNAE records: {descricao_cnae}")
# Executa o comando VACUUM para compactar o banco de dados
en_conn.execute("VACUUM")

# Fecha a conexão com o banco de dados
en_conn.close()

agora = datetime.now()
tempo_execucao = time.process_time()
print(f"Fim do processo: {agora.date()} às {agora.time()}  tempo de execução {tempo_execucao}!")
logging.info(f"Fim do processo: {agora.date()} às {agora.time()}  tempo de execução {tempo_execucao}!")