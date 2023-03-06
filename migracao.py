import pandas as pd
import sqlite3
import os
import time
from datetime import datetime
import logging
import sys
import re
from utilitarios.backup_limpeza import backup_simples
from utilitarios.validacoes import existe_dir
from tqdm import tqdm
import numpy as np
from multiprocessing import Pool, cpu_count


# verificando e gerando o backup dos dados. 
database = r'./database/'

# Definindo data atual e gerando o backup
agora = datetime.now()
datazip = agora.strftime("%Y-%m-%d %H_%M_%S")

all_files_database = list(filter(lambda x: '.db' in x, os.listdir(database)))

if len(all_files_database) >= 1:
    backup_simples(diretorio_origem=database, nome_zipado=f"backup_database_{datazip}.zip", extensao='.db', diretorio_destino=f"{database}backup/")


# Define os diretórios
br_diretorio = r'./br_base/'

# Lista todos os arquivos CSV nos diretórios
br_arquivos = [f for f in os.listdir(br_diretorio) if f.endswith('.gzip')]

# Verifica se existe o diretorios
for diretorio in [database, br_diretorio]:
    existe_dir(diretorio)

# Conecta-se ao banco de dados
br_conn = sqlite3.connect("./database/br_base_cnpj.db")

contador_geral = 0 
contador_novos = 0
contador_inativos = 0
# Loop por todos os br_arquivos
for br_arquivo in br_arquivos:
    # Lê o arquivo para um dataframe
    br_dados = pd.read_parquet(os.path.join(br_diretorio, br_arquivo))
    br_dados['NOME_FANTASIA'].replace('--empty--','', regex=True, inplace=True)

    # Define a query para selecionar todas as linhas da tabela 'estabelecimentos'
    query = "SELECT * FROM estabelecimentos"

    # Cria o gerador de DataFrames a partir da query
    get_all_table = pd.read_sql_query(query, br_conn, chunksize=100000)

    # Itera sobre cada chunk do DataFrame
    for chunk in get_all_table:
        # Define a chave primária da tabela
        primary_key = ['CNPJ']
        # Faz o processamento desejado com o chunk
        # Crie um dataframe com todos os dados que tem no primeiro, mas não tem no segundo
        novos_dados = pd.merge(br_dados, chunk, how='left', indicator=True)
        novos_dados = novos_dados[novos_dados['_merge'] == 'left_only']
        novos_dados.drop(columns='_merge', inplace=True)
        novos_dados.to_sql('estabelecimentos_atualizados', br_conn, if_exists='append', index=False, index_label=primary_key)

# finaliza a transação
br_conn.commit()
# Executa o comando VACUUM para compactar o banco de dados
br_conn.execute("VACUUM")
# Fecha a conexão com o banco de dados
br_conn.close()



agora = datetime.now()
tempo_execucao = time.process_time()
print(f"Fim do processo: {agora.date()} às {agora.time()} tempo de execução {tempo_execucao}!")
print(f"""A Base tem {contador_geral} Registros Ativos
Foram inseridos {contador_novos} novos registros,
""")
logging.info(f"Fim do processo: {agora.date()} às {agora.time()} tempo de execução {tempo_execucao}!")
logging.info(f"""A Base tem {contador_geral} Registros Ativos
Foram inseridos {contador_novos} novos registros,
""")
