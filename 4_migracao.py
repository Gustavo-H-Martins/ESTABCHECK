"""
    # ESSE RODA EM QUARTO
"""

# Libs
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
import duckdb

# Estrutura de log
logging.basicConfig(filename='logs\migracao.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Criando um objeto duckdb
duck = duckdb.connect()

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
# Verifica e cria as tabelas de migração
cursor = br_conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS temp_atualizar_estabelecimentos
    ('CNPJ', 'RAZAO_SOCIAL', 'NOME_FANTASIA', 'RUA', 'NUMERO', 'COMPLEMENTO',
    'BAIRRO', 'CIDADE', 'ESTADO', 'CEP','LATITUDE', 'LONGITUDE', 'TELEFONE1', 'SITE',
    'CNAE_DESCRICAO', 'HORARIO_FUNCIONAMENTO', 'INSTAGRAM', 'FACEBOOK',
    'OPCOES_DE_SERVICO','SITUACAO_CADASTRAL', 'DATA_SITUACAO_CADASTRAL')
    ;""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS versao_anterior_estabelecimentos
    ('CNPJ', 'RAZAO_SOCIAL', 'NOME_FANTASIA', 'RUA', 'NUMERO', 'COMPLEMENTO',
    'BAIRRO', 'CIDADE', 'ESTADO', 'CEP','LATITUDE', 'LONGITUDE', 'TELEFONE1', 'SITE',
    'CNAE_DESCRICAO', 'HORARIO_FUNCIONAMENTO', 'INSTAGRAM', 'FACEBOOK',
    'OPCOES_DE_SERVICO','SITUACAO_CADASTRAL', 'DATA_SITUACAO_CADASTRAL')
    ;""")

br_conn.commit()
contador_geral = 0 
contador_novos = 0
contador_inativos = 0
# Loop por todos os br_arquivos
for br_arquivo in br_arquivos:

    
    # Lê o arquivo para um dataframe
    br_dados = pd.read_parquet(os.path.join(br_diretorio, br_arquivo))
    br_dados['NOME_FANTASIA'].replace('--empty--','', regex=True, inplace=True)    
    # Pegando a descricao cnae
    cnae_descricao = re.sub('.gzip', '', br_arquivo)

    # Pegando a última data de 
    ultima_alteracao = duck.query("""
    SELECT DATA_SITUACAO_CADASTRAL 
    FROM br_dados
    ORDER BY DATA_SITUACAO_CADASTRAL DESC
    LIMIT 1;
    """).fetchone()[0]
    
    # Define a query para selecionar todas as linhas da tabela 'estabelecimentos'
    query = f"""
    SELECT * FROM estabelecimentos
    WHERE CNAE_DESCRICAO = '{cnae_descricao.lower()}';
    """

    # Cria o gerador de DataFrames a partir da query
    get_all_table = pd.read_sql_query(query, br_conn, chunksize=100000)

    # Itera sobre cada chunk do DataFrame
    for chunk in tqdm(get_all_table):
        # Define a chave primária da tabela
        primary_key = ['CNPJ']
        chunk['CNAE_DESCRICAO'] = chunk['CNAE_DESCRICAO'].str.upper()
        # Faz o processamento desejado com o chunk
        # Crie um dataframe com todos os dados que tem no primeiro, mas não tem no segundo
        novos_dados = pd.merge(br_dados, chunk, how='left', indicator=True)
        # Adicionando valor ao contador de novos estabelecimentos
        contador_novos += len(novos_dados["CNPJ"])
        novos_dados = novos_dados[novos_dados['_merge'] == 'left_only']
        novos_dados.drop(columns='_merge', inplace=True)
        novos_dados.to_sql('temp_atualizar_estabelecimentos', br_conn, if_exists='append', index=False, index_label=primary_key)
        
        # Crie um dataframe com todos os dados que tem no segundo, mas não tem no primeiro
        inativos_dados = pd.merge(br_dados, chunk,how='right', indicator=True)
        # Adicionando valor ao contador de estabelecimentos inativos 
        contador_inativos += len(inativos_dados['CNPJ'])
        inativos_dados = inativos_dados[inativos_dados['_merge'] == 'right_only']
        inativos_dados.drop(columns='_merge', inplace=True)
        inativos_dados['SITUACAO_CADASTRAL'] = 1
        inativos_dados['DATA_SITUACAO_CADASTRAL'] = ultima_alteracao
        inativos_dados.to_sql('temp_atualizar_estabelecimentos', br_conn, if_exists='append', index=False, index_label=primary_key)

        # Crie um dataframe com todos os dados que tem em ambos os dataframes
        dados_nao_alterados = pd.merge(br_dados, chunk, how='inner')
        dados_nao_alterados.to_sql('temp_atualizar_estabelecimentos', br_conn, if_exists='append', index=False, index_label=primary_key)

# Consultando todos os dados
get_all_estabelecimentos = """
    SELECT * FROM estabelecimentos;
"""
chunks_get_all_estabelecimentos = pd.read_sql_query(get_all_estabelecimentos, br_conn, chunksize=100000)
for chunk in chunks_get_all_estabelecimentos:
    chunk.to_sql('versao_anterior_estabelecimentos', br_conn, if_exists='append', index=False, index_label=primary_key)
cursor.execute("DROP TABLE estabelecimentos")
br_conn.commit()

get_all_temp_atualizar_estabelecimentos = """
    SELECT * FROM temp_atualizar_estabelecimentos;
"""
chunks_get_all_temp_atualizar_estabelecimentos = pd.read_sql_query(get_all_temp_atualizar_estabelecimentos, br_conn, chunksize=100000)
for chunk in chunks_get_all_temp_atualizar_estabelecimentos:
        contador_geral += len(chunk["CNPJ"])
        chunk.to_sql('estabelecimentos', br_conn, if_exists='append', index=False, index_label=primary_key)
cursor.execute("DROP TABLE temp_atualizar_estabelecimentos")
# finaliza a transação
br_conn.commit()
# Executa o comando VACUUM para compactar o banco de dados
br_conn.execute("VACUUM")
# Fecha a conexão com o banco de dados
br_conn.close()



tempo_execucao = time.process_time()
print(f"Fim do processo: {agora.date()} às {agora.time()} tempo de execução {tempo_execucao / 60:2f} minutos!")
print(f"""A Base tem {contador_geral} registros
Total estabelecimentos inativos: {contador_inativos}
Foram inseridos {contador_novos} novos registros
Total Estabelecimentos Ativos: {contador_geral - contador_inativos}.
""")
logging.info(f"Fim do processo: {agora.date()} às {agora.time()} tempo de execução {tempo_execucao / 60:2f}!")
logging.info(f"""A Base tem {contador_geral} registros
Total estabelecimentos inativos: {contador_inativos}
Foram inseridos {contador_novos} novos registros
Total Estabelecimentos Ativos: {contador_geral - contador_inativos}.
""")
