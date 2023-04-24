"""
    # ESSE RODA TERCEIRO
"""
# libs
import pandas as pd
import json
import re
import os
import logging
from utilitarios.backup_limpeza import backup_limpeza_simples
from datetime import datetime
import sqlite3

# define os diretórios
current_dir = os.path.dirname(os.path.abspath(__file__))
file_log = current_dir + r'\logs\enriquecimento.log'

# Mapeando e conectando ao banco SQLite
db_file = current_dir.replace(r'ESTABCHECK', r'coletor_leads_vouchers\app\files\database.db')
conn = sqlite3.connect(database=db_file)
# gerando log
logging.basicConfig(level=logging.DEBUG, filename=file_log, encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")

# Definindo data atual e gerando o backup
agora = datetime.now()
datazip = agora.strftime("%Y-%m-%d %H_%M_%S")

# verificando e gerando o backup dos dados. 
br_base = current_dir + r'\br_base/'
all_files_br_base = list(filter(lambda x: '.parquet' in x, os.listdir(br_base)))
if len(all_files_br_base) >= 1:
    backup_limpeza_simples(pasta=br_base, nome_zipado=f"{br_base}backup/br_base_{datazip}.zip", extensao='.parquet')

# Diretórios
diretorio = current_dir + r'\merge_base/'
all_files = list(filter(lambda x: '.csv' in x, os.listdir(diretorio)))

for file in all_files:
    # Itera sobre todos os arquivos CSV no repositório
    dados = pd.read_csv(f"{diretorio}{file}", sep=';', dtype='object')
    dados['NOME_FANTASIA'].replace('--empty--','', regex=True, inplace=True)
    dados['SITE'] = "www." + dados['NOME_FANTASIA'].str.lower().replace(" ", "", regex=True) + ".com.br"
    dados['FACEBOOK'] = "https://pt-br.facebook.com/" + dados['NOME_FANTASIA'].str.lower().replace(" ","",regex=True)
    dados['INSTAGRAM']  = "@"+ dados['NOME_FANTASIA'].str.lower().replace(" ","", regex=True)
    dados['HORARIO_FUNCIONAMENTO'] = None
    dados['OPCOES_DE_SERVICO'] = None
    
    
    
    dados = dados[['CNPJ', 'RAZAO_SOCIAL', 'NOME_FANTASIA','ENDERECO', 'RUA', 'NUMERO', 'COMPLEMENTO',
                'BAIRRO', 'CIDADE', 'UF', 'CEP', 'TELEFONE','EMAIL', 'SITE', 'CNAE_PRINCIPAL',
                'CNAE_DESCRICAO', 'HORARIO_FUNCIONAMENTO', 'INSTAGRAM', 'FACEBOOK', 
                'OPCOES_DE_SERVICO','SITUACAO_CADASTRAL', 'DATA_SITUACAO_CADASTRAL']]
    
    parquet_file = re.sub('.csv', '.parquet', file)
    dados.to_parquet(f'br_base/{parquet_file}',compression='gzip')

    #Converter o dataframe em uma tabela no banco de dados
    """
    O parâmetro if_exists=`append` verifica se a tabela já existe e incrementa os dados
    O parâmetro index=False evita que o índice do dataframe seja inserido na tabela
    O parâmetro dtype define o tipo de cada coluna na tabela
    """
    dados.to_sql('tb_rfb', conn, if_exists='append', index=False, 
                dtype={
                    'CNPJ': 'TEXT PRIMARY KEY', 'RAZAO_SOCIAL': 'TEXT', 'NOME_FANTASIA': 'TEXT', 
                    'ENDERECO': 'TEXT', 'NUMERO': 'TEXT', 'COMPLEMENTO' : 'TEXT',
                    'BAIRRO': 'TEXT', 'CIDADE': 'TEXT', 'UF': 'TEXT', 'CEP' : 'TEXT',
                    'TELEFONE': 'TEXT', 'EMAIL' : 'TEXT', 'SITE' : 'TEXT', 
                    'CNAE_PRINCIPAL': 'TEXT', 'CNAE_DESCRICAO' : 'TEXT', 
                    'HORARIO_FUNCIONAMENTO' : 'TEXT', 'INSTAGRAM' : 'TEXT',
                    'FACEBOOK': 'TEXT', 'OPCOES_DE_SERVICO' : 'TEXT', 
                    'SITUACAO_CADASTRAL':'TEXT', 'DATA_SITUACAO_CADASTRAL': 'TEXT'
                    })
    # Finaliza a transação
    conn.commit()
    # Executa o comando VACUUM para compactar o banco de dados
    conn.execute('VACUUM')

# Fechar a conexão com o banco de dados
conn.close()