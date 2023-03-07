"""
    # ESTE RODA PRIMEIRO
"""



# ler todos os arquivos csv do diretório e guardar em um objeto
import numpy as np
import time
import os
import re
import pandas as pd
from datetime import datetime
from utilitarios.backup_limpeza import backup_limpeza_simples

# Definindo data atual e gerando o backup
agora = datetime.now()
datazip = agora.strftime("%Y-%m-%d %H_%M_%S")

# realiza backup dos dados antigos
base_csv_estabelecimentos = r'./base_csv_estabelecimentos/'
all_files_estabelecimentos = list(filter(lambda x: '.csv' in x, os.listdir(base_csv_estabelecimentos)))
if len(all_files_estabelecimentos) >= 1:
    backup_limpeza_simples(pasta=base_csv_estabelecimentos, nome_zipado=f'base_csv_estabelecimentos{datazip}.zip',extensao='.csv')
#diretorio = r'Bases\Base_atualizada/'
diretorio = r'C:\Users\ABRASEL NACIONAL\Documents\CNPJ_PROGRAMATICA\Manipulacao_arquivos\Manipulacao_Arquivos_Python\Bases/'
all_files = list(filter(lambda x: '.csv' in x, os.listdir(diretorio)))

#Warnings: Possui uma série de funções e comandos para tratamento de mensagens de avisos e alertas do Python
import warnings
warnings.filterwarnings("ignore")

# definindo colunas e tipos
ESTABELE = ['CNPJ_BASE', 'CNPJ_ORDEM', 'CNPJ_DV', 'MATRIZ_FILIAL', 'NOME_FANTASIA',
       'SITUACAO_CADASTRAL', 'DATA_SITUACAO_CADASTRAL',
       'MOTIVO_SITUACAO_CADASTRAL', 'DATA_INICIO_ATIVIDADE', 'CNAE_PRINCIPAL',
       'CNAE_SECUNDARIO', 'TIPO_LOGRADOURO', 'LOGRADOURO', 'NUMERO',
       'COMPLEMENTO', 'BAIRRO', 'CEP', 'UF', 'MUNICIPIO', 'TELEFONE1',
       'TELEFONE2', 'FAX', 'EMAIL']

dtypes = {'CNPJ_BASE': 'category',
 'CNPJ_ORDEM': 'category',
 'CNPJ_DV': 'category'}

# processa os dados

for file in all_files:
    dados =pd.read_csv(f'{diretorio}{file}', names=ESTABELE, dtype=dtypes, sep=';')
    descricao_cnae = re.sub('.csv', '', file)
    dados['CNAE_DESCRICAO'] = descricao_cnae.upper()
    dados = dados[[
        'CNPJ_BASE', 'CNPJ_ORDEM', 'CNPJ_DV', 'NOME_FANTASIA', 'CNAE_PRINCIPAL','CNAE_DESCRICAO',
        'TIPO_LOGRADOURO', 'LOGRADOURO', 'NUMERO',
       'COMPLEMENTO', 'BAIRRO', 'CEP', 'UF', 'MUNICIPIO', 'TELEFONE1', 'SITUACAO_CADASTRAL', 'DATA_SITUACAO_CADASTRAL']]
    dados.to_csv(f"./base_csv_estabelecimentos/{file}", mode='w',index=False, sep=';')
