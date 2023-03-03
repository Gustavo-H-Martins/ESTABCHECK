# libs
import pandas as pd
import json
import re
import os
import translators.server as tss
import logging
from utilitarios.backup_limpeza import backup_limpeza_simples
from datetime import datetime

# gerando log
logging.basicConfig(level=logging.INFO, filename="./logs/enriquecimento.log", encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")
# Ceps
ceps = pd.read_csv('CEP\LISTA_CEP_LATITUDE_LONGITUDE.csv', sep=';')

# Definindo data atual e gerando o backup
datazip = f'{datetime.now().year}-{datetime.now().month-1}'

# verificando e gerando o backup dos dados. 
br_base = r'br_base/'
all_files_br_base = list(filter(lambda x: '.csv' in x, os.listdir(br_base)))
if len(all_files_br_base) >= 1:
    try:
        backup_limpeza_simples(diretorio_origem=br_base, nome_zipado=f"backup_br_base_{datazip}.zip", extensao='.csv', diretorio_destino=f"{br_base}backup/")
    except:
        backup_limpeza_simples(diretorio_origem=br_base, nome_zipado=f"backup_br_base_{datazip}.zip", extensao='.gzip', diretorio_destino=f"{br_base}backup/")

en_base = r'en_base/'
all_files_br_base = list(filter(lambda x: '.csv' in x, os.listdir(en_base)))
if len(all_files_br_base) >= 1:
    try:
        backup_limpeza_simples(diretorio_origem=en_base, nome_zipado=f"backup_en_base_{datazip}.zip", extensao='.csv', diretorio_destino=f"{en_base}backup/")
    except:
        backup_limpeza_simples(diretorio_origem=en_base, nome_zipado=f"backup_en_base_{datazip}.zip", extensao='.zgip', diretorio_destino=f"{en_base}backup/")


def traduzir(texto):
    """ Traduz um texto informado do Português para o Inglês"""
    traducao = tss.google(texto, from_language='pt', to_language='en')
    return traducao

dict_comparador ={
    'CATEGORY_(CNAE)' : ['Serviços ambulantes de alimentação',
            'Restaurantes e similares',
            'Lanchonetes casas de chá de sucos e similares',
            'Bares e outros estabelecimentos especializados em servir bebidas sem entretenimento',
            'Bares e outros estabelecimentos especializados em servir bebidas com entretenimento'],
    'CNAE_TRADUCAO' : []
    }
for cnae in dict_comparador['CATEGORY_(CNAE)']:
    dict_comparador['CNAE_TRADUCAO'].append(traduzir(cnae))

comparador = pd.DataFrame(dict_comparador)


# Diretórios
diretorio = r'merge_base/'
all_files = list(filter(lambda x: '.csv' in x, os.listdir(diretorio)))

for file in all_files:
    # Itera sobre todos os arquivos CSV no repositório
    dados = pd.read_csv(f"{diretorio}{file}", sep=';', dtype='object')
    dados['NOME_FANTASIA'].replace('--empty--','', regex=True, inplace=True)
    dados = pd.merge(dados,ceps,how='left', on='CEP')
    dados['SITE'] = "www." + dados['NOME_FANTASIA'].str.lower().replace(" ", "", regex=True) + ".com.br"
    dados['FACEBOOK'] = "https://pt-br.facebook.com/" + dados['NOME_FANTASIA'].str.lower().replace(" ","",regex=True)
    dados['INSTAGRAM']  = "@"+ dados['NOME_FANTASIA'].str.lower().replace(" ","", regex=True)
    dados['HORARIO_FUNCIONAMENTO'] = None
    dados['OPCOES_DE_SERVICO'] = None
    
    
    
    dados = dados[['CNPJ', 'RAZAO_SOCIAL', 'NOME_FANTASIA', 'RUA', 'NUMERO', 'COMPLEMENTO',
                'BAIRRO', 'CIDADE', 'ESTADO', 'CEP','LATITUDE', 'LONGITUDE', 'TELEFONE1', 'SITE',
                'CNAE_DESCRICAO', 'HORARIO_FUNCIONAMENTO', 'INSTAGRAM', 'FACEBOOK', 'OPCOES_DE_SERVICO','SITUACAO_CADASTRAL', 'DATA_SITUACAO_CADASTRAL']]
    # Cria uma cópia para converter para o Inglês
    data = dados.copy()

    # Traduz os nomes das colunas
    data.rename(columns={'CNPJ':"EIN (CNPJ)", 'RAZAO_SOCIAL':'CORPORATE_NAME', 'NOME_FANTASIA':'TRADING NAME', 'RUA':'STREET', 'NUMERO':'ADDRESS_NUMBER', 'COMPLEMENTO':'ADDRESS_COMPLEMENT',
       'BAIRRO':'DISTRICT', 'CIDADE':'CITY', 'ESTADO':'STATE', 'CEP':'ZIP_CODE', 'TELEFONE':'PHONE_NUMBER', 'HORARIO_FUNCIONAMENTO':'OPENING_HOURS', 'OPCOES_DE_SERVICO':'SERVICE_OPTIONS', 'CNAE_DESCRICAO':'CATEGORY_(CNAE)','SITUACAO_CADASTRAL':'REGISTRATION_SITUATION', 'DATA_SITUACAO_CADASTRAL':'DATE_REGISTRATION_SITUATION'}, inplace=True)

    #data['CATEGORY_(CNAE)'] = data['CATEGORY_(CNAE)'].str.replace(f"{data['CATEGORY_(CNAE)'].str}", f"{traduzir(data['CATEGORY_(CNAE)'].str)}", regex=True)
    
    # Tratando os dados para disposição
    data = pd.merge(data, comparador, on='CATEGORY_(CNAE)')
    data['CATEGORY_(CNAE)'] = data['CNAE_TRADUCAO']
    data = data[['EIN (CNPJ)', 'CORPORATE_NAME', 'TRADING NAME', 'STREET', 'ADDRESS_NUMBER', 
                 'ADDRESS_COMPLEMENT', 'DISTRICT', 'CITY', 'STATE', 'ZIP_CODE', 'LATITUDE', 
                 'LONGITUDE', 'TELEFONE1', 'SITE', 'CATEGORY_(CNAE)', 'OPENING_HOURS', 'INSTAGRAM', 'FACEBOOK', 
                 'SERVICE_OPTIONS', 'REGISTRATION_SITUATION', 'DATE_REGISTRATION_SITUATION']]
    parquet_file = re.sub('.csv', '.gzip', file)
    dados.to_parquet(f'br_base/{parquet_file}',compression='gzip')
    data.to_parquet(f'en_base/{parquet_file}',compression='gzip')