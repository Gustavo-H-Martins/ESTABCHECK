# libs
import pandas as pd
import json
import re
import os
import translators.server as tss
import concurrent.futures
import requests
from requests.adapters import HTTPAdapter


def get_lat_long(cep:str):
    """Busca Latitude e Longitude de um CEP válido informado"""
    url = f'https://viacep.com.br/ws/{cep}/json/'
    response = requests.get(url)
    if response.status_code == 200:
        location = response.json()
        lat = location.get("latitude")
        lng = location.get("longitude")
        return lat, lng
    else:
        return -0, 0

def thread_get_lat_long(cep_list):
    """Implanta o multithreading na função `get_lat_long`"""
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = [executor.submit(get_lat_long, cep) for cep in cep_list]

    lat_longs = []
    for f in concurrent.futures.as_completed(results):
        result = f.result()
        if result:
            lat_longs.append(result)

    return lat_longs


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

    dados = pd.read_csv(f"{diretorio}{file}", sep=';')
    lista_cep = dados['CEP'].tolist()
    lat_longs = thread_get_lat_long(lista_cep)
    dados['LATITUDE'], dados['LONGITUDE'] = zip(*lat_longs)
    dados['SITE'] = "www" + dados['NOME_FANTASIA'].str.lower().replace(" ", "", regex=True) + ".com"
    dados['FACEBOOK'] = "https://pt-br.facebook.com/" + dados['NOME_FANTASIA'].str.lower().replace(" ","",regex=True)
    dados['INSTAGRAM']  = "@"+ dados['NOME_FANTASIA'].str.lower().replace(" ","", regex=True)
    dados['HORARIO_FUNCIONAMENTO'] = None
    dados['OPCOES_DE_SERVICO'] = None

    dados = dados[['CNPJ', 'RAZAO_SOCIAL', 'NOME_FANTASIA', 'RUA', 'NUMERO', 'COMPLEMENTO',
       'BAIRRO', 'CIDADE', 'ESTADO', 'CEP','LATITUDE', 'LONGITUDE', 'TELEFONE', 'SITE',
       'CNAE_DESCRICAO', 'HORARIO_FUNCIONAMENTO', 'INSTAGRAM', 'FACEBOOK', 'OPCOES_DE_SERVICO']]
    # Cria uma cópia para converter para o Inglês
    data = dados.copy()

    # Traduz os nomes das colunas
    data.rename(columns={'CNPJ':"EIN (CNPJ)", 'RAZAO_SOCIAL':'CORPORATE_NAME', 'NOME_FANTASIA':'TRADING NAME', 'RUA':'STREET', 'NUMERO':'ADDRESS_NUMBER', 'COMPLEMENTO':'ADDRESS_COMPLEMENT',
       'BAIRRO':'DISTRICT', 'CIDADE':'CITY', 'ESTADO':'STATE', 'CEP':'ZIP_CODE', 'TELEFONE':'PHONE_NUMBER', 'HORARIO_FUNCIONAMENTO':'OPENING_HOURS', 'OPCOES_DE_SERVICO':'SERVICE_OPTIONS', 'CNAE_DESCRICAO':'CATEGORY_(CNAE)'}, inplace=True)

    #data['CATEGORY_(CNAE)'] = data['CATEGORY_(CNAE)'].str.replace(f"{data['CATEGORY_(CNAE)'].str}", f"{traduzir(data['CATEGORY_(CNAE)'].str)}", regex=True)
    # Tratando os dados para disposição
    data = pd.merge(data, comparador, on='CATEGORY_(CNAE)')
    data['CATEGORY_(CNAE)'] = data['CNAE_TRADUCAO']
    dados.to_csv(f'br_base/{file}', index=False, sep=';')
    data.to_csv(f'en_base/{file}', index=False, sep=';')