# libs
import pandas as pd
import json
import re
import os
from googletrans import Translator
import concurrent.futures


import requests

def get_lat_long(cep:str):
    """Busca Latitude e Longitude de um CEP válido informado"""
    url = f"https://api.postmon.com.br/v1/cep/{cep}"
    response = requests.get(url)
    if response.status_code == 200:
        location = response.json()
        lat = location.get("latitude")
        lng = location.get("longitude")
        return lat, lng
    else:
        return None

def main(cep_list):
    """Implanta o multithreading na função `get_lat_long`"""
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = [executor.submit(get_lat_long, cep) for cep in cep_list]

    lat_longs = []
    for f in concurrent.futures.as_completed(results):
        result = f.result()
        if result:
            lat_longs.append(result)

    return lat_longs


def traduzir(texto, dest_lang='en'):
    """ Traduz um texto informado do Português para o Inglês"""
    tradutor = Translator(dest_lang=dest_lang)
    return tradutor.translate(texto).text

# Diretórios
diretorio = r'merge_base/'
all_files = list(filter(lambda x: '.csv' in x, os.listdir(diretorio)))

for file in all_files:
    # Itera sobre todos os arquivos CSV no repositório

    dados = pd.read_csv(f"{diretorio}{file}", sep=';')
    lista_cep = dados['CEP'].tolist()
    lat_longs = main(lista_cep)
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

    data['CATEGORY_(CNAE)'] = data['CATEGORY_(CNAE)'].str.replace(f"{data['CATEGORY_(CNAE)'].str}", f"{traduzir(data['CATEGORY_(CNAE)'].str)}", regex=True)
    
    dados.to_csv(f'br_base_json/{file}', index=False, sep=';')
    data.to_csv(f'en_base/{file}', index=False, sep=';')