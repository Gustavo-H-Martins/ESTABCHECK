# libs
import pandas as pd
import json
import re
from geopy.geocoders import Nominatim
import pgeocode
import os
from googletrans import Translator

def get_lat_long(cep):
    geo = pgeocode.Geo("BR")
    geo = pgeocode.
    lat, lng = geo.coordinates(cep)
    return lat, lng



def traduzir(texto, dest_lang='pt'):
    tradutor = Translator(dest_lang=dest_lang)
    return tradutor.translate(texto).text

# Diretórios
diretorio = r'merge_base/'
all_files = list(filter(lambda x: '.csv' in x, os.listdir(diretorio)))

for file in all_files:
    dados = pd.read_csv(f"{diretorio}{file}", sep=';')
    dados['COORDENADAS'] = dados['CEP'].apply(lambda cep: get_lat_long(cep))
    dados['SITE'] = "www" + dados['NOME_FANTASIA'].str.lower().replace(" ", "", regex=True) + ".com"
    dados['FACEBOOK'] = "https://pt-br.facebook.com/" + dados['NOME_FANTASIA'].str.lower().replace(" ","",regex=True)
    dados['INSTAGRAM']  = "@"+ dados['NOME_FANTASIA'].str.lower().replace(" ","", regex=True)
    dados['HORARIO_FUNCIONAMENTO'] = None
    dados['OPCOES_DE_SERVICO'] = None

    dados = dados[['CNPJ', 'RAZAO_SOCIAL', 'NOME_FANTASIA', 'RUA', 'NUMERO', 'COMPLEMENTO',
       'BAIRRO', 'CIDADE', 'ESTADO', 'CEP','LATITUDE', 'LONGITUDE', 'TELEFONE', 'SITE',
       'CNAE_DESCRICAO', 'HORARIO_FUNCIONAMENTO', 'INSTAGRAM', 'FACEBOOK', 'OPCOES_DE_SERVICO']]
    data = dados.copy()

    data.rename(columns={'CNPJ':"EIN (CNPJ)", 'RAZAO_SOCIAL':'CORPORATE_NAME', 'NOME_FANTASIA':'TRADING NAME', 'RUA':'STREET', 'NUMERO':'ADDRESS_NUMBER', 'COMPLEMENTO':'ADDRESS_COMPLEMENT',
       'BAIRRO':'DISTRICT', 'CIDADE':'CITY', 'ESTADO':'STATE', 'CEP':'ZIP_CODE', 'TELEFONE':'PHONE_NUMBER', 'HORARIO_FUNCIONAMENTO':'OPENING_HOURS', 'OPCOES_DE_SERVICO':'SERVICE_OPTIONS', 'CNAE_DESCRICAO':'CATEGORY_(CNAE)'}, inplace=True)

    data['CATEGORY_(CNAE)'] = data['CATEGORY_(CNAE)'].str.replace(f"{data['CATEGORY_(CNAE)'].str}", f"{traduzir(data['CATEGORY_(CNAE)'].str)}", regex=True)
    
    dados.to_csv(f'br_base_json/{file}', index=False, sep=';')
    data.to_csv(f'en_base/{file}', index=False, sep=';')