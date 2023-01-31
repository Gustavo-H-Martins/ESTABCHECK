import pandas as pd
import os
# Diretórios

CENSUS = r'CEP\census_code_cep_coordinates.csv'
CEP = r'CEP\CEPS_ÚNICOS.csv'


#Warnings: Possui uma série de funções e comandos para tratamento de mensagens de avisos e alertas do Python
import warnings
warnings.filterwarnings("ignore")

#Comando para exibir todas colunas do arquivo
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

cep = pd.read_csv(CEP)

censo = pd.read_csv(CENSUS)
censo.rename(columns={'POSTCODE':'CEP'}, inplace=True)
censo = censo[['CEP','LON', 'LAT']]
censo.drop_duplicates(inplace=True)

lista_cep_latitude = censo.loc[censo['CEP'].isin(list(cep['CEP']))].reset_index()
lista_cep_latitude.rename(columns={'LON':'LONGITUDE', 'LAT':'LATITUDE'})

lista_cep_latitude.to_csv('LISTA_CEP_LATITUDE_LONGITUDE.csv',mode='w', sep=';', index=False)