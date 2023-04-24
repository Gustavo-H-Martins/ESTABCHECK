# libs
import os
import pandas as pd
from geonames import get_geolocalizacao

# diretório atual
current_dir = os.path.dirname(os.path.abspath(__file__))
municipios  = current_dir.replace('utilitarios', r'auxiliares/MUNICIPIOS.CSV')
municipios_lat_lon = current_dir.replace("utilitarios", r"auxiliares/MUNICIPIOS_LAT_LON.csv")

with open(municipios, "r", encoding="utf-8") as m:
    geo_municipios = m.readlines()[1:]
    m.close()
# criando o dataframe vazio para fazer a concatenação dos dados de retorno
dados = pd.DataFrame()
for municipio in geo_municipios:
    # usando os separadores para pegar o campo que quero
    partes_municipio = municipio.split(";")
    
    # chamando e já criando o dataframe com o retorno da função get_geolocalizacao
    base = pd.DataFrame(get_geolocalizacao(cod_municipio=int(partes_municipio[0])))
    # concatenando com o dataframe vazio criado lá em cima
    dados = pd.concat([dados, base])
# retirando eventuais duplicados
dados.drop_duplicates(inplace=True)
# salvando em um CSV para não criar caso por hora
dados.to_csv(municipios_lat_lon, index=False)