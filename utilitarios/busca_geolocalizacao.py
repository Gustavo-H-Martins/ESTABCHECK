# libs
import os
import pandas as pd
from geonames import get_geolocalizacao
import sqlite3
from unidecode import unidecode
# diretório atual
current_dir = os.path.dirname(os.path.abspath(__file__))
municipios  = current_dir.replace('utilitarios', r'auxiliares/MUNICIPIOS.CSV')
municipios_lat_lon = current_dir.replace("utilitarios", r"auxiliares/MUNICIPIOS_LAT_LON.csv")
with open(municipios, "r", encoding="utf-8") as m:
    geo_municipios = m.readlines()[5570:]
    m.close()

for municipio in geo_municipios:
    # usando os separadores para pegar o campo que quero
    partes_municipio = municipio.split(";")
    
    # chamando e já criando o dataframe com o retorno da função get_geolocalizacao
    try:
        dados = pd.DataFrame(get_geolocalizacao(cod_municipio=int(partes_municipio[0]), usuario="diskborste"))

        # salvando em um CSV para não criar caso por hora
        dados.to_csv(municipios_lat_lon, mode='ab',index=False, header=False, sep=";")
    except Exception as e:
        print(f"Deu o erro em {e} no local : {partes_municipio[1]}")
        pass

df_lat_lon = pd.read_csv(municipios_lat_lon, sep=";", dtype="str")
df_municipios = pd.read_csv(municipios, sep=";",dtype="str")
dados = pd.merge(df_lat_lon, df_municipios, how='inner', on="MUNICIPIO")
dados.rename(columns={"COD_UF_y": "COD_UF", "UF_y": "UF"}, inplace=True)
dados = dados[["MUNICIPIO", "CIDADE", "COD_UF", "UF", "ESTADO", "REGIAO", "BAIRRO", "LATITUDE", "LONGITUDE"]]

# decodificando o nome das cidades
CIDADES = []
for cidade in dados["CIDADE"]:
    CIDADES.append(unidecode(cidade))
dados['CIDADE'] = CIDADES
dados.drop_duplicates(inplace=True)
dados.to_csv(municipios_lat_lon, mode='w',index=False, sep=";")

# Mapeando e conectando ao banco SQLite
db_file = current_dir.replace(r'ESTABCHECK\utilitarios', r'coletor_leads_vouchers\app\files\database.db')
conn = sqlite3.connect(database=db_file)

#Converter o dataframe em uma tabela no banco de dados
"""
O parâmetro if_exists=`replace` verifica se a tabela já existe e substitui os dados
O parâmetro index=False evita que o índice do dataframe seja inserido na tabela
O parâmetro dtype define o tipo de cada coluna na tabela
"""
dados.to_sql('tb_municipios', conn, if_exists='replace', index=False, 
             dtype={'UF': 'TEXT', 'COD_UF': 'TEXT', 'MUNICIPIO': 'TEXT', 'BAIRRO': 'TEXT', 
                    'LATITUDE': 'TEXT', 'LONGITUDE': 'TEXT'})
# Finaliza a transação
conn.commit()
# Executa o comando VACUUM para compactar o banco de dados
conn.execute('VACUUM')

# Fechar a conexão com o banco de dados
conn.close()