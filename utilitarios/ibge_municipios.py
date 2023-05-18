import requests
import os
import polars as pl
# diretório atual
current_dir = os.path.dirname(os.path.abspath(__file__))
municipios = current_dir.replace('utilitarios', r'auxiliares/MUNICIPIOS.CSV')

url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios?"

params = {
    "view" : "nivelado"
}
headers = {
    "Accept" : "*/*",
    "Content-type" : "application/json; charset=UTF-8"
}

response = requests.get( url, headers=headers, params=params)
data = response.json()
base = []
for d in data:
    base.append({
        "MUNICIPIO" : d["municipio-id"],
        "CIDADE" : d["municipio-nome"].upper(),
        "COD_UF" : d["UF-id"],
        "UF" : d["UF-sigla"].upper(),
        "ESTADO" : d["UF-nome"].upper(),
        "REGIAO" : d["regiao-nome"].upper()
    })
dados = pl.DataFrame(base)

dados.write_csv(municipios, separator=";", batch_size=1024)