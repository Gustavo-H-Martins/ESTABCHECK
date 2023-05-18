"""
    # ESSE RODA primeiro depois do SEGUNDO 
"""

# Libs
import pandas as pd
import os
import re
#Warnings: Possui uma série de funções e comandos para tratamento de mensagens de avisos e alertas do Python
import warnings
warnings.filterwarnings("ignore")

# diretório atual
current_dir = os.path.dirname(os.path.abspath(__file__))


# DataFrame Vazio
Dados = pd.DataFrame()

# Diretórios
diretorio_estabelecimentos = current_dir + r'/base_csv_estabelecimentos/'
all_files_estabelecimentos = list(filter(lambda x: '.csv' in x, os.listdir(diretorio_estabelecimentos)))

diretorio_empresas = current_dir + r'/base_csv_empresas/'

# Iterando sobre os estabelecimentos
for file_estabelecimento in all_files_estabelecimentos:
    print(f"Lendo arquivo {file_estabelecimento}")
    # DataFrame Vazio
    merged_data = pd.DataFrame()

    CNPJ_estabelecimento = pd.read_csv(f'{diretorio_estabelecimentos}{file_estabelecimento}', sep=';')
    #print(d_estabelecimento.columns)
    CNPJ_estabelecimento = CNPJ_estabelecimento['CNPJ_BASE'].astype(str).str.zfill(8)
    diretorio_empresa = current_dir.replace("ESTABCHECK", r"ETL_CNPJ/Bases_EMPRESAS/" )
    all_files_empresa =  list(filter(lambda x: '.csv' in x, os.listdir(diretorio_empresa)))

    # Comparando com as empresas
    for file_empresa in all_files_empresa:
        d_empresa = pd.read_csv(f'{diretorio_empresa}{file_empresa}',dtype='str',on_bad_lines='warn', sep=';')
        d_empresa['CNPJ_BASE'] = d_empresa['CNPJ_BASE'].astype(str).str.zfill(8)
        filtro = d_empresa[d_empresa['CNPJ_BASE'].isin(CNPJ_estabelecimento)]
        merged_data = pd.concat([merged_data, filtro], ignore_index=True)
    CAMADA1 = re.sub('base_csv_estabelecimentos','', file_estabelecimento)
    name_file = re.sub('["("")"?@|$|/|\|!,:%;"]','', CAMADA1)
    merged_data.to_csv(f'{diretorio_empresas}{name_file}', mode='a', index=False, sep=';')


