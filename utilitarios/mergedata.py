# Libs
import pandas as pd
import os
import re
import logging

#Warnings: Possui uma série de funções e comandos para tratamento de mensagens de avisos e alertas do Python
import warnings
warnings.filterwarnings("ignore")

# gerando log
logging.basicConfig(level=logging.INFO, filename="./logs/mergedata.log", encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")

# DataFrame Vazio
Dados = pd.DataFrame()

# Diretórios
diretorio_estabelecimentos = r'base_csv_estabelecimentos/'
all_files_estabelecimentos = list(filter(lambda x: '.csv' in x, os.listdir(diretorio_estabelecimentos)))


all_files_estabelecimentos 

# Parâmetros Estabelecimentos
dtypes_ESTABELE = {'CNPJ_BASE': 'category',
'CNPJ_ORDEM': 'category',
'CNPJ_DV': 'category'}
# Parâmetros Estabelecimentos
dtypes_EMPRESA = {'CNPJ_BASE': 'category'}

# Municípios
municipios = pd.read_csv('Municipios\municipios.csv', sep=';', names=['MUNICIPIO','CIDADE'])

# Iterando sobre os estabelecimentos
for file_estabelecimento in all_files_estabelecimentos:
    logging.info(f"Lendo arquivo {file_estabelecimento}")
    print(f"Lendo arquivo {file_estabelecimento}")
    # DataFrame Vazio
    dados = pd.DataFrame()


    d_estabelecimento = pd.read_csv(f'{diretorio_estabelecimentos}{file_estabelecimento}', sep=';', dtype=dtypes_ESTABELE)
    #print(d_estabelecimento.columns)
    d_estabelecimento['CNPJ'] = d_estabelecimento[['CNPJ_BASE', 'CNPJ_ORDEM', 'CNPJ_DV']].apply(lambda x: ''.join(x), axis=1)
    #d_estabelecimento['CNPJ_BASE'] = pd.to_numeric(d_estabelecimento['CNPJ_BASE'], downcast='integer')
    
    diretorio_empresa = r'base_csv_empresas/'
    all_files_empresa =  list(filter(lambda x: '.csv' in x, os.listdir(diretorio_empresa)))

    # Comparando com as empresas
    for file_empresa in all_files_empresa:

        chunck_d_empresa = pd.read_csv(f'{diretorio_empresa}{file_empresa}',dtype=dtypes_EMPRESA,error_bad_lines=False, sep=';', chunksize=1000000)
        for d_empresa in chunck_d_empresa:
            #print(d_empresa.columns)
            #d_empresa['CNPJ_BASE'] = pd.to_numeric(d_empresa['CNPJ_BASE'], downcast='integer', errors='ignore')
            merged_data = pd.merge(d_estabelecimento, d_empresa, on='CNPJ_BASE')
        

            # Tratando os dados para disposição
            merged_data = pd.merge(merged_data, municipios, on='MUNICIPIO')
            merged_data['RUA'] = merged_data['TIPO_LOGRADOURO'] +' '+ merged_data['LOGRADOURO']
            merged_data.rename(columns={'UF':'ESTADO'}, inplace=True)
            merged_data = merged_data[['CNPJ', 'RAZAO_SOCIAL', 'NOME_FANTASIA','RUA', 'NUMERO','COMPLEMENTO', 'BAIRRO',
                'CIDADE','ESTADO','CEP', 'TELEFONE1', 'CNAE_PRINCIPAL','CNAE_DESCRICAO',]]
            
            dados = pd.concat([dados, merged_data], ignore_index=True)
            
    CAMADA1 = re.sub('base_csv_estabelecimentos','', file_estabelecimento)
    name_file = re.sub('["("")"?@|$|/|\|!,:%;"]','', CAMADA1)
    name_file_json = re.sub(".csv", '.json', name_file)
    dados.to_csv(f'merge_base/{name_file}', mode='w', index=False, sep=';')
    dados.to_json(f'json_base/{name_file_json}', orient='records')
    print(f"Concluído processo de extração dos dados do CNAE de {name_file}")
    logging.info(f"Concluído processo de extração de {len(dados)} dados do CNAE de {name_file}")

