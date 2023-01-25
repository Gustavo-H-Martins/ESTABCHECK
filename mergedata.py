# Libs
import pandas as pd
import os
import re
import logging

# gerando log
logging.basicConfig(level=logging.INFO, filename="mergedata.log", encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")

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
    # DataFrame Vazio
    dados = pd.DataFrame()


    d_estabelecimento = pd.read_csv(f'{diretorio_estabelecimentos}{file_estabelecimento}', sep=',', dtype=dtypes_ESTABELE)
    #print(d_estabelecimento.columns)
    d_estabelecimento['CNPJ'] = d_estabelecimento[['CNPJ_BASE', 'CNPJ_ORDEM', 'CNPJ_DV']].apply(lambda x: ''.join(x), axis=1)
    d_estabelecimento['CNPJ_BASE'] = pd.to_numeric(d_estabelecimento['CNPJ_BASE'], downcast='integer')
    
    diretorio_empresa = r'base_csv_empresas/'
    all_files_empresa =  list(filter(lambda x: '.csv' in x, os.listdir(diretorio_empresa)))

    # Comparando com as empresas
    for file_empresa in all_files_empresa:
        d_empresa = pd.read_csv(f'{diretorio_empresa}{file_empresa}',dtype=dtypes_EMPRESA,error_bad_lines=False, sep=';')
        #print(d_empresa.columns)
        d_empresa['CNPJ_BASE'] = pd.to_numeric(d_empresa['CNPJ_BASE'], downcast='integer')
        merged_data = pd.merge(d_estabelecimento, d_empresa, on='CNPJ_BASE', how='inner')
    

        # Tratando os dados para disposição
        merged_data = pd.merge(merged_data, municipios, on='MUNICIPIO')
        merged_data['RUA'] = merged_data['TIPO_LOGRADOURO'] +' '+ merged_data['LOGRADOURO']
        merged_data.rename(columns={'UF':'ESTADO'}, inplace=True)
        merged_data = merged_data[['CNPJ', 'RAZAO_SOCIAL', 'NOME_FANTASIA','RUA','COMPLEMENTO', 'BAIRRO','CIDADE','ESTADO','CEP']]
        
        dados = pd.concat([dados, merged_data], ignore_index=True)
        
    CAMADA1 = re.sub('base_csv_estabelecimentos','', file_estabelecimento)
    name_file = re.sub('["("")"?@|$|/|\|!,:%;"]','', CAMADA1)

    dados.to_csv(f'merge_base/{name_file}', mode='w', index=False)
    print(f"Concluído processo de extração dos dados do CNAE de {name_file}")



