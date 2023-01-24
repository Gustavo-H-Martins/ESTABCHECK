# Libs
import pandas as pd
import glob
import re
import logging

# gerando log
logging.basicConfig(level=logging.INFO, filename="mergedata.log", encoding='utf-8', format="%(asctime)s - %(levelname)s - %(message)s")

# Diretórios
diretorio_estabelecimentos = 'base_csv_estabelecimentos'
diretorio_empresa = 'base_csv_empresas'


path = f'{diretorio_estabelecimentos}/*.csv'
all_files_estabelecimentos = glob.glob(path)

# Parâmetros Estabelecimentos
dtypes_ESTABELE = {'CNPJ_BASE': 'category',
'CNPJ_ORDEM': 'category',
'CNPJ_DV': 'category'}
# Parâmetros Estabelecimentos
dtypes_EMPRESA = {'CNPJ_BASE': 'category'}

# Municípios
municipios = pd.read_csv('Municipios\municipios.csv', sep=';', names=['MUNICIPIO','CIDADE'])

# Iterando sobre os estabelecimentos
for file in all_files_estabelecimentos:

    d_estabelecimento = pd.read_csv(file, sep=',', dtype=dtypes_ESTABELE)
    #print(d_estabelecimento.columns)
    d_estabelecimento['CNPJ'] = d_estabelecimento[['CNPJ_BASE', 'CNPJ_ORDEM', 'CNPJ_DV']].apply(lambda x: ''.join(x), axis=1)
    d_estabelecimento['CNPJ_BASE'] = pd.to_numeric(d_estabelecimento['CNPJ_BASE'], downcast='integer')
    
    path = f'{diretorio_empresa}/*.csv'
    all_files_empresa = glob.glob(path)

    # Comparando com as empresas
    for file in all_files_empresa:
        d_empresa = pd.read_csv(file,dtype=dtypes_EMPRESA,error_bad_lines=False)
        #print(d_empresa.columns)
        d_empresa['CNPJ_BASE'] = pd.to_numeric(d_empresa['CNPJ_BASE'], downcast='integer')
        merged_data = pd.merge(d_estabelecimento, d_empresa, on='CNPJ_BASE')
    
    
    # Tratando os dados para disposição
    merged_data = pd.merge(merged_data, municipios, on='MUNICIPIO')
    merged_data['RUA'] = merged_data['TIPO_LOGRADOURO'] + merged_data['LOGRADOURO']
    merged_data.rename(columns={'UF':'ESTADO'}, inplace=True)
    merged_data[['CNPJ', 'RAZAO_SOCIAL', 'NOME_FANTASIA','RUA','COMPLEMENTO', 'BAIRRO','CIDADE','ESTADO','CEP']]
    chunk_size = 500000
    name_file = re.sub('.csv','', file)
    name_file = re.sub(r'base_csv_estabelecimentos','', file)
    name_file = re.sub('["("")"?@|$|.|/|\|!,:%;"]','', file)

    print(f"Concluído processo de extração dos dados do CNAE de {name_file}")

    for i in range(0, len(merged_data), chunk_size):
        df_chunk = merged_data.iloc[i:i+chunk_size]
        df_chunk.to_csv(f'merge_base/CNPJ_{i}_{name_file}.csv', mode='w', index=False)


