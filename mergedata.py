import pandas as pd
import glob
import re

diretorio_estabelecimentos = 'base_csv_estabelecimentos'
diretorio_empresa = 'base_csv_empresas'

path = f'{diretorio_estabelecimentos}/*.csv'
all_files_estabelecimentos = glob.glob(path)

dtypes_ESTABELE = {'CNPJ_BASE': 'category',
'CNPJ_ORDEM': 'category',
'CNPJ_DV': 'category'}

dtypes_EMPRESA = {'CNPJ_BASE': 'category'}

for file in all_files_estabelecimentos:

    d_estabelecimento = pd.read_csv(file, sep=',', dtype=dtypes_ESTABELE)
    #print(d_estabelecimento.columns)
    d_estabelecimento['CNPJ'] = d_estabelecimento[['CNPJ_BASE', 'CNPJ_ORDEM', 'CNPJ_DV']].apply(lambda x: ''.join(x), axis=1)
    d_estabelecimento['CNPJ_BASE'] = pd.to_numeric(d_estabelecimento['CNPJ_BASE'], downcast='integer')
    
    path = f'{diretorio_empresa}/*.csv'
    all_files_empresa = glob.glob(path)

    for file in all_files_empresa:
        d_empresa = pd.read_csv(file,dtype=dtypes_EMPRESA,error_bad_lines=False)
        #print(d_empresa.columns)
        d_empresa['CNPJ_BASE'] = pd.to_numeric(d_empresa['CNPJ_BASE'], downcast='integer')
        merged_data = pd.merge(d_estabelecimento, d_empresa, on='CNPJ_BASE')

    chunk_size = 1000000
    name_file = re.sub('.csv','', file)
    for i in range(0, len(merged_data), chunk_size):
        df_chunk = merged_data.iloc[i:i+chunk_size]
        df_chunk.to_csv(f'merge_base/CNPJ_{name_file}.csv', mode='w', index=False)


