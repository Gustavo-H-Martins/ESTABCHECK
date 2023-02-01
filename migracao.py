import pandas as pd
import sqlite3
import os

# Cria uma conexão com os SQLites
br_conn = sqlite3.connect("./database/br_base_cnpj.db")
en_conn = sqlite3.connect("./database/en_base_cnpj.db")

# Define os diretórios
br_diretorio = r'./br_base/'
en_diretorio = r'./en_base/'

# Lista todos os arquivos CSV nos diretórios
br_arquivos = [f for f in os.listdir(br_diretorio) if f.endswith('.csv')]
en_arquivos = [f for f in os.listdir(en_diretorio) if f.endswith('.csv')]

# Loop por todos os br_arquivos
for br_arquivo in br_arquivos:
    # Lê o arquivo para um dataframe
    dados = pd.read_csv(f'{br_diretorio}{br_arquivo}', sep=';', low_memory=False)
    dados['NOME_FANTASIA'].replace('--empty--','', regex=True)
    #print(dados.info())
    
    # Salva o dataframe como uma tabela no SQLite
    dados.to_sql('br_base', br_conn, if_exists='append', index=False)

# Executa o comando VACUUM para compactar o banco de dados
br_conn.execute("VACUUM")

# Fecha a conexão com o banco de dados
br_conn.close()

# Loop por todos os en_arquivos
for en_arquivo in en_arquivos:
    # Lê o arquivo para um dataframe
    data = pd.read_csv(f'{en_diretorio}{en_arquivo}', sep=';', low_memory=False)
    data['TRADING NAME'].replace('--empty--','', regex=True)
    
    # Salva o dataframe como uma tabela no SQLite
    data.to_sql('en_base', en_conn, if_exists='append', index=False)

# Executa o comando VACUUM para compactar o banco de dados
en_conn.execute("VACUUM")

# Fecha a conexão com o banco de dados
en_conn.close()
